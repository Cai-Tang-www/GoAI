package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"GoAI/config"
	"GoAI/observability"
	"GoAI/requestctx"

	kgo "github.com/segmentio/kafka-go"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type messageWriter interface {
	WriteMessages(context.Context, ...kgo.Message) error
	Close() error
}

// Producer 持有 Kafka Writer，并向运行时提供 Run 执行与恢复事件发布能力。
type Producer struct {
	writer      messageWriter
	topic       string
	resumeTopic string
	telemetry   *observability.Bundle
	closeOnce   sync.Once
	closeErr    error
}

// ProducerOption 配置 Kafka 生产者的可选依赖。
type ProducerOption func(*Producer) error

// WithProducerObservability 注入 Kafka 发布的日志、指标和 Trace 能力。
func WithProducerObservability(bundle *observability.Bundle) ProducerOption {
	return func(producer *Producer) error {
		if bundle == nil {
			return fmt.Errorf("configuring Kafka producer: observability bundle is nil")
		}
		producer.telemetry = bundle
		return nil
	}
}

// NewProducer 创建 Kafka 生产者并探测 Run 执行与恢复 topic 的连通性。
func NewProducer(ctx context.Context, cfg *config.Config, options ...ProducerOption) (*Producer, error) {
	if cfg == nil {
		return nil, fmt.Errorf("creating Kafka producer: config is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	writer := &kgo.Writer{
		Addr:         kgo.TCP(cfg.KafkaBootstrapServers),
		Balancer:     &kgo.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
		RequiredAcks: kgo.RequireAll,
		MaxAttempts:  3,
	}
	for _, topic := range uniqueTopics(cfg.KafkaRunTopic, cfg.KafkaRunResumeTopic) {
		if err := probeTopic(ctx, cfg.KafkaBootstrapServers, topic); err != nil {
			_ = writer.Close()
			return nil, err
		}
	}
	producer := &Producer{writer: writer, topic: cfg.KafkaRunTopic, resumeTopic: cfg.KafkaRunResumeTopic}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(producer); err != nil {
			_ = writer.Close()
			return nil, err
		}
	}
	return producer, nil
}

// RunExecuteMessage 是 Kafka 中 Run 首次执行事件的稳定消息结构。
type RunExecuteMessage struct {
	RunID   string `json:"run_id"`
	TraceID string `json:"trace_id"`
}

// RunResumeMessage 是 Kafka 中 Parent Run 恢复事件的稳定消息结构。
type RunResumeMessage struct {
	RunID        string `json:"run_id"`
	DelegationID string `json:"delegation_id"`
	TraceID      string `json:"trace_id"`
}

// PublishRunExecute 发布包含 trace_id 的 Run 首次执行事件。
func (p *Producer) PublishRunExecute(ctx context.Context, runID string) error {
	payload, err := json.Marshal(newRunExecuteMessage(ctx, runID))
	if err != nil {
		return fmt.Errorf("marshalling run execute event: %w", err)
	}
	return p.sendTo(ctx, p.topic, []byte(runID), payload)
}

// PublishRunResume 发布包含 Delegation 与 trace_id 的 Parent Run 恢复事件。
func (p *Producer) PublishRunResume(ctx context.Context, runID, delegationID string) error {
	payload, err := json.Marshal(newRunResumeMessage(ctx, runID, delegationID))
	if err != nil {
		return fmt.Errorf("marshalling run resume event: %w", err)
	}
	return p.sendTo(ctx, p.resumeTopic, []byte(runID), payload)
}

// Send 向生产者配置的默认 Run 执行 topic 写入一条 Kafka 消息。
func (p *Producer) Send(ctx context.Context, key, value []byte) error {
	if p == nil {
		return fmt.Errorf("sending Kafka message: producer is nil")
	}
	return p.sendTo(ctx, p.topic, key, value)
}

func (p *Producer) sendTo(ctx context.Context, topic string, key, value []byte) error {
	if p == nil || p.writer == nil {
		return fmt.Errorf("sending Kafka message: producer is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	startedAt := time.Now()
	status := "success"
	var span oteltrace.Span
	if p.telemetry != nil && p.telemetry.Tracer != nil {
		ctx, span = p.telemetry.Tracer.Start(ctx, "kafka.publish", observability.SpanAttributes(
			requestctx.TraceIDFromContext(ctx), "", "", "")...)
		defer span.End()
	}
	defer func() {
		if p.telemetry == nil {
			return
		}
		if p.telemetry.Metrics != nil {
			p.telemetry.Metrics.ObserveKafka("publish", topic, status)
		}
		if p.telemetry.Logger != nil {
			p.telemetry.Logger.InfoContext(ctx, "kafka publish",
				slog.String("trace_id", requestctx.TraceIDFromContext(ctx)),
				slog.String("topic", topic),
				slog.String("key", string(key)),
				slog.String("status", status),
				slog.Int64("latency_ms", time.Since(startedAt).Milliseconds()),
			)
		}
	}()

	message := kgo.Message{Topic: topic, Key: key, Value: value, Time: time.Now()}
	if err := p.writer.WriteMessages(ctx, message); err != nil {
		status = "error"
		if span != nil {
			observability.MarkSpanError(span, err)
		}
		if p.telemetry == nil || p.telemetry.Logger == nil {
			log.Printf("kafka send failed trace_id=%s topic=%s err=%v", requestctx.TraceIDFromContext(ctx), topic, err)
		}
		return fmt.Errorf("writing Kafka message: %w", err)
	}
	if p.telemetry == nil || p.telemetry.Logger == nil {
		log.Printf("kafka send success trace_id=%s topic=%s key=%s", requestctx.TraceIDFromContext(ctx), topic, string(key))
	}
	return nil
}

// Close 幂等关闭 Kafka 生产者；重复调用返回首次关闭结果。
func (p *Producer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		if err := p.writer.Close(); err != nil {
			p.closeErr = fmt.Errorf("closing Kafka producer: %w", err)
		}
	})
	return p.closeErr
}

func probeTopic(ctx context.Context, brokers, topic string) error {
	conn, err := kgo.DialLeader(ctx, "tcp", brokers, topic, 0)
	if err != nil {
		return fmt.Errorf("connecting Kafka producer topic %s: %w", topic, err)
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("closing Kafka producer probe for topic %s: %w", topic, err)
	}
	return nil
}

func uniqueTopics(topics ...string) []string {
	result := make([]string, 0, len(topics))
	seen := make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		if _, ok := seen[topic]; ok {
			continue
		}
		seen[topic] = struct{}{}
		result = append(result, topic)
	}
	return result
}

// newRunExecuteMessage 构造包含 trace_id 的 Run 执行消息。
func newRunExecuteMessage(ctx context.Context, runID string) RunExecuteMessage {
	return RunExecuteMessage{RunID: runID, TraceID: requestctx.TraceIDFromContext(ctx)}
}

// newRunResumeMessage 构造包含 trace_id 与 Delegation 的 Parent Run 恢复消息。
func newRunResumeMessage(ctx context.Context, runID, delegationID string) RunResumeMessage {
	return RunResumeMessage{RunID: runID, DelegationID: delegationID, TraceID: requestctx.TraceIDFromContext(ctx)}
}
