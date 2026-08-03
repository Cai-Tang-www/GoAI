package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"GoAI/config"
	"GoAI/observability"
	"GoAI/requestctx"

	kgo "github.com/segmentio/kafka-go"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type messageReader interface {
	ReadMessage(context.Context) (kgo.Message, error)
	Close() error
}

// RunMessageHandler 处理一个反序列化后的 Run 执行事件。
type RunMessageHandler func(context.Context, RunExecuteMessage) error

// RunResumeMessageHandler 处理一个反序列化后的 Parent Run 恢复事件。
type RunResumeMessageHandler func(context.Context, RunResumeMessage) error

// Consumer 持有 Kafka Reader 和对应的业务消息处理器。
type Consumer struct {
	reader        messageReader
	handler       RunMessageHandler
	resumeHandler RunResumeMessageHandler
	logger        *log.Logger
	telemetry     *observability.Bundle
	topic         string
	closed        atomic.Bool
	closeOnce     sync.Once
	closeErr      error
}

// ConsumerOption 配置 Kafka 消费者的可选依赖。
type ConsumerOption func(*Consumer) error

// WithConsumerObservability 注入 Kafka 消费的日志、指标和 Trace 能力。
func WithConsumerObservability(bundle *observability.Bundle) ConsumerOption {
	return func(consumer *Consumer) error {
		if bundle == nil {
			return fmt.Errorf("configuring Kafka consumer: observability bundle is nil")
		}
		consumer.telemetry = bundle
		return nil
	}
}

// NewConsumer 创建 Run 首次执行 topic 消费者。业务处理器由调用方显式注入。
func NewConsumer(cfg *config.Config, handler RunMessageHandler, options ...ConsumerOption) (*Consumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("creating Kafka consumer: handler is nil")
	}
	return newConsumer(cfg, cfgTopic(cfg, false), cfgGroup(cfg, false), handler, nil, options...)
}

// NewResumeConsumer 创建 Parent Run 恢复 topic 消费者。
func NewResumeConsumer(cfg *config.Config, handler RunResumeMessageHandler, options ...ConsumerOption) (*Consumer, error) {
	if handler == nil {
		return nil, fmt.Errorf("creating Kafka resume consumer: handler is nil")
	}
	return newConsumer(cfg, cfgTopic(cfg, true), cfgGroup(cfg, true), nil, handler, options...)
}

func newConsumer(cfg *config.Config, topic, groupID string, handler RunMessageHandler, resumeHandler RunResumeMessageHandler, options ...ConsumerOption) (*Consumer, error) {
	if cfg == nil {
		return nil, fmt.Errorf("creating Kafka consumer: config is nil")
	}
	reader := kgo.NewReader(kgo.ReaderConfig{
		Brokers:        []string{cfg.KafkaBootstrapServers},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		MaxWait:        10 * time.Second,
		CommitInterval: time.Second,
	})
	consumer := &Consumer{reader: reader, handler: handler, resumeHandler: resumeHandler, logger: log.Default(), topic: topic}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(consumer); err != nil {
			_ = reader.Close()
			return nil, err
		}
	}
	return consumer, nil
}

// Start 持续消费消息，直到上下文取消或 Reader 被关闭，并向生命周期返回启动错误。
func (c *Consumer) Start(ctx context.Context) error {
	if c == nil || c.reader == nil {
		return errors.New("starting Kafka consumer: consumer is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	logger := c.logger
	if logger == nil {
		logger = log.Default()
	}
	for {
		message, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || c.closed.Load() {
				logger.Printf("Kafka 消费者停止消费 reason=%v", ctx.Err())
				return nil
			}
			logger.Printf("kafka read failed trace_id=%s err=%v", requestctx.TraceIDFromContext(ctx), err)
			return fmt.Errorf("reading Kafka message: %w", err)
		}
		if err := c.handleMessage(ctx, logger, message); err != nil {
			c.observe("consume", message.Topic, "error")
			logger.Printf("parse Kafka run message failed topic=%s err=%v", message.Topic, err)
		}
	}
}

func (c *Consumer) handleMessage(ctx context.Context, logger *log.Logger, message kgo.Message) error {
	runID, traceID, handle, err := c.decodeMessage(message.Value)
	if err != nil {
		return err
	}
	msgCtx := ctx
	if traceID != "" {
		msgCtx = requestctx.WithTraceID(ctx, traceID)
	}
	traceCtx := observability.ContextWithTraceID(msgCtx, requestctx.TraceIDFromContext(msgCtx))
	startedAt := time.Now()
	status := "success"
	var span oteltrace.Span
	if c.telemetry != nil && c.telemetry.Tracer != nil {
		traceCtx, span = c.telemetry.Tracer.Start(traceCtx, "kafka.consume", observability.SpanAttributes(
			requestctx.TraceIDFromContext(msgCtx), runID, "", "")...)
	}
	if c.telemetry != nil && c.telemetry.Logger != nil {
		c.telemetry.Logger.InfoContext(traceCtx, "kafka consume",
			slog.String("trace_id", requestctx.TraceIDFromContext(msgCtx)),
			slog.String("topic", message.Topic),
			slog.Int("partition", message.Partition),
			slog.Int64("offset", message.Offset),
			slog.String("run_id", runID),
		)
	} else {
		logger.Printf("kafka consume trace_id=%s topic=%s partition=%d offset=%d run_id=%s", requestctx.TraceIDFromContext(msgCtx), message.Topic, message.Partition, message.Offset, runID)
	}
	if err := handle(traceCtx); err != nil {
		status = "error"
		if span != nil {
			observability.MarkSpanError(span, err)
		}
		logger.Printf("handle run message failed trace_id=%s run_id=%s err=%v", requestctx.TraceIDFromContext(msgCtx), runID, err)
	}
	if span != nil {
		span.End()
	}
	c.observe("consume", message.Topic, status)
	if c.telemetry != nil && c.telemetry.Logger != nil {
		c.telemetry.Logger.InfoContext(traceCtx, "kafka consume finished",
			slog.String("trace_id", requestctx.TraceIDFromContext(msgCtx)),
			slog.String("run_id", runID),
			slog.String("status", status),
			slog.Int64("latency_ms", time.Since(startedAt).Milliseconds()),
		)
	}
	return nil
}

func (c *Consumer) decodeMessage(value []byte) (string, string, func(context.Context) error, error) {
	if c.resumeHandler != nil {
		var payload RunResumeMessage
		if err := json.Unmarshal(value, &payload); err != nil {
			return "", "", nil, err
		}
		if payload.RunID == "" || payload.DelegationID == "" {
			return "", "", nil, errors.New("Run 恢复消息缺少 run_id 或 delegation_id")
		}
		return payload.RunID, payload.TraceID, func(ctx context.Context) error {
			return c.resumeHandler(ctx, payload)
		}, nil
	}
	var payload RunExecuteMessage
	if err := json.Unmarshal(value, &payload); err != nil {
		return "", "", nil, err
	}
	if payload.RunID == "" {
		return "", "", nil, errors.New("Run 消息缺少 run_id")
	}
	if c.handler == nil {
		return "", "", nil, errors.New("Run 消息处理器未配置")
	}
	return payload.RunID, payload.TraceID, func(ctx context.Context) error {
		return c.handler(ctx, payload)
	}, nil
}

func cfgTopic(cfg *config.Config, resume bool) string {
	if cfg == nil {
		return ""
	}
	if resume {
		return cfg.KafkaRunResumeTopic
	}
	return cfg.KafkaRunTopic
}

func cfgGroup(cfg *config.Config, resume bool) string {
	if cfg == nil {
		return ""
	}
	if resume {
		return cfg.KafkaRunResumeGroupID
	}
	return cfg.KafkaRunGroupID
}

func (c *Consumer) observe(operation, topic, status string) {
	if c == nil || c.telemetry == nil || c.telemetry.Metrics == nil {
		return
	}
	c.telemetry.Metrics.ObserveKafka(operation, topic, status)
}

// Close 关闭 Kafka 消费者。
func (c *Consumer) Close() error {
	if c == nil || c.reader == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if err := c.reader.Close(); err != nil {
			c.closeErr = fmt.Errorf("closing Kafka consumer: %w", err)
		}
	})
	return c.closeErr
}
