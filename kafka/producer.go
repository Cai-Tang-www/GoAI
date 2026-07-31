package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"GoAI/config"
	"GoAI/requestctx"

	kgo "github.com/segmentio/kafka-go"
)

type messageWriter interface {
	WriteMessages(context.Context, ...kgo.Message) error
	Close() error
}

// Producer 持有 Kafka Writer，并向运行时提供 Run 事件发布能力。
type Producer struct {
	writer messageWriter
}

// NewProducer 创建 Kafka 生产者并探测目标 topic 的连通性。
func NewProducer(ctx context.Context, cfg *config.Config) (*Producer, error) {
	if cfg == nil {
		return nil, fmt.Errorf("creating Kafka producer: config is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	writer := &kgo.Writer{
		Addr:         kgo.TCP(cfg.KafkaBootstrapServers),
		Topic:        cfg.KafkaRunTopic,
		Balancer:     &kgo.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		BatchSize:    100,
		RequiredAcks: kgo.RequireAll,
		MaxAttempts:  3,
	}

	conn, err := kgo.DialLeader(ctx, "tcp", cfg.KafkaBootstrapServers, cfg.KafkaRunTopic, 0)
	if err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("connecting Kafka producer: %w", err)
	}
	if err := conn.Close(); err != nil {
		_ = writer.Close()
		return nil, fmt.Errorf("closing Kafka producer probe: %w", err)
	}
	return &Producer{writer: writer}, nil
}

// RunExecuteMessage 是 Kafka 中 Run 执行事件的稳定消息结构。
type RunExecuteMessage struct {
	RunID   string `json:"run_id"`
	TraceID string `json:"trace_id"`
}

// PublishRunExecute 发布包含 trace_id 的 Run 执行事件。
func (p *Producer) PublishRunExecute(ctx context.Context, runID string) error {
	payload, err := json.Marshal(newRunExecuteMessage(ctx, runID))
	if err != nil {
		return fmt.Errorf("marshalling run execute event: %w", err)
	}
	return p.Send(ctx, []byte(runID), payload)
}

// Send 向生产者配置的 topic 写入一条 Kafka 消息。
func (p *Producer) Send(ctx context.Context, key, value []byte) error {
	if p == nil || p.writer == nil {
		return fmt.Errorf("sending Kafka message: producer is nil")
	}
	message := kgo.Message{Key: key, Value: value, Time: time.Now()}
	if err := p.writer.WriteMessages(ctx, message); err != nil {
		log.Printf("kafka send failed trace_id=%s err=%v", requestctx.TraceIDFromContext(ctx), err)
		return fmt.Errorf("writing Kafka message: %w", err)
	}
	log.Printf("kafka send success trace_id=%s key=%s", requestctx.TraceIDFromContext(ctx), string(key))
	return nil
}

// Close 关闭 Kafka 生产者。
func (p *Producer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("closing Kafka producer: %w", err)
	}
	return nil
}

// newRunExecuteMessage 构造包含 trace_id 的 Run 执行消息。
func newRunExecuteMessage(ctx context.Context, runID string) RunExecuteMessage {
	return RunExecuteMessage{
		RunID:   runID,
		TraceID: requestctx.TraceIDFromContext(ctx),
	}
}
