package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"GoAI/config"
	"GoAI/requestctx"

	kgo "github.com/segmentio/kafka-go"
)

type messageReader interface {
	ReadMessage(context.Context) (kgo.Message, error)
	Close() error
}

// RunMessageHandler 处理一个反序列化后的 Run 执行事件。
type RunMessageHandler func(context.Context, RunExecuteMessage) error

// Consumer 持有 Kafka Reader 和对应的业务消息处理器。
type Consumer struct {
	reader    messageReader
	handler   RunMessageHandler
	logger    *log.Logger
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// NewConsumer 创建 Run topic 消费者。业务处理器由调用方显式注入。
func NewConsumer(cfg *config.Config, handler RunMessageHandler) (*Consumer, error) {
	if cfg == nil {
		return nil, fmt.Errorf("creating Kafka consumer: config is nil")
	}
	if handler == nil {
		return nil, fmt.Errorf("creating Kafka consumer: handler is nil")
	}
	reader := kgo.NewReader(kgo.ReaderConfig{
		Brokers:        []string{cfg.KafkaBootstrapServers},
		Topic:          cfg.KafkaRunTopic,
		GroupID:        cfg.KafkaRunGroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		MaxWait:        10 * time.Second,
		CommitInterval: time.Second,
	})
	return &Consumer{reader: reader, handler: handler, logger: log.Default()}, nil
}

// Start 持续消费消息，直到上下文取消或 Reader 被关闭。
func (c *Consumer) Start(ctx context.Context) {
	if c == nil || c.reader == nil {
		return
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
				return
			}
			logger.Printf("kafka read failed trace_id=%s err=%v", requestctx.TraceIDFromContext(ctx), err)
			continue
		}

		var payload RunExecuteMessage
		if err := json.Unmarshal(message.Value, &payload); err != nil {
			logger.Printf("parse run message failed err=%v", err)
			continue
		}
		if payload.RunID == "" {
			logger.Println("Run 消息缺少 run_id，跳过")
			continue
		}
		msgCtx := ctx
		if payload.TraceID != "" {
			msgCtx = requestctx.WithTraceID(ctx, payload.TraceID)
		}
		logger.Printf("kafka consume trace_id=%s topic=%s partition=%d offset=%d run_id=%s", requestctx.TraceIDFromContext(msgCtx), message.Topic, message.Partition, message.Offset, payload.RunID)
		if err := c.handler(msgCtx, payload); err != nil {
			logger.Printf("handle run message failed trace_id=%s run_id=%s err=%v", requestctx.TraceIDFromContext(msgCtx), payload.RunID, err)
		}
	}
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
