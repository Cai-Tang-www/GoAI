package kafka

import (
	"context"
	"encoding/json"
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

// Consumer 持有 Kafka Reader 和对应的业务消息处理器。
type Consumer struct {
	reader    messageReader
	handler   RunMessageHandler
	logger    *log.Logger
	telemetry *observability.Bundle
	topic     string
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
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

// NewConsumer 创建 Run topic 消费者。业务处理器由调用方显式注入。
func NewConsumer(cfg *config.Config, handler RunMessageHandler, options ...ConsumerOption) (*Consumer, error) {
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
	consumer := &Consumer{reader: reader, handler: handler, logger: log.Default(), topic: cfg.KafkaRunTopic}
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
			c.observe("consume", message.Topic, "error")
			logger.Printf("parse run message failed err=%v", err)
			continue
		}
		if payload.RunID == "" {
			c.observe("consume", message.Topic, "error")
			logger.Println("Run 消息缺少 run_id，跳过")
			continue
		}
		msgCtx := ctx
		if payload.TraceID != "" {
			msgCtx = requestctx.WithTraceID(ctx, payload.TraceID)
		}
		traceCtx := observability.ContextWithTraceID(msgCtx, requestctx.TraceIDFromContext(msgCtx))
		startedAt := time.Now()
		status := "success"
		var span oteltrace.Span
		if c.telemetry != nil && c.telemetry.Tracer != nil {
			traceCtx, span = c.telemetry.Tracer.Start(traceCtx, "kafka.consume", observability.SpanAttributes(
				requestctx.TraceIDFromContext(msgCtx), payload.RunID, "", "")...)
		}
		if c.telemetry != nil && c.telemetry.Logger != nil {
			c.telemetry.Logger.InfoContext(traceCtx, "kafka consume",
				slog.String("trace_id", requestctx.TraceIDFromContext(msgCtx)),
				slog.String("topic", message.Topic),
				slog.Int("partition", message.Partition),
				slog.Int64("offset", message.Offset),
				slog.String("run_id", payload.RunID),
			)
		} else {
			logger.Printf("kafka consume trace_id=%s topic=%s partition=%d offset=%d run_id=%s", requestctx.TraceIDFromContext(msgCtx), message.Topic, message.Partition, message.Offset, payload.RunID)
		}
		if err := c.handler(traceCtx, payload); err != nil {
			status = "error"
			if span != nil {
				observability.MarkSpanError(span, err)
			}
			logger.Printf("handle run message failed trace_id=%s run_id=%s err=%v", requestctx.TraceIDFromContext(msgCtx), payload.RunID, err)
		}
		if span != nil {
			span.End()
		}
		c.observe("consume", message.Topic, status)
		if c.telemetry != nil && c.telemetry.Logger != nil {
			c.telemetry.Logger.InfoContext(traceCtx, "kafka consume finished",
				slog.String("trace_id", requestctx.TraceIDFromContext(msgCtx)),
				slog.String("run_id", payload.RunID),
				slog.String("status", status),
				slog.Int64("latency_ms", time.Since(startedAt).Milliseconds()),
			)
		}
	}
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
