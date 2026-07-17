package kafka

import (
	"GoAI/config"
	"GoAI/requestctx"
	"context"
	"encoding/json"
	"log"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

// Consumer Kafka 消费者
var Consumer *kgo.Reader
var runMessageHandler func(context.Context, RunExecuteMessage) error

// InitConsumer 初始化 Kafka 消费者
func InitConsumer() {
	Consumer = kgo.NewReader(kgo.ReaderConfig{
		Brokers:        []string{config.AppConfig.KafkaBootstrapServers},
		Topic:          config.AppConfig.KafkaRunTopic,
		GroupID:        config.AppConfig.KafkaRunGroupID,
		MinBytes:       10e3,             // 10KB
		MaxBytes:       10e6,             // 10MB
		MaxWait:        10 * time.Second, // 最长等待时间
		CommitInterval: time.Second,      // 自动提交偏移量的时间间隔
	})
	log.Println("Kafka 消费者初始化成功")
}

// RegisterRunMessageHandler 注册 Run 消费处理器。
func RegisterRunMessageHandler(handler func(context.Context, RunExecuteMessage) error) {
	runMessageHandler = handler
}

// StartConsumer 开始消费 Kafka 消息
func StartConsumer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done(): // 上下文取消信号
			log.Println("Kafka 消费者停止消费")
			return
		default:
			message, err := Consumer.ReadMessage(ctx)
			if err != nil {
				log.Printf("kafka read failed trace_id=%s err=%v", requestctx.TraceIDFromContext(ctx), err)
				continue
			}
			if runMessageHandler == nil {
				log.Println("Run message handler 未注册，跳过消息处理")
				continue
			}
			var payload RunExecuteMessage
			if err := json.Unmarshal(message.Value, &payload); err != nil {
				log.Printf("parse run message failed err=%v", err)
				continue
			}
			if payload.RunID == "" {
				log.Println("Run 消息缺少 run_id，跳过")
				continue
			}
			msgCtx := ctx
			if payload.TraceID != "" {
				msgCtx = requestctx.WithTraceID(ctx, payload.TraceID)
			}
			log.Printf("kafka consume trace_id=%s topic=%s partition=%d offset=%d run_id=%s", requestctx.TraceIDFromContext(msgCtx), message.Topic, message.Partition, message.Offset, payload.RunID)
			if err := runMessageHandler(msgCtx, payload); err != nil {
				log.Printf("handle run message failed trace_id=%s run_id=%s err=%v", requestctx.TraceIDFromContext(msgCtx), payload.RunID, err)
			}
		}
	}
}

// CloseConsumer 关闭 Kafka 消费者
func CloseConsumer() {
	if Consumer != nil {
		if err := Consumer.Close(); err != nil {
			log.Printf("关闭 Kafka 消费者失败: %v", err)
		}
		log.Println("Kafka 消费者已关闭")
	}
}
