package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"GoAI/config"
	"GoAI/requestctx"

	kgo "github.com/segmentio/kafka-go"
)

// Producer Kafka 生产者
var Producer *kgo.Writer

// InitProducer 初始化 Kafka 生产者
func InitProducer() {
	Producer = &kgo.Writer{
		Addr:         kgo.TCP(config.AppConfig.KafkaBootstrapServers),
		Topic:        config.AppConfig.KafkaRunTopic,
		Balancer:     &kgo.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond, // 批量发送超时时间
		BatchSize:    100,                   // 批量发送大小
		RequiredAcks: kgo.RequireAll,        // 要求所有副本都确认
		MaxAttempts:  3,                     // 最大重试次数
	}

	// 尝试连接 Kafka
	conn, err := kgo.DialLeader(context.Background(), "tcp", config.AppConfig.KafkaBootstrapServers, config.AppConfig.KafkaRunTopic, 0)
	if err != nil {
		log.Fatalf("连接 Kafka 失败: %v", err)
	}
	defer conn.Close()
	log.Println("Kafka 生产者初始化成功")
}

// SendMessage 发送消息到 Kafka
func SendMessage(ctx context.Context, key, value []byte) error {
	message := kgo.Message{
		Key:   key,
		Value: value,
		Time:  time.Now(),
	}

	err := Producer.WriteMessages(ctx, message)
	if err != nil {
		log.Printf("kafka send failed trace_id=%s err=%v", requestctx.TraceIDFromContext(ctx), err)
		return err
	}
	log.Printf("kafka send success trace_id=%s key=%s", requestctx.TraceIDFromContext(ctx), string(key))
	return nil
}

type RunExecuteMessage struct {
	RunID   string `json:"run_id"`
	TraceID string `json:"trace_id"`
}

// SendRunExecuteEvent 发送 run 执行事件到 Kafka。
func SendRunExecuteEvent(ctx context.Context, runID string) error {
	payload, err := json.Marshal(newRunExecuteMessage(ctx, runID))
	if err != nil {
		return err
	}
	return SendMessage(ctx, []byte(runID), payload)
}

// newRunExecuteMessage 构造包含 trace_id 的 Run 执行消息。
func newRunExecuteMessage(ctx context.Context, runID string) RunExecuteMessage {
	return RunExecuteMessage{
		RunID:   runID,
		TraceID: requestctx.TraceIDFromContext(ctx),
	}
}

// CloseProducer 关闭 Kafka 生产者，并把关闭错误交给生命周期协调器处理。
func CloseProducer() error {
	if Producer == nil {
		return nil
	}
	return Producer.Close()
}
