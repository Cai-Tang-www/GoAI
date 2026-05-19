package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"GoAI/config"

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
		fmt.Printf("发送 Kafka 消息失败: %v\n", err)
		return err
	}
	fmt.Printf("发送 Kafka 消息成功: Key=%s, Value=%s\n", string(key), string(value))
	return nil
}

type RunExecuteMessage struct {
	RunID string `json:"run_id"`
}

// SendRunExecuteEvent 发送 run 执行事件到 Kafka。
func SendRunExecuteEvent(ctx context.Context, runID string) error {
	payload, err := json.Marshal(RunExecuteMessage{RunID: runID})
	if err != nil {
		return err
	}
	return SendMessage(ctx, []byte(runID), payload)
}

// CloseProducer 关闭 Kafka 生产者
func CloseProducer() {
	if Producer != nil {
		if err := Producer.Close(); err != nil {
			log.Printf("关闭 Kafka 生产者失败: %v", err)
		}
		log.Println("Kafka 生产者已关闭")
	}
}
