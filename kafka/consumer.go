package kafka

import (
	"GoAI/config"
	"context"
	"fmt"
	"log"
	"time"

	kgo "github.com/segmentio/kafka-go"
)

var Consumer *kgo.Reader

// InitConsumer 初始化 Kafka 消费者
func InitConsumer() {
	Consumer = kgo.NewReader(kgo.ReaderConfig{
		Brokers:        []string{config.AppConfig.KafkaBootstrapServers},
		Topic:          config.AppConfig.KafkaTopic,
		GroupID:        "my-group",       // 消费者组ID
		MinBytes:       10e3,             // 10KB
		MaxBytes:       10e6,             // 10MB
		MaxWait:        10 * time.Second, // 最长等待时间
		CommitInterval: time.Second,      // 自动提交偏移量的时间间隔
	})
	log.Println("Kafka 消费者初始化成功")
}

// StartConsumer 开始消费 Kafka 消息
func StartConsumer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka 消费者停止消费")
			return
		default:
			message, err := Consumer.ReadMessage(ctx)
			if err != nil {
				log.Printf("读取 Kafka 消息失败: %v", err)
				continue
			}
			fmt.Printf("收到消息 -> Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n",
				message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
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
