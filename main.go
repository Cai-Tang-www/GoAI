package main

import (
	"GoAI/config"
	"GoAI/db"
	"GoAI/kafka"
	"GoAI/redis"
	routers "GoAI/routers"
	"GoAI/services"
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// 加载配置
	err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	// 初始化数据库
	db.InitDB()

	// 初始化 Redis
	redis.InitRedis()

	// 初始化 Kafka 生产者
	kafka.InitProducer()
	defer kafka.CloseProducer()

	// 初始化 Kafka 消费者
	kafka.InitConsumer()
	defer kafka.CloseConsumer()
	kafka.RegisterRunMessageHandler(services.HandleRunExecuteMessage)

	// 创建一个上下文用于控制消费者 goroutine 的生命周期
	ctx, cancel := context.WithCancel(context.Background())

	// 在 goroutine 中启动 Kafka 消费者
	go kafka.StartConsumer(ctx)

	// 初始化Gin路由
	r := routers.InitRouter()

	// 启动服务器
	go func() {
		if err := r.Run(":8080"); err != nil {
			log.Fatalf("Failed to run server: %v", err)
		}
	}()

	log.Println("Server started on :8080")

	// 等待中断信号来优雅地关闭服务器
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	// 取消消费者 goroutine
	cancel()

	log.Println("Server exited")
}
