package main

import (
	"GoAI/config"
	"GoAI/db"
	"GoAI/kafka"
	"GoAI/redis"
	routers "GoAI/routers"
	"GoAI/worker"
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

	db.InitDB()
	redis.InitRedis()

	kafka.InitProducer()
	defer kafka.CloseProducer()

	kafka.InitConsumer()
	defer kafka.CloseConsumer()
	worker.RegisterKafkaRunWorker()

	ctx, cancel := context.WithCancel(context.Background())
	go kafka.StartConsumer(ctx)

	r := routers.InitRouter()
	go func() {
		if err := r.Run(":" + config.AppConfig.ServerPort); err != nil {
			log.Fatalf("Failed to run server: %v", err)
		}
	}()

	log.Printf("Server started on :%s", config.AppConfig.ServerPort)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")
	cancel()
	log.Println("Server exited")
}
