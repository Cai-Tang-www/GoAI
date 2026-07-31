package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"GoAI/config"
	"GoAI/db"
	"GoAI/kafka"
	"GoAI/redis"
	"GoAI/routers"
	"GoAI/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		log.Printf("application exited with error: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	if err := config.LoadConfig(); err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	db.InitDB()
	redis.InitRedis()
	kafka.InitProducer()
	kafka.InitConsumer()
	worker.RegisterKafkaRunWorker()

	server := &http.Server{
		Addr:              ":" + config.AppConfig.ServerPort,
		Handler:           routers.InitRouter(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	runtime := runtimeLifecycle{
		server:          server,
		address:         server.Addr,
		shutdownTimeout: config.AppConfig.ServerShutdownTimeout,
		runWorker:       kafka.StartConsumer,
		closeConsumer:   kafka.CloseConsumer,
		closeProducer:   kafka.CloseProducer,
		closeRedis:      redis.Close,
		closeDB:         db.Close,
		logger:          log.Default(),
	}
	return runtime.Run(ctx)
}
