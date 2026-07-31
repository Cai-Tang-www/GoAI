package main

import (
	"context"
	"errors"
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
	redisinfra "GoAI/redis"
	"GoAI/routers"
	"GoAI/services"
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
	cfg := config.AppConfig

	database, err := db.New(cfg)
	if err != nil {
		return err
	}
	if err := db.Migrate(database); err != nil {
		return errors.Join(err, db.Close(database))
	}
	if err := db.SeedRBAC(database, cfg); err != nil {
		return errors.Join(fmt.Errorf("seeding RBAC: %w", err), db.Close(database))
	}
	log.Println("database initialized")

	redisClient, err := redisinfra.New(ctx, cfg)
	if err != nil {
		return errors.Join(err, db.Close(database))
	}
	log.Println("Redis initialized")

	producer, err := kafka.NewProducer(ctx, cfg)
	if err != nil {
		return errors.Join(err, redisinfra.Close(redisClient), db.Close(database))
	}
	log.Println("Kafka producer initialized")

	runService, err := services.NewRunService(database, producer)
	if err != nil {
		return errors.Join(err, producer.Close(), redisinfra.Close(redisClient), db.Close(database))
	}
	runWorker, err := worker.NewRunWorker(runService)
	if err != nil {
		return errors.Join(err, producer.Close(), redisinfra.Close(redisClient), db.Close(database))
	}
	consumer, err := kafka.NewConsumer(cfg, runWorker.HandleRunExecuteMessage)
	if err != nil {
		return errors.Join(err, producer.Close(), redisinfra.Close(redisClient), db.Close(database))
	}
	log.Println("Kafka consumer initialized")

	router, err := routers.New(routers.Dependencies{
		Database:   database,
		RunService: runService,
	})
	if err != nil {
		return errors.Join(err, consumer.Close(), producer.Close(), redisinfra.Close(redisClient), db.Close(database))
	}
	server := &http.Server{
		Addr:              ":" + cfg.ServerPort,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	runtime := runtimeLifecycle{
		server:          server,
		address:         server.Addr,
		shutdownTimeout: cfg.ServerShutdownTimeout,
		runWorker:       consumer.Start,
		closeConsumer:   consumer.Close,
		closeProducer:   producer.Close,
		closeRedis:      func() error { return redisinfra.Close(redisClient) },
		closeDB:         func() error { return db.Close(database) },
		logger:          log.Default(),
	}
	return runtime.Run(ctx)
}
