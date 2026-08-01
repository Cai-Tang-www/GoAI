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

	"GoAI/a2aclient"
	"GoAI/a2agateway"
	"GoAI/config"
	"GoAI/db"
	"GoAI/governance"
	"GoAI/kafka"
	"GoAI/observability"
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

	telemetry, err := observability.New("goai", os.Stderr)
	if err != nil {
		return fmt.Errorf("initializing observability: %w", err)
	}
	governanceEventSink := func(event governance.Event) {
		if telemetry.Metrics != nil {
			metricScope := event.Target
			if event.Type != "rate_limited" {
				// Downstream hostnames stay in logs, not Prometheus labels.
				metricScope = "downstream"
			}
			telemetry.Metrics.ObserveGovernance(event.Type, metricScope, event.Status)
		}
		if event.Error != nil {
			log.Printf("governance event type=%s target=%s status=%s err=%v", event.Type, event.Target, event.Status, event.Error)
			return
		}
		log.Printf("governance event type=%s target=%s status=%s", event.Type, event.Target, event.Status)
	}
	governanceService, err := governance.New(governance.Config{
		Enabled:                    cfg.ServiceGovernanceEnabled,
		RateLimitRequestsPerSecond: cfg.RateLimitRequestsPerSecond,
		RateLimitBurst:             cfg.RateLimitBurst,
		RateLimitMaxKeys:           cfg.RateLimitMaxKeys,
		DownstreamRequestTimeout:   cfg.DownstreamRequestTimeout,
		CircuitFailureThreshold:    cfg.CircuitFailureThreshold,
		CircuitOpenTimeout:         cfg.CircuitOpenTimeout,
		CircuitMaxTargets:          cfg.CircuitMaxTargets,
		OnEvent:                    governanceEventSink,
	})
	if err != nil {
		return fmt.Errorf("initializing service governance: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if shutdownErr := telemetry.Shutdown(shutdownCtx); shutdownErr != nil {
			log.Printf("observability shutdown failed: %v", shutdownErr)
		}
	}()

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

	producer, err := kafka.NewProducer(ctx, cfg, kafka.WithProducerObservability(telemetry))
	if err != nil {
		return errors.Join(err, redisinfra.Close(redisClient), db.Close(database))
	}
	log.Println("Kafka producer initialized")

	governedHTTPClient := &http.Client{Transport: governanceService.Transport}
	chatService, err := services.NewChatService(cfg, governedHTTPClient)
	if err != nil {
		return errors.Join(err, producer.Close(), redisinfra.Close(redisClient), db.Close(database))
	}
	agentInvoker, err := a2aclient.New(governedHTTPClient, cfg.A2AClientRequestTimeout, cfg.A2AClientPollInterval, a2aclient.WithObservability(telemetry))
	if err != nil {
		return errors.Join(err, producer.Close(), redisinfra.Close(redisClient), db.Close(database))
	}
	loopService, err := services.NewLoopService(database, services.WithLoopObservability(telemetry))
	if err != nil {
		return errors.Join(err, producer.Close(), redisinfra.Close(redisClient), db.Close(database))
	}
	runService, err := services.NewRunService(database, producer,
		services.WithAgentInvoker(agentInvoker),
		services.WithChatService(chatService),
		services.WithLoopService(loopService),
		services.WithRunObservability(telemetry),
	)
	if err != nil {
		return errors.Join(err, producer.Close(), redisinfra.Close(redisClient), db.Close(database))
	}
	runtimeService, err := services.NewRuntimeService(database, runService, services.WithRuntimeObservability(telemetry))
	if err != nil {
		return errors.Join(err, producer.Close(), redisinfra.Close(redisClient), db.Close(database))
	}
	a2aGateway, err := a2agateway.New(runtimeService)
	if err != nil {
		return errors.Join(err, producer.Close(), redisinfra.Close(redisClient), db.Close(database))
	}
	runWorker, err := worker.NewRunWorker(runService, runtimeService)
	if err != nil {
		return errors.Join(err, producer.Close(), redisinfra.Close(redisClient), db.Close(database))
	}
	consumer, err := kafka.NewConsumer(cfg, runWorker.HandleRunExecuteMessage, kafka.WithConsumerObservability(telemetry))
	if err != nil {
		return errors.Join(err, producer.Close(), redisinfra.Close(redisClient), db.Close(database))
	}
	log.Println("Kafka consumer initialized")

	router, err := routers.New(routers.Dependencies{
		Database:         database,
		RunService:       runService,
		Runtime:          runtimeService,
		A2AGateway:       a2aGateway,
		ChatService:      chatService,
		Observability:    telemetry,
		Governance:       governanceService,
		GovernanceScopes: cfg.RateLimitScopes,
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
