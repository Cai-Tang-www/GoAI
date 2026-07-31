package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type lifecycleEventRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *lifecycleEventRecorder) add(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *lifecycleEventRecorder) contains(event string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, item := range r.events {
		if item == event {
			return true
		}
	}
	return false
}

func (r *lifecycleEventRecorder) index(event string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, item := range r.events {
		if item == event {
			return i
		}
	}
	return -1
}

type fakeLifecycleHTTPServer struct {
	started          chan struct{}
	stopped          chan struct{}
	shutdownFunc     func(context.Context) error
	listenErr        error
	recorder         *lifecycleEventRecorder
	startOnce        sync.Once
	stopOnce         sync.Once
	skipShutdownStop bool
}

func newFakeLifecycleHTTPServer(recorder *lifecycleEventRecorder) *fakeLifecycleHTTPServer {
	return &fakeLifecycleHTTPServer{
		started:   make(chan struct{}),
		stopped:   make(chan struct{}),
		listenErr: http.ErrServerClosed,
		recorder:  recorder,
	}
}

func (s *fakeLifecycleHTTPServer) ListenAndServe() error {
	s.startOnce.Do(func() { close(s.started) })
	<-s.stopped
	s.recorder.add("http_listen_stopped")
	return s.listenErr
}

func (s *fakeLifecycleHTTPServer) Shutdown(ctx context.Context) error {
	s.recorder.add("http_shutdown")
	if s.shutdownFunc != nil {
		if err := s.shutdownFunc(ctx); err != nil {
			return err
		}
	}
	if !s.skipShutdownStop {
		s.stopOnce.Do(func() { close(s.stopped) })
	}
	return nil
}

func (s *fakeLifecycleHTTPServer) Close() error {
	s.recorder.add("http_force_close")
	s.stopOnce.Do(func() { close(s.stopped) })
	return nil
}

func TestRuntimeLifecycleWaitsForHTTPDrainBeforeStoppingWorker(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	server := newFakeLifecycleHTTPServer(recorder)
	shutdownStarted := make(chan struct{})
	releaseHTTP := make(chan struct{})
	server.shutdownFunc = func(ctx context.Context) error {
		close(shutdownStarted)
		select {
		case <-releaseHTTP:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	var logs bytes.Buffer
	runtime := runtimeLifecycle{
		server:          server,
		address:         ":0",
		shutdownTimeout: time.Second,
		runWorker: func(ctx context.Context) {
			<-ctx.Done()
			recorder.add("worker_stopped")
		},
		closeConsumer: func() error { recorder.add("consumer_closed"); return nil },
		closeProducer: func() error { recorder.add("producer_closed"); return nil },
		closeRedis:    func() error { recorder.add("redis_closed"); return nil },
		closeDB:       func() error { recorder.add("database_closed"); return nil },
		logger:        log.New(&logs, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	<-server.started
	cancel()
	<-shutdownStarted

	if recorder.contains("consumer_closed") {
		t.Fatal("Kafka consumer closed before in-flight HTTP requests drained")
	}
	close(releaseHTTP)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runtime shutdown failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not finish graceful shutdown")
	}

	// worker cancellation and consumer close happen together, so only assert their causal boundaries.
	assertLifecycleBefore(t, recorder, "http_shutdown", "consumer_closed")
	assertLifecycleBefore(t, recorder, "http_shutdown", "worker_stopped")
	assertLifecycleBefore(t, recorder, "consumer_closed", "producer_closed")
	assertLifecycleBefore(t, recorder, "worker_stopped", "producer_closed")
	assertLifecycleOrder(t, recorder, "producer_closed", "redis_closed", "database_closed")
	for _, want := range []string{"shutdown phase=http status=draining", "shutdown phase=worker status=stopped", "shutdown completed status=success"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("expected log %q, got %s", want, logs.String())
		}
	}
}

func TestRuntimeLifecycleForceClosesHTTPAfterTimeout(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	server := newFakeLifecycleHTTPServer(recorder)
	server.shutdownFunc = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}

	runtime := runtimeLifecycle{
		server:          server,
		shutdownTimeout: 20 * time.Millisecond,
		runWorker:       func(ctx context.Context) { <-ctx.Done() },
		closeConsumer:   func() error { recorder.add("consumer_closed"); return nil },
		closeProducer:   func() error { recorder.add("producer_closed"); return nil },
		closeRedis:      func() error { recorder.add("redis_closed"); return nil },
		closeDB:         func() error { recorder.add("database_closed"); return nil },
		logger:          log.New(&bytes.Buffer{}, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	<-server.started
	cancel()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "shutting down HTTP server") {
			t.Fatalf("expected HTTP shutdown timeout, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not finish forced shutdown")
	}

	if !recorder.contains("http_force_close") {
		t.Fatal("expected HTTP server force close after timeout")
	}
	for _, event := range []string{"consumer_closed", "producer_closed", "redis_closed", "database_closed"} {
		if !recorder.contains(event) {
			t.Fatalf("expected cleanup event %s", event)
		}
	}
}

func TestRuntimeLifecycleForceClosesWhenServeDoesNotExitAfterShutdown(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	server := newFakeLifecycleHTTPServer(recorder)
	server.skipShutdownStop = true

	runtime := runtimeLifecycle{
		server:          server,
		shutdownTimeout: 20 * time.Millisecond,
		runWorker:       func(ctx context.Context) { <-ctx.Done() },
		logger:          log.New(&bytes.Buffer{}, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	<-server.started
	cancel()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "waiting for HTTP server") {
			t.Fatalf("expected server wait timeout, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not finish after forcing the HTTP server closed")
	}
	if !recorder.contains("http_force_close") {
		t.Fatal("expected force close when ListenAndServe did not exit")
	}
	if !recorder.contains("http_listen_stopped") {
		t.Fatal("expected ListenAndServe goroutine to exit after force close")
	}
}

func TestRuntimeLifecycleCleansUpAfterHTTPServeFailure(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	server := newFakeLifecycleHTTPServer(recorder)
	serveFailure := errors.New("listen failed")
	server.listenErr = serveFailure
	server.stopOnce.Do(func() { close(server.stopped) })

	runtime := runtimeLifecycle{
		server:          server,
		shutdownTimeout: time.Second,
		runWorker:       func(ctx context.Context) { <-ctx.Done() },
		closeConsumer:   func() error { recorder.add("consumer_closed"); return nil },
		closeProducer:   func() error { recorder.add("producer_closed"); return nil },
		closeRedis:      func() error { recorder.add("redis_closed"); return nil },
		closeDB:         func() error { recorder.add("database_closed"); return nil },
		logger:          log.New(&bytes.Buffer{}, "", 0),
	}

	err := runtime.Run(context.Background())
	if err == nil || !errors.Is(err, serveFailure) {
		t.Fatalf("expected serve failure, got %v", err)
	}
	for _, event := range []string{"consumer_closed", "producer_closed", "redis_closed", "database_closed"} {
		if !recorder.contains(event) {
			t.Fatalf("expected cleanup event %s", event)
		}
	}
}

func TestRuntimeLifecycleAggregatesCleanupErrorsAndContinues(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	server := newFakeLifecycleHTTPServer(recorder)
	consumerErr := errors.New("consumer close failed")
	producerErr := errors.New("producer close failed")
	redisErr := errors.New("redis close failed")
	databaseErr := errors.New("database close failed")

	runtime := runtimeLifecycle{
		server:          server,
		shutdownTimeout: time.Second,
		runWorker:       func(ctx context.Context) { <-ctx.Done() },
		closeConsumer:   func() error { recorder.add("consumer_closed"); return consumerErr },
		closeProducer:   func() error { recorder.add("producer_closed"); return producerErr },
		closeRedis:      func() error { recorder.add("redis_closed"); return redisErr },
		closeDB:         func() error { recorder.add("database_closed"); return databaseErr },
		logger:          log.New(&bytes.Buffer{}, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	<-server.started
	cancel()

	err := <-done
	for _, want := range []error{consumerErr, producerErr, redisErr, databaseErr} {
		if !errors.Is(err, want) {
			t.Fatalf("expected aggregated error to contain %v, got %v", want, err)
		}
	}
	assertLifecycleOrder(t, recorder, "consumer_closed", "producer_closed", "redis_closed", "database_closed")
}

func assertLifecycleOrder(t *testing.T, recorder *lifecycleEventRecorder, events ...string) {
	t.Helper()
	last := -1
	for _, event := range events {
		index := recorder.index(event)
		if index == -1 {
			t.Fatalf("missing lifecycle event %s", event)
		}
		if index <= last {
			t.Fatalf("event %s occurred out of order", event)
		}
		last = index
	}
}

func assertLifecycleBefore(t *testing.T, recorder *lifecycleEventRecorder, before, after string) {
	t.Helper()
	beforeIndex := recorder.index(before)
	if beforeIndex == -1 {
		t.Fatalf("missing lifecycle event %s", before)
	}
	afterIndex := recorder.index(after)
	if afterIndex == -1 {
		t.Fatalf("missing lifecycle event %s", after)
	}
	if beforeIndex >= afterIndex {
		t.Fatalf("event %s must occur before %s", before, after)
	}
}
