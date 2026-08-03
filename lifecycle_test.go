package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
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
	return r.index(event) >= 0
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
	closeFunc        func() error
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
	if !s.skipShutdownStop {
		s.stopOnce.Do(func() { close(s.stopped) })
	}
	if s.shutdownFunc != nil {
		if err := s.shutdownFunc(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeLifecycleHTTPServer) Close() error {
	s.recorder.add("http_force_close")
	if s.closeFunc != nil {
		if err := s.closeFunc(); err != nil {
			return err
		}
	}
	s.stopOnce.Do(func() { close(s.stopped) })
	return nil
}

type listenerLifecycleHTTPServer struct {
	server   *http.Server
	listener net.Listener
}

func (s *listenerLifecycleHTTPServer) ListenAndServe() error {
	return s.server.Serve(s.listener)
}

func (s *listenerLifecycleHTTPServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *listenerLifecycleHTTPServer) Close() error {
	return s.server.Close()
}

func TestRuntimeLifecycleDrainsRealHTTPRequest(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	producerClosed := make(chan struct{})
	var handlerStartOnce sync.Once
	var producerCloseOnce sync.Once

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	server := &listenerLifecycleHTTPServer{
		listener: listener,
		server: &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			handlerStartOnce.Do(func() { close(handlerStarted) })
			<-releaseHandler
			recorder.add("handler_completed")
			w.WriteHeader(http.StatusNoContent)
		})},
	}
	runtime := runtimeLifecycle{
		server:          server,
		address:         listener.Addr().String(),
		shutdownTimeout: time.Second,
		closeProducer: func(context.Context) error {
			recorder.add("producer_closed")
			producerCloseOnce.Do(func() { close(producerClosed) })
			return nil
		},
		logger: log.New(&bytes.Buffer{}, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	runtimeDone := make(chan error, 1)
	go func() { runtimeDone <- runtime.Run(ctx) }()
	responseDone := make(chan error, 1)
	go func() {
		response, requestErr := http.Get("http://" + listener.Addr().String())
		if response != nil {
			_ = response.Body.Close()
		}
		responseDone <- requestErr
	}()
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not start")
	}

	cancel()
	select {
	case <-producerClosed:
		t.Fatal("producer closed before the active HTTP handler completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseHandler)

	select {
	case requestErr := <-responseDone:
		if requestErr != nil {
			t.Fatalf("HTTP request failed during graceful drain: %v", requestErr)
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP request did not finish during graceful drain")
	}
	select {
	case runtimeErr := <-runtimeDone:
		if runtimeErr != nil {
			t.Fatalf("runtime shutdown failed: %v", runtimeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not finish after HTTP request drained")
	}
	assertLifecycleBefore(t, recorder, "handler_completed", "producer_closed")
}

func TestRuntimeLifecycleForceCloseCancelsRealHTTPRequest(t *testing.T) {
	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	var handlerStartOnce sync.Once
	var handlerCancelOnce sync.Once

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	server := &listenerLifecycleHTTPServer{
		listener: listener,
		server: &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			handlerStartOnce.Do(func() { close(handlerStarted) })
			<-request.Context().Done()
			handlerCancelOnce.Do(func() { close(handlerCanceled) })
		})},
	}
	runtime := runtimeLifecycle{
		server:          server,
		address:         listener.Addr().String(),
		shutdownTimeout: 100 * time.Millisecond,
		logger:          log.New(&bytes.Buffer{}, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	runtimeDone := make(chan error, 1)
	go func() { runtimeDone <- runtime.Run(ctx) }()
	requestDone := make(chan error, 1)
	go func() {
		response, requestErr := http.Get("http://" + listener.Addr().String())
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- requestErr
	}()
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not start")
	}

	cancel()
	select {
	case <-handlerCanceled:
	case <-time.After(time.Second):
		t.Fatal("forced HTTP close did not cancel the request context")
	}
	select {
	case runtimeErr := <-runtimeDone:
		if runtimeErr == nil || !strings.Contains(runtimeErr.Error(), "graceful HTTP drain timed out") {
			t.Fatalf("expected graceful HTTP drain timeout, got %v", runtimeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not return after forced HTTP close")
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("HTTP client did not return after forced close")
	}
}

func TestRuntimeLifecycleForceCloseWaitsForHandlerExit(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	handlerStarted := make(chan struct{})
	handlerCanceled := make(chan struct{})
	releaseHandler := make(chan struct{})
	producerClosed := make(chan struct{})
	var handlerStartOnce sync.Once
	var handlerCancelOnce sync.Once
	var producerCloseOnce sync.Once

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	connectionTracker := newHTTPConnectionTracker()
	server := &listenerLifecycleHTTPServer{
		listener: listener,
		server: &http.Server{
			ConnState: connectionTracker.Track,
			Handler: http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
				handlerStartOnce.Do(func() { close(handlerStarted) })
				<-request.Context().Done()
				handlerCancelOnce.Do(func() { close(handlerCanceled) })
				<-releaseHandler
				recorder.add("handler_completed")
			}),
		},
	}
	runtime := runtimeLifecycle{
		server:          server,
		address:         listener.Addr().String(),
		shutdownTimeout: 500 * time.Millisecond,
		waitHTTP:        connectionTracker.Wait,
		closeProducer: func(context.Context) error {
			recorder.add("producer_closed")
			producerCloseOnce.Do(func() { close(producerClosed) })
			return nil
		},
		logger: log.New(&bytes.Buffer{}, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	runtimeDone := make(chan error, 1)
	go func() { runtimeDone <- runtime.Run(ctx) }()
	requestDone := make(chan error, 1)
	go func() {
		response, requestErr := http.Get("http://" + listener.Addr().String())
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- requestErr
	}()
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not start")
	}

	cancel()
	select {
	case <-handlerCanceled:
	case <-time.After(time.Second):
		t.Fatal("forced HTTP close did not cancel the request context")
	}
	select {
	case <-producerClosed:
		t.Fatal("producer closed while the canceled handler was still unwinding")
	default:
	}
	close(releaseHandler)

	select {
	case runtimeErr := <-runtimeDone:
		if runtimeErr == nil || !strings.Contains(runtimeErr.Error(), "graceful HTTP drain timed out") {
			t.Fatalf("expected graceful HTTP drain timeout, got %v", runtimeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not return after the forced handler exited")
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("HTTP client did not return after forced close")
	}
	assertLifecycleBefore(t, recorder, "handler_completed", "producer_closed")
}

func TestRuntimeLifecycleStopsNewWorkAndWaitsForDrains(t *testing.T) {
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
	consumerClosed := make(chan struct{})
	var closeConsumerOnce sync.Once

	var logs bytes.Buffer
	runtime := runtimeLifecycle{
		server:          server,
		address:         ":0",
		shutdownTimeout: time.Second,
		stopStreams:     func() { recorder.add("streams_stopped") },
		runWorker: func(context.Context) error {
			<-consumerClosed
			recorder.add("worker_stopped")
			return nil
		},
		closeConsumer: func() error {
			recorder.add("consumer_closed")
			closeConsumerOnce.Do(func() { close(consumerClosed) })
			return nil
		},
		closeProducer:  func(context.Context) error { recorder.add("producer_closed"); return nil },
		closeRedis:     func(context.Context) error { recorder.add("redis_closed"); return nil },
		closeDB:        func(context.Context) error { recorder.add("database_closed"); return nil },
		closeTelemetry: func(context.Context) error { recorder.add("observability_closed"); return nil },
		logger:         log.New(&logs, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	<-server.started
	cancel()
	<-shutdownStarted
	waitForLifecycleEvent(t, recorder, "consumer_closed")
	if recorder.contains("producer_closed") {
		t.Fatal("producer closed before in-flight HTTP request drained")
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

	assertLifecycleOrder(t, recorder, "streams_stopped", "http_listen_stopped", "consumer_closed", "producer_closed", "redis_closed", "database_closed", "observability_closed")
	assertLifecycleBefore(t, recorder, "worker_stopped", "producer_closed")
	for _, want := range []string{"shutdown phase=streams status=stopping", "shutdown phase=worker status=stopped", "shutdown completed status=success"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("missing lifecycle log %q in %s", want, logs.String())
		}
	}
}

func TestRuntimeLifecycleForceClosesWithinTotalTimeout(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	server := newFakeLifecycleHTTPServer(recorder)
	server.shutdownFunc = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}

	runtime := runtimeLifecycle{
		server:          server,
		shutdownTimeout: 100 * time.Millisecond,
		runWorker: func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		},
		closeConsumer: func() error { recorder.add("consumer_closed"); return nil },
		closeProducer: func(context.Context) error { recorder.add("producer_closed"); return nil },
		closeRedis:    func(context.Context) error { recorder.add("redis_closed"); return nil },
		closeDB:       func(context.Context) error { recorder.add("database_closed"); return nil },
		logger:        log.New(&bytes.Buffer{}, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	startedAt := time.Now()
	go func() { done <- runtime.Run(ctx) }()
	<-server.started
	cancel()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "graceful HTTP drain timed out") {
			t.Fatalf("expected HTTP drain timeout, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime did not finish forced shutdown")
	}
	if elapsed := time.Since(startedAt); elapsed > 300*time.Millisecond {
		t.Fatalf("shutdown exceeded configured total window: %s", elapsed)
	}
	if !recorder.contains("http_force_close") {
		t.Fatal("expected HTTP server force close")
	}
	assertLifecycleOrder(t, recorder, "consumer_closed", "producer_closed", "redis_closed", "database_closed")
}

func TestRuntimeLifecycleBoundsBlockedConsumerClose(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	server := newFakeLifecycleHTTPServer(recorder)
	releaseConsumer := make(chan struct{})
	workerExited := make(chan struct{})

	runtime := runtimeLifecycle{
		server:          server,
		shutdownTimeout: 80 * time.Millisecond,
		runWorker: func(ctx context.Context) error {
			defer close(workerExited)
			<-ctx.Done()
			return nil
		},
		closeConsumer: func() error {
			recorder.add("consumer_close_started")
			<-releaseConsumer
			return nil
		},
		closeProducer: func(context.Context) error { recorder.add("producer_closed"); return nil },
		closeRedis:    func(context.Context) error { recorder.add("redis_closed"); return nil },
		closeDB:       func(context.Context) error { recorder.add("database_closed"); return nil },
		logger:        log.New(&bytes.Buffer{}, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	<-server.started
	startedAt := time.Now()
	cancel()

	err := <-done
	if err == nil || !strings.Contains(err.Error(), "Kafka consumer close: timed out") {
		t.Fatalf("expected consumer close timeout, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("blocked consumer close exceeded total shutdown budget: %s", elapsed)
	}
	for _, event := range []string{"producer_closed", "redis_closed", "database_closed"} {
		if recorder.contains(event) {
			t.Fatalf("dependency %s closed while consumer close was still running", event)
		}
	}
	close(releaseConsumer)
	select {
	case <-workerExited:
	case <-time.After(time.Second):
		t.Fatal("test worker did not exit")
	}
}

func TestRuntimeLifecycleBoundsBlockedHTTPForceClose(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	server := newFakeLifecycleHTTPServer(recorder)
	releaseShutdown := make(chan struct{})
	releaseClose := make(chan struct{})
	server.shutdownFunc = func(context.Context) error {
		<-releaseShutdown
		return nil
	}
	server.closeFunc = func() error {
		<-releaseClose
		return nil
	}

	runtime := runtimeLifecycle{
		server:          server,
		shutdownTimeout: 80 * time.Millisecond,
		runWorker: func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		},
		closeConsumer: func() error { return nil },
		closeProducer: func(context.Context) error { recorder.add("producer_closed"); return nil },
		closeRedis:    func(context.Context) error { recorder.add("redis_closed"); return nil },
		closeDB:       func(context.Context) error { recorder.add("database_closed"); return nil },
		logger:        log.New(&bytes.Buffer{}, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	<-server.started
	startedAt := time.Now()
	cancel()

	err := <-done
	if err == nil || !strings.Contains(err.Error(), "waiting for HTTP shutdown: timed out") {
		t.Fatalf("expected HTTP shutdown timeout, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("blocked HTTP close exceeded total shutdown budget: %s", elapsed)
	}
	for _, event := range []string{"producer_closed", "redis_closed", "database_closed"} {
		if recorder.contains(event) {
			t.Fatalf("dependency %s closed while HTTP shutdown was still running", event)
		}
	}
	close(releaseShutdown)
	close(releaseClose)
	select {
	case <-server.stopped:
	case <-time.After(time.Second):
		t.Fatal("test HTTP server did not stop")
	}
}

func TestRuntimeLifecycleBoundsBlockedProducerClose(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	server := newFakeLifecycleHTTPServer(recorder)
	releaseProducer := make(chan struct{})
	consumerClosed := make(chan struct{})
	var closeConsumerOnce sync.Once

	runtime := runtimeLifecycle{
		server:          server,
		shutdownTimeout: 80 * time.Millisecond,
		runWorker: func(context.Context) error {
			<-consumerClosed
			return nil
		},
		closeConsumer: func() error {
			closeConsumerOnce.Do(func() { close(consumerClosed) })
			return nil
		},
		closeProducer: func(ctx context.Context) error {
			recorder.add("producer_close_started")
			select {
			case <-releaseProducer:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		closeRedis:     func(context.Context) error { recorder.add("redis_closed"); return nil },
		closeDB:        func(context.Context) error { recorder.add("database_closed"); return nil },
		closeTelemetry: func(context.Context) error { recorder.add("observability_closed"); return nil },
		logger:         log.New(&bytes.Buffer{}, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	<-server.started
	startedAt := time.Now()
	cancel()

	err := <-done
	if err == nil || !strings.Contains(err.Error(), "closing kafka_producer: context deadline exceeded") {
		t.Fatalf("expected producer close timeout, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("blocked producer close exceeded total shutdown budget: %s", elapsed)
	}
	for _, event := range []string{"redis_closed", "database_closed", "observability_closed"} {
		if recorder.contains(event) {
			t.Fatalf("dependency %s closed after producer close timed out", event)
		}
	}
	close(releaseProducer)
}
func TestRuntimeLifecycleDoesNotCloseDependenciesWhileWorkerRuns(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	server := newFakeLifecycleHTTPServer(recorder)
	releaseWorker := make(chan struct{})
	workerExited := make(chan struct{})

	runtime := runtimeLifecycle{
		server:          server,
		shutdownTimeout: 100 * time.Millisecond,
		runWorker: func(context.Context) error {
			defer close(workerExited)
			<-releaseWorker
			return nil
		},
		closeConsumer:  func() error { recorder.add("consumer_closed"); return nil },
		closeProducer:  func(context.Context) error { recorder.add("producer_closed"); return nil },
		closeRedis:     func(context.Context) error { recorder.add("redis_closed"); return nil },
		closeDB:        func(context.Context) error { recorder.add("database_closed"); return nil },
		closeTelemetry: func(context.Context) error { recorder.add("observability_closed"); return nil },
		logger:         log.New(&bytes.Buffer{}, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	<-server.started
	cancel()

	err := <-done
	if err == nil || !strings.Contains(err.Error(), "Kafka worker: timed out") {
		t.Fatalf("expected worker timeout, got %v", err)
	}
	for _, event := range []string{"producer_closed", "redis_closed", "database_closed", "observability_closed"} {
		if recorder.contains(event) {
			t.Fatalf("dependency %s closed while worker was still running", event)
		}
	}
	close(releaseWorker)
	select {
	case <-workerExited:
	case <-time.After(time.Second):
		t.Fatal("test worker did not exit")
	}
}

func TestRuntimeLifecycleSurfacesWorkerFailure(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	server := newFakeLifecycleHTTPServer(recorder)
	workerErr := errors.New("consumer startup failed")

	runtime := runtimeLifecycle{
		server:          server,
		shutdownTimeout: time.Second,
		runWorker:       func(context.Context) error { return workerErr },
		closeConsumer:   func() error { recorder.add("consumer_closed"); return nil },
		closeProducer:   func(context.Context) error { recorder.add("producer_closed"); return nil },
		closeRedis:      func(context.Context) error { recorder.add("redis_closed"); return nil },
		closeDB:         func(context.Context) error { recorder.add("database_closed"); return nil },
		logger:          log.New(&bytes.Buffer{}, "", 0),
	}

	err := runtime.Run(context.Background())
	if !errors.Is(err, workerErr) {
		t.Fatalf("expected worker error to reach main lifecycle, got %v", err)
	}
	assertLifecycleOrder(t, recorder, "consumer_closed", "producer_closed", "redis_closed", "database_closed")
}

func TestRuntimeLifecycleCleansUpAfterHTTPServeFailure(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	server := newFakeLifecycleHTTPServer(recorder)
	serveFailure := errors.New("listen failed")
	server.listenErr = serveFailure
	server.stopOnce.Do(func() { close(server.stopped) })
	consumerClosed := make(chan struct{})
	var closeConsumerOnce sync.Once

	runtime := runtimeLifecycle{
		server:          server,
		shutdownTimeout: time.Second,
		runWorker: func(context.Context) error {
			<-consumerClosed
			return nil
		},
		closeConsumer: func() error {
			recorder.add("consumer_closed")
			closeConsumerOnce.Do(func() { close(consumerClosed) })
			return nil
		},
		closeProducer: func(context.Context) error { recorder.add("producer_closed"); return nil },
		closeRedis:    func(context.Context) error { recorder.add("redis_closed"); return nil },
		closeDB:       func(context.Context) error { recorder.add("database_closed"); return nil },
		logger:        log.New(&bytes.Buffer{}, "", 0),
	}

	err := runtime.Run(context.Background())
	if err == nil || !errors.Is(err, serveFailure) {
		t.Fatalf("expected serve failure, got %v", err)
	}
	if !recorder.contains("http_shutdown") {
		t.Fatal("expected Shutdown to drain active handlers after Serve returned")
	}
	assertLifecycleOrder(t, recorder, "consumer_closed", "producer_closed", "redis_closed", "database_closed")
}

func TestRuntimeLifecycleAggregatesCleanupErrorsAndContinues(t *testing.T) {
	recorder := &lifecycleEventRecorder{}
	server := newFakeLifecycleHTTPServer(recorder)
	consumerErr := errors.New("consumer close failed")
	producerErr := errors.New("producer close failed")
	redisErr := errors.New("redis close failed")
	databaseErr := errors.New("database close failed")
	observabilityErr := errors.New("observability close failed")
	consumerClosed := make(chan struct{})
	var closeConsumerOnce sync.Once

	runtime := runtimeLifecycle{
		server:          server,
		shutdownTimeout: time.Second,
		runWorker: func(context.Context) error {
			<-consumerClosed
			return nil
		},
		closeConsumer: func() error {
			recorder.add("consumer_closed")
			closeConsumerOnce.Do(func() { close(consumerClosed) })
			return consumerErr
		},
		closeProducer:  func(context.Context) error { recorder.add("producer_closed"); return producerErr },
		closeRedis:     func(context.Context) error { recorder.add("redis_closed"); return redisErr },
		closeDB:        func(context.Context) error { recorder.add("database_closed"); return databaseErr },
		closeTelemetry: func(context.Context) error { recorder.add("observability_closed"); return observabilityErr },
		logger:         log.New(&bytes.Buffer{}, "", 0),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	<-server.started
	cancel()

	err := <-done
	for _, want := range []error{consumerErr, producerErr, redisErr, databaseErr, observabilityErr} {
		if !errors.Is(err, want) {
			t.Fatalf("expected aggregated error to contain %v, got %v", want, err)
		}
	}
	assertLifecycleOrder(t, recorder, "consumer_closed", "producer_closed", "redis_closed", "database_closed", "observability_closed")
}

func TestContextCloserHonorsDeadline(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	closer := contextCloser(func() error {
		close(started)
		defer close(finished)
		<-release
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := closer(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline, got %v", err)
	}
	<-started
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("contextless closer did not finish after release")
	}
}
func TestRuntimeLifecycleRejectsNilHTTPServer(t *testing.T) {
	err := (runtimeLifecycle{}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "HTTP server is nil") {
		t.Fatalf("expected nil server error, got %v", err)
	}
}

func waitForLifecycleEvent(t *testing.T, recorder *lifecycleEventRecorder, event string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if recorder.contains(event) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("missing lifecycle event %s", event)
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
