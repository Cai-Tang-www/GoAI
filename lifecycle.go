package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	defaultShutdownTimeout = 15 * time.Second
	maxForceStopWindow     = time.Second
)

type lifecycleHTTPServer interface {
	ListenAndServe() error
	Shutdown(context.Context) error
	Close() error
}

type runtimeLifecycle struct {
	server          lifecycleHTTPServer
	address         string
	shutdownTimeout time.Duration
	waitHTTP        func(context.Context) error
	runWorker       func(context.Context) error
	stopStreams     func()
	closeConsumer   func() error
	closeProducer   func(context.Context) error
	closeRedis      func(context.Context) error
	closeDB         func(context.Context) error
	closeTelemetry  func(context.Context) error
	logger          *log.Logger
}

func (r runtimeLifecycle) Run(ctx context.Context) error {
	if r.server == nil {
		return errors.New("running lifecycle: HTTP server is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if r.logger == nil {
		r.logger = log.Default()
	}

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	var workerDone chan error
	workerStopped := r.runWorker == nil
	if r.runWorker != nil {
		workerDone = make(chan error, 1)
		go func() {
			workerDone <- r.runWorker(workerCtx)
		}()
	}

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- r.server.ListenAndServe()
	}()
	r.logger.Printf("http server started addr=%s", r.address)

	var runErr error
	serverStopped := false
	select {
	case <-ctx.Done():
		r.logger.Printf("shutdown requested reason=%v", ctx.Err())
	case err := <-serverDone:
		serverStopped = true
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("serving HTTP: %w", err)
			r.logger.Printf("http server stopped unexpectedly err=%v", err)
		}
	case err := <-workerDone:
		workerStopped = true
		if err != nil {
			runErr = fmt.Errorf("running Kafka worker: %w", err)
		} else {
			runErr = errors.New("running Kafka worker: worker stopped unexpectedly")
		}
		r.logger.Printf("Kafka worker stopped unexpectedly err=%v", runErr)
	}

	shutdownErr := r.shutdown(cancelWorker, workerDone, serverDone, workerStopped, serverStopped)
	return errors.Join(runErr, shutdownErr)
}

func (r runtimeLifecycle) shutdown(
	cancelWorker context.CancelFunc,
	workerDone <-chan error,
	serverDone <-chan error,
	workerStopped bool,
	serverStopped bool,
) error {
	timeout := r.shutdownTimeout
	if timeout <= 0 {
		timeout = defaultShutdownTimeout
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), timeout)
	defer cancelShutdown()
	drainCtx, cancelDrain := context.WithTimeout(shutdownCtx, gracefulDrainWindow(timeout))
	defer cancelDrain()

	var shutdownErrs []error
	if r.stopStreams != nil {
		r.logger.Printf("shutdown phase=streams status=stopping")
		r.stopStreams()
	}

	httpDrained := false
	var httpShutdownDoneCh <-chan error
	r.logger.Printf("shutdown phase=http status=draining timeout=%s", gracefulDrainWindow(timeout))
	httpShutdownDone := make(chan error, 1)
	httpShutdownDoneCh = httpShutdownDone
	go func() {
		httpShutdownDone <- r.server.Shutdown(drainCtx)
	}()

	workerDoneCh := workerDone
	serverDoneCh := serverDone
	if workerStopped {
		workerDoneCh = nil
	}
	if serverStopped {
		serverDoneCh = nil
	}

	consumerStopped := r.closeConsumer == nil
	consumerCloseStarted := false
	var consumerDoneCh <-chan error
	startConsumerClose := func() {
		if consumerStopped || consumerCloseStarted {
			return
		}
		consumerCloseStarted = true
		r.logger.Printf("shutdown phase=kafka_consumer status=stopping")
		consumerDone := make(chan error, 1)
		consumerDoneCh = consumerDone
		go func() {
			consumerDone <- r.closeConsumer()
		}()
	}
	if serverStopped {
		startConsumerClose()
	}

	drainDoneCh := drainCtx.Done()
	var httpForceDoneCh <-chan error
	var httpWaitDoneCh <-chan error
	forceStopStarted := false
	httpWaitStarted := false
	startHTTPWait := func(forceCloseErr error) {
		if httpWaitStarted {
			return
		}
		httpWaitStarted = true
		if forceCloseErr != nil && !errors.Is(forceCloseErr, http.ErrServerClosed) {
			shutdownErrs = append(shutdownErrs, fmt.Errorf("forcing HTTP server close: %w", forceCloseErr))
		}
		if r.waitHTTP == nil {
			httpDrained = forceCloseErr == nil || errors.Is(forceCloseErr, http.ErrServerClosed)
			return
		}
		httpWaitDone := make(chan error, 1)
		httpWaitDoneCh = httpWaitDone
		go func() {
			httpWaitDone <- r.waitHTTP(shutdownCtx)
		}()
	}
	startForceStop := func(drainTimedOut bool) {
		if forceStopStarted {
			return
		}
		forceStopStarted = true
		r.logger.Printf("shutdown phase=force_stop status=starting reason=%v", drainCtx.Err())
		if drainTimedOut {
			if !serverStopped || !httpDrained {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("graceful HTTP drain timed out after %s", gracefulDrainWindow(timeout)))
			}
			if !workerStopped {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("graceful Kafka worker drain timed out after %s", gracefulDrainWindow(timeout)))
			}
			if !consumerStopped {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("graceful Kafka consumer close timed out after %s", gracefulDrainWindow(timeout)))
			}
		}
		cancelWorker()
		if !serverStopped || !httpDrained {
			httpForceDone := make(chan error, 1)
			httpForceDoneCh = httpForceDone
			go func() {
				httpForceDone <- r.server.Close()
			}()
		}
	}

	for !workerStopped || !serverStopped || !httpDrained || !consumerStopped {
		select {
		case err := <-httpShutdownDoneCh:
			httpShutdownDoneCh = nil
			if err == nil {
				httpDrained = true
				continue
			}
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("shutting down HTTP server: %w", err))
				startForceStop(false)
			}
		case err := <-httpForceDoneCh:
			httpForceDoneCh = nil
			startHTTPWait(err)
		case err := <-httpWaitDoneCh:
			httpWaitDoneCh = nil
			if err == nil {
				httpDrained = true
				continue
			}
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("waiting for active HTTP handlers: %w", err))
			}
		case err := <-serverDoneCh:
			serverDoneCh = nil
			serverStopped = true
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("waiting for HTTP server: %w", err))
			}
			startConsumerClose()
		case err := <-workerDoneCh:
			workerDoneCh = nil
			workerStopped = true
			if err != nil && !errors.Is(err, context.Canceled) {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("waiting for Kafka worker: %w", err))
			}
		case err := <-consumerDoneCh:
			consumerDoneCh = nil
			consumerStopped = true
			if err != nil {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("closing Kafka consumer: %w", err))
			}
		case <-drainDoneCh:
			drainDoneCh = nil
			startForceStop(true)
		case <-shutdownCtx.Done():
			if !serverStopped || !httpDrained {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("waiting for HTTP shutdown: timed out after %s", timeout))
			}
			if !workerStopped {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("waiting for Kafka worker: timed out after %s", timeout))
			}
			if !consumerStopped {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("waiting for Kafka consumer close: timed out after %s", timeout))
			}
			goto closeResources
		}
	}

closeResources:
	if workerStopped {
		r.logger.Printf("shutdown phase=worker status=stopped")
	} else {
		r.logger.Printf("shutdown phase=worker status=timeout timeout=%s", timeout)
	}
	if consumerStopped {
		r.logger.Printf("shutdown phase=kafka_consumer status=stopped")
	} else {
		r.logger.Printf("shutdown phase=kafka_consumer status=timeout timeout=%s", timeout)
	}
	if serverStopped && httpDrained {
		r.logger.Printf("shutdown phase=http status=stopped")
	} else {
		r.logger.Printf("shutdown phase=http status=timeout timeout=%s", timeout)
	}

	// Never close dependencies while an HTTP handler, consumer, or worker can still use them.
	// On this hard timeout path the process exits and the OS reclaims descriptors.
	if workerStopped && consumerStopped && serverStopped && httpDrained {
		resources := []struct {
			name    string
			closeFn func(context.Context) error
		}{
			{name: "kafka_producer", closeFn: r.closeProducer},
			{name: "redis", closeFn: r.closeRedis},
			{name: "database", closeFn: r.closeDB},
			{name: "observability", closeFn: r.closeTelemetry},
		}
		for _, resource := range resources {
			completed, err := closeRuntimeResource(shutdownCtx, r.logger, resource.name, resource.closeFn)
			shutdownErrs = append(shutdownErrs, err)
			if !completed {
				break
			}
		}
	} else {
		shutdownErrs = append(shutdownErrs, errors.New("skipping dependent resource close because HTTP, Kafka consumer, or worker work is still running"))
	}

	result := errors.Join(shutdownErrs...)
	if result != nil {
		r.logger.Printf("shutdown completed status=error err=%v", result)
		return result
	}
	r.logger.Printf("shutdown completed status=success")
	return nil
}

func gracefulDrainWindow(timeout time.Duration) time.Duration {
	forceWindow := timeout / 5
	if forceWindow > maxForceStopWindow {
		forceWindow = maxForceStopWindow
	}
	if forceWindow <= 0 {
		return timeout
	}
	return timeout - forceWindow
}

func contextCloser(closeFn func() error) func(context.Context) error {
	if closeFn == nil {
		return nil
	}
	return func(ctx context.Context) error {
		if ctx == nil {
			ctx = context.Background()
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		done := make(chan error, 1)
		go func() {
			done <- closeFn()
		}()
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func closeRuntimeResource(ctx context.Context, logger *log.Logger, name string, closeFn func(context.Context) error) (bool, error) {
	if closeFn == nil {
		return true, nil
	}
	if err := ctx.Err(); err != nil {
		logger.Printf("shutdown phase=%s status=timeout err=%v", name, err)
		return false, fmt.Errorf("closing %s: %w", name, err)
	}

	err := closeFn(ctx)
	if err != nil {
		logger.Printf("shutdown phase=%s status=error err=%v", name, err)
		if ctx.Err() != nil {
			return false, fmt.Errorf("closing %s: %w", name, err)
		}
		return true, fmt.Errorf("closing %s: %w", name, err)
	}
	logger.Printf("shutdown phase=%s status=stopped", name)
	return true, nil
}

// httpConnectionTracker waits until standard net/http connections have fully
// left their serve goroutines. Server.Close can close sockets before a handler
// has finished unwinding and released shared dependencies.
type httpConnectionTracker struct {
	mu     sync.Mutex
	active map[net.Conn]struct{}
	idle   chan struct{}
}

func newHTTPConnectionTracker() *httpConnectionTracker {
	idle := make(chan struct{})
	close(idle)
	return &httpConnectionTracker{
		active: make(map[net.Conn]struct{}),
		idle:   idle,
	}
}

func (t *httpConnectionTracker) Track(conn net.Conn, state http.ConnState) {
	if t == nil || conn == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	switch state {
	case http.StateNew:
		if _, exists := t.active[conn]; exists {
			return
		}
		if len(t.active) == 0 {
			t.idle = make(chan struct{})
		}
		t.active[conn] = struct{}{}
	case http.StateClosed:
		if _, exists := t.active[conn]; !exists {
			return
		}
		delete(t.active, conn)
		if len(t.active) == 0 {
			close(t.idle)
		}
	}
}

func (t *httpConnectionTracker) Wait(ctx context.Context) error {
	if t == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	t.mu.Lock()
	idle := t.idle
	t.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
