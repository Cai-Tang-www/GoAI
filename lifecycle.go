package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"
)

const defaultShutdownTimeout = 15 * time.Second

type lifecycleHTTPServer interface {
	ListenAndServe() error
	Shutdown(context.Context) error
	Close() error
}

type runtimeLifecycle struct {
	server          lifecycleHTTPServer
	address         string
	shutdownTimeout time.Duration
	runWorker       func(context.Context)
	closeConsumer   func() error
	closeProducer   func() error
	closeRedis      func() error
	closeDB         func() error
	logger          *log.Logger
}

func (r runtimeLifecycle) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if r.logger == nil {
		r.logger = log.Default()
	}

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		if r.runWorker != nil {
			r.runWorker(workerCtx)
		}
	}()

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
	}

	shutdownErr := r.shutdown(cancelWorker, workerDone, serverDone, serverStopped)
	return errors.Join(runErr, shutdownErr)
}

func (r runtimeLifecycle) shutdown(
	cancelWorker context.CancelFunc,
	workerDone <-chan struct{},
	serverDone <-chan error,
	serverStopped bool,
) error {
	timeout := r.shutdownTimeout
	if timeout <= 0 {
		timeout = defaultShutdownTimeout
	}

	var shutdownErrs []error
	r.logger.Printf("shutdown phase=http status=draining timeout=%s", timeout)
	httpCtx, cancelHTTP := context.WithTimeout(context.Background(), timeout)
	if err := r.server.Shutdown(httpCtx); err != nil {
		shutdownErrs = append(shutdownErrs, fmt.Errorf("shutting down HTTP server: %w", err))
		r.logger.Printf("shutdown phase=http status=force_closing err=%v", err)
		if closeErr := r.server.Close(); closeErr != nil {
			shutdownErrs = append(shutdownErrs, fmt.Errorf("forcing HTTP server close: %w", closeErr))
		}
	} else {
		r.logger.Printf("shutdown phase=http status=stopped")
	}
	cancelHTTP()

	if !serverStopped {
		serverExitErr, exited := waitForHTTPServer(serverDone, timeout)
		if serverExitErr != nil {
			shutdownErrs = append(shutdownErrs, serverExitErr)
		}
		if !exited {
			r.logger.Printf("shutdown phase=http status=force_closing reason=serve_wait_timeout")
			if closeErr := r.server.Close(); closeErr != nil {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("forcing HTTP server close after wait timeout: %w", closeErr))
			}
			if finalErr, _ := waitForHTTPServer(serverDone, timeout); finalErr != nil {
				shutdownErrs = append(shutdownErrs, finalErr)
			}
		}
	}

	r.logger.Printf("shutdown phase=worker status=stopping")
	cancelWorker()
	if r.closeConsumer != nil {
		if err := r.closeConsumer(); err != nil {
			shutdownErrs = append(shutdownErrs, fmt.Errorf("closing Kafka consumer: %w", err))
		}
	}

	workerTimer := time.NewTimer(timeout)
	select {
	case <-workerDone:
		r.logger.Printf("shutdown phase=worker status=stopped")
	case <-workerTimer.C:
		shutdownErrs = append(shutdownErrs, fmt.Errorf("waiting for Kafka worker: shutdown timed out after %s", timeout))
		r.logger.Printf("shutdown phase=worker status=timeout timeout=%s", timeout)
	}
	if !workerTimer.Stop() {
		select {
		case <-workerTimer.C:
		default:
		}
	}

	shutdownErrs = append(shutdownErrs, closeRuntimeResource(r.logger, "kafka_producer", r.closeProducer))
	shutdownErrs = append(shutdownErrs, closeRuntimeResource(r.logger, "redis", r.closeRedis))
	shutdownErrs = append(shutdownErrs, closeRuntimeResource(r.logger, "database", r.closeDB))

	result := errors.Join(shutdownErrs...)
	if result != nil {
		r.logger.Printf("shutdown completed status=error err=%v", result)
		return result
	}
	r.logger.Printf("shutdown completed status=success")
	return nil
}

func waitForHTTPServer(serverDone <-chan error, timeout time.Duration) (error, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-serverDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("waiting for HTTP server: %w", err), true
		}
		return nil, true
	case <-timer.C:
		return fmt.Errorf("waiting for HTTP server: shutdown timed out after %s", timeout), false
	}
}

func closeRuntimeResource(logger *log.Logger, name string, closeFn func() error) error {
	if closeFn == nil {
		return nil
	}
	if err := closeFn(); err != nil {
		logger.Printf("shutdown phase=infra resource=%s status=error err=%v", name, err)
		return fmt.Errorf("closing %s: %w", name, err)
	}
	logger.Printf("shutdown phase=infra resource=%s status=closed", name)
	return nil
}
