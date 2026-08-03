package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type recordingRecoveryService struct {
	mu            sync.Mutex
	callbackCalls int
	resumeCalls   int
	called        chan struct{}
}

func (s *recordingRecoveryService) RecoverPendingCallbacks(context.Context, int) error {
	s.mu.Lock()
	s.callbackCalls++
	s.mu.Unlock()
	return nil
}

func (s *recordingRecoveryService) RecoverPendingResumes(context.Context, int) error {
	s.mu.Lock()
	s.resumeCalls++
	calls := s.resumeCalls
	s.mu.Unlock()
	if calls == 1 && s.called != nil {
		close(s.called)
	}
	return nil
}

func TestRecoveryWorkerRunsImmediatelyAndStopsWithContext(t *testing.T) {
	service := &recordingRecoveryService{called: make(chan struct{})}
	worker, err := NewRecoveryWorker(service, 10*time.Millisecond, 25)
	if err != nil {
		t.Fatalf("create recovery worker failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Start(ctx) }()
	select {
	case <-service.called:
	case <-time.After(time.Second):
		t.Fatal("initial recovery scan did not run")
	}
	time.Sleep(25 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("recovery worker stop failed: %v", err)
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.callbackCalls < 2 || service.resumeCalls < 2 {
		t.Fatalf("recovery scans callback=%d resume=%d want at least 2", service.callbackCalls, service.resumeCalls)
	}
}

func TestRunGroupCancelsRemainingWorkers(t *testing.T) {
	failure := errors.New("consumer stopped")
	cancelled := make(chan struct{})
	err := RunGroup(context.Background(),
		func(context.Context) error { return failure },
		func(ctx context.Context) error {
			<-ctx.Done()
			close(cancelled)
			return ctx.Err()
		},
	)
	if !errors.Is(err, failure) {
		t.Fatalf("run group error=%v want=%v", err, failure)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("run group did not cancel remaining worker")
	}
}
