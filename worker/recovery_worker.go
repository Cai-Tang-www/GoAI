package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

type recoveryService interface {
	RecoverPendingCallbacks(context.Context, int) error
	RecoverPendingResumes(context.Context, int) error
}

// RecoveryWorker 周期扫描可恢复的 A2A callback 与 Parent Run resume 记录。
type RecoveryWorker struct {
	service   recoveryService
	interval  time.Duration
	batchSize int
	logger    *log.Logger
}

// NewRecoveryWorker 使用显式恢复服务、扫描间隔和批量大小构造恢复 Worker。
func NewRecoveryWorker(service recoveryService, interval time.Duration, batchSize int) (*RecoveryWorker, error) {
	if service == nil {
		return nil, errors.New("creating recovery worker: service is nil")
	}
	if interval <= 0 {
		return nil, errors.New("creating recovery worker: interval must be positive")
	}
	if batchSize <= 0 {
		return nil, errors.New("creating recovery worker: batch size must be positive")
	}
	return &RecoveryWorker{service: service, interval: interval, batchSize: batchSize, logger: log.Default()}, nil
}

// Start 立即执行一次恢复扫描，之后按固定周期运行直到 context 取消。
func (w *RecoveryWorker) Start(ctx context.Context) error {
	if w == nil || w.service == nil {
		return errors.New("starting recovery worker: worker is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	w.recover(ctx)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.recover(ctx)
		}
	}
}

func (w *RecoveryWorker) recover(ctx context.Context) {
	logger := w.logger
	if logger == nil {
		logger = log.Default()
	}
	if err := w.service.RecoverPendingCallbacks(ctx, w.batchSize); err != nil {
		logger.Printf("A2A callback recovery failed err=%v", err)
	}
	if err := w.service.RecoverPendingResumes(ctx, w.batchSize); err != nil {
		logger.Printf("run resume recovery failed err=%v", err)
	}
}

// RunGroup 并发运行多个后台入口；任一入口提前退出时取消其余入口并汇总错误。
func RunGroup(ctx context.Context, runners ...func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(runners) == 0 {
		return errors.New("running worker group: no runners configured")
	}
	groupCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(runners))
	started := 0
	for _, runner := range runners {
		if runner == nil {
			continue
		}
		started++
		go func(run func(context.Context) error) {
			results <- run(groupCtx)
		}(runner)
	}
	if started == 0 {
		return errors.New("running worker group: no runnable workers configured")
	}
	firstErr := <-results
	cancel()
	joined := firstErr
	for range started - 1 {
		if err := <-results; err != nil && !errors.Is(err, context.Canceled) {
			joined = errors.Join(joined, err)
		}
	}
	if joined != nil {
		return fmt.Errorf("running worker group: %w", joined)
	}
	return nil
}
