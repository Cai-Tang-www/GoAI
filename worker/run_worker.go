package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"GoAI/kafka"
	"GoAI/requestctx"
	"GoAI/services"
)

type delegationReconciler interface {
	ReconcileDelegation(context.Context, string) error
}

// RunWorker 将 Kafka Run 消息分发给运行时服务，并在终态后收敛多 Agent 委派结果。
type RunWorker struct {
	service    *services.RunService
	reconciler delegationReconciler
}

// NewRunWorker 使用显式 RunService 与委派协调器构造异步执行入口。
func NewRunWorker(service *services.RunService, reconciler delegationReconciler) (*RunWorker, error) {
	if service == nil {
		return nil, fmt.Errorf("creating run worker: run service is nil")
	}
	if reconciler == nil {
		return nil, fmt.Errorf("creating run worker: delegation reconciler is nil")
	}
	return &RunWorker{service: service, reconciler: reconciler}, nil
}

// HandleRunExecuteMessage 处理一个 Kafka Run 首次执行事件，并保证 Delegation 与 Child Run 终态一致。
func (w *RunWorker) HandleRunExecuteMessage(ctx context.Context, msg kafka.RunExecuteMessage) error {
	return w.executeAndReconcile(ctx, msg.RunID, func() error {
		return w.service.HandleRunExecute(ctx, msg.RunID)
	})
}

// HandleRunResumeMessage 处理一个 Kafka Parent Run 恢复事件，并从已持久化游标继续执行。
func (w *RunWorker) HandleRunResumeMessage(ctx context.Context, msg kafka.RunResumeMessage) error {
	return w.executeAndReconcile(ctx, msg.RunID, func() error {
		return w.service.HandleRunResume(ctx, msg.RunID, msg.DelegationID)
	})
}

func (w *RunWorker) executeAndReconcile(ctx context.Context, runID string, execute func() error) error {
	executeErr := execute()
	if executeErr != nil {
		log.Printf("run worker execute failed trace_id=%s run_id=%s err=%v", requestctx.TraceIDFromContext(ctx), runID, executeErr)
	}
	reconcileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	reconcileErr := w.reconciler.ReconcileDelegation(reconcileCtx, runID)
	if reconcileErr != nil {
		log.Printf("run worker delegation reconcile failed trace_id=%s run_id=%s err=%v", requestctx.TraceIDFromContext(ctx), runID, reconcileErr)
	}
	return errors.Join(executeErr, reconcileErr)
}
