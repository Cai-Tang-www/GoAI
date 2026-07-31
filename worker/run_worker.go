package worker

import (
	"context"
	"fmt"
	"log"

	"GoAI/kafka"
	"GoAI/requestctx"
	"GoAI/services"
)

// RunWorker 将 Kafka Run 消息分发给运行时服务。
type RunWorker struct {
	service *services.RunService
}

// NewRunWorker 使用显式 RunService 构造异步执行入口。
func NewRunWorker(service *services.RunService) (*RunWorker, error) {
	if service == nil {
		return nil, fmt.Errorf("creating run worker: run service is nil")
	}
	return &RunWorker{service: service}, nil
}

// HandleRunExecuteMessage 处理一个 Kafka Run 执行事件。
func (w *RunWorker) HandleRunExecuteMessage(ctx context.Context, msg kafka.RunExecuteMessage) error {
	if err := w.service.HandleRunExecute(ctx, msg.RunID); err != nil {
		log.Printf("run worker execute failed trace_id=%s run_id=%s err=%v", requestctx.TraceIDFromContext(ctx), msg.RunID, err)
		return err
	}
	return nil
}
