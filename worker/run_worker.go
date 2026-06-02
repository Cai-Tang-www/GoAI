package worker

import (
	"GoAI/kafka"
	"GoAI/requestctx"
	"GoAI/services"
	"context"
	"log"
)

func RegisterKafkaRunWorker() {
	kafka.RegisterRunMessageHandler(HandleRunExecuteMessage)
}

func HandleRunExecuteMessage(ctx context.Context, msg kafka.RunExecuteMessage) error {
	if err := services.HandleRunExecuteMessage(ctx, msg); err != nil {
		log.Printf("run worker execute failed trace_id=%s run_id=%s err=%v", requestctx.TraceIDFromContext(ctx), msg.RunID, err)
		return err
	}
	return nil
}
