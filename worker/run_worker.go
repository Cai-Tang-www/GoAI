package worker

import (
	"GoAI/kafka"
	"GoAI/services"
	"context"
	"log"
)

func RegisterKafkaRunWorker() {
	kafka.RegisterRunMessageHandler(HandleRunExecuteMessage)
}

func HandleRunExecuteMessage(ctx context.Context, msg kafka.RunExecuteMessage) error {
	if err := services.HandleRunExecuteMessage(ctx, msg); err != nil {
		log.Printf("run worker execute failed (run_id=%s): %v", msg.RunID, err)
		return err
	}
	return nil
}
