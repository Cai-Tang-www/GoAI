package kafka

import (
	"GoAI/requestctx"
	"context"
	"testing"
)

// TestNewRunExecuteMessageCarriesTraceID 验证 Kafka run 消息会携带请求 trace_id。
func TestNewRunExecuteMessageCarriesTraceID(t *testing.T) {
	ctx := requestctx.WithTraceID(context.Background(), "trace-kafka-test")
	msg := newRunExecuteMessage(ctx, "run_123")
	if msg.RunID != "run_123" {
		t.Fatalf("unexpected run id: %s", msg.RunID)
	}
	if msg.TraceID != "trace-kafka-test" {
		t.Fatalf("unexpected trace id: %s", msg.TraceID)
	}
}
