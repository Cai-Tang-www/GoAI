package observability

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestContextWithTraceIDCreatesValidRemoteParent(t *testing.T) {
	ctx := ContextWithTraceID(context.Background(), "trace-worker-1")
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		t.Fatal("expected a valid OTel parent span context")
	}
	if !spanContext.IsRemote() {
		t.Fatal("expected the parent span context to be remote")
	}
	if !spanContext.IsSampled() {
		t.Fatal("expected the parent span context to be sampled")
	}

	other := ContextWithTraceID(context.Background(), "trace-worker-1")
	otherSpanContext := trace.SpanContextFromContext(other)
	if spanContext.TraceID() != otherSpanContext.TraceID() || spanContext.SpanID() != otherSpanContext.SpanID() {
		t.Fatal("expected stable OTel parent identifiers for the same GoAI trace_id")
	}
}

func TestContextWithTraceIDLeavesContextUnchangedForEmptyTraceID(t *testing.T) {
	ctx := context.WithValue(context.Background(), "marker", "value")
	withTrace := ContextWithTraceID(ctx, " ")
	if got := withTrace.Value("marker"); got != "value" {
		t.Fatalf("context marker was not preserved: %v", got)
	}
	if spanContext := trace.SpanContextFromContext(withTrace); spanContext.IsValid() {
		t.Fatal("expected no parent span context for an empty trace_id")
	}
}
