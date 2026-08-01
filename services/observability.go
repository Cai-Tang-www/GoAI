package services

import (
	"context"
	"log/slog"
	"time"

	"GoAI/observability"
	"GoAI/requestctx"

	oteltrace "go.opentelemetry.io/otel/trace"
)

// startServiceObservation starts a short-lived service span when telemetry is configured.
func startServiceObservation(ctx context.Context, bundle *observability.Bundle, name, runID, threadID, delegationID string) (context.Context, oteltrace.Span, time.Time) {
	if ctx == nil {
		ctx = context.Background()
	}
	startedAt := time.Now()
	if bundle == nil || bundle.Tracer == nil {
		return ctx, nil, startedAt
	}
	ctx, span := bundle.Tracer.Start(ctx, name, observability.SpanAttributes(
		requestctx.TraceIDFromContext(ctx), runID, threadID, delegationID,
	)...)
	return ctx, span, startedAt
}

// finishServiceObservation records one service operation without exposing business data.
func finishServiceObservation(
	bundle *observability.Bundle,
	ctx context.Context,
	span oteltrace.Span,
	operation, status string,
	startedAt time.Time,
	runID, threadID, delegationID string,
	observe func(*observability.Metrics, string, string, time.Duration),
	err error,
	extraAttrs ...any,
) {
	if err != nil && span != nil {
		observability.MarkSpanError(span, err)
	}
	if span != nil {
		span.End()
	}
	if bundle == nil {
		return
	}
	latency := time.Since(startedAt)
	if bundle.Metrics != nil && observe != nil {
		observe(bundle.Metrics, operation, status, latency)
	}
	if bundle.Logger == nil {
		return
	}
	attrs := []any{
		slog.String("trace_id", requestctx.TraceIDFromContext(ctx)),
		slog.String("operation", operation),
		slog.String("status", status),
		slog.Int64("latency_ms", latency.Milliseconds()),
	}
	if runID != "" {
		attrs = append(attrs, slog.String("run_id", runID))
	}
	if threadID != "" {
		attrs = append(attrs, slog.String("thread_id", threadID))
	}
	if delegationID != "" {
		attrs = append(attrs, slog.String("delegation_id", delegationID))
	}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
	}
	attrs = append(attrs, extraAttrs...)
	bundle.Logger.InfoContext(ctx, "runtime operation", attrs...)
}
