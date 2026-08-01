package observability

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"GoAI/requestctx"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// HTTPMiddleware 记录 HTTP 指标、结构化访问日志和服务端 OTel span。
func HTTPMiddleware(bundle *Bundle) gin.HandlerFunc {
	return func(c *gin.Context) {
		startedAt := time.Now()
		ctx := c.Request.Context()
		traceID := requestctx.TraceIDFromContext(ctx)
		route := c.FullPath()
		if route == "" {
			route = c.Request.URL.Path
		}
		spanName := c.Request.Method + " " + route
		if bundle != nil && bundle.Tracer != nil {
			ctx, span := bundle.Tracer.Start(ctx, spanName,
				attribute.String("http.method", c.Request.Method),
				attribute.String("http.route", route),
				attribute.String("goai.trace_id", traceID),
			)
			defer span.End()
			c.Request = c.Request.WithContext(ctx)
		}

		c.Next()

		elapsed := time.Since(startedAt)
		status := c.Writer.Status()
		if bundle == nil {
			return
		}
		if bundle.Metrics != nil {
			bundle.Metrics.ObserveHTTPRequest(c.Request.Method, route, status, elapsed)
		}
		if bundle.Logger != nil {
			bundle.Logger.InfoContext(c.Request.Context(), "http request",
				slog.String("trace_id", traceID),
				slog.String("method", c.Request.Method),
				slog.String("path", c.Request.URL.Path),
				slog.String("route", route),
				slog.Int("status", status),
				slog.Int64("latency_ms", elapsed.Milliseconds()),
			)
		}
	}
}

// MarkSpanError 将业务错误写入当前 OTel span。
func MarkSpanError(span oteltrace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// ContextWithTraceID 为异步入口把 GoAI trace_id 映射到 OTel parent context。
func ContextWithTraceID(ctx context.Context, traceID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	id, err := traceIDForOTel(traceID)
	if err != nil {
		return ctx
	}
	parent := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    id,
		SpanID:     spanIDForTraceID(traceID),
		TraceFlags: oteltrace.FlagsSampled,
		Remote:     true,
	})
	return oteltrace.ContextWithRemoteSpanContext(ctx, parent)
}

func spanIDForTraceID(traceID string) oteltrace.SpanID {
	sum := sha256.Sum256([]byte("goai-span:" + strings.TrimSpace(traceID)))
	var id oteltrace.SpanID
	copy(id[:], sum[:8])
	return id
}
func traceIDForOTel(traceID string) (oteltrace.TraceID, error) {
	traceID = strings.TrimSpace(traceID)
	if traceID == "" {
		return oteltrace.TraceID{}, fmt.Errorf("trace id is empty")
	}
	if id, err := oteltrace.TraceIDFromHex(traceID); err == nil {
		return id, nil
	}
	// GoAI trace IDs are opaque and historically shorter than OTel TraceIDs.
	// Hashing keeps a stable parent relationship without changing the public ID.
	sum := sha256.Sum256([]byte(traceID))
	var id oteltrace.TraceID
	copy(id[:], sum[:16])
	return id, nil
}

// SpanAttributes 提供常用业务键，避免各入口重复拼写属性名。
func SpanAttributes(traceID, runID, threadID, delegationID string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String("goai.trace_id", traceID)}
	if runID != "" {
		attrs = append(attrs, attribute.String("goai.run_id", runID))
	}
	if threadID != "" {
		attrs = append(attrs, attribute.String("goai.thread_id", threadID))
	}
	if delegationID != "" {
		attrs = append(attrs, attribute.String("goai.delegation_id", delegationID))
	}
	return attrs
}
