package requestctx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

type contextKey string

const (
	traceIDContextKey contextKey = "trace_id"
	TraceIDHeader                = "X-Trace-ID"
)

// WithTraceID 将 trace_id 写入标准 context，供 HTTP、Kafka 和 worker 共享。
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDContextKey, traceID)
}

// TraceIDFromContext 从标准 context 中读取 trace_id。
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if traceID, ok := ctx.Value(traceIDContextKey).(string); ok {
		return traceID
	}
	return ""
}

// NewTraceID 生成新的请求链路标识。
func NewTraceID() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return "trace_fallback"
	}
	return hex.EncodeToString(buf)
}
