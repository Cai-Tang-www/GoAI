package middlewares

import (
	"GoAI/requestctx"
	"log"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// TraceMiddleware 为每个请求生成或透传 trace_id，并同步写回响应头与 context。
func TraceMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := strings.TrimSpace(c.GetHeader(requestctx.TraceIDHeader))
		if traceID == "" {
			traceID = requestctx.NewTraceID()
		}

		ctx := requestctx.WithTraceID(c.Request.Context(), traceID)
		c.Request = c.Request.WithContext(ctx)
		c.Set(contextTraceIDKey, traceID)
		c.Header(requestctx.TraceIDHeader, traceID)
		c.Next()
	}
}

// RequestLogMiddleware 记录最小可用的 HTTP 访问日志，确保日志与 trace_id 对齐。
func RequestLogMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		startedAt := time.Now()
		c.Next()
		latencyMS := time.Since(startedAt).Milliseconds()
		log.Printf("http request trace_id=%s method=%s path=%s status=%d latency_ms=%d",
			TraceID(c), c.Request.Method, c.Request.URL.Path, c.Writer.Status(), latencyMS)
	}
}
