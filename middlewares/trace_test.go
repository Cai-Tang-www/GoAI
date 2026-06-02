package middlewares

import (
	"GoAI/requestctx"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestTraceMiddlewareGeneratesAndEchoesTraceID 验证 trace_id 会自动生成并回写响应头。
func TestTraceMiddlewareGeneratesAndEchoesTraceID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(TraceMiddleware())
	router.GET("/trace", func(c *gin.Context) {
		traceID := TraceID(c)
		if traceID == "" {
			t.Fatal("trace id should not be empty")
		}
		if requestctx.TraceIDFromContext(c.Request.Context()) != traceID {
			t.Fatal("trace id should be stored in request context")
		}
		Success(c, http.StatusOK, gin.H{"trace_id": traceID}, "success")
	})

	req := httptest.NewRequest(http.MethodGet, "/trace", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Header().Get(requestctx.TraceIDHeader) == "" {
		t.Fatal("trace header should be set")
	}
}

// TestTraceMiddlewareUsesIncomingTraceID 验证请求头中的 trace_id 会被原样透传。
func TestTraceMiddlewareUsesIncomingTraceID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(TraceMiddleware())
	router.GET("/trace", func(c *gin.Context) {
		Success(c, http.StatusOK, nil, "success")
	})

	req := httptest.NewRequest(http.MethodGet, "/trace", nil)
	req.Header.Set(requestctx.TraceIDHeader, "trace-from-client")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if got := w.Header().Get(requestctx.TraceIDHeader); got != "trace-from-client" {
		t.Fatalf("expected echoed trace header, got %q", got)
	}
}
