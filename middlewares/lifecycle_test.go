package middlewares

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestStreamShutdownMiddlewareCancelsStreamingRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	shutdownCtx, stopStreams := context.WithCancel(context.Background())
	router := gin.New()
	router.Use(StreamShutdownMiddleware(shutdownCtx))
	started := make(chan struct{})
	stopped := make(chan error, 1)
	router.GET("/stream", func(c *gin.Context) {
		c.Header("Content-Type", "text/event-stream")
		c.Status(http.StatusOK)
		close(started)
		<-c.Request.Context().Done()
		stopped <- c.Request.Context().Err()
	})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/stream", nil)
	done := make(chan struct{})
	go func() {
		router.ServeHTTP(response, request)
		close(done)
	}()
	<-started
	stopStreams()

	select {
	case err := <-stopped:
		if err != context.Canceled {
			t.Fatalf("expected stream context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream request did not receive shutdown cancellation")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream handler did not exit after cancellation")
	}
}
