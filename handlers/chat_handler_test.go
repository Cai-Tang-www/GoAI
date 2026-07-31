package handlers_test

import (
	"GoAI/ai"
	"GoAI/config"
	"GoAI/middlewares"
	"GoAI/requestctx"
	routers "GoAI/routers"
	"GoAI/services"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestChatValidationErrorUsesEnvelope 验证聊天接口前置参数错误会返回统一 JSON envelope。
func TestChatValidationErrorUsesEnvelope(t *testing.T) {
	config.AppConfig = &config.Config{
		JWTSecret:      "chat-test-secret",
		RBACEnable:     false,
		ModelProviders: map[string]config.ModelProviderConfig{},
	}
	services.ResetProviderRegistryForTest()

	token, err := middlewares.GenerateToken(1)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}

	router := routers.InitRouter()
	req := httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader("{"))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
	}
	env := decodeEnvelope(t, w)
	if env.Code != "VALIDATION_FAILED" {
		t.Fatalf("unexpected code: %s body=%s", env.Code, w.Body.String())
	}
}

// TestChatMessageValidation 验证消息为空时会被明确拦截。
func TestChatMessageValidation(t *testing.T) {
	config.AppConfig = &config.Config{
		JWTSecret:      "chat-test-secret",
		RBACEnable:     false,
		ModelProviders: map[string]config.ModelProviderConfig{},
	}
	services.ResetProviderRegistryForTest()

	token, err := middlewares.GenerateToken(1)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}

	body, _ := json.Marshal(map[string]any{"messages": []map[string]any{}})
	router := routers.InitRouter()
	req := httptest.NewRequest(http.MethodPost, "/api/chat", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
	}
	env := decodeEnvelope(t, w)
	if env.Code != "VALIDATION_FAILED" {
		t.Fatalf("unexpected code: %s body=%s", env.Code, w.Body.String())
	}
}

// TestChatSSEUsesUnifiedEnvelope 验证聊天流会输出统一 envelope 的 chunk/done 事件并回写 trace_id。
func TestChatSSEUsesUnifiedEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer srv.Close()

	config.AppConfig = &config.Config{
		JWTSecret:            "chat-test-secret",
		RBACEnable:           false,
		ModelProviderDefault: "deepseek",
		ModelProviders: map[string]config.ModelProviderConfig{
			"deepseek": {
				Driver:       ai.DriverOpenAICompatible,
				BaseURL:      srv.URL,
				APIKey:       "test-key",
				DefaultModel: "deepseek-chat",
				EndpointPath: "/chat/completions",
			},
		},
	}
	services.ResetProviderRegistryForTest()

	token, err := middlewares.GenerateToken(1)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"messages": []map[string]any{{"role": "user", "content": "hello"}},
	})
	router := routers.InitRouter()
	req := httptest.NewRequest(http.MethodPost, "/api/chat", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(requestctx.TraceIDHeader, "trace-chat-success")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get(requestctx.TraceIDHeader); got != "trace-chat-success" {
		t.Fatalf("unexpected trace header: %q", got)
	}
	bodyText := w.Body.String()
	if !strings.Contains(bodyText, "event: chunk") || !strings.Contains(bodyText, "\"trace_id\":\"trace-chat-success\"") {
		t.Fatalf("unexpected sse chunk body: %s", bodyText)
	}
	if !strings.Contains(bodyText, "event: done") || !strings.Contains(bodyText, "\"done\":true") {
		t.Fatalf("unexpected sse done body: %s", bodyText)
	}
}

// TestChatSSEStreamErrorUsesEnvelope 验证流中断时会返回统一 error 事件。
func TestChatSSEStreamErrorUsesEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"))
	}))
	defer srv.Close()

	config.AppConfig = &config.Config{
		JWTSecret:            "chat-test-secret",
		RBACEnable:           false,
		ModelProviderDefault: "deepseek",
		ModelProviders: map[string]config.ModelProviderConfig{
			"deepseek": {
				Driver:       ai.DriverOpenAICompatible,
				BaseURL:      srv.URL,
				APIKey:       "test-key",
				DefaultModel: "deepseek-chat",
				EndpointPath: "/chat/completions",
			},
		},
	}
	services.ResetProviderRegistryForTest()

	token, err := middlewares.GenerateToken(1)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"messages": []map[string]any{{"role": "user", "content": "hello"}},
	})
	router := routers.InitRouter()
	req := httptest.NewRequest(http.MethodPost, "/api/chat", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", w.Code, w.Body.String())
	}
	bodyText := w.Body.String()
	if !strings.Contains(bodyText, "event: error") || !strings.Contains(bodyText, "\"code\":\"STREAM_INTERRUPTED\"") {
		t.Fatalf("unexpected sse error body: %s", bodyText)
	}
}

// TestChatSSEStopsWhenRequestContextIsCancelled 验证客户端断开会取消上游模型流并结束 handler。
func TestChatSSEStopsWhenRequestContextIsCancelled(t *testing.T) {
	upstreamStarted := make(chan struct{})
	upstreamStopped := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(upstreamStarted)
		<-r.Context().Done()
		close(upstreamStopped)
	}))
	defer srv.Close()

	config.AppConfig = &config.Config{
		JWTSecret:            "chat-test-secret",
		RBACEnable:           false,
		ModelProviderDefault: "deepseek",
		ModelProviders: map[string]config.ModelProviderConfig{
			"deepseek": {
				Driver:       ai.DriverOpenAICompatible,
				BaseURL:      srv.URL,
				APIKey:       "test-key",
				DefaultModel: "deepseek-chat",
				EndpointPath: "/chat/completions",
			},
		},
	}
	services.ResetProviderRegistryForTest()

	token, err := middlewares.GenerateToken(1)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}
	body, _ := json.Marshal(map[string]any{
		"messages": []map[string]any{{"role": "user", "content": "hello"}},
	})
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/api/chat", bytes.NewReader(body)).WithContext(requestCtx)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		routers.InitRouter().ServeHTTP(w, req)
		close(handlerDone)
	}()

	select {
	case <-upstreamStarted:
	case <-time.After(time.Second):
		t.Fatal("provider stream did not start")
	}
	cancelRequest()

	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("chat handler did not stop after request cancellation")
	}
	select {
	case <-upstreamStopped:
	case <-time.After(time.Second):
		t.Fatal("provider request was not cancelled")
	}
}
