package services

import (
	"GoAI/ai"
	"GoAI/config"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func resetProviderRegistryForTest() {
	ResetProviderRegistryForTest()
}

func TestChatUsesDefaultProvider(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer srv.Close()

	config.AppConfig = &config.Config{
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
	resetProviderRegistryForTest()

	stream, err := Chat(context.Background(), []ai.Message{{Role: "user", Content: "hello"}}, "", "")
	if err != nil {
		t.Fatalf("chat should succeed: %v", err)
	}
	// 消费通道确保完整执行
	for range stream.Chunks {
	}
	for e := range stream.Errs {
		if e != nil {
			t.Fatalf("unexpected stream err: %v", e)
		}
	}
}

func TestChatUnknownProvider(t *testing.T) {
	config.AppConfig = &config.Config{
		ModelProviderDefault: "deepseek",
		ModelProviders: map[string]config.ModelProviderConfig{
			"deepseek": {
				Driver:       ai.DriverOpenAICompatible,
				BaseURL:      "https://example.com",
				APIKey:       "test-key",
				DefaultModel: "deepseek-chat",
				EndpointPath: "/chat/completions",
			},
		},
	}
	resetProviderRegistryForTest()

	_, err := Chat(context.Background(), []ai.Message{{Role: "user", Content: "hello"}}, "missing", "")
	if !errors.Is(err, ai.ErrProviderNotFound) {
		t.Fatalf("expected ErrProviderNotFound, got %v", err)
	}
}
