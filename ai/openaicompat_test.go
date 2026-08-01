package ai

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseOpenAICompatibleSSEStream(t *testing.T) {
	raw := strings.Join([]string{
		"data: {\"choices\":[{\"delta\":{\"content\":\"你\"}}]}",
		"",
		"data: {\"choices\":[{\"delta\":{\"content\":\"好\"}}]}",
		"",
		"data: [DONE]",
		"",
	}, "\n")

	out := make(chan string, 4)
	err := ParseOpenAICompatibleSSEStream(context.Background(), strings.NewReader(raw), out)
	close(out)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}

	var got strings.Builder
	for s := range out {
		got.WriteString(s)
	}
	if got.String() != "你好" {
		t.Fatalf("unexpected content: %q", got.String())
	}
}

func TestParseOpenAICompatibleSSEStreamMissingDone(t *testing.T) {
	raw := "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"
	out := make(chan string, 1)
	err := ParseOpenAICompatibleSSEStream(context.Background(), strings.NewReader(raw), out)
	if !errors.Is(err, ErrStreamInterrupted) {
		t.Fatalf("expected ErrStreamInterrupted, got %v", err)
	}
}

func TestOpenAICompatProviderChat(t *testing.T) {
	var authHeader string
	var requestPath string
	var requestBody string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader = r.Header.Get("Authorization")
		requestPath = r.URL.Path
		bs, _ := io.ReadAll(r.Body)
		requestBody = string(bs)

		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer srv.Close()

	provider, err := NewOpenAICompatProvider(ProviderProfile{
		Name:         "deepseek",
		Driver:       DriverOpenAICompatible,
		BaseURL:      srv.URL,
		APIKey:       "secret",
		DefaultModel: "deepseek-chat",
		EndpointPath: "/v1/chat/completions",
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}

	stream, err := provider.Chat(context.Background(), ChatRequest{
		Messages: []Message{{Role: "user", Content: "hello"}},
		Stream:   true,
	})
	if err != nil {
		t.Fatalf("chat failed: %v", err)
	}

	var got strings.Builder
	for c := range stream.Chunks {
		got.WriteString(c)
	}
	for e := range stream.Errs {
		if e != nil {
			t.Fatalf("stream err: %v", e)
		}
	}

	if got.String() != "ok" {
		t.Fatalf("unexpected stream content: %q", got.String())
	}
	if authHeader != "Bearer secret" {
		t.Fatalf("unexpected auth header: %q", authHeader)
	}
	if requestPath != "/v1/chat/completions" {
		t.Fatalf("unexpected path: %q", requestPath)
	}
	if !strings.Contains(requestBody, "\"model\":\"deepseek-chat\"") {
		t.Fatalf("default model not applied, body: %s", requestBody)
	}
}

type providerRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f providerRoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestOpenAICompatProviderUsesInjectedHTTPClient(t *testing.T) {
	var calls int
	client := &http.Client{Transport: providerRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")),
			Request:    req,
		}, nil
	})}
	provider, err := NewOpenAICompatProvider(ProviderProfile{
		Name:         "deepseek",
		Driver:       DriverOpenAICompatible,
		BaseURL:      "https://provider.example",
		APIKey:       "secret",
		DefaultModel: "deepseek-chat",
		EndpointPath: "/v1/chat/completions",
		HTTPClient:   client,
	})
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	stream, err := provider.Chat(context.Background(), ChatRequest{Messages: []Message{{Role: "user", Content: "hello"}}})
	if err != nil {
		t.Fatalf("chat failed: %v", err)
	}
	for range stream.Chunks {
	}
	for err := range stream.Errs {
		if err != nil {
			t.Fatalf("stream failed: %v", err)
		}
	}
	if calls != 1 {
		t.Fatalf("injected HTTP client calls = %d, want 1", calls)
	}
}
