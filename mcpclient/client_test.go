package mcpclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"GoAI/requestctx"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type mapResolver map[string]string

func (r mapResolver) Resolve(_ context.Context, ref string) ([]byte, error) {
	secret, ok := r[ref]
	if !ok {
		return nil, errors.New("missing credential")
	}
	return []byte(secret), nil
}

type leakingResolver struct{}

func (leakingResolver) Resolve(context.Context, string) ([]byte, error) {
	return nil, errors.New("resolver accidentally included top-secret-value")
}

type echoInput struct {
	Text string `json:"text" jsonschema:"required"`
}

type echoOutput struct {
	Echo string `json:"echo"`
}

func TestClientDiscoverAndCallToolOverStreamableHTTP(t *testing.T) {
	server := mcp.NewServer(&mcp.Implementation{Name: "test-server", Version: "v1"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "echo", Description: "Echo input"}, func(_ context.Context, _ *mcp.CallToolRequest, input echoInput) (*mcp.CallToolResult, echoOutput, error) {
		return nil, echoOutput{Echo: input.Text}, nil
	})

	var mu sync.Mutex
	var authorization string
	var traceID string
	var deleteCount int
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authorization = r.Header.Get("Authorization")
		traceID = r.Header.Get(requestctx.TraceIDHeader)
		if r.Method == http.MethodDelete {
			deleteCount++
		}
		mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	defer httpServer.Close()

	client := New(httpServer.Client(), mapResolver{"mcp-test": "secret-token"})
	ctx := requestctx.WithTraceID(context.Background(), "trace-mcp-1")
	config := ServerConfig{Endpoint: httpServer.URL, AuthType: AuthTypeBearer, CredentialRef: "mcp-test"}
	tools, err := client.Discover(ctx, config)
	if err != nil {
		t.Fatalf("discover tools failed: %v", err)
	}
	if len(tools) != 1 || tools[0].Name != "echo" || tools[0].Description != "Echo input" {
		t.Fatalf("unexpected discovered tools: %#v", tools)
	}
	if !strings.Contains(tools[0].InputSchemaJSON, `"text"`) {
		t.Fatalf("expected input schema snapshot, got %s", tools[0].InputSchemaJSON)
	}

	result, err := client.CallTool(ctx, config, "echo", map[string]any{"text": "hello"})
	if err != nil {
		t.Fatalf("call tool failed: %v", err)
	}
	structured, ok := result.StructuredContent.(map[string]any)
	if !ok || structured["echo"] != "hello" {
		t.Fatalf("unexpected structured result: %#v", result)
	}

	mu.Lock()
	defer mu.Unlock()
	if authorization != "Bearer secret-token" {
		t.Fatalf("authorization header mismatch: %q", authorization)
	}
	if traceID != "trace-mcp-1" {
		t.Fatalf("trace header mismatch: %q", traceID)
	}
	if deleteCount != 2 {
		t.Fatalf("expected every MCP session to be closed, got %d DELETE requests", deleteCount)
	}
}

func TestClientMapsToolReportedError(t *testing.T) {
	server := mcp.NewServer(&mcp.Implementation{Name: "test-server", Version: "v1"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "fail"}, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "downstream rejected request"}}}, nil, nil
	})
	httpServer := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil))
	defer httpServer.Close()

	_, err := New(httpServer.Client(), nil).CallTool(context.Background(), ServerConfig{Endpoint: httpServer.URL, AuthType: AuthTypeNone}, "fail", nil)
	if !errors.Is(err, ErrToolReportedError) {
		t.Fatalf("expected tool reported error, got %v", err)
	}
	if strings.Contains(err.Error(), "downstream rejected request") {
		t.Fatalf("tool error content must not leak through stable error: %v", err)
	}
}

func TestClientPropagatesCancellation(t *testing.T) {
	server := mcp.NewServer(&mcp.Implementation{Name: "test-server", Version: "v1"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "wait"}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	})
	httpServer := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil))
	defer httpServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := New(httpServer.Client(), nil).CallTool(ctx, ServerConfig{Endpoint: httpServer.URL, AuthType: AuthTypeNone}, "wait", nil)
	if !errors.Is(err, ErrTransportFailed) {
		t.Fatalf("expected transport error for timeout, got %v", err)
	}
}

func TestClientRejectsInvalidConfigAndHidesCredentialErrors(t *testing.T) {
	client := New(nil, leakingResolver{})
	for _, test := range []struct {
		name   string
		config ServerConfig
		want   error
	}{
		{name: "missing endpoint", config: ServerConfig{AuthType: AuthTypeNone}, want: ErrInvalidConfig},
		{name: "unknown auth", config: ServerConfig{Endpoint: "https://example.com", AuthType: "basic"}, want: ErrInvalidConfig},
		{name: "none with credential", config: ServerConfig{Endpoint: "https://example.com", AuthType: AuthTypeNone, CredentialRef: "secret"}, want: ErrInvalidConfig},
		{name: "missing bearer credential", config: ServerConfig{Endpoint: "https://example.com", AuthType: AuthTypeBearer}, want: ErrCredentialNotFound},
		{name: "resolver failure", config: ServerConfig{Endpoint: "https://example.com", AuthType: AuthTypeBearer, CredentialRef: "missing"}, want: ErrCredentialNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.Discover(context.Background(), test.config)
			if !errors.Is(err, test.want) {
				t.Fatalf("expected %v, got %v", test.want, err)
			}
			if err != nil && strings.Contains(err.Error(), "top-secret-value") {
				t.Fatalf("credential resolver details leaked: %v", err)
			}
		})
	}
}

func TestClientMapsTransportFailure(t *testing.T) {
	client := New(&http.Client{Timeout: 100 * time.Millisecond}, nil)
	_, err := client.Discover(context.Background(), ServerConfig{Endpoint: "http://127.0.0.1:1", AuthType: AuthTypeNone})
	if !errors.Is(err, ErrProtocolFailed) {
		t.Fatalf("expected protocol failure, got %v", err)
	}
}
