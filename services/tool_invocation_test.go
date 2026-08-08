package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"GoAI/mcpclient"
	"GoAI/models"
)

type fakeMCPToolClient struct {
	config    mcpclient.ServerConfig
	toolName  string
	arguments map[string]any
	result    *mcpclient.CallResult
	err       error
}

func (f *fakeMCPToolClient) CallTool(_ context.Context, config mcpclient.ServerConfig, toolName string, arguments map[string]any) (*mcpclient.CallResult, error) {
	f.config = config
	f.toolName = toolName
	f.arguments = arguments
	return f.result, f.err
}

func TestMCPToolInvokerUsesOwnedActiveSnapshotAndValidatesInput(t *testing.T) {
	database, _ := setupMCPRegistryTest(t, &fakeMCPProtocolClient{})
	server := models.MCPServer{
		OwnerUserID: 11, ServerCode: "docs", Name: "Docs", Transport: models.MCPServerTransportStreamableHTTP,
		Endpoint: "https://tools.example.com/mcp", AuthType: models.MCPServerAuthTypeBearer,
		CredentialRef: "vault://docs", Status: models.MCPServerStatusActive, ConfigVersion: 1,
	}
	if err := database.Create(&server).Error; err != nil {
		t.Fatalf("create server: %v", err)
	}
	tool := models.MCPTool{
		ServerID: server.ID, ToolName: "search",
		InputSchemaJSON: `{"type":"object","required":["query"],"properties":{"query":{"type":"string"}},"additionalProperties":false}`,
	}
	if err := database.Create(&tool).Error; err != nil {
		t.Fatalf("create tool: %v", err)
	}
	client := &fakeMCPToolClient{result: &mcpclient.CallResult{Content: []any{map[string]any{"text": "ok"}}}}
	invoker, err := NewMCPToolInvoker(database, client)
	if err != nil {
		t.Fatalf("create invoker: %v", err)
	}
	result, err := invoker.Invoke(context.Background(), ToolInvocationRequest{
		OwnerUserID: 11, ServerCode: "docs", ToolName: "search", Arguments: map[string]any{"query": "GoAI"},
	})
	if err != nil {
		t.Fatalf("invoke tool: %v", err)
	}
	if result == nil || client.toolName != "search" || client.config.CredentialRef != "vault://docs" || client.arguments["query"] != "GoAI" {
		t.Fatalf("unexpected invocation: result=%+v client=%+v", result, client)
	}
	if _, err := invoker.Invoke(context.Background(), ToolInvocationRequest{
		OwnerUserID: 11, ServerCode: "docs", ToolName: "search", Arguments: map[string]any{"unexpected": true},
	}); !errors.Is(err, ErrMCPRegistryValidation()) {
		t.Fatalf("expected schema validation error, got %v", err)
	}
	if _, err := invoker.Invoke(context.Background(), ToolInvocationRequest{
		OwnerUserID: 22, ServerCode: "docs", ToolName: "search", Arguments: map[string]any{"query": "GoAI"},
	}); !errors.Is(err, ErrMCPServerNotFound()) {
		t.Fatalf("expected owner isolation, got %v", err)
	}
}

func TestMCPToolInvokerMapsClientFailure(t *testing.T) {
	database, _ := setupMCPRegistryTest(t, &fakeMCPProtocolClient{})
	server := models.MCPServer{
		OwnerUserID: 11, ServerCode: "docs", Name: "Docs", Transport: models.MCPServerTransportStreamableHTTP,
		Endpoint: "http://127.0.0.1:8090/mcp", AuthType: models.MCPServerAuthTypeNone,
		Status: models.MCPServerStatusActive, ConfigVersion: 1,
	}
	if err := database.Create(&server).Error; err != nil {
		t.Fatalf("create server: %v", err)
	}
	if err := database.Create(&models.MCPTool{ServerID: server.ID, ToolName: "search", InputSchemaJSON: `{"type":"object"}`}).Error; err != nil {
		t.Fatalf("create tool: %v", err)
	}
	client := &fakeMCPToolClient{err: mcpclient.ErrToolReportedError}
	invoker, _ := NewMCPToolInvoker(database, client)
	_, err := invoker.Invoke(context.Background(), ToolInvocationRequest{
		OwnerUserID: 11, ServerCode: "docs", ToolName: "search", Arguments: map[string]any{},
	})
	if !errors.Is(err, ErrMCPToolInvocationFailed()) || !errors.Is(err, mcpclient.ErrToolReportedError) {
		t.Fatalf("unexpected mapped error: %v", err)
	}
	client.err = fmt.Errorf("%w: downstream secret", mcpclient.ErrProtocolFailed)
	_, err = invoker.Invoke(context.Background(), ToolInvocationRequest{
		OwnerUserID: 11, ServerCode: "docs", ToolName: "search", Arguments: map[string]any{},
	})
	if err == nil || strings.Contains(err.Error(), "downstream secret") {
		t.Fatalf("tool error leaked downstream details: %v", err)
	}
}
