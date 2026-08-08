package handlers_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	"GoAI/mcpclient"
	"GoAI/middlewares"
	"GoAI/models"
)

type mcpHandlerProtocolClient struct {
	mu      sync.Mutex
	tools   []mcpclient.Tool
	err     error
	configs []mcpclient.ServerConfig
}

func (c *mcpHandlerProtocolClient) Discover(_ context.Context, config mcpclient.ServerConfig) ([]mcpclient.Tool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.configs = append(c.configs, config)
	return append([]mcpclient.Tool(nil), c.tools...), c.err
}

func (c *mcpHandlerProtocolClient) lastConfig(t *testing.T) mcpclient.ServerConfig {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.configs) == 0 {
		t.Fatal("expected MCP discovery call")
	}
	return c.configs[len(c.configs)-1]
}

func setupMCPRegistryHTTPFixture(t *testing.T, client *mcpHandlerProtocolClient) registryHTTPFixture {
	t.Helper()
	fixture := setupRegistryHTTPFixture(t)
	fixture.router = newTestRouterWithRegistryAndMCPClient(t, fixture.database, nil, fixture.checker, client)
	return fixture
}

func TestMCPRegistryAPICompleteLifecycleAndOwnership(t *testing.T) {
	client := &mcpHandlerProtocolClient{tools: []mcpclient.Tool{
		{Name: "search", Description: "Search documents", InputSchemaJSON: `{"type":"object","required":["query"],"properties":{"query":{"type":"string"}}}`, OutputSchemaJSON: `{"type":"object"}`},
	}}
	fixture := setupMCPRegistryHTTPFixture(t, client)

	create := registryRequest(t, fixture.router, http.MethodPost, "/api/mcp/servers", fixture.ownerToken, map[string]any{
		"server_code": "docs", "name": "Docs", "transport": "streamable_http",
		"endpoint": "http://127.0.0.1:8090/mcp", "auth_type": "bearer", "credential_ref": "env:MCP_DOCS_TOKEN",
	})
	createEnvelope := requireRegistryResponse(t, create, http.StatusCreated, middlewares.CodeOK)
	if !bytes.Contains(createEnvelope.Data, []byte(`"status":"inactive"`)) {
		t.Fatalf("new MCP server should be inactive: %s", create.Body.String())
	}

	otherRead := registryRequest(t, fixture.router, http.MethodGet, "/api/mcp/servers/docs", fixture.otherToken, nil)
	requireRegistryResponse(t, otherRead, http.StatusNotFound, middlewares.CodeMCPServerNotFound)
	adminRead := registryRequest(t, fixture.router, http.MethodGet, "/api/mcp/servers/docs", fixture.adminToken, nil)
	requireRegistryResponse(t, adminRead, http.StatusOK, middlewares.CodeOK)

	health := registryRequest(t, fixture.router, http.MethodPost, "/api/mcp/servers/docs/health-check", fixture.ownerToken, nil)
	healthEnvelope := requireRegistryResponse(t, health, http.StatusOK, middlewares.CodeOK)
	if !bytes.Contains(healthEnvelope.Data, []byte(`"status":"active"`)) {
		t.Fatalf("successful discovery should activate MCP server: %s", health.Body.String())
	}
	config := client.lastConfig(t)
	if config.Endpoint != "http://127.0.0.1:8090/mcp" || config.AuthType != "bearer" || config.CredentialRef != "env:MCP_DOCS_TOKEN" {
		t.Fatalf("unexpected discovery config: %+v", config)
	}

	tools := registryRequest(t, fixture.router, http.MethodGet, "/api/mcp/servers/docs/tools", fixture.ownerToken, nil)
	toolsEnvelope := requireRegistryResponse(t, tools, http.StatusOK, middlewares.CodeOK)
	if !bytes.Contains(toolsEnvelope.Data, []byte(`"tool_name":"search"`)) {
		t.Fatalf("discovered tool missing from API: %s", tools.Body.String())
	}

	update := registryRequest(t, fixture.router, http.MethodPut, "/api/mcp/servers/docs", fixture.ownerToken, map[string]any{
		"name": "Docs v2", "transport": "streamable_http", "endpoint": "http://localhost:8091/mcp",
		"auth_type": "none",
	})
	updateEnvelope := requireRegistryResponse(t, update, http.StatusOK, middlewares.CodeOK)
	if !bytes.Contains(updateEnvelope.Data, []byte(`"status":"inactive"`)) || !bytes.Contains(updateEnvelope.Data, []byte(`"config_version":2`)) {
		t.Fatalf("protocol update should invalidate health state: %s", update.Body.String())
	}
	emptyTools := registryRequest(t, fixture.router, http.MethodGet, "/api/mcp/servers/docs/tools", fixture.ownerToken, nil)
	emptyEnvelope := requireRegistryResponse(t, emptyTools, http.StatusOK, middlewares.CodeOK)
	if string(emptyEnvelope.Data) != "[]" {
		t.Fatalf("protocol update should clear tool snapshot: %s", emptyTools.Body.String())
	}

	deactivate := registryRequest(t, fixture.router, http.MethodPost, "/api/mcp/servers/docs/deactivate", fixture.ownerToken, nil)
	deactivateEnvelope := requireRegistryResponse(t, deactivate, http.StatusOK, middlewares.CodeOK)
	if !bytes.Contains(deactivateEnvelope.Data, []byte(`"status":"inactive"`)) {
		t.Fatalf("deactivate response should be inactive: %s", deactivate.Body.String())
	}
}

func TestMCPRegistryAPIRejectsSecretsUnknownFieldsAndDuplicateCodes(t *testing.T) {
	fixture := setupMCPRegistryHTTPFixture(t, &mcpHandlerProtocolClient{})
	secret := "do-not-persist-this-secret"
	invalid := registryRequest(t, fixture.router, http.MethodPost, "/api/mcp/servers", fixture.ownerToken,
		`{"server_code":"docs","name":"Docs","endpoint":"http://127.0.0.1:8090/mcp","token":"`+secret+`"}`)
	requireRegistryResponse(t, invalid, http.StatusBadRequest, middlewares.CodeValidationFailed)
	if bytes.Contains(invalid.Body.Bytes(), []byte(secret)) {
		t.Fatalf("rejected secret leaked into response: %s", invalid.Body.String())
	}

	validBody := map[string]any{
		"server_code": "docs", "name": "Docs", "endpoint": "http://127.0.0.1:8090/mcp", "auth_type": "none",
	}
	first := registryRequest(t, fixture.router, http.MethodPost, "/api/mcp/servers", fixture.ownerToken, validBody)
	requireRegistryResponse(t, first, http.StatusCreated, middlewares.CodeOK)
	duplicate := registryRequest(t, fixture.router, http.MethodPost, "/api/mcp/servers", fixture.ownerToken, validBody)
	requireRegistryResponse(t, duplicate, http.StatusConflict, middlewares.CodeMCPServerAlreadyExists)

	var count int64
	if err := fixture.database.Model(&models.MCPServer{}).Where("server_code = ?", "docs").Count(&count).Error; err != nil {
		t.Fatalf("count MCP servers: %v", err)
	}
	if count != 1 {
		t.Fatalf("duplicate create persisted %d servers", count)
	}
}

func TestMCPRegistryAPIMapsHealthErrorsWithoutLeakingDetails(t *testing.T) {
	client := &mcpHandlerProtocolClient{err: errors.New("should be replaced")}
	client.err = errors.Join(mcpclient.ErrCredentialNotFound, client.err)
	fixture := setupMCPRegistryHTTPFixture(t, client)
	create := registryRequest(t, fixture.router, http.MethodPost, "/api/mcp/servers", fixture.ownerToken, map[string]any{
		"server_code": "secure", "name": "Secure", "endpoint": "https://tools.example.com/mcp",
		"auth_type": "bearer", "credential_ref": "vault:mcp/secure",
	})
	requireRegistryResponse(t, create, http.StatusCreated, middlewares.CodeOK)

	health := registryRequest(t, fixture.router, http.MethodPost, "/api/mcp/servers/secure/health-check", fixture.ownerToken, nil)
	requireRegistryResponse(t, health, http.StatusServiceUnavailable, middlewares.CodeMCPCredentialNotFound)
	if bytes.Contains(health.Body.Bytes(), []byte("should be replaced")) {
		t.Fatalf("health response leaked downstream detail: %s", health.Body.String())
	}
	var server models.MCPServer
	if err := fixture.database.Where("server_code = ?", "secure").First(&server).Error; err != nil {
		t.Fatalf("load unhealthy MCP server: %v", err)
	}
	if server.Status != models.MCPServerStatusUnhealthy || server.LastError != "credential unavailable" {
		t.Fatalf("unexpected sanitized health state: %+v", server)
	}

	var storedSecret string
	if err := fixture.database.Model(&models.MCPServer{}).Select("credential_ref").Where("server_code = ?", "secure").Scan(&storedSecret).Error; err != nil {
		t.Fatalf("read credential reference: %v", err)
	}
	if storedSecret != "vault:mcp/secure" {
		t.Fatalf("unexpected persisted credential reference: %q", storedSecret)
	}
}
