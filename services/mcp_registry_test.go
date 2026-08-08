package services

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"GoAI/mcpclient"
	"GoAI/models"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type fakeMCPProtocolClient struct {
	mu        sync.Mutex
	tools     []mcpclient.Tool
	err       error
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

func (f *fakeMCPProtocolClient) Discover(ctx context.Context, _ mcpclient.ServerConfig) ([]mcpclient.Tool, error) {
	if f.started != nil {
		f.startOnce.Do(func() { close(f.started) })
	}
	if f.release != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.release:
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]mcpclient.Tool(nil), f.tools...), f.err
}

func setupMCPRegistryTest(t *testing.T, client MCPProtocolClient) (*gorm.DB, *MCPRegistryService) {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	database, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	if err := database.AutoMigrate(&models.Agent{}, &models.Workflow{}, &models.MCPServer{}, &models.MCPTool{}); err != nil {
		t.Fatalf("migrate MCP registry models: %v", err)
	}
	service, err := NewMCPRegistryService(database, client)
	if err != nil {
		t.Fatalf("create MCP registry service: %v", err)
	}
	return database, service
}

func validMCPServerCommand(code string) UpsertMCPServerCommand {
	return UpsertMCPServerCommand{
		ServerCode: code, Name: "Local Tools", Transport: models.MCPServerTransportStreamableHTTP,
		Endpoint: "http://127.0.0.1:8090/mcp", AuthType: models.MCPServerAuthTypeNone,
	}
}

func TestMCPRegistryCreateOwnershipAndValidation(t *testing.T) {
	_, service := setupMCPRegistryTest(t, &fakeMCPProtocolClient{})
	owner := MCPRegistryActor{UserID: 11}
	created, err := service.Create(context.Background(), owner, validMCPServerCommand("local-tools"))
	if err != nil {
		t.Fatalf("create MCP server: %v", err)
	}
	if created.Status != models.MCPServerStatusInactive || created.ConfigVersion != 1 {
		t.Fatalf("unexpected initial view: %+v", created)
	}
	if _, err := service.Create(context.Background(), owner, validMCPServerCommand("local-tools")); !errors.Is(err, ErrMCPServerAlreadyExists()) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
	invalid := validMCPServerCommand("remote-http")
	invalid.Endpoint = "http://example.com/mcp"
	if _, err := service.Create(context.Background(), owner, invalid); !errors.Is(err, ErrMCPRegistryValidation()) {
		t.Fatalf("expected remote HTTP validation error, got %v", err)
	}
	if _, err := service.Get(context.Background(), MCPRegistryActor{UserID: 22}, "local-tools"); !errors.Is(err, ErrMCPServerNotFound()) {
		t.Fatalf("owner isolation failed: %v", err)
	}
	if _, err := service.Get(context.Background(), MCPRegistryActor{UserID: 22, CanManage: true}, "local-tools"); err != nil {
		t.Fatalf("admin bypass failed: %v", err)
	}
}

func TestMCPRegistryHealthCheckReplacesToolSnapshot(t *testing.T) {
	client := &fakeMCPProtocolClient{tools: []mcpclient.Tool{{
		Name: "search", Description: "Search documents", InputSchemaJSON: `{"type":"object"}`,
	}}}
	database, service := setupMCPRegistryTest(t, client)
	actor := MCPRegistryActor{UserID: 11}
	if _, err := service.Create(context.Background(), actor, validMCPServerCommand("docs")); err != nil {
		t.Fatalf("create server: %v", err)
	}
	view, err := service.CheckHealth(context.Background(), actor, "docs")
	if err != nil {
		t.Fatalf("check health: %v", err)
	}
	if view.Status != models.MCPServerStatusActive || view.LastHealthyAt == nil || view.LastError != "" {
		t.Fatalf("unexpected healthy view: %+v", view)
	}
	tools, err := service.ListTools(context.Background(), actor, "docs")
	if err != nil || len(tools) != 1 || tools[0].ToolName != "search" {
		t.Fatalf("unexpected tool snapshot: %+v err=%v", tools, err)
	}
	client.mu.Lock()
	client.tools = []mcpclient.Tool{{Name: "fetch", InputSchemaJSON: `{"type":"object"}`}}
	client.mu.Unlock()
	if _, err := service.CheckHealth(context.Background(), actor, "docs"); err != nil {
		t.Fatalf("refresh health: %v", err)
	}
	var count int64
	if err := database.Model(&models.MCPTool{}).Where("tool_name = ?", "search").Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("old snapshot was not removed: count=%d err=%v", count, err)
	}
}

func TestMCPRegistryHealthCheckUsesOfficialSDKServer(t *testing.T) {
	server := mcp.NewServer(&mcp.Implementation{Name: "registry-test-server", Version: "v1"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "echo", Description: "Echo"}, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "ok"}}}, nil, nil
	})
	httpServer := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil))
	t.Cleanup(httpServer.Close)

	database, service := setupMCPRegistryTest(t, mcpclient.New(httpServer.Client(), nil))
	command := validMCPServerCommand("official")
	command.Endpoint = httpServer.URL
	actor := MCPRegistryActor{UserID: 11}
	if _, err := service.Create(context.Background(), actor, command); err != nil {
		t.Fatalf("create official MCP server: %v", err)
	}
	view, err := service.CheckHealth(context.Background(), actor, "official")
	if err != nil {
		t.Fatalf("official SDK health check failed: %v", err)
	}
	if view.Status != models.MCPServerStatusActive {
		t.Fatalf("official SDK health status = %s, want active", view.Status)
	}
	tools, err := service.ListTools(context.Background(), actor, "official")
	if err != nil || len(tools) != 1 || tools[0].ToolName != "echo" {
		t.Fatalf("unexpected official SDK discovery snapshot: tools=%+v err=%v", tools, err)
	}
	var persisted models.MCPTool
	if err := database.Where("tool_name = ?", "echo").First(&persisted).Error; err != nil {
		t.Fatalf("load official SDK tool snapshot: %v", err)
	}
}

func TestMCPRegistryHealthFailureIsSanitized(t *testing.T) {
	client := &fakeMCPProtocolClient{err: fmt.Errorf("%w: Authorization Bearer top-secret", mcpclient.ErrTransportFailed)}
	database, service := setupMCPRegistryTest(t, client)
	actor := MCPRegistryActor{UserID: 11}
	if _, err := service.Create(context.Background(), actor, validMCPServerCommand("broken")); err != nil {
		t.Fatalf("create server: %v", err)
	}
	if _, err := service.CheckHealth(context.Background(), actor, "broken"); !errors.Is(err, ErrMCPServerUnhealthy()) {
		t.Fatalf("expected unhealthy error, got %v", err)
	} else if strings.Contains(err.Error(), "top-secret") {
		t.Fatalf("health error leaked downstream details: %v", err)
	}
	var server models.MCPServer
	if err := database.Where("server_code = ?", "broken").First(&server).Error; err != nil {
		t.Fatalf("load server: %v", err)
	}
	if server.Status != models.MCPServerStatusUnhealthy || server.LastError != "MCP transport failed" {
		t.Fatalf("unexpected sanitized failure: %+v", server)
	}
}

func TestMCPRegistryStaleHealthCannotOverwriteUpdatedConfig(t *testing.T) {
	client := &fakeMCPProtocolClient{
		tools:   []mcpclient.Tool{{Name: "search", InputSchemaJSON: `{"type":"object"}`}},
		started: make(chan struct{}), release: make(chan struct{}),
	}
	database, service := setupMCPRegistryTest(t, client)
	actor := MCPRegistryActor{UserID: 11}
	command := validMCPServerCommand("stale")
	if _, err := service.Create(context.Background(), actor, command); err != nil {
		t.Fatalf("create server: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := service.CheckHealth(context.Background(), actor, "stale")
		result <- err
	}()
	<-client.started
	command.Endpoint = "http://127.0.0.1:8091/mcp"
	if _, err := service.Update(context.Background(), actor, "stale", command); err != nil {
		t.Fatalf("update server during health check: %v", err)
	}
	close(client.release)
	if err := <-result; !errors.Is(err, ErrMCPServerInvalidState()) {
		t.Fatalf("expected stale health rejection, got %v", err)
	}
	var server models.MCPServer
	if err := database.Where("server_code = ?", "stale").First(&server).Error; err != nil {
		t.Fatalf("load server: %v", err)
	}
	if server.Status != models.MCPServerStatusInactive || server.ConfigVersion != 2 || server.Endpoint != command.Endpoint {
		t.Fatalf("stale result overwrote configuration: %+v", server)
	}
}

func TestMCPRegistryRejectsMutationReferencedByActiveWorkflow(t *testing.T) {
	client := &fakeMCPProtocolClient{}
	database, service := setupMCPRegistryTest(t, client)
	actor := MCPRegistryActor{UserID: 11}
	if _, err := service.Create(context.Background(), actor, validMCPServerCommand("docs")); err != nil {
		t.Fatalf("create server: %v", err)
	}
	if err := database.Model(&models.MCPServer{}).Where("server_code = ?", "docs").Update("status", models.MCPServerStatusActive).Error; err != nil {
		t.Fatalf("activate server fixture: %v", err)
	}
	agent := models.Agent{AgentCode: "writer", Name: "Writer", OwnerUserID: actor.UserID, Status: models.AgentStatusActive}
	if err := database.Create(&agent).Error; err != nil {
		t.Fatalf("create agent: %v", err)
	}
	definition := `{"entry_node":"search","nodes":[{"key":"search","type":"tool","config":{"server_code":"docs","tool_name":"search","input":{}}}],"edges":[]}`
	workflow := models.Workflow{AgentID: agent.ID, Version: 1, DefinitionJSON: definition, Checksum: "v1", IsActive: true, CreatedBy: actor.UserID}
	if err := database.Create(&workflow).Error; err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	if _, err := service.Deactivate(context.Background(), actor, "docs"); !errors.Is(err, ErrMCPServerInvalidState()) {
		t.Fatalf("expected active workflow guard, got %v", err)
	}
	updated := validMCPServerCommand("docs")
	updated.Endpoint = "http://127.0.0.1:8099/mcp"
	if _, err := service.Update(context.Background(), actor, "docs", updated); !errors.Is(err, ErrMCPServerInvalidState()) {
		t.Fatalf("expected configuration mutation guard, got %v", err)
	}
}
