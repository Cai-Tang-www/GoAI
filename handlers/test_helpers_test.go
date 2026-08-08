package handlers_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"GoAI/config"
	"GoAI/mcpclient"
	"GoAI/models"
	"GoAI/routers"
	"GoAI/services"

	"github.com/gin-gonic/gin"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type apiEnvelope struct {
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
	TraceID string          `json:"trace_id"`
}

type testMCPProtocolClient struct {
	discover func(context.Context, mcpclient.ServerConfig) ([]mcpclient.Tool, error)
}

func (c testMCPProtocolClient) Discover(ctx context.Context, config mcpclient.ServerConfig) ([]mcpclient.Tool, error) {
	if c.discover != nil {
		return c.discover(ctx, config)
	}
	return []mcpclient.Tool{{Name: "echo", InputSchemaJSON: `{"type":"object"}`}}, nil
}

// decodeEnvelope 解析统一响应 envelope，并校验 trace_id 已返回。
func decodeEnvelope(t *testing.T, w *httptest.ResponseRecorder) apiEnvelope {
	t.Helper()
	var env apiEnvelope
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode envelope failed: %v body=%s", err, w.Body.String())
	}
	if strings.TrimSpace(env.TraceID) == "" {
		t.Fatalf("trace_id should not be empty: body=%s", w.Body.String())
	}
	return env
}

var sqliteTestSequence atomic.Uint64

// openSQLiteTestDB 为每次测试建立独立的 SQLite 内存库，并在测试结束时释放连接。
func openSQLiteTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	name := strings.NewReplacer("/", "_", " ", "_", ":", "_").Replace(t.Name())
	dsn := fmt.Sprintf("file:%s_%d?mode=memory&cache=shared", name, sqliteTestSequence.Add(1))
	database, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}

	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get sqlite connection failed: %v", err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close sqlite connection failed: %v", err)
		}
	})
	return database
}

func newTestRouter(t *testing.T, database *gorm.DB, publisher services.RunEventPublisher) *gin.Engine {
	t.Helper()
	return newTestRouterWithRegistryChecker(t, database, publisher, nil)
}

func newTestRouterWithRegistryChecker(t *testing.T, database *gorm.DB, publisher services.RunEventPublisher, checker services.AgentCardHealthChecker) *gin.Engine {
	return newTestRouterWithRegistryAndMCPClient(t, database, publisher, checker, testMCPProtocolClient{})
}

func newTestRouterWithRegistryAndMCPClient(t *testing.T, database *gorm.DB, publisher services.RunEventPublisher, checker services.AgentCardHealthChecker, mcpProtocol services.MCPProtocolClient) *gin.Engine {
	t.Helper()
	if database == nil {
		database = openSQLiteTestDB(t)
		if err := database.AutoMigrate(
			&models.User{},
			&models.Role{},
			&models.Permission{},
			&models.UserRole{},
			&models.RolePermission{},
			&models.Agent{},
			&models.AgentCapability{},
			&models.AgentEndpoint{},
			&models.MCPServer{},
			&models.MCPTool{},
			&models.Workflow{},
			&models.Thread{},
			&models.Message{},
			&models.Run{},
			&models.RunStep{},
			&models.RunInterrupt{},
			&models.RunIdempotency{},
			&models.DelegationGroup{},
		); err != nil {
			t.Fatalf("auto migrate test database failed: %v", err)
		}
	}
	if publisher == nil {
		publisher = services.RunEventPublisherFunc(func(_ context.Context, _ string) error { return nil })
	}
	runService, err := services.NewRunService(database, publisher)
	if err != nil {
		t.Fatalf("create run service failed: %v", err)
	}
	runtimeService, err := services.NewRuntimeService(database, runService)
	if err != nil {
		t.Fatalf("create runtime service failed: %v", err)
	}
	appConfig := config.AppConfig
	if appConfig == nil {
		appConfig = &config.Config{}
	}
	chatService, err := services.NewChatService(appConfig, &http.Client{})
	if err != nil {
		t.Fatalf("create chat service failed: %v", err)
	}
	if checker == nil {
		checker = services.AgentCardHealthCheckerFunc(func(context.Context, services.AgentCardHealthCheckRequest) error { return nil })
	}
	agentRegistry, err := services.NewAgentRegistryService(database, checker, nil, false)
	if err != nil {
		t.Fatalf("create agent registry service failed: %v", err)
	}
	if mcpProtocol == nil {
		mcpProtocol = testMCPProtocolClient{}
	}
	mcpRegistry, err := services.NewMCPRegistryService(database, mcpProtocol)
	if err != nil {
		t.Fatalf("create MCP registry service failed: %v", err)
	}
	router, err := routers.New(routers.Dependencies{
		Database:      database,
		RBACEnable:    appConfig != nil && appConfig.RBACEnable,
		RunService:    runService,
		ChatService:   chatService,
		AgentRegistry: agentRegistry,
		MCPRegistry:   mcpRegistry,
		Runtime:       runtimeService,
		A2AGateway:    http.NotFoundHandler(),
	})
	if err != nil {
		t.Fatalf("create router failed: %v", err)
	}
	return router
}
