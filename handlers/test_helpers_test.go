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
	if database == nil {
		database = openSQLiteTestDB(t)
		if err := database.AutoMigrate(
			&models.User{},
			&models.Role{},
			&models.Permission{},
			&models.UserRole{},
			&models.RolePermission{},
			&models.Agent{},
			&models.Workflow{},
			&models.Thread{},
			&models.Message{},
			&models.Run{},
			&models.RunStep{},
			&models.RunIdempotency{},
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
	router, err := routers.New(routers.Dependencies{
		Database:    database,
		RunService:  runService,
		ChatService: chatService,
		Runtime:     runtimeService,
		A2AGateway:  http.NotFoundHandler(),
	})
	if err != nil {
		t.Fatalf("create router failed: %v", err)
	}
	return router
}
