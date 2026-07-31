package handlers_test

import (
	"GoAI/models"
	"GoAI/routers"
	"GoAI/services"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

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

// uniqueSQLiteDSN 为每个测试生成独立的内存库 DSN，避免整包执行时相互污染。
func uniqueSQLiteDSN(t *testing.T) string {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_", ":", "_").Replace(t.Name())
	return "file:" + name + "?mode=memory&cache=shared"
}

func newTestRouter(t *testing.T, database *gorm.DB, publisher services.RunEventPublisher) *gin.Engine {
	t.Helper()
	if database == nil {
		var err error
		database, err = gorm.Open(sqlite.Open(uniqueSQLiteDSN(t)), &gorm.Config{})
		if err != nil {
			t.Fatalf("open sqlite failed: %v", err)
		}
		if err := database.AutoMigrate(
			&models.User{},
			&models.Role{},
			&models.Permission{},
			&models.UserRole{},
			&models.RolePermission{},
			&models.Agent{},
			&models.Workflow{},
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
	router, err := routers.New(routers.Dependencies{Database: database, RunService: runService})
	if err != nil {
		t.Fatalf("create router failed: %v", err)
	}
	return router
}
