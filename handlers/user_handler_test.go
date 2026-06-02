package handlers_test

import (
	"GoAI/config"
	"GoAI/db"
	"GoAI/models"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	routers "GoAI/routers"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TestUserAuthAndEnvelope 验证 register/login 与缺 token 场景都会走统一响应结构。
func TestUserAuthAndEnvelope(t *testing.T) {
	gdb, err := gorm.Open(sqlite.Open("file:user_handler_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := gdb.AutoMigrate(&models.User{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	db.DB = gdb

	config.AppConfig = &config.Config{
		JWTSecret:      "user-test-secret",
		RBACEnable:     false,
		ModelProviders: map[string]config.ModelProviderConfig{},
	}
	router := routers.InitRouter()

	registerBody, _ := json.Marshal(map[string]any{
		"username": "user1",
		"email":    "user1@test.com",
		"password": "pass123",
	})
	registerReq := httptest.NewRequest(http.MethodPost, "/auth/register", bytes.NewReader(registerBody))
	registerReq.Header.Set("Content-Type", "application/json")
	registerW := httptest.NewRecorder()
	router.ServeHTTP(registerW, registerReq)

	if registerW.Code != http.StatusCreated {
		t.Fatalf("register expected 201, got %d body=%s", registerW.Code, registerW.Body.String())
	}
	registerEnv := decodeEnvelope(t, registerW)
	if registerEnv.Code != "OK" {
		t.Fatalf("unexpected register code: %s body=%s", registerEnv.Code, registerW.Body.String())
	}

	loginBody, _ := json.Marshal(map[string]any{
		"username": "user1",
		"password": "pass123",
	})
	loginReq := httptest.NewRequest(http.MethodPost, "/auth/login", bytes.NewReader(loginBody))
	loginReq.Header.Set("Content-Type", "application/json")
	loginW := httptest.NewRecorder()
	router.ServeHTTP(loginW, loginReq)

	if loginW.Code != http.StatusOK {
		t.Fatalf("login expected 200, got %d body=%s", loginW.Code, loginW.Body.String())
	}
	loginEnv := decodeEnvelope(t, loginW)
	if loginEnv.Code != "OK" {
		t.Fatalf("unexpected login code: %s body=%s", loginEnv.Code, loginW.Body.String())
	}

	protectedReq := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	protectedW := httptest.NewRecorder()
	router.ServeHTTP(protectedW, protectedReq)

	if protectedW.Code != http.StatusUnauthorized {
		t.Fatalf("missing token expected 401, got %d body=%s", protectedW.Code, protectedW.Body.String())
	}
	protectedEnv := decodeEnvelope(t, protectedW)
	if protectedEnv.Code != "AUTH_MISSING_TOKEN" {
		t.Fatalf("unexpected auth code: %s body=%s", protectedEnv.Code, protectedW.Body.String())
	}
}
