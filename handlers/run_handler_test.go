package handlers_test

import (
	"GoAI/config"
	"GoAI/db"
	"GoAI/middlewares"
	"GoAI/models"
	"GoAI/requestctx"
	routers "GoAI/routers"
	"GoAI/services"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupRunIntegrationDB(t *testing.T) {
	t.Helper()
	gdb, err := gorm.Open(sqlite.Open(uniqueSQLiteDSN(t)), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := gdb.AutoMigrate(&models.User{}, &models.Agent{}, &models.Workflow{}, &models.Run{}, &models.RunStep{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	db.DB = gdb
}

func seedRunIntegrationData(t *testing.T) (models.User, models.User, models.Agent, models.Workflow) {
	t.Helper()

	user1 := models.User{Username: "u1", Email: "u1@test.com", Password: "p1"}
	user2 := models.User{Username: "u2", Email: "u2@test.com", Password: "p2"}
	if err := db.DB.Create(&user1).Error; err != nil {
		t.Fatalf("create user1 failed: %v", err)
	}
	if err := db.DB.Create(&user2).Error; err != nil {
		t.Fatalf("create user2 failed: %v", err)
	}

	agent := models.Agent{
		AgentCode:   "agent_api",
		Name:        "Agent API",
		Description: "for integration",
		OwnerUserID: uint64(user1.ID),
		Status:      models.AgentStatusActive,
	}
	if err := db.DB.Create(&agent).Error; err != nil {
		t.Fatalf("create agent failed: %v", err)
	}

	workflow := models.Workflow{
		AgentID:        agent.ID,
		Version:        1,
		DefinitionJSON: `{"entry_node":"planner","nodes":[{"key":"planner","type":"planner"}],"edges":[]}`,
		Checksum:       "sum1",
		IsActive:       true,
		CreatedBy:      uint64(user1.ID),
	}
	if err := db.DB.Create(&workflow).Error; err != nil {
		t.Fatalf("create workflow failed: %v", err)
	}

	return user1, user2, agent, workflow
}

func TestRunAPIs_CreateAndQuery(t *testing.T) {
	setupRunIntegrationDB(t)
	user1, user2, _, _ := seedRunIntegrationData(t)
	config.AppConfig = &config.Config{
		JWTSecret: "test-secret",
	}

	var publishedTraceID string
	services.SetPublishRunExecuteEventForTest(func(ctx context.Context, runID string) error {
		publishedTraceID = requestctx.TraceIDFromContext(ctx)
		return nil
	})
	defer services.SetPublishRunExecuteEventForTest(nil)

	token1, err := middlewares.GenerateToken(user1.ID)
	if err != nil {
		t.Fatalf("generate token1 failed: %v", err)
	}
	token2, err := middlewares.GenerateToken(user2.ID)
	if err != nil {
		t.Fatalf("generate token2 failed: %v", err)
	}

	router := routers.InitRouter()

	createBody := map[string]any{
		"agent_code":   "agent_api",
		"trigger_type": "api",
		"input": map[string]any{
			"prompt": "hello",
		},
	}
	raw, _ := json.Marshal(createBody)
	req := httptest.NewRequest(http.MethodPost, "/api/runs", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token1)
	req.Header.Set(requestctx.TraceIDHeader, "trace-run-create")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("create run expected 202, got %d, body=%s", w.Code, w.Body.String())
	}
	createEnv := decodeEnvelope(t, w)
	if createEnv.Code != "OK" {
		t.Fatalf("unexpected create code: %s body=%s", createEnv.Code, w.Body.String())
	}
	if publishedTraceID != "trace-run-create" {
		t.Fatalf("expected published trace id, got %q", publishedTraceID)
	}
	var createResp map[string]any
	if err := json.Unmarshal(createEnv.Data, &createResp); err != nil {
		t.Fatalf("parse create data failed: %v", err)
	}
	runID, _ := createResp["run_id"].(string)
	if runID == "" {
		t.Fatalf("run_id is empty in response: %v", createResp)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/api/runs/"+runID, nil)
	getReq.Header.Set("Authorization", "Bearer "+token1)
	getW := httptest.NewRecorder()
	router.ServeHTTP(getW, getReq)
	if getW.Code != http.StatusOK {
		t.Fatalf("get run expected 200, got %d, body=%s", getW.Code, getW.Body.String())
	}
	getEnv := decodeEnvelope(t, getW)
	if getEnv.Code != "OK" {
		t.Fatalf("unexpected get code: %s body=%s", getEnv.Code, getW.Body.String())
	}

	forbiddenReq := httptest.NewRequest(http.MethodGet, "/api/runs/"+runID, nil)
	forbiddenReq.Header.Set("Authorization", "Bearer "+token2)
	forbiddenW := httptest.NewRecorder()
	router.ServeHTTP(forbiddenW, forbiddenReq)
	if forbiddenW.Code != http.StatusForbidden {
		t.Fatalf("forbidden query expected 403, got %d, body=%s", forbiddenW.Code, forbiddenW.Body.String())
	}
	forbiddenEnv := decodeEnvelope(t, forbiddenW)
	if forbiddenEnv.Code != "AUTH_FORBIDDEN" {
		t.Fatalf("unexpected forbidden code: %s body=%s", forbiddenEnv.Code, forbiddenW.Body.String())
	}
}

func TestCreateRunValidationError(t *testing.T) {
	setupRunIntegrationDB(t)
	user1, _, _, _ := seedRunIntegrationData(t)
	config.AppConfig = &config.Config{JWTSecret: "test-secret"}
	token1, err := middlewares.GenerateToken(user1.ID)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}

	router := routers.InitRouter()
	invalidBody := map[string]any{
		"agent_code":       "",
		"trigger_type":     "bad-trigger",
		"workflow_version": -1,
	}
	raw, _ := json.Marshal(invalidBody)
	req := httptest.NewRequest(http.MethodPost, "/api/runs", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token1)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
	}
	env := decodeEnvelope(t, w)
	if env.Code != "VALIDATION_FAILED" {
		t.Fatalf("unexpected code: %s body=%s", env.Code, w.Body.String())
	}
}

func TestRunPathValidationError(t *testing.T) {
	setupRunIntegrationDB(t)
	user1, _, _, _ := seedRunIntegrationData(t)
	config.AppConfig = &config.Config{JWTSecret: "test-secret"}
	token1, err := middlewares.GenerateToken(user1.ID)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}

	router := routers.InitRouter()
	req := httptest.NewRequest(http.MethodGet, "/api/runs/%20", nil)
	req.Header.Set("Authorization", "Bearer "+token1)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
	}
	env := decodeEnvelope(t, w)
	if env.Code != "VALIDATION_FAILED" {
		t.Fatalf("unexpected code: %s body=%s", env.Code, w.Body.String())
	}
}
