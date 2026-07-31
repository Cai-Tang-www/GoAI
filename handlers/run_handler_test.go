package handlers_test

import (
	"GoAI/config"
	"GoAI/middlewares"
	"GoAI/models"
	"GoAI/requestctx"
	"GoAI/services"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"gorm.io/gorm"
)

func setupRunIntegrationDB(t *testing.T) *gorm.DB {
	t.Helper()
	gdb := openSQLiteTestDB(t)
	if err := gdb.AutoMigrate(&models.User{}, &models.Agent{}, &models.Workflow{}, &models.Run{}, &models.RunStep{}, &models.RunIdempotency{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	return gdb
}

func seedRunIntegrationData(t *testing.T, gdb *gorm.DB) (models.User, models.User, models.Agent, models.Workflow) {
	t.Helper()

	user1 := models.User{Username: "u1", Email: "u1@test.com", Password: "p1"}
	user2 := models.User{Username: "u2", Email: "u2@test.com", Password: "p2"}
	if err := gdb.Create(&user1).Error; err != nil {
		t.Fatalf("create user1 failed: %v", err)
	}
	if err := gdb.Create(&user2).Error; err != nil {
		t.Fatalf("create user2 failed: %v", err)
	}

	agent := models.Agent{
		AgentCode:   "agent_api",
		Name:        "Agent API",
		Description: "for integration",
		OwnerUserID: uint64(user1.ID),
		Status:      models.AgentStatusActive,
	}
	if err := gdb.Create(&agent).Error; err != nil {
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
	if err := gdb.Create(&workflow).Error; err != nil {
		t.Fatalf("create workflow failed: %v", err)
	}

	return user1, user2, agent, workflow
}

func TestRunAPIs_CreateAndQuery(t *testing.T) {
	gdb := setupRunIntegrationDB(t)
	user1, user2, _, _ := seedRunIntegrationData(t, gdb)
	config.AppConfig = &config.Config{
		JWTSecret: "test-secret",
	}

	var publishedTraceID string
	publisher := services.RunEventPublisherFunc(func(ctx context.Context, runID string) error {
		publishedTraceID = requestctx.TraceIDFromContext(ctx)
		return nil
	})

	token1, err := middlewares.GenerateToken(user1.ID)
	if err != nil {
		t.Fatalf("generate token1 failed: %v", err)
	}
	token2, err := middlewares.GenerateToken(user2.ID)
	if err != nil {
		t.Fatalf("generate token2 failed: %v", err)
	}

	router := newTestRouter(t, gdb, publisher)

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
	gdb := setupRunIntegrationDB(t)
	user1, _, _, _ := seedRunIntegrationData(t, gdb)
	config.AppConfig = &config.Config{JWTSecret: "test-secret"}
	token1, err := middlewares.GenerateToken(user1.ID)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}

	router := newTestRouter(t, gdb, nil)
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
	gdb := setupRunIntegrationDB(t)
	user1, _, _, _ := seedRunIntegrationData(t, gdb)
	config.AppConfig = &config.Config{JWTSecret: "test-secret"}
	token1, err := middlewares.GenerateToken(user1.ID)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}

	router := newTestRouter(t, gdb, nil)
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

func TestRunIdempotencyCreateAndReplay(t *testing.T) {
	gdb := setupRunIntegrationDB(t)
	user1, _, _, _ := seedRunIntegrationData(t, gdb)
	config.AppConfig = &config.Config{JWTSecret: "test-secret"}

	token1, err := middlewares.GenerateToken(user1.ID)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}
	router := newTestRouter(t, gdb, nil)

	createBody := map[string]any{
		"agent_code":   "agent_api",
		"trigger_type": "api",
		"input": map[string]any{
			"prompt": "hello",
		},
	}
	raw, _ := json.Marshal(createBody)

	firstReq := httptest.NewRequest(http.MethodPost, "/api/runs", bytes.NewReader(raw))
	firstReq.Header.Set("Content-Type", "application/json")
	firstReq.Header.Set("Authorization", "Bearer "+token1)
	firstReq.Header.Set("Idempotency-Key", "create-key-1")
	firstW := httptest.NewRecorder()
	router.ServeHTTP(firstW, firstReq)
	if firstW.Code != http.StatusAccepted {
		t.Fatalf("first create expected 202, got %d body=%s", firstW.Code, firstW.Body.String())
	}
	firstEnv := decodeEnvelope(t, firstW)
	var firstResp map[string]any
	if err := json.Unmarshal(firstEnv.Data, &firstResp); err != nil {
		t.Fatalf("parse first create data failed: %v", err)
	}
	runID, _ := firstResp["run_id"].(string)
	if runID == "" {
		t.Fatal("first create run_id empty")
	}

	secondReq := httptest.NewRequest(http.MethodPost, "/api/runs", bytes.NewReader(raw))
	secondReq.Header.Set("Content-Type", "application/json")
	secondReq.Header.Set("Authorization", "Bearer "+token1)
	secondReq.Header.Set("Idempotency-Key", "create-key-1")
	secondW := httptest.NewRecorder()
	router.ServeHTTP(secondW, secondReq)
	if secondW.Code != http.StatusOK {
		t.Fatalf("second create expected 200, got %d body=%s", secondW.Code, secondW.Body.String())
	}
	secondEnv := decodeEnvelope(t, secondW)
	if secondEnv.Code != "OK" {
		t.Fatalf("unexpected second create code: %s body=%s", secondEnv.Code, secondW.Body.String())
	}
	var secondResp map[string]any
	if err := json.Unmarshal(secondEnv.Data, &secondResp); err != nil {
		t.Fatalf("parse second create data failed: %v", err)
	}
	if secondResp["run_id"] != runID {
		t.Fatalf("expected same run_id, got %v and %v", runID, secondResp["run_id"])
	}

	replayReq := httptest.NewRequest(http.MethodPost, "/api/runs/"+runID+"/replay", nil)
	replayReq.Header.Set("Authorization", "Bearer "+token1)
	replayReq.Header.Set("Idempotency-Key", "replay-key-1")
	replayW := httptest.NewRecorder()
	router.ServeHTTP(replayW, replayReq)
	if replayW.Code != http.StatusAccepted {
		t.Fatalf("first replay expected 202, got %d body=%s", replayW.Code, replayW.Body.String())
	}
	replayEnv := decodeEnvelope(t, replayW)
	var replayResp map[string]any
	if err := json.Unmarshal(replayEnv.Data, &replayResp); err != nil {
		t.Fatalf("parse replay data failed: %v", err)
	}
	replayRunID, _ := replayResp["run_id"].(string)
	if replayRunID == "" {
		t.Fatal("first replay run_id empty")
	}

	replayReq2 := httptest.NewRequest(http.MethodPost, "/api/runs/"+runID+"/replay", nil)
	replayReq2.Header.Set("Authorization", "Bearer "+token1)
	replayReq2.Header.Set("Idempotency-Key", "replay-key-1")
	replayW2 := httptest.NewRecorder()
	router.ServeHTTP(replayW2, replayReq2)
	if replayW2.Code != http.StatusOK {
		t.Fatalf("second replay expected 200, got %d body=%s", replayW2.Code, replayW2.Body.String())
	}
	replayEnv2 := decodeEnvelope(t, replayW2)
	var replayResp2 map[string]any
	if err := json.Unmarshal(replayEnv2.Data, &replayResp2); err != nil {
		t.Fatalf("parse second replay data failed: %v", err)
	}
	if replayResp2["run_id"] != replayRunID {
		t.Fatalf("expected same replay run_id, got %v and %v", replayRunID, replayResp2["run_id"])
	}
}
