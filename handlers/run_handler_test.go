package handlers_test

import (
	"GoAI/config"
	"GoAI/db"
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
	"time"

	"gorm.io/gorm"
)

func setupRunIntegrationDB(t *testing.T) *gorm.DB {
	t.Helper()
	gdb := openSQLiteTestDB(t)
	if err := gdb.AutoMigrate(&models.User{}, &models.Agent{}, &models.Workflow{}, &models.Thread{}, &models.Message{}, &models.Run{}, &models.RunStep{}, &models.RunInterrupt{}, &models.RunIdempotency{}, &models.Delegation{}, &models.DelegationGroup{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	return gdb
}

func TestThreadReplayAPIUsesHistoryIdempotencyAndOwnership(t *testing.T) {
	gdb := setupRunIntegrationDB(t)
	user1, user2, agent, workflow := seedRunIntegrationData(t, gdb)
	config.AppConfig = &config.Config{JWTSecret: "test-secret", RBACEnable: false}
	thread := models.Thread{ThreadID: "thread-api-replay", OwnerUserID: uint64(user1.ID), Status: models.ThreadStatusActive, MetadataJSON: "{}"}
	if err := gdb.Create(&thread).Error; err != nil {
		t.Fatalf("create thread failed: %v", err)
	}
	if err := gdb.Create(&models.Message{
		MessageID: "message-api-replay", ThreadID: thread.ThreadID, RunID: "run-api-source",
		SenderType: models.MessageSenderUser, ReceiverType: models.MessageSenderAgent,
		MessageType: models.MessageTypeInput, ContentType: "text", ContentJSON: `{"text":"hello"}`,
		MetadataJSON: "{}", Status: models.MessageStatusDelivered,
	}).Error; err != nil {
		t.Fatalf("create thread message failed: %v", err)
	}
	loopID := "loop-api-source"
	if err := gdb.Create(&models.Run{
		RunID: "run-api-source", ThreadID: thread.ThreadID, TraceID: "trace-api-source", LoopID: &loopID,
		AgentID: agent.ID, WorkflowID: workflow.ID, UserID: uint64(user1.ID), TriggerType: "agui",
		InputJSON: `{}`, Status: models.RunStatusSuccess, Provider: "deepseek", Model: "deepseek-chat",
	}).Error; err != nil {
		t.Fatalf("create source run failed: %v", err)
	}
	token1, err := middlewares.GenerateToken(user1.ID)
	if err != nil {
		t.Fatalf("generate token1 failed: %v", err)
	}
	token2, err := middlewares.GenerateToken(user2.ID)
	if err != nil {
		t.Fatalf("generate token2 failed: %v", err)
	}
	router := newTestRouter(t, gdb, nil)

	replay := func(token, key string, body []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/threads/"+thread.ThreadID+"/replay", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}

	first := replay(token1, "thread-replay-key", []byte(`{"source_run_id":"run-api-source"}`))
	if first.Code != http.StatusAccepted {
		t.Fatalf("first thread replay expected 202, got %d body=%s", first.Code, first.Body.String())
	}
	firstEnvelope := decodeEnvelope(t, first)
	if firstEnvelope.Code != "OK" {
		t.Fatalf("unexpected first replay response: %s", first.Body.String())
	}
	var firstData services.CreateRunResponse
	if err := json.Unmarshal(firstEnvelope.Data, &firstData); err != nil {
		t.Fatalf("decode first replay data failed: %v", err)
	}
	if firstData.RunID == "" || firstData.Status != models.RunStatusQueued {
		t.Fatalf("unexpected first replay data: %+v", firstData)
	}

	repeated := replay(token1, "thread-replay-key", []byte(`{"source_run_id":"run-api-source"}`))
	if repeated.Code != http.StatusOK {
		t.Fatalf("repeated thread replay expected 200, got %d body=%s", repeated.Code, repeated.Body.String())
	}
	var repeatedData services.CreateRunResponse
	if err := json.Unmarshal(decodeEnvelope(t, repeated).Data, &repeatedData); err != nil {
		t.Fatalf("decode repeated replay data failed: %v", err)
	}
	if repeatedData.RunID != firstData.RunID {
		t.Fatalf("idempotent replay returned different run: first=%s repeated=%s", firstData.RunID, repeatedData.RunID)
	}

	forbidden := replay(token2, "other-key", []byte(`{"source_run_id":"run-api-source"}`))
	if forbidden.Code != http.StatusForbidden || decodeEnvelope(t, forbidden).Code != middlewares.CodeAuthForbidden {
		t.Fatalf("foreign thread replay should be forbidden, status=%d body=%s", forbidden.Code, forbidden.Body.String())
	}
}

func TestThreadReplayAPIAppliesRBACOwnershipAndAdminOverride(t *testing.T) {
	gdb := setupRBACIntegrationDB(t)
	owner, foreign, agent, workflow := seedRunIntegrationData(t, gdb)
	admin := models.User{Username: "thread-replay-admin", Email: "thread-replay-admin@test.com", Password: "p3"}
	if err := gdb.Create(&admin).Error; err != nil {
		t.Fatalf("create admin failed: %v", err)
	}
	config.AppConfig = &config.Config{
		JWTSecret:                  "thread-replay-rbac-secret",
		RBACEnable:                 true,
		RBACBootstrapAdminUsername: admin.Username,
		ModelProviders:             map[string]config.ModelProviderConfig{},
	}
	if err := db.SeedRBAC(gdb, config.AppConfig); err != nil {
		t.Fatalf("seed RBAC failed: %v", err)
	}
	thread := models.Thread{ThreadID: "thread-rbac-replay", OwnerUserID: uint64(owner.ID), Status: models.ThreadStatusActive, MetadataJSON: "{}"}
	if err := gdb.Create(&thread).Error; err != nil {
		t.Fatalf("create thread failed: %v", err)
	}
	loopID := "loop-rbac-replay-source"
	if err := gdb.Create(&models.Run{
		RunID: "run-rbac-replay-source", ThreadID: thread.ThreadID, TraceID: "trace-rbac-replay-source", LoopID: &loopID,
		AgentID: agent.ID, WorkflowID: workflow.ID, UserID: uint64(owner.ID), TriggerType: "agui", InputJSON: `{}`, Status: models.RunStatusSuccess,
	}).Error; err != nil {
		t.Fatalf("create source run failed: %v", err)
	}
	ownerToken, err := middlewares.GenerateToken(owner.ID)
	if err != nil {
		t.Fatalf("generate owner token failed: %v", err)
	}
	foreignToken, err := middlewares.GenerateToken(foreign.ID)
	if err != nil {
		t.Fatalf("generate foreign token failed: %v", err)
	}
	adminToken, err := middlewares.GenerateToken(admin.ID)
	if err != nil {
		t.Fatalf("generate admin token failed: %v", err)
	}
	router := newTestRouter(t, gdb, nil)
	replay := func(token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/threads/"+thread.ThreadID+"/replay", bytes.NewBufferString(`{"source_run_id":"run-rbac-replay-source"}`))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response
	}

	ownerResponse := replay(ownerToken)
	if ownerResponse.Code != http.StatusAccepted {
		t.Fatalf("owner replay expected 202, got %d body=%s", ownerResponse.Code, ownerResponse.Body.String())
	}
	foreignResponse := replay(foreignToken)
	if foreignResponse.Code != http.StatusForbidden || decodeEnvelope(t, foreignResponse).Code != middlewares.CodeAuthForbidden {
		t.Fatalf("foreign member replay expected 403 AUTH_FORBIDDEN, got %d body=%s", foreignResponse.Code, foreignResponse.Body.String())
	}
	adminResponse := replay(adminToken)
	if adminResponse.Code != http.StatusAccepted {
		t.Fatalf("admin replay expected 202, got %d body=%s", adminResponse.Code, adminResponse.Body.String())
	}
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
	user1, user2, agent, _ := seedRunIntegrationData(t, gdb)
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

	var persistedRun models.Run
	if err := gdb.First(&persistedRun, "run_id = ?", runID).Error; err != nil {
		t.Fatalf("load created run failed: %v", err)
	}
	claimedAt := time.Date(2026, time.August, 3, 12, 0, 0, 0, time.UTC)
	heartbeatAt := claimedAt.Add(5 * time.Second)
	expiresAt := claimedAt.Add(30 * time.Second)
	delegation := models.Delegation{
		DelegationID:           "delegation-resume-api",
		ThreadID:               persistedRun.ThreadID,
		ParentRunID:            runID,
		ChildRunID:             "run-child-resume-api",
		TraceID:                persistedRun.TraceID,
		SourceAgentID:          agent.ID,
		TargetAgentID:          agent.ID,
		CapabilityCode:         "review",
		RequestMessageID:       "message-resume-api",
		ParentStepKey:          "delegate-reviewer",
		ResumeNodeKey:          "summarize",
		InputJSON:              `{"prompt":"review"}`,
		OutputJSON:             `{"result":"approved"}`,
		Status:                 models.DelegationStatusSucceeded,
		ResumeStatus:           models.DelegationResumeStatusClaimed,
		ResumeError:            "previous worker lease expired",
		ResumeAttemptCount:     2,
		ResumeExecutionAttempt: 3,
		ResumeLeaseOwner:       "resume-worker-2",
		ResumeLeaseClaimedAt:   &claimedAt,
		ResumeLeaseHeartbeatAt: &heartbeatAt,
		ResumeLeaseExpiresAt:   &expiresAt,
	}
	if err := gdb.Create(&delegation).Error; err != nil {
		t.Fatalf("create resume delegation failed: %v", err)
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
	var detail struct {
		RunID  string `json:"RunID"`
		Status string `json:"Status"`
		Resume *struct {
			DelegationID     string  `json:"delegation_id"`
			Status           string  `json:"status"`
			Error            string  `json:"error"`
			PublishAttempts  int     `json:"publish_attempts"`
			ExecutionAttempt int     `json:"execution_attempt"`
			LeaseOwner       string  `json:"lease_owner"`
			LeaseClaimedAt   *string `json:"lease_claimed_at"`
			LeaseHeartbeatAt *string `json:"lease_heartbeat_at"`
			LeaseExpiresAt   *string `json:"lease_expires_at"`
		} `json:"resume"`
	}
	if err := json.Unmarshal(getEnv.Data, &detail); err != nil {
		t.Fatalf("parse run detail failed: %v", err)
	}
	if detail.RunID != runID {
		t.Fatalf("expected flattened RunID %q, got %q body=%s", runID, detail.RunID, getW.Body.String())
	}
	if detail.Resume == nil {
		t.Fatalf("expected resume state in run detail: %s", getW.Body.String())
	}
	if detail.Resume.DelegationID != delegation.DelegationID || detail.Resume.Status != models.DelegationResumeStatusClaimed {
		t.Fatalf("unexpected resume identity/status: %+v", detail.Resume)
	}
	if detail.Resume.Error != delegation.ResumeError || detail.Resume.PublishAttempts != 2 || detail.Resume.ExecutionAttempt != 3 {
		t.Fatalf("unexpected resume diagnostic fields: %+v", detail.Resume)
	}
	if detail.Resume.LeaseOwner != delegation.ResumeLeaseOwner || detail.Resume.LeaseClaimedAt == nil || detail.Resume.LeaseHeartbeatAt == nil || detail.Resume.LeaseExpiresAt == nil {
		t.Fatalf("unexpected resume lease fields: %+v", detail.Resume)
	}
	var rawDetail map[string]json.RawMessage
	if err := json.Unmarshal(getEnv.Data, &rawDetail); err != nil {
		t.Fatalf("parse raw run detail failed: %v", err)
	}
	if _, nested := rawDetail["Run"]; nested {
		t.Fatalf("run detail must keep Run fields flattened: %s", getW.Body.String())
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
