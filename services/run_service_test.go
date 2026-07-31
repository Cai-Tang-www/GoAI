package services

import (
	"GoAI/db"
	"GoAI/kafka"
	"GoAI/models"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupRunTestDB(t *testing.T) {
	t.Helper()
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "_") + "?mode=memory&cache=shared"
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := gdb.AutoMigrate(&models.User{}, &models.Agent{}, &models.Workflow{}, &models.Run{}, &models.RunStep{}, &models.RunIdempotency{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	db.DB = gdb
}

func seedAgentWorkflow(t *testing.T) (models.Agent, models.Workflow) {
	t.Helper()
	agent := models.Agent{
		AgentCode:   "agent_test",
		Name:        "Agent Test",
		Description: "for test",
		OwnerUserID: 1,
		Status:      models.AgentStatusActive,
	}
	if err := db.DB.Create(&agent).Error; err != nil {
		t.Fatalf("create agent failed: %v", err)
	}
	workflow := models.Workflow{
		AgentID:        agent.ID,
		Version:        1,
		DefinitionJSON: `{"entry_node":"planner","nodes":[{"key":"planner","type":"planner"},{"key":"tool1","type":"tool"}],"edges":[{"from":"planner","to":"tool1"}]}`,
		Checksum:       "abc",
		IsActive:       true,
		CreatedBy:      1,
	}
	if err := db.DB.Create(&workflow).Error; err != nil {
		t.Fatalf("create workflow failed: %v", err)
	}
	return agent, workflow
}

func TestCreateRunSuccessAndQueued(t *testing.T) {
	setupRunTestDB(t)
	seedAgentWorkflow(t)

	origPublisher := publishRunExecuteEvent
	defer func() { publishRunExecuteEvent = origPublisher }()
	var publishedRunID string
	publishRunExecuteEvent = func(ctx context.Context, runID string) error {
		publishedRunID = runID
		return nil
	}

	result, err := CreateRun(context.Background(), 1, CreateRunRequest{
		AgentCode:   "agent_test",
		TriggerType: "api",
		Input:       []byte(`{"prompt":"hello"}`),
	})
	if err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	if result.IdempotentHit {
		t.Fatal("expected first create to miss idempotency")
	}
	if result.Run.Status != models.RunStatusQueued {
		t.Fatalf("expected queued status, got %s", result.Run.Status)
	}
	if result.Run.RunID == "" || publishedRunID != result.Run.RunID {
		t.Fatalf("publisher not called with expected run id: run=%s published=%s", result.Run.RunID, publishedRunID)
	}
}

func TestCreateRunValidation(t *testing.T) {
	if err := ValidateCreateRunRequest(CreateRunRequest{AgentCode: "", WorkflowVersion: -1, TriggerType: "bad"}); err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if err := ValidateCreateRunRequest(CreateRunRequest{AgentCode: "agent", TriggerType: "api", Input: []byte(`{"prompt":"ok"}`)}); err != nil {
		t.Fatalf("expected validation success, got %v", err)
	}
}

func TestCreateRunRejectsInvalidInputJSON(t *testing.T) {
	setupRunTestDB(t)
	seedAgentWorkflow(t)

	_, err := CreateRun(context.Background(), 1, CreateRunRequest{
		AgentCode:   "agent_test",
		TriggerType: "api",
		Input:       []byte(`{"prompt":`),
	})
	if err == nil {
		t.Fatal("expected invalid json error, got nil")
	}
}

func TestCreateRunDispatchFailMarksRunFailed(t *testing.T) {
	setupRunTestDB(t)
	seedAgentWorkflow(t)

	origPublisher := publishRunExecuteEvent
	defer func() { publishRunExecuteEvent = origPublisher }()
	publishRunExecuteEvent = func(ctx context.Context, runID string) error {
		return errors.New("kafka down")
	}

	result, err := CreateRun(context.Background(), 1, CreateRunRequest{
		AgentCode:   "agent_test",
		TriggerType: "api",
		Input:       []byte(`{"prompt":"hello"}`),
	})
	if err == nil {
		t.Fatal("expected dispatch error but got nil")
	}
	var saved models.Run
	if dbErr := db.DB.Where("run_id = ?", result.Run.RunID).First(&saved).Error; dbErr != nil {
		t.Fatalf("query run failed: %v", dbErr)
	}
	if saved.Status != models.RunStatusFailed {
		t.Fatalf("expected run failed status, got %s", saved.Status)
	}
}

func TestCreateRunIdempotencyReturnsSameRun(t *testing.T) {
	setupRunTestDB(t)
	seedAgentWorkflow(t)

	origPublisher := publishRunExecuteEvent
	defer func() { publishRunExecuteEvent = origPublisher }()
	publishRunExecuteEvent = func(ctx context.Context, runID string) error {
		return nil
	}

	req := CreateRunRequest{
		AgentCode:      "agent_test",
		TriggerType:    "api",
		Input:          []byte(`{"prompt":"hello"}`),
		IdempotencyKey: "create-key-1",
	}
	first, err := CreateRun(context.Background(), 1, req)
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	second, err := CreateRun(context.Background(), 1, req)
	if err != nil {
		t.Fatalf("second create failed: %v", err)
	}
	if !second.IdempotentHit {
		t.Fatal("expected second create to hit idempotency")
	}
	if second.Run.RunID != first.Run.RunID {
		t.Fatalf("expected same run id, got %s and %s", first.Run.RunID, second.Run.RunID)
	}
}

func TestCreateRunIdempotencyRejectsDifferentRequest(t *testing.T) {
	setupRunTestDB(t)
	seedAgentWorkflow(t)

	origPublisher := publishRunExecuteEvent
	defer func() { publishRunExecuteEvent = origPublisher }()
	publishRunExecuteEvent = func(ctx context.Context, runID string) error {
		return nil
	}

	firstReq := CreateRunRequest{
		AgentCode:      "agent_test",
		TriggerType:    "api",
		Input:          []byte(`{"prompt":"hello"}`),
		IdempotencyKey: "create-key-2",
	}
	if _, err := CreateRun(context.Background(), 1, firstReq); err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	secondReq := firstReq
	secondReq.Input = []byte(`{"prompt":"changed"}`)
	_, err := CreateRun(context.Background(), 1, secondReq)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
	if !errors.Is(err, ErrIdempotencyKeyReused()) {
		t.Fatalf("expected idempotency reuse error, got %v", err)
	}
}

func TestHandleRunExecuteMessageSuccess(t *testing.T) {
	setupRunTestDB(t)
	agent, workflow := seedAgentWorkflow(t)

	run := models.Run{
		RunID:       "run_test_success",
		ThreadID:    "thread-1",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "api",
		InputJSON:   `{"prompt":"hello"}`,
		Status:      models.RunStatusQueued,
	}
	if err := db.DB.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	origBackoffs := stepRetryBackoffs
	stepRetryBackoffs = []time.Duration{0, 0, 0}
	defer func() { stepRetryBackoffs = origBackoffs }()

	err := HandleRunExecuteMessage(context.Background(), kafka.RunExecuteMessage{RunID: run.RunID})
	if err != nil {
		t.Fatalf("handle run message failed: %v", err)
	}

	var saved models.Run
	if err := db.DB.Where("run_id = ?", run.RunID).First(&saved).Error; err != nil {
		t.Fatalf("query run failed: %v", err)
	}
	if saved.Status != models.RunStatusSuccess {
		t.Fatalf("expected run success, got %s", saved.Status)
	}
	var steps []models.RunStep
	if err := db.DB.Where("run_id = ?", run.RunID).Find(&steps).Error; err != nil {
		t.Fatalf("query steps failed: %v", err)
	}
	if len(steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(steps))
	}
}

func TestClaimRunForExecutionIsAtomic(t *testing.T) {
	setupRunTestDB(t)
	agent, workflow := seedAgentWorkflow(t)

	run := models.Run{
		RunID:       "run_claim_atomic",
		ThreadID:    "thread-claim",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "api",
		InputJSON:   `{"prompt":"hello"}`,
		Status:      models.RunStatusQueued,
	}
	if err := db.DB.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	first, claimed, err := claimRunForExecution(context.Background(), run.RunID)
	if err != nil {
		t.Fatalf("first claim failed: %v", err)
	}
	if !claimed || first.Status != models.RunStatusRunning {
		t.Fatalf("expected first claim to succeed, got claimed=%v status=%s", claimed, first.Status)
	}

	second, claimed, err := claimRunForExecution(context.Background(), run.RunID)
	if err != nil {
		t.Fatalf("second claim failed: %v", err)
	}
	if claimed {
		t.Fatalf("expected second claim to fail, got run=%+v", second)
	}
	if second.Status != models.RunStatusRunning {
		t.Fatalf("expected claimed run to remain running, got %s", second.Status)
	}
}

func TestHandleRunExecuteMessageDuplicateIsNoop(t *testing.T) {
	setupRunTestDB(t)
	agent, workflow := seedAgentWorkflow(t)

	run := models.Run{
		RunID:       "run_duplicate_noop",
		ThreadID:    "thread-dup",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "api",
		InputJSON:   `{"prompt":"hello"}`,
		Status:      models.RunStatusQueued,
	}
	if err := db.DB.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	origBackoffs := stepRetryBackoffs
	stepRetryBackoffs = []time.Duration{0, 0, 0}
	defer func() { stepRetryBackoffs = origBackoffs }()

	if err := HandleRunExecuteMessage(context.Background(), kafka.RunExecuteMessage{RunID: run.RunID}); err != nil {
		t.Fatalf("first handle failed: %v", err)
	}
	if err := HandleRunExecuteMessage(context.Background(), kafka.RunExecuteMessage{RunID: run.RunID}); err != nil {
		t.Fatalf("second handle should be noop, got %v", err)
	}

	var steps []models.RunStep
	if err := db.DB.Where("run_id = ?", run.RunID).Find(&steps).Error; err != nil {
		t.Fatalf("query steps failed: %v", err)
	}
	if len(steps) != 2 {
		t.Fatalf("expected duplicate consume to keep single execution, got %d steps", len(steps))
	}
}

func TestHandleRunExecuteMessageFailsRunWhenTransactionErrorsAfterClaim(t *testing.T) {
	setupRunTestDB(t)
	agent, workflow := seedAgentWorkflow(t)

	run := models.Run{
		RunID:       "run_claim_tx_error",
		ThreadID:    "thread-tx-error",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "api",
		InputJSON:   `{"prompt":"hello"}`,
		Status:      models.RunStatusQueued,
	}
	if err := db.DB.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	callbackName := "test:run-current-step-update-error"
	injectOnce := true
	if err := db.DB.Callback().Update().Before("gorm:update").Register(callbackName, func(tx *gorm.DB) {
		if !injectOnce || tx.Statement == nil || tx.Statement.Schema == nil {
			return
		}
		if tx.Statement.Schema.Name != "Run" {
			return
		}
		updates, ok := tx.Statement.Dest.(map[string]any)
		if !ok {
			return
		}
		if _, exists := updates["current_step"]; !exists {
			return
		}
		injectOnce = false
		tx.AddError(errors.New("forced current_step update failure"))
	}); err != nil {
		t.Fatalf("register callback failed: %v", err)
	}
	defer func() {
		_ = db.DB.Callback().Update().Remove(callbackName)
	}()

	origBackoffs := stepRetryBackoffs
	stepRetryBackoffs = []time.Duration{0, 0, 0}
	defer func() { stepRetryBackoffs = origBackoffs }()

	err := HandleRunExecuteMessage(context.Background(), kafka.RunExecuteMessage{RunID: run.RunID})
	if err == nil {
		t.Fatal("expected transaction error, got nil")
	}

	var saved models.Run
	if err := db.DB.Where("run_id = ?", run.RunID).First(&saved).Error; err != nil {
		t.Fatalf("query run failed: %v", err)
	}
	if saved.Status != models.RunStatusFailed {
		t.Fatalf("expected run failed after transaction error, got %s", saved.Status)
	}
	if !strings.Contains(saved.ErrorMessage, "forced current_step update failure") {
		t.Fatalf("expected error message to be persisted, got %q", saved.ErrorMessage)
	}
}
