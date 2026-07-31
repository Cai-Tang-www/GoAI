package services

import (
	"GoAI/models"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type recordingRunPublisher struct {
	runIDs []string
	err    error
}

func (p *recordingRunPublisher) PublishRunExecute(_ context.Context, runID string) error {
	p.runIDs = append(p.runIDs, runID)
	return p.err
}

func setupRunTestService(t *testing.T) (*gorm.DB, *RunService, *recordingRunPublisher) {
	t.Helper()
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "_") + "?mode=memory&cache=shared"
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := gdb.AutoMigrate(&models.User{}, &models.Agent{}, &models.Workflow{}, &models.Run{}, &models.RunStep{}, &models.RunIdempotency{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	publisher := &recordingRunPublisher{}
	service, err := NewRunService(gdb, publisher)
	if err != nil {
		t.Fatalf("create run service failed: %v", err)
	}
	return gdb, service, publisher
}

func seedAgentWorkflow(t *testing.T, gdb *gorm.DB) (models.Agent, models.Workflow) {
	t.Helper()
	agent := models.Agent{
		AgentCode:   "agent_test",
		Name:        "Agent Test",
		Description: "for test",
		OwnerUserID: 1,
		Status:      models.AgentStatusActive,
	}
	if err := gdb.Create(&agent).Error; err != nil {
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
	if err := gdb.Create(&workflow).Error; err != nil {
		t.Fatalf("create workflow failed: %v", err)
	}
	return agent, workflow
}
func TestCreateRunSuccessAndQueued(t *testing.T) {
	gdb, service, publisher := setupRunTestService(t)
	seedAgentWorkflow(t, gdb)

	result, err := service.CreateRun(context.Background(), 1, CreateRunRequest{
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
	if result.Run.RunID == "" || len(publisher.runIDs) != 1 || publisher.runIDs[0] != result.Run.RunID {
		t.Fatalf("publisher not called with expected run id: run=%s published=%s", result.Run.RunID, publisher.runIDs)
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
	gdb, service, _ := setupRunTestService(t)
	seedAgentWorkflow(t, gdb)

	_, err := service.CreateRun(context.Background(), 1, CreateRunRequest{
		AgentCode:   "agent_test",
		TriggerType: "api",
		Input:       []byte(`{"prompt":`),
	})
	if err == nil {
		t.Fatal("expected invalid json error, got nil")
	}
}

func TestCreateRunDispatchFailMarksRunFailed(t *testing.T) {
	gdb, service, publisher := setupRunTestService(t)
	seedAgentWorkflow(t, gdb)

	publisher.err = errors.New("kafka down")

	result, err := service.CreateRun(context.Background(), 1, CreateRunRequest{
		AgentCode:   "agent_test",
		TriggerType: "api",
		Input:       []byte(`{"prompt":"hello"}`),
	})
	if err == nil {
		t.Fatal("expected dispatch error but got nil")
	}
	var saved models.Run
	if dbErr := gdb.Where("run_id = ?", result.Run.RunID).First(&saved).Error; dbErr != nil {
		t.Fatalf("query run failed: %v", dbErr)
	}
	if saved.Status != models.RunStatusFailed {
		t.Fatalf("expected run failed status, got %s", saved.Status)
	}
}

func TestCreateRunIdempotencyReturnsSameRun(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	seedAgentWorkflow(t, gdb)

	req := CreateRunRequest{
		AgentCode:      "agent_test",
		TriggerType:    "api",
		Input:          []byte(`{"prompt":"hello"}`),
		IdempotencyKey: "create-key-1",
	}
	first, err := service.CreateRun(context.Background(), 1, req)
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	second, err := service.CreateRun(context.Background(), 1, req)
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
	gdb, service, _ := setupRunTestService(t)
	seedAgentWorkflow(t, gdb)

	firstReq := CreateRunRequest{
		AgentCode:      "agent_test",
		TriggerType:    "api",
		Input:          []byte(`{"prompt":"hello"}`),
		IdempotencyKey: "create-key-2",
	}
	if _, err := service.CreateRun(context.Background(), 1, firstReq); err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	secondReq := firstReq
	secondReq.Input = []byte(`{"prompt":"changed"}`)
	_, err := service.CreateRun(context.Background(), 1, secondReq)
	if err == nil {
		t.Fatal("expected conflict error, got nil")
	}
	if !errors.Is(err, ErrIdempotencyKeyReused()) {
		t.Fatalf("expected idempotency reuse error, got %v", err)
	}
}

func TestHandleRunExecuteMessageSuccess(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, gdb)

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
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	service.stepRetryBackoffs = []time.Duration{0, 0, 0}

	err := service.HandleRunExecute(context.Background(), run.RunID)
	if err != nil {
		t.Fatalf("handle run message failed: %v", err)
	}

	var saved models.Run
	if err := gdb.Where("run_id = ?", run.RunID).First(&saved).Error; err != nil {
		t.Fatalf("query run failed: %v", err)
	}
	if saved.Status != models.RunStatusSuccess {
		t.Fatalf("expected run success, got %s", saved.Status)
	}
	var steps []models.RunStep
	if err := gdb.Where("run_id = ?", run.RunID).Find(&steps).Error; err != nil {
		t.Fatalf("query steps failed: %v", err)
	}
	if len(steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(steps))
	}
}

func TestClaimRunForExecutionIsAtomic(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, gdb)

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
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	first, claimed, err := service.claimRunForExecution(context.Background(), run.RunID)
	if err != nil {
		t.Fatalf("first claim failed: %v", err)
	}
	if !claimed || first.Status != models.RunStatusRunning {
		t.Fatalf("expected first claim to succeed, got claimed=%v status=%s", claimed, first.Status)
	}

	second, claimed, err := service.claimRunForExecution(context.Background(), run.RunID)
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
	gdb, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, gdb)

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
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	service.stepRetryBackoffs = []time.Duration{0, 0, 0}

	if err := service.HandleRunExecute(context.Background(), run.RunID); err != nil {
		t.Fatalf("first handle failed: %v", err)
	}
	if err := service.HandleRunExecute(context.Background(), run.RunID); err != nil {
		t.Fatalf("second handle should be noop, got %v", err)
	}

	var steps []models.RunStep
	if err := gdb.Where("run_id = ?", run.RunID).Find(&steps).Error; err != nil {
		t.Fatalf("query steps failed: %v", err)
	}
	if len(steps) != 2 {
		t.Fatalf("expected duplicate consume to keep single execution, got %d steps", len(steps))
	}
}

func TestHandleRunExecuteMessageFailsRunWhenTransactionErrorsAfterClaim(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, gdb)

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
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	callbackName := "test:run-current-step-update-error"
	injectOnce := true
	if err := gdb.Callback().Update().Before("gorm:update").Register(callbackName, func(tx *gorm.DB) {
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
		_ = gdb.Callback().Update().Remove(callbackName)
	}()

	service.stepRetryBackoffs = []time.Duration{0, 0, 0}

	err := service.HandleRunExecute(context.Background(), run.RunID)
	if err == nil {
		t.Fatal("expected transaction error, got nil")
	}

	var saved models.Run
	if err := gdb.Where("run_id = ?", run.RunID).First(&saved).Error; err != nil {
		t.Fatalf("query run failed: %v", err)
	}
	if saved.Status != models.RunStatusFailed {
		t.Fatalf("expected run failed after transaction error, got %s", saved.Status)
	}
	if !strings.Contains(saved.ErrorMessage, "forced current_step update failure") {
		t.Fatalf("expected error message to be persisted, got %q", saved.ErrorMessage)
	}
}

func TestNewRunServiceRejectsMissingDependencies(t *testing.T) {
	publisher := RunEventPublisherFunc(func(context.Context, string) error { return nil })
	if _, err := NewRunService(nil, publisher); err == nil {
		t.Fatal("expected nil database error")
	}

	gdb, _, _ := setupRunTestService(t)
	if _, err := NewRunService(gdb, nil); err == nil {
		t.Fatal("expected nil publisher error")
	}
}

func TestRunServiceInstancesKeepDatabaseAndPublisherIsolated(t *testing.T) {
	openDatabase := func(suffix string) *gorm.DB {
		dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "_") + "_" + suffix + "?mode=memory&cache=shared"
		gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
		if err != nil {
			t.Fatalf("open sqlite %s failed: %v", suffix, err)
		}
		if err := gdb.AutoMigrate(&models.User{}, &models.Agent{}, &models.Workflow{}, &models.Run{}, &models.RunStep{}, &models.RunIdempotency{}); err != nil {
			t.Fatalf("auto migrate %s failed: %v", suffix, err)
		}
		return gdb
	}

	databaseOne := openDatabase("one")
	databaseTwo := openDatabase("two")
	seedAgentWorkflow(t, databaseOne)
	seedAgentWorkflow(t, databaseTwo)
	publisherOne := &recordingRunPublisher{}
	publisherTwo := &recordingRunPublisher{}
	serviceOne, err := NewRunService(databaseOne, publisherOne)
	if err != nil {
		t.Fatalf("create first service failed: %v", err)
	}
	serviceTwo, err := NewRunService(databaseTwo, publisherTwo)
	if err != nil {
		t.Fatalf("create second service failed: %v", err)
	}

	result, err := serviceOne.CreateRun(context.Background(), 1, CreateRunRequest{
		AgentCode: "agent_test",
		Input:     []byte(`{"prompt":"isolated"}`),
	})
	if err != nil {
		t.Fatalf("create run with first service failed: %v", err)
	}
	if len(publisherOne.runIDs) != 1 || publisherOne.runIDs[0] != result.Run.RunID {
		t.Fatalf("first publisher received unexpected calls: %v", publisherOne.runIDs)
	}
	if len(publisherTwo.runIDs) != 0 {
		t.Fatalf("second publisher should not be called: %v", publisherTwo.runIDs)
	}

	var firstCount int64
	if err := databaseOne.Model(&models.Run{}).Count(&firstCount).Error; err != nil {
		t.Fatalf("count first database runs failed: %v", err)
	}
	var secondCount int64
	if err := databaseTwo.Model(&models.Run{}).Count(&secondCount).Error; err != nil {
		t.Fatalf("count second database runs failed: %v", err)
	}
	if firstCount != 1 || secondCount != 0 {
		t.Fatalf("run databases leaked state: first=%d second=%d", firstCount, secondCount)
	}

	if _, err := serviceTwo.GetRunByRunID(context.Background(), 1, false, result.Run.RunID); !errors.Is(err, ErrRunNotFound()) {
		t.Fatalf("second service unexpectedly found first run: %v", err)
	}
}
