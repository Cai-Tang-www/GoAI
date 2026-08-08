package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"GoAI/models"
	"GoAI/requestctx"

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
	gdb := openSQLiteTestDB(t)
	if err := gdb.AutoMigrate(&models.User{}, &models.Agent{}, &models.AgentCapability{}, &models.AgentEndpoint{}, &models.Workflow{}, &models.Thread{}, &models.Run{}, &models.RunStep{}, &models.RunInterrupt{}, &models.RunIdempotency{}, &models.Delegation{}, &models.DelegationGroup{}, &models.Message{}); err != nil {
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
		DefinitionJSON: `{"entry_node":"planner","nodes":[{"key":"planner","type":"planner"},{"key":"tool1","type":"noop"}],"edges":[{"from":"planner","to":"tool1"}]}`,
		Checksum:       "abc",
		IsActive:       true,
		CreatedBy:      1,
	}
	if err := gdb.Create(&workflow).Error; err != nil {
		t.Fatalf("create workflow failed: %v", err)
	}
	return agent, workflow
}
func TestCanonicalizeJSONPreservesLargeIntegers(t *testing.T) {
	got, err := canonicalizeJSON([]byte(`{"id":9007199254740993,"nested":{"value":9223372036854775807}}`))
	if err != nil {
		t.Fatalf("canonicalize JSON failed: %v", err)
	}
	want := `{"id":9007199254740993,"nested":{"value":9223372036854775807}}`
	if got != want {
		t.Fatalf("large integer precision changed: got=%s want=%s", got, want)
	}
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

func TestCreateRunReturnsStableMissingResourceErrors(t *testing.T) {
	t.Run("agent not found", func(t *testing.T) {
		_, service, _ := setupRunTestService(t)

		_, err := service.CreateRun(context.Background(), 1, CreateRunRequest{
			AgentCode:   "missing_agent",
			TriggerType: "api",
			Input:       []byte(`{"prompt":"hello"}`),
		})
		if !errors.Is(err, ErrAgentNotFound()) {
			t.Fatalf("expected agent not found, got %v", err)
		}
	})

	t.Run("workflow not found", func(t *testing.T) {
		gdb, service, _ := setupRunTestService(t)
		agent := models.Agent{
			AgentCode:   "agent_without_workflow",
			Name:        "Agent Without Workflow",
			OwnerUserID: 1,
			Status:      models.AgentStatusActive,
		}
		if err := gdb.Create(&agent).Error; err != nil {
			t.Fatalf("create agent failed: %v", err)
		}

		_, err := service.CreateRun(context.Background(), 1, CreateRunRequest{
			AgentCode:   agent.AgentCode,
			TriggerType: "api",
			Input:       []byte(`{"prompt":"hello"}`),
		})
		if !errors.Is(err, ErrWorkflowNotFound()) {
			t.Fatalf("expected workflow not found, got %v", err)
		}
	})
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

func TestCreateRunDispatchFailurePersistsAfterContextCancellation(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	seedAgentWorkflow(t, gdb)

	ctx, cancel := context.WithCancel(context.Background())
	service.publisher = RunEventPublisherFunc(func(context.Context, string) error {
		cancel()
		return context.Canceled
	})

	result, err := service.CreateRun(ctx, 1, CreateRunRequest{
		AgentCode:   "agent_test",
		TriggerType: "api",
		Input:       []byte(`{"prompt":"hello"}`),
	})
	if !errors.Is(err, ErrRunDispatchFailed()) {
		t.Fatalf("expected dispatch error, got %v", err)
	}
	var saved models.Run
	if dbErr := gdb.Where("run_id = ?", result.Run.RunID).First(&saved).Error; dbErr != nil {
		t.Fatalf("query run failed: %v", dbErr)
	}
	if saved.Status != models.RunStatusFailed || saved.FinishedAt == nil {
		t.Fatalf("cancelled dispatch left run unfinished: %+v", saved)
	}
}

func TestCreateRunDispatchSurvivesRequestCancellationAfterCommit(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	seedAgentWorkflow(t, gdb)

	ctx, cancel := context.WithCancel(context.Background())
	service.publisher = RunEventPublisherFunc(func(publishCtx context.Context, _ string) error {
		cancel()
		return publishCtx.Err()
	})

	result, err := service.CreateRun(ctx, 1, CreateRunRequest{
		AgentCode:   "agent_test",
		TriggerType: "api",
		Input:       []byte(`{"prompt":"hello"}`),
	})
	if err != nil {
		t.Fatalf("dispatch should survive request cancellation after commit: %v", err)
	}
	var saved models.Run
	if dbErr := gdb.Where("run_id = ?", result.Run.RunID).First(&saved).Error; dbErr != nil {
		t.Fatalf("query run failed: %v", dbErr)
	}
	if saved.Status != models.RunStatusQueued {
		t.Fatalf("request cancellation changed durable run status: %s", saved.Status)
	}
}
func TestReplayRunDispatchSurvivesRequestCancellationAfterCommit(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	seedAgentWorkflow(t, gdb)

	origin, err := service.CreateRun(context.Background(), 1, CreateRunRequest{
		AgentCode:   "agent_test",
		TriggerType: "api",
		Input:       []byte(`{"prompt":"hello"}`),
	})
	if err != nil {
		t.Fatalf("create origin run failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	service.publisher = RunEventPublisherFunc(func(publishCtx context.Context, _ string) error {
		cancel()
		return publishCtx.Err()
	})

	replay, err := service.ReplayRun(ctx, 1, false, origin.Run.RunID, "")
	if err != nil {
		t.Fatalf("replay dispatch should survive request cancellation after commit: %v", err)
	}
	var saved models.Run
	if dbErr := gdb.Where("run_id = ?", replay.Run.RunID).First(&saved).Error; dbErr != nil {
		t.Fatalf("query replay run failed: %v", dbErr)
	}
	if saved.Status != models.RunStatusQueued {
		t.Fatalf("request cancellation changed durable replay status: %s", saved.Status)
	}
}
func TestCreateRunRequestedIDWithIdempotencyBindsExistingRun(t *testing.T) {
	gdb, service, publisher := setupRunTestService(t)
	seedAgentWorkflow(t, gdb)

	req := CreateRunRequest{
		AgentCode:      "agent_test",
		ThreadID:       "thread-requested-idempotency",
		TriggerType:    "agui",
		Input:          []byte(`{"messages":[{"role":"user","content":"hello"}]}`),
		RequestedRunID: "run-requested-idempotency",
	}
	first, err := service.CreateRun(context.Background(), 1, req)
	if err != nil {
		t.Fatalf("create requested run failed: %v", err)
	}

	req.IdempotencyKey = "bind-existing-run"
	second, err := service.CreateRun(context.Background(), 1, req)
	if err != nil {
		t.Fatalf("bind idempotency to existing requested run failed: %v", err)
	}
	if !second.IdempotentHit || second.Run.RunID != first.Run.RunID || len(publisher.runIDs) != 1 {
		t.Fatalf("expected existing run binding without republish: first=%+v second=%+v published=%v", first, second, publisher.runIDs)
	}
	var mapping models.RunIdempotency
	if err := gdb.Where("idempotency_key = ?", req.IdempotencyKey).First(&mapping).Error; err != nil {
		t.Fatalf("load idempotency mapping failed: %v", err)
	}
	if mapping.RunID != first.Run.RunID {
		t.Fatalf("idempotency mapping points to %s, want %s", mapping.RunID, first.Run.RunID)
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

func TestHandleRunExecuteCommitsRunningStepBeforeNodeReturns(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, gdb)
	workflow.DefinitionJSON = `{"entry_node":"blocking","nodes":[{"key":"blocking","type":"noop"}],"edges":[]}`
	if err := gdb.Save(&workflow).Error; err != nil {
		t.Fatalf("update workflow failed: %v", err)
	}

	run := models.Run{
		RunID:       "run_observable_step",
		ThreadID:    "thread-observable",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "agui",
		InputJSON:   `{"prompt":"hello"}`,
		Status:      models.RunStatusQueued,
	}
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	service.executeNode = func(ctx context.Context, _ *models.Run, _ WorkflowNode, _ int) (string, error) {
		close(started)
		select {
		case <-release:
			return `{"message":"completed"}`, nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- service.HandleRunExecute(context.Background(), run.RunID)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("node executor did not start")
	}

	var step models.RunStep
	if err := gdb.Where("run_id = ? AND step_key = ?", run.RunID, "blocking").First(&step).Error; err != nil {
		t.Fatalf("running step was not visible while node executed: %v", err)
	}
	if step.Status != models.RunStepStatusRunning || step.StartedAt == nil {
		t.Fatalf("expected visible running step, got status=%s started_at=%v", step.Status, step.StartedAt)
	}

	var running models.Run
	if err := gdb.Where("run_id = ?", run.RunID).First(&running).Error; err != nil {
		t.Fatalf("query running run failed: %v", err)
	}
	if running.Status != models.RunStatusRunning || running.CurrentStep != "blocking" {
		t.Fatalf("expected observable run progress, got status=%s current_step=%s", running.Status, running.CurrentStep)
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("handle run failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("run did not finish after releasing executor")
	}
}

func TestHandleRunExecutePersistsResultMessage(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, gdb)
	workflow.DefinitionJSON = `{"entry_node":"respond","nodes":[{"key":"respond","type":"noop"}],"edges":[]}`
	if err := gdb.Save(&workflow).Error; err != nil {
		t.Fatalf("update workflow failed: %v", err)
	}

	run := models.Run{
		RunID:       "run_result_message",
		ThreadID:    "thread-result",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "agui",
		InputJSON:   `{"prompt":"hello"}`,
		Status:      models.RunStatusQueued,
	}
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	service.executeNode = func(context.Context, *models.Run, WorkflowNode, int) (string, error) {
		return `{"message":"hello from agent"}`, nil
	}

	if err := service.HandleRunExecute(context.Background(), run.RunID); err != nil {
		t.Fatalf("handle run failed: %v", err)
	}

	var message models.Message
	if err := gdb.Where("run_id = ? AND message_type = ?", run.RunID, models.MessageTypeResult).First(&message).Error; err != nil {
		t.Fatalf("query result message failed: %v", err)
	}
	if message.ThreadID != run.ThreadID || message.SenderType != models.MessageSenderAgent || message.SenderID != agent.AgentCode || message.ReceiverType != models.MessageSenderUser {
		t.Fatalf("unexpected result message routing: %+v", message)
	}
	if message.ContentJSON != `{"text":"hello from agent"}` || message.MetadataJSON != "{}" || message.Status != models.MessageStatusDelivered {
		t.Fatalf("unexpected result message payload: %+v", message)
	}
	wantMessageID := resultMessageID(run.RunID, "respond", 1)
	if message.MessageID != wantMessageID {
		t.Fatalf("expected deterministic result message id %q, got %q", wantMessageID, message.MessageID)
	}
}

func TestCompleteRunStepRollsBackSuccessWhenResultMessageFails(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, gdb)
	run := models.Run{
		RunID:       "run_result_message_failure",
		ThreadID:    "thread-result-message-failure",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "agui",
		InputJSON:   `{}`,
		Status:      models.RunStatusRunning,
	}
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	startedAt := time.Now().Add(-time.Second)
	step := models.RunStep{
		RunID:      run.RunID,
		StepKey:    "respond",
		StepType:   "tool",
		Attempt:    1,
		Status:     models.RunStepStatusRunning,
		InputJSON:  `{}`,
		OutputJSON: `{}`,
		StartedAt:  &startedAt,
	}
	if err := gdb.Create(&step).Error; err != nil {
		t.Fatalf("create run step failed: %v", err)
	}

	callbackName := "test:result-message-create-error"
	if err := gdb.Callback().Create().Before("gorm:create").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement != nil && tx.Statement.Schema != nil && tx.Statement.Schema.Name == "Message" {
			tx.AddError(errors.New("forced result message create failure"))
		}
	}); err != nil {
		t.Fatalf("register callback failed: %v", err)
	}
	defer func() {
		_ = gdb.Callback().Create().Remove(callbackName)
	}()

	finishedAt := time.Now()
	err := service.completeRunStep(context.Background(), &run, &step, `{"message":"hello"}`, 10, &finishedAt)
	if err == nil || !strings.Contains(err.Error(), "forced result message create failure") {
		t.Fatalf("expected result message create failure, got %v", err)
	}

	var savedStep models.RunStep
	if err := gdb.First(&savedStep, step.ID).Error; err != nil {
		t.Fatalf("query run step failed: %v", err)
	}
	if savedStep.Status != models.RunStepStatusRunning || savedStep.OutputJSON != "{}" || savedStep.FinishedAt != nil {
		t.Fatalf("step success should roll back with result message: %+v", savedStep)
	}
	if step.Status != models.RunStepStatusRunning {
		t.Fatalf("in-memory step status should be restored after rollback, got %s", step.Status)
	}
	var messageCount int64
	if err := gdb.Model(&models.Message{}).Where("run_id = ?", run.RunID).Count(&messageCount).Error; err != nil {
		t.Fatalf("count result messages failed: %v", err)
	}
	if messageCount != 0 {
		t.Fatalf("expected no result message after rollback, got %d", messageCount)
	}
}

func TestHandleRunExecuteFailsRunWhenResultMessageCannotPersist(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, gdb)
	workflow.DefinitionJSON = `{"entry_node":"respond","nodes":[{"key":"respond","type":"noop"}],"edges":[]}`
	if err := gdb.Save(&workflow).Error; err != nil {
		t.Fatalf("update workflow failed: %v", err)
	}
	run := models.Run{
		RunID:       "run_result_message_worker_failure",
		ThreadID:    "thread-result-message-worker-failure",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "agui",
		InputJSON:   `{}`,
		Status:      models.RunStatusQueued,
	}
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	service.executeNode = func(context.Context, *models.Run, WorkflowNode, int) (string, error) {
		return `{"message":"hello"}`, nil
	}

	callbackName := "test:worker-result-message-create-error"
	if err := gdb.Callback().Create().Before("gorm:create").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement != nil && tx.Statement.Schema != nil && tx.Statement.Schema.Name == "Message" {
			tx.AddError(errors.New("forced worker result message create failure"))
		}
	}); err != nil {
		t.Fatalf("register callback failed: %v", err)
	}
	defer func() {
		_ = gdb.Callback().Create().Remove(callbackName)
	}()

	err := service.HandleRunExecute(context.Background(), run.RunID)
	if err == nil || !strings.Contains(err.Error(), "forced worker result message create failure") {
		t.Fatalf("expected result message persistence error, got %v", err)
	}
	var savedRun models.Run
	if err := gdb.Where("run_id = ?", run.RunID).First(&savedRun).Error; err != nil {
		t.Fatalf("query run failed: %v", err)
	}
	if savedRun.Status != models.RunStatusFailed || savedRun.FinishedAt == nil {
		t.Fatalf("run should fail after result message persistence error: %+v", savedRun)
	}
	var savedStep models.RunStep
	if err := gdb.Where("run_id = ?", run.RunID).First(&savedStep).Error; err != nil {
		t.Fatalf("query run step failed: %v", err)
	}
	if savedStep.Status != models.RunStepStatusFailed || savedStep.FinishedAt == nil {
		t.Fatalf("run step should fail after result message persistence error: %+v", savedStep)
	}
	var messageCount int64
	if err := gdb.Model(&models.Message{}).Where("run_id = ?", run.RunID).Count(&messageCount).Error; err != nil {
		t.Fatalf("count result messages failed: %v", err)
	}
	if messageCount != 0 {
		t.Fatalf("expected no result message after persistence failure, got %d", messageCount)
	}
}

func TestHandleRunExecuteRejectsInvalidNodeOutputJSON(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, gdb)
	workflow.DefinitionJSON = `{"entry_node":"respond","nodes":[{"key":"respond","type":"noop"}],"edges":[]}`
	if err := gdb.Save(&workflow).Error; err != nil {
		t.Fatalf("update workflow failed: %v", err)
	}

	run := models.Run{
		RunID:       "run_invalid_output",
		ThreadID:    "thread-invalid-output",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "agui",
		InputJSON:   `{}`,
		Status:      models.RunStatusQueued,
	}
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	service.stepRetryBackoffs = []time.Duration{0, 0, 0}
	service.executeNode = func(context.Context, *models.Run, WorkflowNode, int) (string, error) {
		return "not-json", nil
	}

	err := service.HandleRunExecute(context.Background(), run.RunID)
	if err == nil || !strings.Contains(err.Error(), "node output must be valid JSON") {
		t.Fatalf("expected invalid output failure, got %v", err)
	}

	var saved models.Run
	if err := gdb.Where("run_id = ?", run.RunID).First(&saved).Error; err != nil {
		t.Fatalf("query failed run: %v", err)
	}
	if saved.Status != models.RunStatusFailed {
		t.Fatalf("invalid output must fail run, got %s", saved.Status)
	}

	var steps []models.RunStep
	if err := gdb.Where("run_id = ?", run.RunID).Order("attempt ASC").Find(&steps).Error; err != nil {
		t.Fatalf("query failed steps: %v", err)
	}
	if len(steps) != maxNodeRetries+1 {
		t.Fatalf("unexpected attempt count %d", len(steps))
	}
	for _, step := range steps {
		if step.Status != models.RunStepStatusFailed || step.OutputJSON != "{}" {
			t.Fatalf("invalid output attempt was not closed with valid JSON: %+v", step)
		}
	}
}

func TestHandleRunExecuteRetriesAndFails(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, gdb)
	workflow.DefinitionJSON = `{"entry_node":"failing","nodes":[{"key":"failing","type":"noop"}],"edges":[]}`
	if err := gdb.Save(&workflow).Error; err != nil {
		t.Fatalf("update workflow failed: %v", err)
	}

	run := models.Run{
		RunID:       "run_retry_failure",
		ThreadID:    "thread-retry",
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
	service.executeNode = func(context.Context, *models.Run, WorkflowNode, int) (string, error) {
		return "", errors.New("executor unavailable")
	}

	err := service.HandleRunExecute(context.Background(), run.RunID)
	if err == nil || !strings.Contains(err.Error(), "executor unavailable") {
		t.Fatalf("expected executor failure, got %v", err)
	}

	var saved models.Run
	if err := gdb.Where("run_id = ?", run.RunID).First(&saved).Error; err != nil {
		t.Fatalf("query failed run: %v", err)
	}
	if saved.Status != models.RunStatusFailed || saved.RetryCount != 3 || saved.FinishedAt == nil {
		t.Fatalf("unexpected failed run: status=%s retries=%d finished_at=%v", saved.Status, saved.RetryCount, saved.FinishedAt)
	}

	var steps []models.RunStep
	if err := gdb.Where("run_id = ?", run.RunID).Order("attempt ASC").Find(&steps).Error; err != nil {
		t.Fatalf("query retry steps failed: %v", err)
	}
	if len(steps) != 4 {
		t.Fatalf("expected 1 initial attempt and 3 retries, got %d attempts", len(steps))
	}
	for index, step := range steps {
		if step.Attempt != index+1 || step.Status != models.RunStepStatusFailed || step.OutputJSON != "{}" || step.FinishedAt == nil {
			t.Fatalf("unexpected retry step %d: %+v", index, step)
		}
	}
}

type wrappedNonRetryableError struct{}

func (wrappedNonRetryableError) Error() string   { return "capability is not supported" }
func (wrappedNonRetryableError) Retryable() bool { return false }

func TestHandleRunExecuteDoesNotRetryWrappedNonRetryableError(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, gdb)
	run := models.Run{
		RunID:       "run_non_retryable",
		ThreadID:    "thread-non-retryable",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "a2a",
		InputJSON:   `{"prompt":"hello"}`,
		Status:      models.RunStatusQueued,
	}
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	attempts := 0
	service.stepRetryBackoffs = []time.Duration{0, 0, 0}
	service.executeNode = func(context.Context, *models.Run, WorkflowNode, int) (string, error) {
		attempts++
		return "", fmt.Errorf("calling target agent: %w", wrappedNonRetryableError{})
	}

	err := service.HandleRunExecute(context.Background(), run.RunID)
	if err == nil || !strings.Contains(err.Error(), "capability is not supported") {
		t.Fatalf("expected wrapped non-retryable error, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("non-retryable error executed %d attempts, want 1", attempts)
	}

	var saved models.Run
	if err := gdb.Where("run_id = ?", run.RunID).First(&saved).Error; err != nil {
		t.Fatalf("load failed run: %v", err)
	}
	if saved.Status != models.RunStatusFailed || saved.RetryCount != 0 {
		t.Fatalf("unexpected failed run: status=%s retries=%d", saved.Status, saved.RetryCount)
	}
	var stepCount int64
	if err := gdb.Model(&models.RunStep{}).Where("run_id = ?", run.RunID).Count(&stepCount).Error; err != nil {
		t.Fatalf("count run steps: %v", err)
	}
	if stepCount != 1 {
		t.Fatalf("run step count=%d, want 1", stepCount)
	}
}
func TestExecuteWorkflowNodeRejectsUnsupportedType(t *testing.T) {
	_, err := executeWorkflowNode(context.Background(), &models.Run{}, WorkflowNode{Key: "delegate", Type: "agent"}, 1)
	if err == nil || !strings.Contains(err.Error(), "unsupported workflow node type") {
		t.Fatalf("expected unsupported node error, got %v", err)
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

func TestHandleRunExecuteCancellationPersistsFailedRunAndStep(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, gdb)
	workflow.DefinitionJSON = `{"entry_node":"blocking","nodes":[{"key":"blocking","type":"noop"}],"edges":[]}`
	if err := gdb.Save(&workflow).Error; err != nil {
		t.Fatalf("update workflow failed: %v", err)
	}
	run := models.Run{
		RunID:       "run_cancelled_context",
		ThreadID:    "thread-cancelled-context",
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

	started := make(chan struct{})
	service.executeNode = func(ctx context.Context, _ *models.Run, _ WorkflowNode, _ int) (string, error) {
		close(started)
		<-ctx.Done()
		return "", ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- service.HandleRunExecute(ctx, run.RunID)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("executor did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled execution did not return")
	}

	var savedRun models.Run
	if err := gdb.Where("run_id = ?", run.RunID).First(&savedRun).Error; err != nil {
		t.Fatalf("query cancelled run failed: %v", err)
	}
	if savedRun.Status != models.RunStatusFailed || savedRun.FinishedAt == nil {
		t.Fatalf("cancelled run was not finalized: %+v", savedRun)
	}
	var step models.RunStep
	if err := gdb.Where("run_id = ?", run.RunID).First(&step).Error; err != nil {
		t.Fatalf("query cancelled step failed: %v", err)
	}
	if step.Status != models.RunStepStatusFailed || step.FinishedAt == nil {
		t.Fatalf("cancelled step was not finalized: %+v", step)
	}
}

func TestRunCancellationContextIsSharedAcrossDuplicateConsumers(t *testing.T) {
	_, service, _ := setupRunTestService(t)
	first, stopFirst := service.withRunCancellation(context.Background(), "run-shared-cancel")
	second, stopSecond := service.withRunCancellation(context.Background(), "run-shared-cancel")
	if first.Done() != second.Done() {
		t.Fatal("duplicate consumers must share one cancellation context")
	}
	stopSecond()
	select {
	case <-first.Done():
		t.Fatal("releasing one duplicate consumer cancelled the shared context")
	default:
	}
	service.cancelActiveRun("run-shared-cancel")
	select {
	case <-first.Done():
	case <-time.After(time.Second):
		t.Fatal("shared cancellation context was not cancelled")
	}
	stopFirst()
	service.executionMu.Lock()
	defer service.executionMu.Unlock()
	if _, ok := service.executionCancels["run-shared-cancel"]; ok {
		t.Fatal("run cancellation registration was not cleaned up")
	}
}

func TestStartRunStepRejectsCancelledRunUnderRunLock(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	run := models.Run{
		RunID:       "run_cancelled_before_step",
		AgentID:     1,
		WorkflowID:  1,
		UserID:      1,
		TriggerType: "a2a",
		InputJSON:   `{}`,
		Status:      models.RunStatusCancelled,
	}
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create cancelled run failed: %v", err)
	}
	_, err := service.startRunStep(context.Background(), &run, WorkflowNode{Key: "work", Type: "noop"}, 1)
	if err == nil {
		t.Fatal("creating a step for a cancelled run should fail")
	}
	var stepCount int64
	if err := gdb.Model(&models.RunStep{}).Where("run_id = ?", run.RunID).Count(&stepCount).Error; err != nil {
		t.Fatalf("count run steps failed: %v", err)
	}
	if stepCount != 0 {
		t.Fatalf("cancelled run created %d run steps", stepCount)
	}
}

func TestTransitionRunStatusUsesCompareAndSwap(t *testing.T) {
	gdb, _, _ := setupRunTestService(t)
	run := models.Run{
		RunID:       "run_transition_cas",
		AgentID:     1,
		WorkflowID:  1,
		UserID:      1,
		TriggerType: "api",
		InputJSON:   `{}`,
		Status:      models.RunStatusRunning,
	}
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	stale := run
	if err := gdb.Model(&models.Run{}).Where("run_id = ?", run.RunID).Update("status", models.RunStatusCancelled).Error; err != nil {
		t.Fatalf("cancel run failed: %v", err)
	}

	err := gdb.Transaction(func(tx *gorm.DB) error {
		return transitionRunStatus(context.Background(), tx, &stale, models.RunStatusSuccess, "")
	})
	if !errors.Is(err, errInvalidRunTransition) {
		t.Fatalf("expected run CAS conflict, got %v", err)
	}
	var saved models.Run
	if err := gdb.Where("run_id = ?", run.RunID).First(&saved).Error; err != nil {
		t.Fatalf("query run failed: %v", err)
	}
	if saved.Status != models.RunStatusCancelled || stale.Status != models.RunStatusRunning || stale.FinishedAt != nil {
		t.Fatalf("stale transition overwrote state: saved=%+v stale=%+v", saved, stale)
	}
}

func TestTransitionStepStatusUsesCompareAndSwap(t *testing.T) {
	gdb, _, _ := setupRunTestService(t)
	step := models.RunStep{
		RunID:     "run_step_transition_cas",
		StepKey:   "tool",
		StepType:  "tool",
		Attempt:   1,
		Status:    models.RunStepStatusRunning,
		InputJSON: `{}`,
	}
	if err := gdb.Create(&step).Error; err != nil {
		t.Fatalf("create step failed: %v", err)
	}
	stale := step
	if err := gdb.Model(&models.RunStep{}).Where("id = ?", step.ID).Update("status", models.RunStepStatusFailed).Error; err != nil {
		t.Fatalf("fail step failed: %v", err)
	}
	finishedAt := time.Now()
	err := gdb.Transaction(func(tx *gorm.DB) error {
		return transitionStepStatus(context.Background(), tx, &stale, models.RunStepStatusSuccess, `{}`, "", 1, &finishedAt)
	})
	if !errors.Is(err, errInvalidStepTransition) {
		t.Fatalf("expected step CAS conflict, got %v", err)
	}
	var saved models.RunStep
	if err := gdb.First(&saved, step.ID).Error; err != nil {
		t.Fatalf("query step failed: %v", err)
	}
	if saved.Status != models.RunStepStatusFailed || stale.Status != models.RunStepStatusRunning {
		t.Fatalf("stale step transition overwrote state: saved=%+v stale=%+v", saved, stale)
	}
}

func TestIncrementRunRetryRejectsNonRunningRun(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	run := models.Run{
		RunID:       "run_retry_terminal",
		AgentID:     1,
		WorkflowID:  1,
		UserID:      1,
		TriggerType: "api",
		InputJSON:   `{}`,
		Status:      models.RunStatusCancelled,
	}
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	if err := service.incrementRunRetry(context.Background(), run.RunID); !errors.Is(err, errInvalidRunTransition) {
		t.Fatalf("expected retry state conflict, got %v", err)
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
		gdb := openSQLiteTestDB(t, suffix)
		if err := gdb.AutoMigrate(&models.User{}, &models.Agent{}, &models.Workflow{}, &models.Thread{}, &models.Run{}, &models.RunStep{}, &models.RunInterrupt{}, &models.RunIdempotency{}, &models.Delegation{}, &models.DelegationGroup{}, &models.Message{}); err != nil {
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

type recordingAgentInvoker struct {
	requests []AgentInvocationRequest
	result   *AgentInvocationResult
	err      error
	invoke   func(AgentInvocationRequest) (*AgentInvocationResult, error)
}

func (f *recordingAgentInvoker) Invoke(_ context.Context, request AgentInvocationRequest) (*AgentInvocationResult, error) {
	f.requests = append(f.requests, request)
	if f.invoke != nil {
		return f.invoke(request)
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.result, nil
}

func TestHandleRunExecuteAgentNodeUsesA2AInvokerAndAggregatedInput(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	source, workflow := seedAgentWorkflow(t, gdb)
	workflow.DefinitionJSON = `{"entry_node":"planner","nodes":[{"key":"planner","type":"planner"},{"key":"delegate","type":"agent","config":{"target_agent":"writer","capability":"write","input_from":["planner"]}}],"edges":[{"from":"planner","to":"delegate"}]}`
	if err := gdb.Save(&workflow).Error; err != nil {
		t.Fatalf("update workflow failed: %v", err)
	}
	target := models.Agent{AgentCode: "writer", Name: "Writer", OwnerUserID: 1, Status: models.AgentStatusActive}
	if err := gdb.Create(&target).Error; err != nil {
		t.Fatalf("create target agent failed: %v", err)
	}
	targetWorkflow := models.Workflow{
		AgentID: target.ID, Version: 1,
		DefinitionJSON: `{"entry_node":"worker","nodes":[{"key":"worker","type":"noop"}],"edges":[]}`,
		Checksum:       "target-workflow", IsActive: true, CreatedBy: 1,
	}
	if err := gdb.Create(&targetWorkflow).Error; err != nil {
		t.Fatalf("create target workflow failed: %v", err)
	}
	capability := models.AgentCapability{
		AgentID: target.ID, CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeWorkflow,
		WorkflowID: &targetWorkflow.ID, Version: "1", InputSchemaJSON: "{}", OutputSchemaJSON: "{}", ConfigJSON: "{}", Status: models.AgentCapabilityStatusActive,
	}
	if err := gdb.Create(&capability).Error; err != nil {
		t.Fatalf("create capability failed: %v", err)
	}
	endpoint := models.AgentEndpoint{
		AgentID: target.ID, EndpointCode: "writer-local", Protocol: models.AgentEndpointProtocolA2A,
		Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1:18080/a2a/agents/writer",
		Status: models.AgentEndpointStatusActive,
	}
	if err := gdb.Create(&endpoint).Error; err != nil {
		t.Fatalf("create endpoint failed: %v", err)
	}
	invoker := &recordingAgentInvoker{result: &AgentInvocationResult{TaskID: "child-task", State: AgentInvocationStateCompleted, OutputJSON: `{"document":"ok"}`}}
	service.agentInvoker = invoker
	service.stepRetryBackoffs = []time.Duration{0, 0, 0}

	run := models.Run{
		RunID: "run_agent_node", ThreadID: "thread-agent", AgentID: source.ID, WorkflowID: workflow.ID,
		UserID: 1, TriggerType: "api", InputJSON: `{"prompt":"draft"}`, Status: models.RunStatusQueued,
	}
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	if err := service.HandleRunExecute(context.Background(), run.RunID); err != nil {
		t.Fatalf("handle run failed: %v", err)
	}
	if len(invoker.requests) != 1 {
		t.Fatalf("expected one A2A invocation, got %d", len(invoker.requests))
	}
	request := invoker.requests[0]
	if request.SourceAgentCode != source.AgentCode || request.TargetAgentCode != target.AgentCode || request.CapabilityCode != "write" {
		t.Fatalf("unexpected invocation routing: %+v", request)
	}
	if request.TaskID != stableA2AID("task", run.RunID, "delegate") || request.MessageID != stableA2AID("message", run.RunID, "delegate") {
		t.Fatalf("invocation ids are not deterministic: %+v", request)
	}
	if !strings.Contains(request.InputJSON, `"step_outputs"`) || !strings.Contains(request.InputJSON, `"planner"`) {
		t.Fatalf("expected aggregated input, got %s", request.InputJSON)
	}

	var steps []models.RunStep
	if err := gdb.Where("run_id = ?", run.RunID).Order("id ASC").Find(&steps).Error; err != nil {
		t.Fatalf("load run steps failed: %v", err)
	}
	if len(steps) != 2 || steps[1].StepKey != "delegate" || steps[1].InputJSON != request.InputJSON {
		t.Fatalf("agent step input was not persisted: %+v", steps)
	}
	var output map[string]any
	if err := json.Unmarshal([]byte(steps[1].OutputJSON), &output); err != nil {
		t.Fatalf("decode agent step output: %v", err)
	}
	if output["target_agent"] != target.AgentCode || output["capability"] != capability.CapabilityCode {
		t.Fatalf("unexpected agent step output: %+v", output)
	}
}

func TestHandleRunExecuteAgentNodeRoutesByRegistryCapability(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	source, workflow := seedAgentWorkflow(t, gdb)
	workflow.DefinitionJSON = `{"entry_node":"delegate","nodes":[{"key":"delegate","type":"agent","config":{"capability":"write","routing_policy":"registry"}}],"edges":[]}`
	if err := gdb.Save(&workflow).Error; err != nil {
		t.Fatalf("update workflow failed: %v", err)
	}
	createWorker := func(code string) {
		t.Helper()
		agent := models.Agent{AgentCode: code, Name: code, OwnerUserID: 1, Status: models.AgentStatusActive}
		if err := gdb.Create(&agent).Error; err != nil {
			t.Fatalf("create worker %s: %v", code, err)
		}
		workerWorkflow := models.Workflow{AgentID: agent.ID, Version: 1, DefinitionJSON: `{"entry_node":"worker","nodes":[{"key":"worker","type":"noop"}],"edges":[]}`, Checksum: code + "-checksum", IsActive: true, CreatedBy: 1}
		if err := gdb.Create(&workerWorkflow).Error; err != nil {
			t.Fatalf("create worker workflow %s: %v", code, err)
		}
		capability := models.AgentCapability{AgentID: agent.ID, CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeWorkflow, WorkflowID: &workerWorkflow.ID, Version: "1", Status: models.AgentCapabilityStatusActive}
		if err := gdb.Create(&capability).Error; err != nil {
			t.Fatalf("create worker capability %s: %v", code, err)
		}
		endpoint := models.AgentEndpoint{AgentID: agent.ID, EndpointCode: code + "-local", Protocol: models.AgentEndpointProtocolA2A, Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1/a2a/agents/" + code, Status: models.AgentEndpointStatusActive}
		if err := gdb.Create(&endpoint).Error; err != nil {
			t.Fatalf("create worker endpoint %s: %v", code, err)
		}
	}
	createWorker("writer-z")
	createWorker("writer-a")

	invoker := &recordingAgentInvoker{result: &AgentInvocationResult{TaskID: "dynamic-task", State: AgentInvocationStateCompleted, OutputJSON: `{"document":"ok"}`}}
	service.agentInvoker = invoker
	service.stepRetryBackoffs = []time.Duration{0, 0, 0}
	run := models.Run{RunID: "run_registry_route", ThreadID: "thread-registry-route", AgentID: source.ID, WorkflowID: workflow.ID, UserID: 1, TriggerType: "api", InputJSON: `{}`, Status: models.RunStatusQueued}
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	if err := service.HandleRunExecute(context.Background(), run.RunID); err != nil {
		t.Fatalf("handle dynamically routed run failed: %v", err)
	}
	if len(invoker.requests) != 1 || invoker.requests[0].TargetAgentCode != "writer-a" {
		t.Fatalf("router did not select deterministic worker: %+v", invoker.requests)
	}
	var step models.RunStep
	if err := gdb.Where("run_id = ? AND step_key = ? AND status = ?", run.RunID, "delegate", models.RunStepStatusSuccess).First(&step).Error; err != nil {
		t.Fatalf("load routed step: %v", err)
	}
	var output map[string]any
	if err := json.Unmarshal([]byte(step.OutputJSON), &output); err != nil {
		t.Fatalf("decode routed output: %v", err)
	}
	if output["routing_policy"] != "registry" || output["selection_reason"] != "registry:agent_code=writer-a;endpoint_code=writer-a-local" || output["workflow_version"] != float64(1) {
		t.Fatalf("route metadata was not persisted: %+v", output)
	}
}

func TestHandleRunResumeCompletesPreviousDelegationWhenNextAgentSuspends(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	source, workflow := seedAgentWorkflow(t, gdb)
	workflow.DefinitionJSON = `{"entry_node":"planner","nodes":[{"key":"planner","type":"planner"},{"key":"delegate_writer","type":"agent","config":{"target_agent":"writer","capability":"write"}},{"key":"delegate_reviewer","type":"agent","config":{"target_agent":"reviewer","capability":"review"}}],"edges":[{"from":"planner","to":"delegate_writer"},{"from":"delegate_writer","to":"delegate_reviewer"}]}`
	if err := gdb.Save(&workflow).Error; err != nil {
		t.Fatalf("update workflow failed: %v", err)
	}

	for _, target := range []struct {
		code       string
		capability string
	}{
		{code: "writer", capability: "write"},
		{code: "reviewer", capability: "review"},
	} {
		agent := models.Agent{AgentCode: target.code, Name: target.code, OwnerUserID: 1, Status: models.AgentStatusActive}
		if err := gdb.Create(&agent).Error; err != nil {
			t.Fatalf("create target agent %s failed: %v", target.code, err)
		}
		workflow := models.Workflow{
			AgentID: agent.ID, Version: 1,
			DefinitionJSON: `{"entry_node":"worker","nodes":[{"key":"worker","type":"noop"}],"edges":[]}`,
			Checksum:       target.code + "-workflow", IsActive: true, CreatedBy: 1,
		}
		if err := gdb.Create(&workflow).Error; err != nil {
			t.Fatalf("create target workflow %s failed: %v", target.code, err)
		}
		capability := models.AgentCapability{
			AgentID: agent.ID, CapabilityCode: target.capability, Name: target.capability,
			CapabilityType: models.AgentCapabilityTypeWorkflow, WorkflowID: &workflow.ID, Version: "1", Status: models.AgentCapabilityStatusActive,
		}
		if err := gdb.Create(&capability).Error; err != nil {
			t.Fatalf("create target capability %s failed: %v", target.capability, err)
		}
		endpoint := models.AgentEndpoint{
			AgentID: agent.ID, EndpointCode: target.code + "-local", Protocol: models.AgentEndpointProtocolA2A,
			Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1:18080/a2a/agents/" + target.code,
			Status: models.AgentEndpointStatusActive,
		}
		if err := gdb.Create(&endpoint).Error; err != nil {
			t.Fatalf("create target endpoint %s failed: %v", target.code, err)
		}
	}

	invoker := &recordingAgentInvoker{}
	invoker.invoke = func(request AgentInvocationRequest) (*AgentInvocationResult, error) {
		return &AgentInvocationResult{
			TaskID: request.TaskID, State: AgentInvocationStateAccepted,
			OutputJSON: `{"accepted":true}`, NotificationToken: "token-" + request.TargetAgentCode,
		}, nil
	}
	service.agentInvoker = invoker
	service.stepRetryBackoffs = []time.Duration{0, 0, 0}

	run := models.Run{
		RunID: "run_agent_chain", ThreadID: "thread-agent-chain", AgentID: source.ID, WorkflowID: workflow.ID,
		UserID: 1, TriggerType: "api", InputJSON: `{"prompt":"draft and review"}`, Status: models.RunStatusQueued,
	}
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	if err := service.HandleRunExecute(context.Background(), run.RunID); err != nil {
		t.Fatalf("initial run execution failed: %v", err)
	}

	var firstDelegation models.Delegation
	if err := gdb.Where("parent_run_id = ? AND parent_step_key = ?", run.RunID, "delegate_writer").First(&firstDelegation).Error; err != nil {
		t.Fatalf("load first delegation failed: %v", err)
	}
	finishedAt := time.Now()
	if err := gdb.Model(&models.RunStep{}).
		Where("run_id = ? AND step_key = ? AND status = ?", run.RunID, "delegate_writer", models.RunStepStatusWaitingExternal).
		Updates(map[string]any{
			"status": models.RunStepStatusSuccess, "output_json": `{"document":"draft"}`,
			"finished_at": finishedAt, "error_message": "",
		}).Error; err != nil {
		t.Fatalf("apply first callback step result failed: %v", err)
	}
	if err := gdb.Model(&models.Delegation{}).Where("id = ?", firstDelegation.ID).Updates(map[string]any{
		"status": models.DelegationStatusSucceeded, "output_json": `{"document":"draft"}`,
		"callback_event_hash": "writer-terminal-event", "callback_received_at": finishedAt,
		"resume_status": models.DelegationResumeStatusPublished,
	}).Error; err != nil {
		t.Fatalf("apply first callback delegation result failed: %v", err)
	}

	if err := service.HandleRunResume(context.Background(), run.RunID, firstDelegation.DelegationID); err != nil {
		t.Fatalf("resume run failed: %v", err)
	}

	var storedRun models.Run
	if err := gdb.Where("run_id = ?", run.RunID).First(&storedRun).Error; err != nil {
		t.Fatalf("reload parent run failed: %v", err)
	}
	if storedRun.Status != models.RunStatusWaitingExternal || storedRun.CurrentStep != "delegate_reviewer" {
		t.Fatalf("parent run did not suspend on reviewer: %+v", storedRun)
	}
	if len(invoker.requests) != 2 || invoker.requests[0].TargetAgentCode != "writer" || invoker.requests[1].TargetAgentCode != "reviewer" {
		t.Fatalf("unexpected A2A invocation sequence: %+v", invoker.requests)
	}

	var completedFirst models.Delegation
	if err := gdb.First(&completedFirst, firstDelegation.ID).Error; err != nil {
		t.Fatalf("reload first delegation failed: %v", err)
	}
	if completedFirst.ResumeStatus != models.DelegationResumeStatusCompleted {
		t.Fatalf("first delegation resume was not completed: %+v", completedFirst)
	}
	var secondDelegation models.Delegation
	if err := gdb.Where("parent_run_id = ? AND parent_step_key = ?", run.RunID, "delegate_reviewer").First(&secondDelegation).Error; err != nil {
		t.Fatalf("load second delegation failed: %v", err)
	}
	if secondDelegation.Status != models.DelegationStatusAccepted || secondDelegation.ResumeStatus != models.DelegationResumeStatusNone {
		t.Fatalf("unexpected second delegation state: %+v", secondDelegation)
	}
	var steps []models.RunStep
	if err := gdb.Where("run_id = ?", run.RunID).Order("id ASC").Find(&steps).Error; err != nil {
		t.Fatalf("load run steps failed: %v", err)
	}
	attempts := make(map[string]int)
	statuses := make(map[string]string)
	for _, step := range steps {
		attempts[step.StepKey]++
		statuses[step.StepKey] = step.Status
	}
	if attempts["planner"] != 1 || attempts["delegate_writer"] != 1 || attempts["delegate_reviewer"] != 1 {
		t.Fatalf("workflow prefix was replayed or a node was skipped: attempts=%v steps=%+v", attempts, steps)
	}
	if statuses["delegate_writer"] != models.RunStepStatusSuccess || statuses["delegate_reviewer"] != models.RunStepStatusWaitingExternal {
		t.Fatalf("unexpected agent step states: %v", statuses)
	}
}
func TestHandleRunExecuteAgentNodeDoesNotInvokeWithoutInvoker(t *testing.T) {
	gdb, service, _ := setupRunTestService(t)
	source, workflow := seedAgentWorkflow(t, gdb)
	workflow.DefinitionJSON = `{"entry_node":"delegate","nodes":[{"key":"delegate","type":"agent","config":{"target_agent":"writer","capability":"write"}}],"edges":[]}`
	if err := gdb.Save(&workflow).Error; err != nil {
		t.Fatalf("update workflow failed: %v", err)
	}
	target := models.Agent{AgentCode: "writer", Name: "Writer", OwnerUserID: 1, Status: models.AgentStatusActive}
	if err := gdb.Create(&target).Error; err != nil {
		t.Fatalf("create target agent failed: %v", err)
	}
	targetWorkflow := models.Workflow{AgentID: target.ID, Version: 1, DefinitionJSON: `{"entry_node":"worker","nodes":[{"key":"worker","type":"noop"}],"edges":[]}`, Checksum: "target-workflow", IsActive: true, CreatedBy: 1}
	if err := gdb.Create(&targetWorkflow).Error; err != nil {
		t.Fatalf("create target workflow failed: %v", err)
	}
	if err := gdb.Create(&models.AgentCapability{AgentID: target.ID, CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeWorkflow, WorkflowID: &targetWorkflow.ID, Version: "1", Status: models.AgentCapabilityStatusActive}).Error; err != nil {
		t.Fatalf("create capability failed: %v", err)
	}
	run := models.Run{RunID: "run_agent_no_invoker", ThreadID: "thread-agent", AgentID: source.ID, WorkflowID: workflow.ID, UserID: 1, TriggerType: "api", InputJSON: `{}`, Status: models.RunStatusQueued}
	if err := gdb.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	if err := service.HandleRunExecute(context.Background(), run.RunID); err == nil || !strings.Contains(err.Error(), "A2A agent invoker") {
		t.Fatalf("expected missing invoker error, got %v", err)
	}
}

func setupRunLoopTestService(t *testing.T) (*gorm.DB, *RunService, *recordingRunPublisher) {
	t.Helper()
	gdb := openSQLiteTestDB(t)
	if err := gdb.AutoMigrate(
		&models.User{},
		&models.Agent{},
		&models.AgentCapability{},
		&models.AgentEndpoint{},
		&models.Workflow{},
		&models.Thread{},
		&models.Run{},
		&models.RunStep{},
		&models.RunInterrupt{},
		&models.RunIdempotency{},
		&models.Delegation{},
		&models.DelegationGroup{},
		&models.Message{},
		&models.LoopRecord{},
		&models.LoopEvaluation{},
	); err != nil {
		t.Fatalf("auto migrate run loop models failed: %v", err)
	}
	publisher := &recordingRunPublisher{}
	loopService, err := NewLoopService(gdb)
	if err != nil {
		t.Fatalf("create loop service failed: %v", err)
	}
	service, err := NewRunService(gdb, publisher, WithLoopService(loopService))
	if err != nil {
		t.Fatalf("create run service failed: %v", err)
	}
	return gdb, service, publisher
}

func TestRunExecutionPersistsRootAndStepLoops(t *testing.T) {
	gdb, service, _ := setupRunLoopTestService(t)
	seedAgentWorkflow(t, gdb)
	service.stepRetryBackoffs = []time.Duration{0, 0, 0}

	ctx := requestctx.WithTraceID(context.Background(), "trace-run-loop")
	result, err := service.CreateRun(ctx, 1, CreateRunRequest{
		AgentCode:   "agent_test",
		TriggerType: "api",
		Input:       []byte(`{"prompt":"hello"}`),
	})
	if err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	if result.Run.LoopID == nil || *result.Run.LoopID == "" {
		t.Fatal("create run did not assign root loop")
	}

	var root models.LoopRecord
	if err := gdb.Where("loop_id = ?", *result.Run.LoopID).First(&root).Error; err != nil {
		t.Fatalf("load root loop failed: %v", err)
	}
	if root.Status != models.LoopStatusRunning || root.TraceID != "trace-run-loop" || root.LoopType != models.LoopTypeRun {
		t.Fatalf("unexpected root loop before execution: %+v", root)
	}

	if err := service.HandleRunExecute(ctx, result.Run.RunID); err != nil {
		t.Fatalf("execute run failed: %v", err)
	}

	if err := gdb.Where("loop_id = ?", *result.Run.LoopID).First(&root).Error; err != nil {
		t.Fatalf("reload root loop failed: %v", err)
	}
	if root.Status != models.LoopStatusSuccess || root.FinishedAt == nil {
		t.Fatalf("root loop did not finish successfully: %+v", root)
	}

	var steps []models.RunStep
	if err := gdb.Where("run_id = ?", result.Run.RunID).Order("id ASC").Find(&steps).Error; err != nil {
		t.Fatalf("load steps failed: %v", err)
	}
	if len(steps) != 2 {
		t.Fatalf("expected two run steps, got %d", len(steps))
	}
	for _, step := range steps {
		if step.LoopID == "" {
			t.Fatalf("step %s has no loop id", step.StepKey)
		}
		var loop models.LoopRecord
		if err := gdb.Where("loop_id = ?", step.LoopID).First(&loop).Error; err != nil {
			t.Fatalf("load step loop %s failed: %v", step.StepKey, err)
		}
		if loop.ParentLoopID != *result.Run.LoopID || loop.Status != models.LoopStatusSuccess || loop.LoopType != models.LoopTypeStep {
			t.Fatalf("unexpected step loop for %s: %+v", step.StepKey, loop)
		}
	}
}

func TestReplayRunCreatesIndependentRootLoop(t *testing.T) {
	gdb, service, _ := setupRunLoopTestService(t)
	seedAgentWorkflow(t, gdb)
	ctx := requestctx.WithTraceID(context.Background(), "trace-replay")

	origin, err := service.CreateRun(ctx, 1, CreateRunRequest{
		AgentCode:   "agent_test",
		TriggerType: "api",
		Input:       []byte(`{"prompt":"original"}`),
	})
	if err != nil {
		t.Fatalf("create origin run failed: %v", err)
	}
	replay, err := service.ReplayRun(ctx, 1, false, origin.Run.RunID, "")
	if err != nil {
		t.Fatalf("replay run failed: %v", err)
	}
	if replay.Run.RunID == origin.Run.RunID || replay.Run.LoopID == nil || origin.Run.LoopID == nil || *replay.Run.LoopID == *origin.Run.LoopID {
		t.Fatalf("replay did not create an independent root loop: origin=%+v replay=%+v", origin.Run, replay.Run)
	}
	if replay.Run.InputJSON != origin.Run.InputJSON || replay.Run.WorkflowID != origin.Run.WorkflowID {
		t.Fatalf("replay did not preserve execution source: origin=%+v replay=%+v", origin.Run, replay.Run)
	}
	var replayLoop models.LoopRecord
	if err := gdb.Where("loop_id = ?", *replay.Run.LoopID).First(&replayLoop).Error; err != nil {
		t.Fatalf("load replay root loop failed: %v", err)
	}
	if replayLoop.TraceID != "trace-replay" || replayLoop.RunID != replay.Run.RunID {
		t.Fatalf("replay loop metadata is incorrect: %+v", replayLoop)
	}
}
