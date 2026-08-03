package services

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"GoAI/models"
	"GoAI/requestctx"
)

type recordingResumePublisher struct {
	mu    sync.Mutex
	calls []resumePublishCall
	err   error
}

type resumePublishCall struct {
	runID        string
	delegationID string
	traceID      string
}

func (p *recordingResumePublisher) PublishRunResume(ctx context.Context, runID, delegationID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, resumePublishCall{runID: runID, delegationID: delegationID, traceID: requestctx.TraceIDFromContext(ctx)})
	return p.err
}

func (p *recordingResumePublisher) setError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

func (p *recordingResumePublisher) snapshot() []resumePublishCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]resumePublishCall(nil), p.calls...)
}

type recordingDelegationCallbackSender struct {
	mu    sync.Mutex
	calls []DelegationCallbackDelivery
	err   error
}

func (s *recordingDelegationCallbackSender) SendDelegationCallback(_ context.Context, delivery DelegationCallbackDelivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, delivery)
	return s.err
}

func (s *recordingDelegationCallbackSender) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *recordingDelegationCallbackSender) snapshot() []DelegationCallbackDelivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]DelegationCallbackDelivery(nil), s.calls...)
}

type callbackFixture struct {
	delegationFixture
	publisher  *recordingResumePublisher
	delegation models.Delegation
	token      string
}

func setupCallbackFixture(t *testing.T) callbackFixture {
	t.Helper()
	fixture := setupDelegationFixture(t)
	publisher := &recordingResumePublisher{}
	runtimeService, err := NewRuntimeService(fixture.database, fixture.runtime.runService, WithRunResumePublisher(publisher))
	if err != nil {
		t.Fatalf("create callback runtime service failed: %v", err)
	}
	fixture.runtime = runtimeService

	startedAt := time.Now().Add(-time.Second)
	step := models.RunStep{
		RunID: fixture.parentRun.RunID, StepKey: "delegate", StepType: "agent", Attempt: 1,
		Status: models.RunStepStatusWaitingExternal, InputJSON: "{}", OutputJSON: "{}", StartedAt: &startedAt,
	}
	if err := fixture.database.Create(&step).Error; err != nil {
		t.Fatalf("create waiting parent step failed: %v", err)
	}
	if err := fixture.database.Model(&models.Run{}).Where("run_id = ?", fixture.parentRun.RunID).Updates(map[string]any{
		"status": models.RunStatusWaitingExternal, "current_step": step.StepKey,
	}).Error; err != nil {
		t.Fatalf("mark parent run waiting failed: %v", err)
	}

	taskID := "a2a_task_callback"
	token := "callback-token-for-tests"
	delegation := models.Delegation{
		DelegationID: "dlg_callback", ThreadID: fixture.parentRun.ThreadID, ParentRunID: fixture.parentRun.RunID,
		ChildRunID: taskID, A2ATaskID: &taskID, TraceID: "trace_callback", LoopID: "loop_delegate",
		SourceAgentID: fixture.sourceAgent.ID, TargetAgentID: fixture.targetAgent.ID, CapabilityCode: "write",
		RequestMessageID: "msg_delegate_callback", ParentStepKey: step.StepKey, InputJSON: "{}", OutputJSON: "{}",
		Status: models.DelegationStatusAccepted, CallbackTokenHash: callbackTokenHash(token), ResumeStatus: models.DelegationResumeStatusNone,
	}
	if err := fixture.database.Create(&delegation).Error; err != nil {
		t.Fatalf("create callback delegation failed: %v", err)
	}
	return callbackFixture{delegationFixture: fixture, publisher: publisher, delegation: delegation, token: token}
}

func (f callbackFixture) command(state string) DelegationCallbackCommand {
	return DelegationCallbackCommand{
		SourceAgentCode: f.sourceAgent.AgentCode, TargetAgentCode: f.targetAgent.AgentCode,
		TaskID: *f.delegation.A2ATaskID, State: state, OutputJSON: `{"answer":"done"}`,
		ErrorMessage: "child failed", NotificationToken: f.token, EventJSON: `{"event":"terminal"}`,
	}
}

func TestRecoverPendingCallbacksAfterRuntimeRestart(t *testing.T) {
	fixture := setupDelegationFixture(t)
	endpoint := models.AgentEndpoint{
		AgentID: fixture.targetAgent.ID, EndpointCode: "primary", Protocol: models.AgentEndpointProtocolA2A,
		Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1/a2a/agents/writer",
		AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "writer-key", Status: models.AgentEndpointStatusActive,
	}
	if err := fixture.database.Create(&endpoint).Error; err != nil {
		t.Fatalf("create target endpoint failed: %v", err)
	}
	sender := &recordingDelegationCallbackSender{err: errors.New("callback unavailable")}
	runtimeService, err := NewRuntimeService(fixture.database, fixture.runtime.runService, WithDelegationCallbackSender(sender))
	if err != nil {
		t.Fatalf("create callback runtime failed: %v", err)
	}
	command := delegationCommand()
	command.PushConfig = &DelegationPushConfig{
		ConfigID: "push-recovery", TaskID: command.RequestedChildRunID,
		CallbackURL: "http://127.0.0.1/a2a/agents/planner/callbacks/tasks/" + command.RequestedChildRunID, Token: "notification-token",
	}
	result, err := runtimeService.AcceptDelegation(context.Background(), command)
	if err != nil {
		t.Fatalf("accept callback-enabled delegation failed: %v", err)
	}
	now := time.Now()
	if err := fixture.database.Model(&models.Run{}).Where("run_id = ?", result.Run.RunID).Updates(map[string]any{
		"status": models.RunStatusSuccess, "started_at": now, "finished_at": now,
	}).Error; err != nil {
		t.Fatalf("mark child run successful failed: %v", err)
	}
	step := models.RunStep{
		RunID: result.Run.RunID, StepKey: "work", StepType: "noop", Attempt: 1, Status: models.RunStepStatusSuccess,
		InputJSON: "{}", OutputJSON: `{"answer":"done"}`,
	}
	if err := fixture.database.Create(&step).Error; err != nil {
		t.Fatalf("create child result step failed: %v", err)
	}
	if err := runtimeService.ReconcileDelegation(context.Background(), result.Run.RunID); err == nil {
		t.Fatal("expected initial callback delivery failure")
	}
	var pushConfig models.A2APushConfig
	if err := fixture.database.Where("delegation_id = ?", result.Delegation.DelegationID).First(&pushConfig).Error; err != nil {
		t.Fatalf("load failed push config: %v", err)
	}
	if pushConfig.Status != models.A2APushStatusFailed || pushConfig.AttemptCount != 1 {
		t.Fatalf("unexpected failed push state: %+v", pushConfig)
	}
	past := time.Now().Add(-time.Second)
	if err := fixture.database.Model(&models.A2APushConfig{}).Where("id = ?", pushConfig.ID).Update("next_attempt_at", past).Error; err != nil {
		t.Fatalf("make callback retry eligible failed: %v", err)
	}
	sender.setError(nil)
	restarted, err := NewRuntimeService(fixture.database, fixture.runtime.runService, WithDelegationCallbackSender(sender))
	if err != nil {
		t.Fatalf("recreate callback runtime failed: %v", err)
	}
	if err := restarted.RecoverPendingCallbacks(context.Background(), 10); err != nil {
		t.Fatalf("recover pending callback failed: %v", err)
	}
	if calls := sender.snapshot(); len(calls) != 2 || calls[1].TaskID != command.RequestedChildRunID || calls[1].State != DelegationCallbackStateSucceeded {
		t.Fatalf("unexpected recovered callback calls: %+v", calls)
	}
	if err := fixture.database.First(&pushConfig, "id = ?", pushConfig.ID).Error; err != nil {
		t.Fatalf("reload recovered push config failed: %v", err)
	}
	if pushConfig.Status != models.A2APushStatusSent || pushConfig.AttemptCount != 2 || pushConfig.SentAt == nil {
		t.Fatalf("callback recovery did not persist sent state: %+v", pushConfig)
	}
}

func TestAcceptDelegationCallbackCompletesWaitingStepAndPublishesOnce(t *testing.T) {
	fixture := setupCallbackFixture(t)
	command := fixture.command(DelegationCallbackStateSucceeded)
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), command); err != nil {
		t.Fatalf("accept callback failed: %v", err)
	}
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), command); err != nil {
		t.Fatalf("duplicate callback failed: %v", err)
	}

	calls := fixture.publisher.snapshot()
	if len(calls) != 1 || calls[0].runID != fixture.parentRun.RunID || calls[0].delegationID != fixture.delegation.DelegationID || calls[0].traceID != fixture.delegation.TraceID {
		t.Fatalf("unexpected resume publishes: %+v", calls)
	}
	var run models.Run
	if err := fixture.database.Where("run_id = ?", fixture.parentRun.RunID).First(&run).Error; err != nil {
		t.Fatalf("load parent run failed: %v", err)
	}
	if run.Status != models.RunStatusWaitingExternal {
		t.Fatalf("parent run status=%s want=%s", run.Status, models.RunStatusWaitingExternal)
	}
	var step models.RunStep
	if err := fixture.database.Where("run_id = ? AND step_key = ?", fixture.parentRun.RunID, fixture.delegation.ParentStepKey).First(&step).Error; err != nil {
		t.Fatalf("load parent step failed: %v", err)
	}
	if step.Status != models.RunStepStatusSuccess || step.OutputJSON != `{"answer":"done"}` {
		t.Fatalf("unexpected callback step: %+v", step)
	}
	var delegation models.Delegation
	if err := fixture.database.Where("delegation_id = ?", fixture.delegation.DelegationID).First(&delegation).Error; err != nil {
		t.Fatalf("load delegation failed: %v", err)
	}
	if delegation.Status != models.DelegationStatusSucceeded || delegation.ResumeStatus != models.DelegationResumeStatusPublished || delegation.CallbackEventHash == "" {
		t.Fatalf("unexpected callback delegation: %+v", delegation)
	}
	var messages []models.Message
	if err := fixture.database.Where("delegation_id = ? AND message_type = ?", fixture.delegation.DelegationID, models.MessageTypeResult).Find(&messages).Error; err != nil {
		t.Fatalf("load result messages failed: %v", err)
	}
	if len(messages) != 1 || messages[0].RunID != fixture.parentRun.RunID || messages[0].MessageID != delegationResultMessageID(fixture.delegation.DelegationID) {
		t.Fatalf("unexpected result messages: %+v", messages)
	}
}

func TestAcceptDelegationCallbackConcurrentDuplicatesRemainIdempotent(t *testing.T) {
	fixture := setupCallbackFixture(t)
	sqlDB, err := fixture.database.DB()
	if err != nil {
		t.Fatalf("get SQL database failed: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	command := fixture.command(DelegationCallbackStateSucceeded)

	const workers = 8
	var wait sync.WaitGroup
	errorsCh := make(chan error, workers)
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errorsCh <- fixture.runtime.AcceptDelegationCallback(context.Background(), command)
		}()
	}
	wait.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("concurrent callback failed: %v", err)
		}
	}
	if calls := fixture.publisher.snapshot(); len(calls) != 1 {
		t.Fatalf("resume publish count=%d want=1 calls=%+v", len(calls), calls)
	}
	var count int64
	if err := fixture.database.Model(&models.Message{}).Where("delegation_id = ? AND message_type = ?", fixture.delegation.DelegationID, models.MessageTypeResult).Count(&count).Error; err != nil {
		t.Fatalf("count result messages failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("result message count=%d want=1", count)
	}
}

func TestAcceptDelegationCallbackRejectsConflictAndTokenMismatch(t *testing.T) {
	fixture := setupCallbackFixture(t)
	command := fixture.command(DelegationCallbackStateSucceeded)
	badToken := command
	badToken.NotificationToken = "wrong-token"
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), badToken); !errors.Is(err, ErrDelegationForbidden()) {
		t.Fatalf("token mismatch error=%v want forbidden", err)
	}
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), command); err != nil {
		t.Fatalf("accept callback failed: %v", err)
	}
	conflict := command
	conflict.EventJSON = `{"event":"different-terminal"}`
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), conflict); !errors.Is(err, ErrInvalidDelegation()) {
		t.Fatalf("conflicting callback error=%v want invalid delegation", err)
	}
}

func TestAcceptDelegationCallbackFailureAndCancellationAreTerminal(t *testing.T) {
	for _, test := range []struct {
		name       string
		state      string
		wantRun    string
		wantStep   string
		wantStatus string
	}{
		{name: "failed", state: DelegationCallbackStateFailed, wantRun: models.RunStatusFailed, wantStep: models.RunStepStatusFailed, wantStatus: models.DelegationStatusFailed},
		{name: "cancelled", state: DelegationCallbackStateCancelled, wantRun: models.RunStatusCancelled, wantStep: models.RunStepStatusSkipped, wantStatus: models.DelegationStatusCancelled},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := setupCallbackFixture(t)
			if err := fixture.runtime.AcceptDelegationCallback(context.Background(), fixture.command(test.state)); err != nil {
				t.Fatalf("accept terminal callback failed: %v", err)
			}
			if calls := fixture.publisher.snapshot(); len(calls) != 0 {
				t.Fatalf("terminal failure published resume: %+v", calls)
			}
			var run models.Run
			if err := fixture.database.Where("run_id = ?", fixture.parentRun.RunID).First(&run).Error; err != nil {
				t.Fatalf("load run failed: %v", err)
			}
			var step models.RunStep
			if err := fixture.database.Where("run_id = ? AND step_key = ?", fixture.parentRun.RunID, fixture.delegation.ParentStepKey).First(&step).Error; err != nil {
				t.Fatalf("load step failed: %v", err)
			}
			var delegation models.Delegation
			if err := fixture.database.Where("delegation_id = ?", fixture.delegation.DelegationID).First(&delegation).Error; err != nil {
				t.Fatalf("load delegation failed: %v", err)
			}
			if run.Status != test.wantRun || step.Status != test.wantStep || delegation.Status != test.wantStatus || delegation.ResumeStatus != models.DelegationResumeStatusNone {
				t.Fatalf("unexpected terminal state run=%s step=%s delegation=%+v", run.Status, step.Status, delegation)
			}
		})
	}
}

func TestRecoverPendingResumesRetriesPublishFailureAndStalePublished(t *testing.T) {
	fixture := setupCallbackFixture(t)
	fixture.publisher.setError(errors.New("kafka unavailable"))
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), fixture.command(DelegationCallbackStateSucceeded)); err == nil {
		t.Fatal("expected initial resume publish failure")
	}
	var delegation models.Delegation
	if err := fixture.database.Where("delegation_id = ?", fixture.delegation.DelegationID).First(&delegation).Error; err != nil {
		t.Fatalf("load failed publish delegation: %v", err)
	}
	if delegation.ResumeStatus != models.DelegationResumeStatusPublishFailed || delegation.ResumeAttemptCount != 1 {
		t.Fatalf("unexpected failed publish state: %+v", delegation)
	}

	fixture.publisher.setError(nil)
	restarted, err := NewRuntimeService(fixture.database, fixture.runtime.runService, WithRunResumePublisher(fixture.publisher))
	if err != nil {
		t.Fatalf("recreate runtime service failed: %v", err)
	}
	if err := restarted.RecoverPendingResumes(context.Background(), 10); err != nil {
		t.Fatalf("recover failed resume publish: %v", err)
	}
	if calls := fixture.publisher.snapshot(); len(calls) != 2 {
		t.Fatalf("publish count after failed recovery=%d want=2", len(calls))
	}

	old := time.Now().Add(-2 * resumeRepublishDelay)
	if err := fixture.database.Model(&models.Delegation{}).Where("delegation_id = ?", fixture.delegation.DelegationID).UpdateColumns(map[string]any{
		"resume_status": models.DelegationResumeStatusPublished, "resume_published_at": old, "updated_at": old,
	}).Error; err != nil {
		t.Fatalf("mark published resume stale failed: %v", err)
	}
	if err := restarted.RecoverPendingResumes(context.Background(), 10); err != nil {
		t.Fatalf("recover stale published resume: %v", err)
	}
	if calls := fixture.publisher.snapshot(); len(calls) != 3 {
		t.Fatalf("publish count after stale recovery=%d want=3", len(calls))
	}
}
