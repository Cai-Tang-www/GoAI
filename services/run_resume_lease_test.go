package services

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"GoAI/models"
	"GoAI/requestctx"

	"gorm.io/gorm"
)

type resumeLeaseFixture struct {
	database   *gorm.DB
	service    *RunService
	run        models.Run
	workflow   models.Workflow
	delegation models.Delegation
}

type resumeLeaseTestPublisher struct {
	mu    sync.Mutex
	calls []resumePublishCall
}

func (p *resumeLeaseTestPublisher) PublishRunResume(ctx context.Context, runID, delegationID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, resumePublishCall{
		runID: runID, delegationID: delegationID, traceID: requestctx.TraceIDFromContext(ctx),
	})
	return nil
}

func (p *resumeLeaseTestPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

func setupResumeLeaseFixture(t *testing.T, definitionJSON, resumeNodeKey string) resumeLeaseFixture {
	t.Helper()
	database := openSQLiteTestDB(t)
	if err := database.AutoMigrate(
		&models.Agent{},
		&models.Workflow{},
		&models.Run{},
		&models.RunStep{},
		&models.Delegation{},
		&models.DelegationGroup{},
		&models.Message{},
	); err != nil {
		t.Fatalf("auto migrate resume fixture failed: %v", err)
	}

	source := models.Agent{AgentCode: "resume_source", Name: "Resume Source", OwnerUserID: 1, Status: models.AgentStatusActive}
	target := models.Agent{AgentCode: "resume_target", Name: "Resume Target", OwnerUserID: 1, Status: models.AgentStatusActive}
	if err := database.Create(&source).Error; err != nil {
		t.Fatalf("create source agent failed: %v", err)
	}
	if err := database.Create(&target).Error; err != nil {
		t.Fatalf("create target agent failed: %v", err)
	}
	workflow := models.Workflow{
		AgentID: source.ID, Version: 1, DefinitionJSON: definitionJSON,
		Checksum: "resume-checksum", IsActive: true, CreatedBy: 1,
	}
	if err := database.Create(&workflow).Error; err != nil {
		t.Fatalf("create resume workflow failed: %v", err)
	}

	run := models.Run{
		RunID: "run_resume_lease", ThreadID: "thread_resume_lease", TraceID: "trace_resume_lease",
		AgentID: source.ID, WorkflowID: workflow.ID, UserID: 1, TriggerType: "api",
		InputJSON: `{"prompt":"resume"}`, Status: models.RunStatusWaitingExternal, CurrentStep: "delegate_parent",
	}
	if err := database.Create(&run).Error; err != nil {
		t.Fatalf("create parent run failed: %v", err)
	}

	callbackStep := models.RunStep{
		RunID: run.RunID, TraceID: run.TraceID, StepKey: "delegate_parent", StepType: "agent",
		Attempt: 1, Status: models.RunStepStatusSuccess, InputJSON: "{}", OutputJSON: `{"answer":"child-result"}`,
	}
	if err := database.Create(&callbackStep).Error; err != nil {
		t.Fatalf("create callback step failed: %v", err)
	}
	taskID := "task_resume_lease"
	now := time.Now()
	delegation := models.Delegation{
		DelegationID: "delegation_resume_lease", ThreadID: run.ThreadID, ParentRunID: run.RunID,
		ChildRunID: "child_resume_lease", A2ATaskID: &taskID, TraceID: run.TraceID,
		SourceAgentID: source.ID, TargetAgentID: target.ID, CapabilityCode: "resume",
		RequestMessageID: "message_resume_lease", ParentStepKey: callbackStep.StepKey, ResumeNodeKey: resumeNodeKey,
		InputJSON: "{}", OutputJSON: callbackStep.OutputJSON, Status: models.DelegationStatusSucceeded,
		CallbackEventHash: "callback_hash", CallbackReceivedAt: &now,
		ResumeStatus: models.DelegationResumeStatusPublished, ResumePublishedAt: &now,
	}
	if err := database.Create(&delegation).Error; err != nil {
		t.Fatalf("create resume delegation failed: %v", err)
	}

	publisher := &recordingRunPublisher{}
	service, err := NewRunService(database, publisher, WithRunResumeLease(500*time.Millisecond, 100*time.Millisecond))
	if err != nil {
		t.Fatalf("create resume run service failed: %v", err)
	}
	return resumeLeaseFixture{database: database, service: service, run: run, workflow: workflow, delegation: delegation}
}

func TestClaimRunResumeSupportsExclusiveExpiredLeaseTakeover(t *testing.T) {
	fixture := setupResumeLeaseFixture(t, `{"entry_node":"delegate_parent","nodes":[{"key":"delegate_parent","type":"noop"}],"edges":[]}`, "")

	run, delegation, firstLease, claimed, err := fixture.service.claimRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if err != nil {
		t.Fatalf("claim initial resume failed: %v", err)
	}
	if !claimed || run.Status != models.RunStatusRunning || firstLease.Attempt != 1 || firstLease.Owner == "" {
		t.Fatalf("unexpected initial claim: claimed=%v run=%+v lease=%+v", claimed, run, firstLease)
	}
	if delegation.ResumeLeaseExpiresAt == nil || delegation.ResumeLeaseHeartbeatAt == nil || delegation.ResumeLeaseClaimedAt == nil {
		t.Fatalf("initial claim did not persist lease timestamps: %+v", delegation)
	}

	_, _, _, claimed, err = fixture.service.claimRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if err != nil || claimed {
		t.Fatalf("valid lease should not be claimed again: claimed=%v err=%v", claimed, err)
	}

	expiredAt := time.Now().Add(-time.Second)
	if err := fixture.database.Model(&models.Delegation{}).Where("id = ?", fixture.delegation.ID).
		Update("resume_lease_expires_at", expiredAt).Error; err != nil {
		t.Fatalf("expire first lease failed: %v", err)
	}
	_, _, secondLease, claimed, err := fixture.service.claimRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if err != nil {
		t.Fatalf("take over expired lease failed: %v", err)
	}
	if !claimed || secondLease.Attempt != 2 || secondLease.Owner == firstLease.Owner {
		t.Fatalf("unexpected takeover lease: first=%+v second=%+v claimed=%v", firstLease, secondLease, claimed)
	}
}

func TestClaimRunResumeConcurrentTakeoverHasSingleWinner(t *testing.T) {
	fixture := setupResumeLeaseFixture(t, `{"entry_node":"delegate_parent","nodes":[{"key":"delegate_parent","type":"noop"}],"edges":[]}`, "")
	_, _, _, claimed, err := fixture.service.claimRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if err != nil || !claimed {
		t.Fatalf("prepare initial claim failed: claimed=%v err=%v", claimed, err)
	}
	expiredAt := time.Now().Add(-time.Second)
	if err := fixture.database.Model(&models.Delegation{}).Where("id = ?", fixture.delegation.ID).
		Update("resume_lease_expires_at", expiredAt).Error; err != nil {
		t.Fatalf("expire lease failed: %v", err)
	}

	sqlDB, err := fixture.database.DB()
	if err != nil {
		t.Fatalf("get sqlite database failed: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)

	var winners atomic.Int32
	errs := make(chan error, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			_, _, _, won, claimErr := fixture.service.claimRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
			if won {
				winners.Add(1)
			}
			errs <- claimErr
		}()
	}
	close(start)
	for range 2 {
		if claimErr := <-errs; claimErr != nil {
			t.Fatalf("concurrent takeover failed: %v", claimErr)
		}
	}
	if winners.Load() != 1 {
		t.Fatalf("expected exactly one lease winner, got %d", winners.Load())
	}
}

func TestResumeLeaseHeartbeatAndFencingRejectOldOwner(t *testing.T) {
	fixture := setupResumeLeaseFixture(t, `{"entry_node":"delegate_parent","nodes":[{"key":"delegate_parent","type":"noop"}],"edges":[]}`, "")
	fixture.service.resumeLeaseDuration = 300 * time.Millisecond
	fixture.service.resumeHeartbeatInterval = 40 * time.Millisecond
	_, delegation, firstLease, claimed, err := fixture.service.claimRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if err != nil || !claimed {
		t.Fatalf("claim lease failed: claimed=%v err=%v", claimed, err)
	}
	initialExpiry := *delegation.ResumeLeaseExpiresAt
	_, stopHeartbeat := fixture.service.startResumeLeaseHeartbeat(context.Background(), firstLease)
	t.Cleanup(func() { _ = stopHeartbeat() })
	time.Sleep(130 * time.Millisecond)

	var renewed models.Delegation
	if err := fixture.database.First(&renewed, fixture.delegation.ID).Error; err != nil {
		t.Fatalf("reload renewed lease failed: %v", err)
	}
	if renewed.ResumeLeaseExpiresAt == nil || !renewed.ResumeLeaseExpiresAt.After(initialExpiry) {
		t.Fatalf("heartbeat did not extend lease: initial=%s current=%v", initialExpiry, renewed.ResumeLeaseExpiresAt)
	}
	if err := stopHeartbeat(); err != nil {
		t.Fatalf("stop healthy heartbeat failed: %v", err)
	}

	expiredAt := time.Now().Add(-time.Second)
	if err := fixture.database.Model(&models.Delegation{}).Where("id = ?", fixture.delegation.ID).
		Update("resume_lease_expires_at", expiredAt).Error; err != nil {
		t.Fatalf("expire renewed lease failed: %v", err)
	}
	_, _, secondLease, claimed, err := fixture.service.claimRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if err != nil || !claimed {
		t.Fatalf("take over renewed lease failed: claimed=%v err=%v", claimed, err)
	}
	if err := fixture.service.renewResumeLease(context.Background(), firstLease); !errors.Is(err, errResumeLeaseLost) {
		t.Fatalf("old heartbeat should be fenced after takeover, got %v", err)
	}
	var current models.Delegation
	if err := fixture.database.First(&current, fixture.delegation.ID).Error; err != nil {
		t.Fatalf("reload takeover lease failed: %v", err)
	}
	if current.ResumeLeaseOwner != secondLease.Owner || current.ResumeExecutionAttempt != secondLease.Attempt {
		t.Fatalf("old heartbeat changed new lease: %+v", current)
	}
	if err := fixture.service.updateCurrentStep(withResumeLease(context.Background(), firstLease), fixture.run.RunID, "stale-owner-write"); !errors.Is(err, errResumeLeaseLost) {
		t.Fatalf("old lease owner should be fenced, got %v", err)
	}
	if err := fixture.service.updateCurrentStep(withResumeLease(context.Background(), secondLease), fixture.run.RunID, "new-owner-write"); err != nil {
		t.Fatalf("new lease owner could not write: %v", err)
	}
}

func TestResumeLeaseHeartbeatStopIsBoundedWhenDatabaseStalls(t *testing.T) {
	fixture := setupResumeLeaseFixture(t, `{"entry_node":"delegate_parent","nodes":[{"key":"delegate_parent","type":"noop"}],"edges":[]}`, "")
	fixture.service.resumeHeartbeatInterval = 5 * time.Millisecond
	fixture.service.resumePersistenceTimeout = 30 * time.Millisecond
	_, _, lease, claimed, err := fixture.service.claimRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if err != nil || !claimed {
		t.Fatalf("claim lease failed: claimed=%v err=%v", claimed, err)
	}

	blocked := make(chan struct{})
	var blockOnce atomic.Bool
	if err := fixture.database.Callback().Update().Before("gorm:update").Register("test:block_resume_renewal", func(tx *gorm.DB) {
		if tx.Statement.Table != "delegations" || !blockOnce.CompareAndSwap(false, true) {
			return
		}
		close(blocked)
		<-tx.Statement.Context.Done()
		tx.AddError(tx.Statement.Context.Err())
	}); err != nil {
		t.Fatalf("register blocking update callback failed: %v", err)
	}

	_, stopHeartbeat := fixture.service.startResumeLeaseHeartbeat(context.Background(), lease)
	select {
	case <-blocked:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not enter the simulated stalled database update")
	}

	startedAt := time.Now()
	err = stopHeartbeat()
	if err == nil {
		t.Fatal("expected stalled heartbeat renewal to return an error")
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("stopping heartbeat exceeded bounded persistence timeout: %s", elapsed)
	}
}

func TestHandleRunResumeSkipsSuccessfulCheckpoint(t *testing.T) {
	definition := `{"entry_node":"delegate_parent","nodes":[{"key":"delegate_parent","type":"noop"},{"key":"postprocess","type":"noop"},{"key":"finalize","type":"noop"}],"edges":[{"from":"delegate_parent","to":"postprocess"},{"from":"postprocess","to":"finalize"}]}`
	fixture := setupResumeLeaseFixture(t, definition, "postprocess")
	if err := fixture.database.Create(&models.RunStep{
		RunID: fixture.run.RunID, TraceID: fixture.run.TraceID, StepKey: "postprocess", StepType: "noop",
		Attempt: 1, Status: models.RunStepStatusSuccess, InputJSON: "{}", OutputJSON: `{"stage":"postprocessed"}`,
	}).Error; err != nil {
		t.Fatalf("create successful checkpoint failed: %v", err)
	}

	var executed []string
	fixture.service.executeNode = func(_ context.Context, _ *models.Run, node WorkflowNode, _ int) (string, error) {
		executed = append(executed, node.Key)
		return `{"message":"done"}`, nil
	}
	if err := fixture.service.HandleRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID); err != nil {
		t.Fatalf("resume from successful checkpoint failed: %v", err)
	}
	if len(executed) != 1 || executed[0] != "finalize" {
		t.Fatalf("successful checkpoint was replayed: executed=%v", executed)
	}

	var postprocessCount int64
	if err := fixture.database.Model(&models.RunStep{}).
		Where("run_id = ? AND step_key = ?", fixture.run.RunID, "postprocess").Count(&postprocessCount).Error; err != nil {
		t.Fatalf("count checkpoint steps failed: %v", err)
	}
	if postprocessCount != 1 {
		t.Fatalf("expected one persisted postprocess step, got %d", postprocessCount)
	}
	var storedRun models.Run
	if err := fixture.database.Where("run_id = ?", fixture.run.RunID).First(&storedRun).Error; err != nil {
		t.Fatalf("reload completed run failed: %v", err)
	}
	if storedRun.Status != models.RunStatusSuccess {
		t.Fatalf("expected resumed run success, got %+v", storedRun)
	}
}

func TestHandleRunResumeKeepsPersistedExternalWaitWithoutReinvokingAgent(t *testing.T) {
	definition := `{"entry_node":"delegate_parent","nodes":[{"key":"delegate_parent","type":"noop"},{"key":"delegate_reviewer","type":"agent","config":{"target_agent":"reviewer","capability":"review"}}],"edges":[{"from":"delegate_parent","to":"delegate_reviewer"}]}`
	fixture := setupResumeLeaseFixture(t, definition, "delegate_reviewer")
	if err := fixture.database.Model(&models.Run{}).Where("id = ?", fixture.run.ID).Update("current_step", "delegate_reviewer").Error; err != nil {
		t.Fatalf("persist external wait cursor failed: %v", err)
	}
	startedAt := time.Now().Add(-time.Second)
	if err := fixture.database.Create(&models.RunStep{
		RunID: fixture.run.RunID, TraceID: fixture.run.TraceID, StepKey: "delegate_reviewer", StepType: "agent",
		Attempt: 1, Status: models.RunStepStatusWaitingExternal, InputJSON: "{}", OutputJSON: "{}", StartedAt: &startedAt,
	}).Error; err != nil {
		t.Fatalf("create persisted external wait failed: %v", err)
	}
	taskID := "task_reviewer_existing"
	secondDelegation := models.Delegation{
		DelegationID: "delegation_reviewer_existing", ThreadID: fixture.run.ThreadID, ParentRunID: fixture.run.RunID,
		ChildRunID: "child_reviewer_existing", A2ATaskID: &taskID, TraceID: fixture.run.TraceID,
		SourceAgentID: fixture.delegation.SourceAgentID, TargetAgentID: fixture.delegation.TargetAgentID,
		CapabilityCode: "review", RequestMessageID: "message_reviewer_existing", ParentStepKey: "delegate_reviewer",
		InputJSON: "{}", OutputJSON: "{}", Status: models.DelegationStatusAccepted,
	}
	if err := fixture.database.Create(&secondDelegation).Error; err != nil {
		t.Fatalf("create existing reviewer delegation failed: %v", err)
	}
	invoker := &countingResumeAgentInvoker{}
	fixture.service.agentInvoker = invoker

	if err := fixture.service.HandleRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID); err != nil {
		t.Fatalf("resume persisted wait failed: %v", err)
	}
	if invoker.calls.Load() != 0 {
		t.Fatalf("persisted A2A delegation was invoked again %d times", invoker.calls.Load())
	}
	var storedRun models.Run
	if err := fixture.database.Where("run_id = ?", fixture.run.RunID).First(&storedRun).Error; err != nil {
		t.Fatalf("reload suspended run failed: %v", err)
	}
	if storedRun.Status != models.RunStatusWaitingExternal || storedRun.CurrentStep != "delegate_reviewer" {
		t.Fatalf("run did not remain on persisted external wait: %+v", storedRun)
	}
	var oldDelegation models.Delegation
	if err := fixture.database.First(&oldDelegation, fixture.delegation.ID).Error; err != nil {
		t.Fatalf("reload completed resume delegation failed: %v", err)
	}
	if oldDelegation.ResumeStatus != models.DelegationResumeStatusCompleted {
		t.Fatalf("old resume lease was not completed: %+v", oldDelegation)
	}
}

type countingResumeAgentInvoker struct {
	calls atomic.Int32
}

func (i *countingResumeAgentInvoker) Invoke(context.Context, AgentInvocationRequest) (*AgentInvocationResult, error) {
	i.calls.Add(1)
	return nil, errors.New("unexpected A2A invocation")
}

func TestRecoverPendingResumesRepublishesExpiredClaimOnce(t *testing.T) {
	fixture := setupResumeLeaseFixture(t, `{"entry_node":"delegate_parent","nodes":[{"key":"delegate_parent","type":"noop"}],"edges":[]}`, "")
	_, _, _, claimed, err := fixture.service.claimRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if err != nil || !claimed {
		t.Fatalf("prepare expired recovery claim failed: claimed=%v err=%v", claimed, err)
	}
	expiredAt := time.Now().Add(-time.Second)
	if err := fixture.database.Model(&models.Delegation{}).Where("id = ?", fixture.delegation.ID).
		Update("resume_lease_expires_at", expiredAt).Error; err != nil {
		t.Fatalf("expire recovery lease failed: %v", err)
	}

	publisher := &resumeLeaseTestPublisher{}
	runtimeService, err := NewRuntimeService(fixture.database, fixture.service, WithRunResumePublisher(publisher))
	if err != nil {
		t.Fatalf("create recovery runtime failed: %v", err)
	}
	if err := runtimeService.RecoverPendingResumes(context.Background(), 10); err != nil {
		t.Fatalf("recover expired resume failed: %v", err)
	}
	if publisher.count() != 1 {
		t.Fatalf("expected one recovery publish, got %d", publisher.count())
	}
	var recovered models.Delegation
	if err := fixture.database.First(&recovered, fixture.delegation.ID).Error; err != nil {
		t.Fatalf("reload recovered delegation failed: %v", err)
	}
	if recovered.ResumeStatus != models.DelegationResumeStatusPublished || recovered.ResumeLeaseOwner != "" || recovered.ResumeLeaseExpiresAt != nil {
		t.Fatalf("unexpected recovered delegation state: %+v", recovered)
	}
	if recovered.ResumeExecutionAttempt != 1 {
		t.Fatalf("recovery publish should preserve fencing attempt, got %d", recovered.ResumeExecutionAttempt)
	}

	if err := runtimeService.RecoverPendingResumes(context.Background(), 10); err != nil {
		t.Fatalf("repeat recovery scan failed: %v", err)
	}
	if publisher.count() != 1 {
		t.Fatalf("repeat scan republished fresh event: calls=%d", publisher.count())
	}
}

func TestHandleRunResumeRecoversCrashAfterClaimBeforeExecution(t *testing.T) {
	definition := `{"entry_node":"delegate_parent","nodes":[{"key":"delegate_parent","type":"noop"},{"key":"finalize","type":"noop"}],"edges":[{"from":"delegate_parent","to":"finalize"}]}`
	fixture := setupResumeLeaseFixture(t, definition, "finalize")
	_, _, _, claimed, err := fixture.service.claimRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if err != nil || !claimed {
		t.Fatalf("simulate crashed worker claim failed: claimed=%v err=%v", claimed, err)
	}
	if err := fixture.database.Model(&models.Delegation{}).Where("id = ?", fixture.delegation.ID).
		Update("resume_lease_expires_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatalf("expire crashed worker lease failed: %v", err)
	}

	publisher := &resumeLeaseTestPublisher{}
	runtimeService, err := NewRuntimeService(fixture.database, fixture.service, WithRunResumePublisher(publisher))
	if err != nil {
		t.Fatalf("create recovery runtime failed: %v", err)
	}
	if err := runtimeService.RecoverPendingResumes(context.Background(), 10); err != nil {
		t.Fatalf("recover crashed claim failed: %v", err)
	}
	detail, err := fixture.service.GetRunDetailByRunID(context.Background(), fixture.run.UserID, false, fixture.run.RunID)
	if err != nil {
		t.Fatalf("query observable recovery state failed: %v", err)
	}
	if detail.Resume == nil || detail.Resume.Status != models.DelegationResumeStatusPublished || !strings.Contains(detail.Resume.Error, "lease expired") {
		t.Fatalf("recovery state is not observable: %+v", detail.Resume)
	}

	var executions atomic.Int32
	fixture.service.executeNode = func(_ context.Context, _ *models.Run, node WorkflowNode, _ int) (string, error) {
		if node.Key != "finalize" {
			t.Fatalf("unexpected resumed node %s", node.Key)
		}
		executions.Add(1)
		return `{"message":"recovered"}`, nil
	}
	if err := fixture.service.HandleRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID); err != nil {
		t.Fatalf("takeover resume failed: %v", err)
	}
	if err := fixture.service.HandleRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID); err != nil {
		t.Fatalf("duplicate resume message should be a no-op: %v", err)
	}
	if executions.Load() != 1 {
		t.Fatalf("resumed workflow executed %d times", executions.Load())
	}
	var run models.Run
	if err := fixture.database.Where("run_id = ?", fixture.run.RunID).First(&run).Error; err != nil {
		t.Fatalf("reload recovered run failed: %v", err)
	}
	if run.Status != models.RunStatusSuccess {
		t.Fatalf("recovered run did not complete: %+v", run)
	}
	var delegation models.Delegation
	if err := fixture.database.First(&delegation, fixture.delegation.ID).Error; err != nil {
		t.Fatalf("reload recovered delegation failed: %v", err)
	}
	if delegation.ResumeStatus != models.DelegationResumeStatusCompleted || delegation.ResumeExecutionAttempt != 2 || delegation.ResumeLeaseOwner != "" {
		t.Fatalf("unexpected completed takeover state: %+v", delegation)
	}
}

func TestHandleRunResumeClosesAbandonedStepAndUsesNextAttempt(t *testing.T) {
	definition := `{"entry_node":"delegate_parent","nodes":[{"key":"delegate_parent","type":"noop"},{"key":"postprocess","type":"noop"}],"edges":[{"from":"delegate_parent","to":"postprocess"}]}`
	fixture := setupResumeLeaseFixture(t, definition, "postprocess")
	_, _, _, claimed, err := fixture.service.claimRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if err != nil || !claimed {
		t.Fatalf("simulate crashed step owner failed: claimed=%v err=%v", claimed, err)
	}
	startedAt := time.Now().Add(-time.Second)
	if err := fixture.database.Create(&models.RunStep{
		RunID: fixture.run.RunID, TraceID: fixture.run.TraceID, StepKey: "postprocess", StepType: "noop",
		Attempt: 1, Status: models.RunStepStatusRunning, InputJSON: "{}", OutputJSON: "{}", StartedAt: &startedAt,
	}).Error; err != nil {
		t.Fatalf("create abandoned running step failed: %v", err)
	}
	if err := fixture.database.Model(&models.Delegation{}).Where("id = ?", fixture.delegation.ID).
		Update("resume_lease_expires_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatalf("expire crashed step lease failed: %v", err)
	}
	publisher := &resumeLeaseTestPublisher{}
	runtimeService, err := NewRuntimeService(fixture.database, fixture.service, WithRunResumePublisher(publisher))
	if err != nil {
		t.Fatalf("create recovery runtime failed: %v", err)
	}
	if err := runtimeService.RecoverPendingResumes(context.Background(), 10); err != nil {
		t.Fatalf("recover abandoned step failed: %v", err)
	}
	fixture.service.executeNode = func(context.Context, *models.Run, WorkflowNode, int) (string, error) {
		return `{"message":"attempt-two"}`, nil
	}
	if err := fixture.service.HandleRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID); err != nil {
		t.Fatalf("resume after abandoned step failed: %v", err)
	}
	var steps []models.RunStep
	if err := fixture.database.Where("run_id = ? AND step_key = ?", fixture.run.RunID, "postprocess").Order("attempt ASC").Find(&steps).Error; err != nil {
		t.Fatalf("load resumed step attempts failed: %v", err)
	}
	if len(steps) != 2 || steps[0].Attempt != 1 || steps[0].Status != models.RunStepStatusFailed || !strings.Contains(steps[0].ErrorMessage, abandonedResumeStepError) || steps[1].Attempt != 2 || steps[1].Status != models.RunStepStatusSuccess {
		t.Fatalf("unexpected recovered step attempts: %+v", steps)
	}
}

func TestHandleRunResumeFailsWhenPersistedWorkflowIsMissing(t *testing.T) {
	definition := `{"entry_node":"delegate_parent","nodes":[{"key":"delegate_parent","type":"noop"},{"key":"finalize","type":"noop"}],"edges":[{"from":"delegate_parent","to":"finalize"}]}`
	fixture := setupResumeLeaseFixture(t, definition, "finalize")
	if err := fixture.database.Delete(&models.Workflow{}, fixture.workflow.ID).Error; err != nil {
		t.Fatalf("delete persisted workflow failed: %v", err)
	}
	err := fixture.service.HandleRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if err == nil {
		t.Fatal("missing workflow should fail resumed run")
	}
	var run models.Run
	if loadErr := fixture.database.Where("run_id = ?", fixture.run.RunID).First(&run).Error; loadErr != nil {
		t.Fatalf("reload failed run failed: %v", loadErr)
	}
	if run.Status != models.RunStatusFailed || run.ErrorMessage == "" {
		t.Fatalf("missing workflow did not fail run diagnostically: %+v", run)
	}
	var delegation models.Delegation
	if loadErr := fixture.database.First(&delegation, fixture.delegation.ID).Error; loadErr != nil {
		t.Fatalf("reload completed lease failed: %v", loadErr)
	}
	if delegation.ResumeStatus != models.DelegationResumeStatusCompleted || delegation.ResumeError == "" || delegation.ResumeLeaseOwner != "" {
		t.Fatalf("missing workflow did not terminate resume lease: %+v", delegation)
	}
}

func TestHandleRunResumeTreatsNodeDeadlineAsTerminalFailure(t *testing.T) {
	definition := `{"entry_node":"delegate_parent","nodes":[{"key":"delegate_parent","type":"noop"},{"key":"finalize","type":"noop"}],"edges":[{"from":"delegate_parent","to":"finalize"}]}`
	fixture := setupResumeLeaseFixture(t, definition, "finalize")
	fixture.service.executeNode = func(context.Context, *models.Run, WorkflowNode, int) (string, error) {
		return "", context.DeadlineExceeded
	}

	err := fixture.service.HandleRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected node deadline failure, got %v", err)
	}

	var run models.Run
	if loadErr := fixture.database.Where("run_id = ?", fixture.run.RunID).First(&run).Error; loadErr != nil {
		t.Fatalf("reload deadline-failed run failed: %v", loadErr)
	}
	if run.Status != models.RunStatusFailed || !strings.Contains(run.ErrorMessage, context.DeadlineExceeded.Error()) {
		t.Fatalf("node deadline was incorrectly left for crash recovery: %+v", run)
	}
	var delegation models.Delegation
	if loadErr := fixture.database.First(&delegation, fixture.delegation.ID).Error; loadErr != nil {
		t.Fatalf("reload deadline-failed delegation failed: %v", loadErr)
	}
	if delegation.ResumeStatus != models.DelegationResumeStatusCompleted || delegation.ResumeLeaseOwner != "" || delegation.ResumeLeaseExpiresAt != nil {
		t.Fatalf("node deadline did not close the resume lease: %+v", delegation)
	}
}

func TestRecoverPendingResumesCompletesTerminalRunClaim(t *testing.T) {
	fixture := setupResumeLeaseFixture(t, `{"entry_node":"delegate_parent","nodes":[{"key":"delegate_parent","type":"noop"}],"edges":[]}`, "")
	_, _, _, claimed, err := fixture.service.claimRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if err != nil || !claimed {
		t.Fatalf("claim terminal fixture failed: claimed=%v err=%v", claimed, err)
	}
	if err := fixture.database.Model(&models.Run{}).Where("id = ?", fixture.run.ID).Update("status", models.RunStatusFailed).Error; err != nil {
		t.Fatalf("force terminal run failed: %v", err)
	}
	publisher := &resumeLeaseTestPublisher{}
	runtimeService, err := NewRuntimeService(fixture.database, fixture.service, WithRunResumePublisher(publisher))
	if err != nil {
		t.Fatalf("create terminal recovery runtime failed: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := runtimeService.RecoverPendingResumes(context.Background(), 10); err != nil {
			t.Fatalf("terminal recovery scan %d failed: %v", i+1, err)
		}
	}
	if publisher.count() != 0 {
		t.Fatalf("terminal run should not publish resume messages: %d", publisher.count())
	}
	var delegation models.Delegation
	if err := fixture.database.First(&delegation, fixture.delegation.ID).Error; err != nil {
		t.Fatalf("reload terminal delegation failed: %v", err)
	}
	if delegation.ResumeStatus != models.DelegationResumeStatusCompleted || delegation.ResumeLeaseOwner != "" || delegation.ResumeLeaseExpiresAt != nil || !strings.Contains(delegation.ResumeError, "terminal state") {
		t.Fatalf("terminal claim was not converged: %+v", delegation)
	}
}

func TestHandleRunResumeFailsOnContradictoryCheckpoint(t *testing.T) {
	definition := `{"entry_node":"delegate_parent","nodes":[{"key":"delegate_parent","type":"noop"},{"key":"postprocess","type":"noop"}],"edges":[{"from":"delegate_parent","to":"postprocess"}]}`
	fixture := setupResumeLeaseFixture(t, definition, "postprocess")
	if err := fixture.database.Create(&models.RunStep{
		RunID: fixture.run.RunID, TraceID: fixture.run.TraceID, StepKey: "postprocess", StepType: "noop",
		Attempt: 1, Status: models.RunStepStatusFailed, InputJSON: "{}", OutputJSON: "{}", ErrorMessage: "business failure",
	}).Error; err != nil {
		t.Fatalf("create contradictory checkpoint failed: %v", err)
	}

	err := fixture.service.HandleRunResume(context.Background(), fixture.run.RunID, fixture.delegation.DelegationID)
	if err == nil {
		t.Fatal("expected contradictory checkpoint failure")
	}
	var storedRun models.Run
	if loadErr := fixture.database.Where("run_id = ?", fixture.run.RunID).First(&storedRun).Error; loadErr != nil {
		t.Fatalf("reload failed run failed: %v", loadErr)
	}
	if storedRun.Status != models.RunStatusFailed || storedRun.ErrorMessage == "" {
		t.Fatalf("contradictory checkpoint did not produce diagnosable failure: %+v", storedRun)
	}
	var delegation models.Delegation
	if loadErr := fixture.database.First(&delegation, fixture.delegation.ID).Error; loadErr != nil {
		t.Fatalf("reload failed delegation failed: %v", loadErr)
	}
	if delegation.ResumeStatus != models.DelegationResumeStatusCompleted || delegation.ResumeError == "" {
		t.Fatalf("failed resume lease was not terminated: %+v", delegation)
	}
}
