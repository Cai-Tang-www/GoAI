package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"GoAI/models"

	"gorm.io/gorm"
)

type delegationFixture struct {
	database       *gorm.DB
	runtime        *RuntimeService
	publisher      *recordingRunPublisher
	sourceAgent    models.Agent
	targetAgent    models.Agent
	sourceWorkflow models.Workflow
	targetWorkflow models.Workflow
	parentRun      models.Run
}

func setupDelegationFixture(t *testing.T) delegationFixture {
	t.Helper()
	database := openSQLiteTestDB(t)
	if err := database.AutoMigrate(
		&models.Agent{},
		&models.AgentEndpoint{},
		&models.AgentCapability{},
		&models.Workflow{},
		&models.Thread{},
		&models.Message{},
		&models.Run{},
		&models.RunStep{},
		&models.RunIdempotency{},
		&models.Delegation{},
	); err != nil {
		t.Fatalf("auto migrate delegation fixture failed: %v", err)
	}

	source := models.Agent{AgentCode: "planner", Name: "Planner", OwnerUserID: 11, Status: models.AgentStatusActive}
	target := models.Agent{AgentCode: "writer", Name: "Writer", OwnerUserID: 22, Status: models.AgentStatusActive}
	if err := database.Create(&source).Error; err != nil {
		t.Fatalf("create source agent failed: %v", err)
	}
	if err := database.Create(&target).Error; err != nil {
		t.Fatalf("create target agent failed: %v", err)
	}
	definition := `{"entry_node":"work","nodes":[{"key":"work","type":"noop"}],"edges":[]}`
	sourceWorkflow := models.Workflow{AgentID: source.ID, Version: 1, DefinitionJSON: definition, Checksum: "source-v1", IsActive: true, CreatedBy: 11}
	targetWorkflow := models.Workflow{AgentID: target.ID, Version: 1, DefinitionJSON: definition, Checksum: "target-v1", IsActive: true, CreatedBy: 22}
	latestTargetWorkflow := models.Workflow{AgentID: target.ID, Version: 2, DefinitionJSON: definition, Checksum: "target-v2", IsActive: true, CreatedBy: 22}
	for _, workflow := range []*models.Workflow{&sourceWorkflow, &targetWorkflow, &latestTargetWorkflow} {
		if err := database.Create(workflow).Error; err != nil {
			t.Fatalf("create workflow failed: %v", err)
		}
	}
	capability := models.AgentCapability{
		AgentID: source.ID, CapabilityCode: "plan", Name: "Plan", CapabilityType: models.AgentCapabilityTypeWorkflow,
		WorkflowID: &sourceWorkflow.ID, Version: "1", Status: models.AgentCapabilityStatusActive,
	}
	if err := database.Create(&capability).Error; err != nil {
		t.Fatalf("create source capability failed: %v", err)
	}
	capability = models.AgentCapability{
		AgentID: target.ID, CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeWorkflow,
		WorkflowID: &targetWorkflow.ID, Version: "1", Status: models.AgentCapabilityStatusActive,
	}
	if err := database.Create(&capability).Error; err != nil {
		t.Fatalf("create target capability failed: %v", err)
	}
	parentRun := models.Run{
		RunID: "run_parent", ThreadID: "thread_shared", AgentID: source.ID, WorkflowID: sourceWorkflow.ID,
		UserID: 42, TriggerType: "agui", InputJSON: `{}`, Status: models.RunStatusRunning,
	}
	if err := database.Create(&parentRun).Error; err != nil {
		t.Fatalf("create parent run failed: %v", err)
	}
	publisher := &recordingRunPublisher{}
	runService, err := NewRunService(database, publisher)
	if err != nil {
		t.Fatalf("create run service failed: %v", err)
	}
	runtimeService, err := NewRuntimeService(database, runService)
	if err != nil {
		t.Fatalf("create runtime service failed: %v", err)
	}
	return delegationFixture{
		database: database, runtime: runtimeService, publisher: publisher,
		sourceAgent: source, targetAgent: target, sourceWorkflow: sourceWorkflow,
		targetWorkflow: targetWorkflow, parentRun: parentRun,
	}
}

func delegationCommand() AcceptDelegationCommand {
	return AcceptDelegationCommand{
		SourceAgentCode: "planner", TargetAgentCode: "writer", CapabilityCode: "write",
		ParentRunID: "run_parent", ThreadID: "thread_shared", RequestedChildRunID: "run_child",
		RequestMessageID: "msg_delegate", Input: []byte(`{"prompt":"draft"}`),
		MetadataJSON: `{"protocol":"a2a"}`,
	}
}

func TestAcceptDelegationCreatesAtomicCollaborationState(t *testing.T) {
	fixture := setupDelegationFixture(t)
	result, err := fixture.runtime.AcceptDelegation(context.Background(), delegationCommand())
	if err != nil {
		t.Fatalf("accept delegation failed: %v", err)
	}
	if result.Reused {
		t.Fatal("first delegation must not be reused")
	}
	if result.Run.WorkflowID != fixture.targetWorkflow.ID {
		t.Fatalf("capability workflow mismatch: got=%d want=%d", result.Run.WorkflowID, fixture.targetWorkflow.ID)
	}
	if result.Run.TriggerType != "a2a" || result.Run.UserID != fixture.parentRun.UserID || result.Run.Status != models.RunStatusQueued {
		t.Fatalf("unexpected child run: %+v", result.Run)
	}
	if result.Delegation.Status != models.DelegationStatusAccepted || result.Delegation.ChildRunID != result.Run.RunID {
		t.Fatalf("unexpected delegation: %+v", result.Delegation)
	}
	if len(fixture.publisher.runIDs) != 1 || fixture.publisher.runIDs[0] != result.Run.RunID {
		t.Fatalf("unexpected published runs: %v", fixture.publisher.runIDs)
	}

	var thread models.Thread
	if err := fixture.database.Where("thread_id = ?", "thread_shared").First(&thread).Error; err != nil {
		t.Fatalf("thread was not persisted: %v", err)
	}
	if thread.OwnerUserID != fixture.parentRun.UserID {
		t.Fatalf("thread owner mismatch: got=%d want=%d", thread.OwnerUserID, fixture.parentRun.UserID)
	}
	var message models.Message
	if err := fixture.database.Where("message_id = ?", "msg_delegate").First(&message).Error; err != nil {
		t.Fatalf("request message was not persisted: %v", err)
	}
	if message.SenderID != "planner" || message.ReceiverID != "writer" || message.MessageType != models.MessageTypeDelegation {
		t.Fatalf("unexpected request message: %+v", message)
	}
}

func TestAcceptDelegationSupportsRemoteParentRun(t *testing.T) {
	fixture := setupDelegationFixture(t)
	command := delegationCommand()
	command.ParentRunID = "remote_parent"
	command.ThreadID = "remote_thread"
	command.RequestedChildRunID = "remote_child"
	command.RequestMessageID = "remote_message"

	result, err := fixture.runtime.AcceptDelegation(context.Background(), command)
	if err != nil {
		t.Fatalf("accept remote delegation failed: %v", err)
	}
	if result.Run.UserID != fixture.targetAgent.OwnerUserID {
		t.Fatalf("remote delegation owner mismatch: got=%d want=%d", result.Run.UserID, fixture.targetAgent.OwnerUserID)
	}
}

func TestAcceptDelegationIsIdempotentAndDetectsConflict(t *testing.T) {
	fixture := setupDelegationFixture(t)
	first, err := fixture.runtime.AcceptDelegation(context.Background(), delegationCommand())
	if err != nil {
		t.Fatalf("first accept failed: %v", err)
	}
	second, err := fixture.runtime.AcceptDelegation(context.Background(), delegationCommand())
	if err != nil {
		t.Fatalf("duplicate accept failed: %v", err)
	}
	if !second.Reused || second.Run.RunID != first.Run.RunID || second.Delegation.DelegationID != first.Delegation.DelegationID {
		t.Fatalf("duplicate request did not reuse delegation: first=%+v second=%+v", first, second)
	}
	if len(fixture.publisher.runIDs) != 1 {
		t.Fatalf("duplicate request published more than once: %v", fixture.publisher.runIDs)
	}

	changed := delegationCommand()
	changed.Input = []byte(`{"prompt":"changed"}`)
	if _, err := fixture.runtime.AcceptDelegation(context.Background(), changed); !errors.Is(err, ErrRunAlreadyExists()) {
		t.Fatalf("expected requested run conflict, got %v", err)
	}
}

func TestAcceptDelegationIdempotencyDetectsMetadataConflict(t *testing.T) {
	fixture := setupDelegationFixture(t)
	if _, err := fixture.runtime.AcceptDelegation(context.Background(), delegationCommand()); err != nil {
		t.Fatalf("accept first delegation failed: %v", err)
	}
	changed := delegationCommand()
	changed.MetadataJSON = "{\"protocol\":\"different\"}"
	if _, err := fixture.runtime.AcceptDelegation(context.Background(), changed); !errors.Is(err, ErrDelegationConflict()) {
		t.Fatalf("expected metadata conflict, got %v", err)
	}
}

func TestAcceptDelegationValidatesAgentsCapabilityAndParent(t *testing.T) {
	tests := []struct {
		name string
		edit func(*delegationFixture, *AcceptDelegationCommand)
		want error
	}{
		{name: "missing target", edit: func(_ *delegationFixture, command *AcceptDelegationCommand) { command.TargetAgentCode = "missing" }, want: ErrAgentNotFound()},
		{name: "missing capability", edit: func(_ *delegationFixture, command *AcceptDelegationCommand) { command.CapabilityCode = "missing" }, want: ErrCapabilityNotFound()},
		{name: "same agent", edit: func(_ *delegationFixture, command *AcceptDelegationCommand) {
			command.TargetAgentCode = command.SourceAgentCode
			command.CapabilityCode = "plan"
		}, want: ErrInvalidDelegation()},
		{name: "parent ownership", edit: func(fixture *delegationFixture, command *AcceptDelegationCommand) {
			fixture.database.Model(&models.Run{}).Where("run_id = ?", command.ParentRunID).Update("agent_id", fixture.targetAgent.ID)
		}, want: ErrInvalidDelegation()},
		{name: "thread mismatch", edit: func(_ *delegationFixture, command *AcceptDelegationCommand) { command.ThreadID = "different_thread" }, want: ErrInvalidDelegation()},
		{name: "inactive workflow", edit: func(fixture *delegationFixture, _ *AcceptDelegationCommand) {
			fixture.database.Model(&models.Workflow{}).Where("id = ?", fixture.targetWorkflow.ID).Update("is_active", false)
		}, want: ErrInvalidDelegation()},
		{name: "workflow belongs to another agent", edit: func(fixture *delegationFixture, _ *AcceptDelegationCommand) {
			fixture.database.Model(&models.AgentCapability{}).Where("agent_id = ? AND capability_code = ?", fixture.targetAgent.ID, "write").Update("workflow_id", fixture.sourceWorkflow.ID)
		}, want: ErrInvalidDelegation()},
		{name: "workflow version mismatch", edit: func(fixture *delegationFixture, _ *AcceptDelegationCommand) {
			fixture.database.Model(&models.AgentCapability{}).Where("agent_id = ? AND capability_code = ?", fixture.targetAgent.ID, "write").Update("version", "2")
		}, want: ErrInvalidDelegation()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := setupDelegationFixture(t)
			command := delegationCommand()
			test.edit(&fixture, &command)
			if _, err := fixture.runtime.AcceptDelegation(context.Background(), command); !errors.Is(err, test.want) {
				t.Fatalf("got error %v, want %v", err, test.want)
			}
		})
	}
}

func TestAcceptDelegationRollsBackWhenRequestMessageConflicts(t *testing.T) {
	fixture := setupDelegationFixture(t)
	seed := models.Message{
		MessageID: "msg_delegate", ThreadID: "other_thread", SenderType: models.MessageSenderAgent,
		SenderID: "other", ReceiverType: models.MessageSenderAgent, ReceiverID: "writer",
		MessageType: models.MessageTypeDelegation, ContentType: "application/json", ContentJSON: `{}`,
		MetadataJSON: `{}`, Status: models.MessageStatusDelivered,
	}
	if err := fixture.database.Create(&seed).Error; err != nil {
		t.Fatalf("seed conflicting message failed: %v", err)
	}
	if _, err := fixture.runtime.AcceptDelegation(context.Background(), delegationCommand()); err == nil {
		t.Fatal("expected delegation creation failure")
	}
	var runCount int64
	if err := fixture.database.Model(&models.Run{}).Where("run_id = ?", "run_child").Count(&runCount).Error; err != nil {
		t.Fatalf("count runs failed: %v", err)
	}
	var delegationCount int64
	if err := fixture.database.Model(&models.Delegation{}).Where("child_run_id = ?", "run_child").Count(&delegationCount).Error; err != nil {
		t.Fatalf("count delegations failed: %v", err)
	}
	if runCount != 0 || delegationCount != 0 {
		t.Fatalf("transaction should roll back: runs=%d delegations=%d", runCount, delegationCount)
	}
}

func TestAcceptDelegationKafkaFailureCanBeReconciled(t *testing.T) {
	fixture := setupDelegationFixture(t)
	fixture.publisher.err = errors.New("kafka unavailable")
	if _, err := fixture.runtime.AcceptDelegation(context.Background(), delegationCommand()); err == nil {
		t.Fatal("expected publish failure")
	}
	var run models.Run
	if err := fixture.database.Where("run_id = ?", "run_child").First(&run).Error; err != nil {
		t.Fatalf("load failed child run failed: %v", err)
	}
	if run.Status != models.RunStatusFailed {
		t.Fatalf("run status got=%s want=%s", run.Status, models.RunStatusFailed)
	}
	if err := fixture.runtime.ReconcileDelegation(context.Background(), run.RunID); err != nil {
		t.Fatalf("reconcile failed run failed: %v", err)
	}
	var delegation models.Delegation
	if err := fixture.database.Where("child_run_id = ?", run.RunID).First(&delegation).Error; err != nil {
		t.Fatalf("load delegation failed: %v", err)
	}
	if delegation.Status != models.DelegationStatusFailed || delegation.ResultMessageID == "" {
		t.Fatalf("failed delegation was not finalized: %+v", delegation)
	}
}

func TestReconcileDelegationMapsRunStateAndCreatesOneResult(t *testing.T) {
	fixture := setupDelegationFixture(t)
	result, err := fixture.runtime.AcceptDelegation(context.Background(), delegationCommand())
	if err != nil {
		t.Fatalf("accept delegation failed: %v", err)
	}
	now := time.Now()
	if err := fixture.database.Model(&models.Run{}).Where("run_id = ?", result.Run.RunID).Updates(map[string]any{
		"status": models.RunStatusSuccess, "started_at": now, "finished_at": now,
	}).Error; err != nil {
		t.Fatalf("mark child run success failed: %v", err)
	}
	step := models.RunStep{
		RunID: result.Run.RunID, StepKey: "work", StepType: "noop", Attempt: 1,
		Status: models.RunStepStatusSuccess, InputJSON: `{}`, OutputJSON: `{"answer":"ok"}`,
	}
	if err := fixture.database.Create(&step).Error; err != nil {
		t.Fatalf("create successful step failed: %v", err)
	}

	for range 3 {
		if err := fixture.runtime.ReconcileDelegation(context.Background(), result.Run.RunID); err != nil {
			t.Fatalf("reconcile success failed: %v", err)
		}
	}
	var delegation models.Delegation
	if err := fixture.database.Where("child_run_id = ?", result.Run.RunID).First(&delegation).Error; err != nil {
		t.Fatalf("load delegation failed: %v", err)
	}
	if delegation.Status != models.DelegationStatusSucceeded || delegation.OutputJSON != step.OutputJSON || delegation.ResultMessageID == "" {
		t.Fatalf("unexpected reconciled delegation: %+v", delegation)
	}
	var messages []models.Message
	if err := fixture.database.Where("delegation_id = ? AND message_type = ?", delegation.DelegationID, models.MessageTypeResult).Find(&messages).Error; err != nil {
		t.Fatalf("load result messages failed: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("result message count got=%d want=1", len(messages))
	}
	if messages[0].SenderID != "writer" || messages[0].ReceiverID != "planner" {
		t.Fatalf("result message routing is invalid: %+v", messages[0])
	}
}

func TestReconcileDelegationConcurrentCallsRemainIdempotent(t *testing.T) {
	fixture := setupDelegationFixture(t)
	sqlDB, err := fixture.database.DB()
	if err != nil {
		t.Fatalf("get SQL database failed: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	result, err := fixture.runtime.AcceptDelegation(context.Background(), delegationCommand())
	if err != nil {
		t.Fatalf("accept delegation failed: %v", err)
	}
	if err := fixture.database.Model(&models.Run{}).Where("run_id = ?", result.Run.RunID).Update("status", models.RunStatusFailed).Error; err != nil {
		t.Fatalf("mark child run failed: %v", err)
	}

	const workers = 8
	var wait sync.WaitGroup
	errorsCh := make(chan error, workers)
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errorsCh <- fixture.runtime.ReconcileDelegation(context.Background(), result.Run.RunID)
		}()
	}
	wait.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("concurrent reconcile failed: %v", err)
		}
	}
	var count int64
	if err := fixture.database.Model(&models.Message{}).
		Where("delegation_id = ? AND message_type = ?", result.Delegation.DelegationID, models.MessageTypeResult).
		Count(&count).Error; err != nil {
		t.Fatalf("count result messages failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("result message count got=%d want=1", count)
	}
}

func TestDelegationSnapshotRestrictsTargetAgent(t *testing.T) {
	fixture := setupDelegationFixture(t)
	if _, err := fixture.runtime.AcceptDelegation(context.Background(), delegationCommand()); err != nil {
		t.Fatalf("accept delegation failed: %v", err)
	}
	if _, err := fixture.runtime.DelegationSnapshot(context.Background(), "planner", "run_child"); !errors.Is(err, ErrDelegationNotFound()) {
		t.Fatalf("source agent should not query target task endpoint, got %v", err)
	}
	snapshot, err := fixture.runtime.DelegationSnapshot(context.Background(), "writer", "run_child")
	if err != nil {
		t.Fatalf("target snapshot failed: %v", err)
	}
	if snapshot.Delegation.ChildRunID != "run_child" || len(snapshot.Messages) != 1 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
}

func TestDescribeAgentReturnsOnlyActiveA2AAssets(t *testing.T) {
	fixture := setupDelegationFixture(t)
	for index, endpoint := range []models.AgentEndpoint{
		{AgentID: fixture.targetAgent.ID, EndpointCode: "local", Protocol: models.AgentEndpointProtocolA2A, Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1:8080/a2a/agents/writer", Status: models.AgentEndpointStatusActive},
		{AgentID: fixture.targetAgent.ID, EndpointCode: "inactive", Protocol: models.AgentEndpointProtocolA2A, Transport: models.AgentEndpointTransportHTTPS, Address: "https://agents.example.com/writer", Status: models.AgentEndpointStatusInactive},
	} {
		endpoint.EndpointCode = fmt.Sprintf("%s-%d", endpoint.EndpointCode, index)
		if err := fixture.database.Create(&endpoint).Error; err != nil {
			t.Fatalf("create endpoint failed: %v", err)
		}
	}
	invalidCapability := models.AgentCapability{
		AgentID: fixture.targetAgent.ID, CapabilityCode: "stale", Name: "Stale", CapabilityType: models.AgentCapabilityTypeWorkflow,
		WorkflowID: &fixture.targetWorkflow.ID, Version: "2", Status: models.AgentCapabilityStatusActive,
	}
	if err := fixture.database.Create(&invalidCapability).Error; err != nil {
		t.Fatalf("create stale capability failed: %v", err)
	}
	descriptor, err := fixture.runtime.DescribeAgent(context.Background(), fixture.targetAgent.AgentCode)
	if err != nil {
		t.Fatalf("describe agent failed: %v", err)
	}
	if len(descriptor.Capabilities) != 1 || descriptor.Capabilities[0].Code != "write" {
		t.Fatalf("unexpected capabilities: %+v", descriptor.Capabilities)
	}
	if len(descriptor.Endpoints) != 1 || descriptor.Endpoints[0].Transport != models.AgentEndpointTransportHTTP {
		t.Fatalf("unexpected endpoints: %+v", descriptor.Endpoints)
	}
}
