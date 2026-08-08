package services

import (
	"context"
	"errors"
	"testing"

	"GoAI/models"

	"gorm.io/gorm"
)

func setupRunTraceService(t *testing.T) (*gorm.DB, *RunService) {
	t.Helper()
	database := openSQLiteTestDB(t)
	if err := database.AutoMigrate(
		&models.Run{},
		&models.RunStep{},
		&models.LoopRecord{},
		&models.LoopEvaluation{},
		&models.Delegation{},
		&models.DelegationGroup{},
		&models.Message{},
	); err != nil {
		t.Fatalf("auto migrate trace models failed: %v", err)
	}
	service, err := NewRunService(database, RunEventPublisherFunc(func(context.Context, string) error { return nil }))
	if err != nil {
		t.Fatalf("create run service failed: %v", err)
	}
	return database, service
}

func TestGetRunTraceReturnsStableOwnedExecutionSnapshot(t *testing.T) {
	database, service := setupRunTraceService(t)
	root := models.Run{RunID: "run-trace-root", ThreadID: "thread-trace", TraceID: "trace-root", AgentID: 1, WorkflowID: 1, UserID: 7, TriggerType: "api", InputJSON: `{}`, Status: models.RunStatusSuccess}
	child := models.Run{RunID: "run-trace-child", ThreadID: "thread-trace", TraceID: "trace-child", AgentID: 2, WorkflowID: 2, UserID: 7, TriggerType: "a2a", InputJSON: `{}`, Status: models.RunStatusSuccess}
	other := models.Run{RunID: "run-trace-other", ThreadID: "thread-other", TraceID: "trace-other", AgentID: 3, WorkflowID: 3, UserID: 8, TriggerType: "api", InputJSON: `{}`, Status: models.RunStatusSuccess}
	for _, run := range []*models.Run{&root, &child, &other} {
		if err := database.Create(run).Error; err != nil {
			t.Fatalf("create run %s failed: %v", run.RunID, err)
		}
	}
	rootLoop := models.LoopRecord{LoopID: "loop-trace-root", TraceID: root.TraceID, RunID: root.RunID, AgentID: root.AgentID, LoopType: models.LoopTypeRun, Status: models.LoopStatusSuccess, InputSnapshotJSON: `{}`, OutputSnapshotJSON: `{}`}
	childLoop := models.LoopRecord{LoopID: "loop-trace-child", TraceID: child.TraceID, RunID: child.RunID, AgentID: child.AgentID, LoopType: models.LoopTypeDelegation, Status: models.LoopStatusSuccess, InputSnapshotJSON: `{}`, OutputSnapshotJSON: `{}`}
	otherLoop := models.LoopRecord{LoopID: "loop-trace-other", TraceID: other.TraceID, RunID: other.RunID, AgentID: other.AgentID, LoopType: models.LoopTypeRun, Status: models.LoopStatusSuccess, InputSnapshotJSON: `{}`, OutputSnapshotJSON: `{}`}
	for _, loop := range []*models.LoopRecord{&rootLoop, &childLoop, &otherLoop} {
		if err := database.Create(loop).Error; err != nil {
			t.Fatalf("create loop %s failed: %v", loop.LoopID, err)
		}
	}
	if err := database.Create(&models.RunStep{RunID: root.RunID, TraceID: root.TraceID, LoopID: rootLoop.LoopID, StepKey: "plan", StepType: "planner", Attempt: 1, Status: models.RunStepStatusSuccess, InputJSON: `{}`, OutputJSON: `{}`}).Error; err != nil {
		t.Fatalf("create root step failed: %v", err)
	}
	if err := database.Create(&models.RunStep{RunID: child.RunID, TraceID: child.TraceID, LoopID: childLoop.LoopID, StepKey: "write", StepType: "noop", Attempt: 1, Status: models.RunStepStatusSuccess, InputJSON: `{}`, OutputJSON: `{}`}).Error; err != nil {
		t.Fatalf("create child step failed: %v", err)
	}
	if err := database.Create(&models.Delegation{DelegationID: "delegation-trace", ThreadID: root.ThreadID, ParentRunID: root.RunID, ChildRunID: child.RunID, TraceID: root.TraceID, SourceAgentID: root.AgentID, TargetAgentID: child.AgentID, CapabilityCode: "write", RequestMessageID: "message-request", ParentStepKey: "delegate", InputJSON: `{}`, OutputJSON: `{}`, Status: models.DelegationStatusSucceeded}).Error; err != nil {
		t.Fatalf("create delegation failed: %v", err)
	}
	if err := database.Create(&models.DelegationGroup{GroupID: "group-trace", ThreadID: root.ThreadID, ParentRunID: root.RunID, ParentStepKey: "delegate", CoordinatorDelegationID: "delegation-trace", TraceID: root.TraceID, Strategy: models.DelegationGroupStrategyAll, RequiredSuccesses: 1, TotalMembers: 1, SucceededMembers: 1, Status: models.DelegationGroupStatusSucceeded, ResultJSON: `{}`}).Error; err != nil {
		t.Fatalf("create delegation group failed: %v", err)
	}
	for _, message := range []*models.Message{
		{MessageID: "message-request", ThreadID: root.ThreadID, RunID: root.RunID, DelegationID: "delegation-trace", SenderType: models.MessageSenderAgent, SenderID: "source", ReceiverType: models.MessageSenderAgent, ReceiverID: "target", MessageType: models.MessageTypeDelegation, ContentType: "application/json", ContentJSON: `{}`, MetadataJSON: `{}`, Status: models.MessageStatusDelivered},
		{MessageID: "message-child", ThreadID: child.ThreadID, RunID: child.RunID, DelegationID: "delegation-trace", SenderType: models.MessageSenderAgent, SenderID: "target", ReceiverType: models.MessageSenderAgent, ReceiverID: "source", MessageType: models.MessageTypeResult, ContentType: "application/json", ContentJSON: `{}`, MetadataJSON: `{}`, Status: models.MessageStatusDelivered},
	} {
		if err := database.Create(message).Error; err != nil {
			t.Fatalf("create trace message failed: %v", err)
		}
	}
	if err := database.Create(&models.LoopEvaluation{LoopID: childLoop.LoopID, EvaluatorCode: "quality-v1", Status: models.EvaluationStatusSuccess, ResultJSON: `{"label":"good"}`}).Error; err != nil {
		t.Fatalf("create loop evaluation failed: %v", err)
	}

	snapshot, err := service.GetRunTrace(context.Background(), 7, false, root.RunID)
	if err != nil {
		t.Fatalf("get run trace failed: %v", err)
	}
	if snapshot.RootRun.RunID != root.RunID || len(snapshot.Runs) != 2 || len(snapshot.Steps) != 2 || len(snapshot.Loops) != 2 || len(snapshot.Delegations) != 1 || len(snapshot.DelegationGroups) != 1 || len(snapshot.Messages) != 2 || len(snapshot.Evaluations) != 1 {
		t.Fatalf("unexpected trace snapshot: %+v", snapshot)
	}
	if snapshot.Runs[0].ID >= snapshot.Runs[1].ID {
		t.Fatalf("runs are not deterministically ordered: %+v", snapshot.Runs)
	}
	if snapshot.Evaluations[0].LoopID != childLoop.LoopID {
		t.Fatalf("unexpected trace evaluation: %+v", snapshot.Evaluations[0])
	}

	loops, err := service.GetRunLoops(context.Background(), 7, false, root.RunID)
	if err != nil || len(loops) != 1 || loops[0].LoopID != rootLoop.LoopID {
		t.Fatalf("unexpected run loops: loops=%+v err=%v", loops, err)
	}
	detail, err := service.GetLoopDetail(context.Background(), 7, false, childLoop.LoopID)
	if err != nil || len(detail.Evaluations) != 1 {
		t.Fatalf("unexpected loop detail: detail=%+v err=%v", detail, err)
	}
}

func TestRunTraceEnforcesOwnershipAndStableMissingLoopError(t *testing.T) {
	database, service := setupRunTraceService(t)
	run := models.Run{RunID: "run-trace-private", AgentID: 1, WorkflowID: 1, UserID: 7, TriggerType: "api", InputJSON: `{}`, Status: models.RunStatusSuccess}
	if err := database.Create(&run).Error; err != nil {
		t.Fatalf("create private run failed: %v", err)
	}
	if err := database.Create(&models.LoopRecord{LoopID: "loop-trace-private", RunID: run.RunID, AgentID: run.AgentID, LoopType: models.LoopTypeRun, Status: models.LoopStatusSuccess, InputSnapshotJSON: `{}`, OutputSnapshotJSON: `{}`}).Error; err != nil {
		t.Fatalf("create private loop failed: %v", err)
	}
	if _, err := service.GetRunTrace(context.Background(), 8, false, run.RunID); !errors.Is(err, ErrRunForbidden()) {
		t.Fatalf("expected trace ownership error, got %v", err)
	}
	if _, err := service.GetLoopDetail(context.Background(), 8, false, "loop-trace-private"); !errors.Is(err, ErrRunForbidden()) {
		t.Fatalf("expected loop ownership error, got %v", err)
	}
	if _, err := service.GetLoopDetail(context.Background(), 7, false, "missing-loop"); !errors.Is(err, ErrLoopNotFound()) {
		t.Fatalf("expected missing loop error, got %v", err)
	}
	empty, err := service.GetRunTrace(context.Background(), 7, false, run.RunID)
	if err != nil {
		t.Fatalf("get empty trace failed: %v", err)
	}
	if empty.Runs == nil || empty.Steps == nil || empty.Loops == nil || empty.Delegations == nil || empty.DelegationGroups == nil || empty.Messages == nil || empty.Evaluations == nil {
		t.Fatalf("empty trace collections must be non-nil: %+v", empty)
	}
}
