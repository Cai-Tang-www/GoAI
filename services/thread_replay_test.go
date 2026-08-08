package services

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"GoAI/models"
	"GoAI/requestctx"

	"gorm.io/gorm"
)

type traceRecordingRunPublisher struct {
	runIDs   []string
	traceIDs []string
}

func (p *traceRecordingRunPublisher) PublishRunExecute(ctx context.Context, runID string) error {
	p.runIDs = append(p.runIDs, runID)
	p.traceIDs = append(p.traceIDs, requestctx.TraceIDFromContext(ctx))
	return nil
}

func loadThreadReplayAgentWorkflow(t *testing.T, database *gorm.DB) (models.Agent, models.Workflow) {
	t.Helper()
	var agent models.Agent
	if err := database.Where("agent_code = ?", "agent_test").First(&agent).Error; err != nil {
		t.Fatalf("load test agent failed: %v", err)
	}
	var workflow models.Workflow
	if err := database.Where("agent_id = ? AND is_active = ?", agent.ID, true).First(&workflow).Error; err != nil {
		t.Fatalf("load test workflow failed: %v", err)
	}
	return agent, workflow
}

func TestReplayThreadBuildsImmutableMessageSnapshotAndIsIdempotent(t *testing.T) {
	database, runtimeService, _, _ := setupRuntimeTestService(t)
	publisher := &traceRecordingRunPublisher{}
	runtimeService.runService.publisher = publisher
	agent, workflow := loadThreadReplayAgentWorkflow(t, database)
	thread := models.Thread{ThreadID: "thread-replay", OwnerUserID: 1, Status: models.ThreadStatusActive, MetadataJSON: "{}"}
	if err := database.Create(&thread).Error; err != nil {
		t.Fatalf("create thread failed: %v", err)
	}
	createdAt := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	for _, message := range []models.Message{
		{MessageID: "message-b", ThreadID: thread.ThreadID, RunID: "run-source", SenderType: models.MessageSenderAgent, ReceiverType: models.MessageSenderUser, MessageType: models.MessageTypeResult, ContentType: "application/json", ContentJSON: `{"text":"second"}`, MetadataJSON: `{"z":2,"a":1}`, Status: models.MessageStatusDelivered, CreatedAt: createdAt},
		{MessageID: "message-a", ThreadID: thread.ThreadID, RunID: "run-source", SenderType: models.MessageSenderUser, ReceiverType: models.MessageSenderAgent, MessageType: models.MessageTypeInput, ContentType: "application/json", ContentJSON: `{"text":"first"}`, MetadataJSON: `{}`, Status: models.MessageStatusDelivered, CreatedAt: createdAt},
	} {
		if err := database.Create(&message).Error; err != nil {
			t.Fatalf("create message %s failed: %v", message.MessageID, err)
		}
	}
	loopID := "loop-source"
	source := models.Run{
		RunID: "run-source", ThreadID: thread.ThreadID, TraceID: "trace-source", LoopID: &loopID,
		AgentID: agent.ID, WorkflowID: workflow.ID, UserID: 1, TriggerType: "agui",
		InputJSON: `{"prompt":"old"}`, Status: models.RunStatusSuccess, Provider: "deepseek", Model: "deepseek-chat",
		CreatedAt: createdAt,
	}
	if err := database.Create(&source).Error; err != nil {
		t.Fatalf("create source run failed: %v", err)
	}

	ctx := requestctx.WithTraceID(context.Background(), source.TraceID)
	first, err := runtimeService.ReplayThread(ctx, ThreadReplayCommand{
		OwnerUserID: 1, ThreadID: thread.ThreadID, SourceRunID: source.RunID, IdempotencyKey: "replay-key",
	})
	if err != nil {
		t.Fatalf("replay thread failed: %v", err)
	}
	if first.IdempotentHit || first.Run.Status != models.RunStatusQueued || first.SourceRunID != source.RunID {
		t.Fatalf("unexpected first replay result: %+v", first)
	}
	if first.Run.TraceID == source.TraceID || len(publisher.traceIDs) != 1 || publisher.traceIDs[0] != first.Run.TraceID {
		t.Fatalf("replay trace was not regenerated and propagated: source=%s run=%s published=%v", source.TraceID, first.Run.TraceID, publisher.traceIDs)
	}
	if first.Run.RunID == source.RunID || first.Run.TraceID == source.TraceID || first.Run.LoopID == nil || source.LoopID == nil || *first.Run.LoopID == *source.LoopID {
		t.Fatalf("replay identifiers were reused: source=%+v replay=%+v", source, first.Run)
	}
	if first.Run.UserID != thread.OwnerUserID || first.Run.AgentID != source.AgentID || first.Run.WorkflowID != source.WorkflowID || first.Run.Provider != source.Provider || first.Run.Model != source.Model {
		t.Fatalf("replay did not preserve execution ownership/source: thread=%+v source=%+v replay=%+v", thread, source, first.Run)
	}
	if len(publisher.runIDs) != 1 || publisher.runIDs[0] != first.Run.RunID {
		t.Fatalf("unexpected published runs: %v", publisher.runIDs)
	}

	var snapshot threadReplayInput
	if err := json.Unmarshal([]byte(first.Run.InputJSON), &snapshot); err != nil {
		t.Fatalf("decode replay snapshot failed: %v", err)
	}
	if snapshot.ThreadID != thread.ThreadID || snapshot.SourceRunID != source.RunID || len(snapshot.Messages) != 2 {
		t.Fatalf("unexpected replay snapshot: %+v", snapshot)
	}
	if snapshot.Messages[0].MessageID != "message-b" || snapshot.Messages[1].MessageID != "message-a" {
		t.Fatalf("snapshot did not preserve created_at/id order: %+v", snapshot.Messages)
	}
	if snapshot.Messages[0].Role != "assistant" || snapshot.Messages[0].Content != "second" || snapshot.Messages[1].Role != "user" || snapshot.Messages[1].Content != "first" {
		t.Fatalf("snapshot was not executable as LLM messages: %+v", snapshot.Messages)
	}
	if string(snapshot.Messages[0].Metadata) != `{"a":1,"z":2}` {
		t.Fatalf("metadata was not canonicalized: %s", snapshot.Messages[0].Metadata)
	}
	var messageCount int64
	if err := database.Model(&models.Message{}).Where("thread_id = ?", thread.ThreadID).Count(&messageCount).Error; err != nil {
		t.Fatalf("count thread messages failed: %v", err)
	}
	if messageCount != 2 {
		t.Fatalf("replay copied old messages: count=%d", messageCount)
	}

	repeated, err := runtimeService.ReplayThread(ctx, ThreadReplayCommand{
		OwnerUserID: 1, ThreadID: thread.ThreadID, SourceRunID: source.RunID, IdempotencyKey: "replay-key",
	})
	if err != nil {
		t.Fatalf("repeat replay failed: %v", err)
	}
	if !repeated.IdempotentHit || repeated.Run.RunID != first.Run.RunID || len(publisher.runIDs) != 1 {
		t.Fatalf("repeat replay was not idempotent: first=%+v repeated=%+v published=%v", first, repeated, publisher.runIDs)
	}
	if err := database.Model(&models.Thread{}).Where("thread_id = ?", thread.ThreadID).Update("status", models.ThreadStatusClosed).Error; err != nil {
		t.Fatalf("close thread after replay failed: %v", err)
	}
	repeatedAfterClose, err := runtimeService.ReplayThread(ctx, ThreadReplayCommand{
		OwnerUserID: 1, ThreadID: thread.ThreadID, SourceRunID: source.RunID, IdempotencyKey: "replay-key",
	})
	if err != nil {
		t.Fatalf("idempotent replay after thread close failed: %v", err)
	}
	if !repeatedAfterClose.IdempotentHit || repeatedAfterClose.Run.RunID != first.Run.RunID {
		t.Fatalf("idempotent replay after thread close returned a different run: first=%s repeated=%+v", first.Run.RunID, repeatedAfterClose)
	}
}

func TestReplayThreadSelectsLatestTerminalRunAndDetectsKeyConflict(t *testing.T) {
	database, runtimeService, _, _ := setupRuntimeTestService(t)
	agent, workflow := loadThreadReplayAgentWorkflow(t, database)
	thread := models.Thread{ThreadID: "thread-latest", OwnerUserID: 1, Status: models.ThreadStatusActive, MetadataJSON: "{}"}
	if err := database.Create(&thread).Error; err != nil {
		t.Fatalf("create thread failed: %v", err)
	}
	for _, source := range []models.Run{
		{RunID: "run-old", ThreadID: thread.ThreadID, AgentID: agent.ID, WorkflowID: workflow.ID, UserID: 1, TriggerType: "agui", InputJSON: `{}`, Status: models.RunStatusSuccess, CreatedAt: time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)},
		{RunID: "run-new", ThreadID: thread.ThreadID, AgentID: agent.ID, WorkflowID: workflow.ID, UserID: 1, TriggerType: "agui", InputJSON: `{}`, Status: models.RunStatusFailed, CreatedAt: time.Date(2026, time.August, 8, 13, 0, 0, 0, time.UTC)},
	} {
		loopID := "loop-" + source.RunID
		source.LoopID = &loopID
		if err := database.Create(&source).Error; err != nil {
			t.Fatalf("create source %s failed: %v", source.RunID, err)
		}
	}
	first, err := runtimeService.ReplayThread(context.Background(), ThreadReplayCommand{OwnerUserID: 1, ThreadID: thread.ThreadID, IdempotencyKey: "latest-key"})
	if err != nil {
		t.Fatalf("replay latest thread failed: %v", err)
	}
	if first.SourceRunID != "run-new" {
		t.Fatalf("latest terminal run was not selected: %s", first.SourceRunID)
	}

	if _, err := runtimeService.ReplayThread(context.Background(), ThreadReplayCommand{OwnerUserID: 1, ThreadID: thread.ThreadID, SourceRunID: "run-old", IdempotencyKey: "latest-key"}); !errors.Is(err, ErrIdempotencyKeyReused()) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
}

func TestReplayThreadEnforcesOwnershipStateAndSourceValidation(t *testing.T) {
	database, runtimeService, _, _ := setupRuntimeTestService(t)
	agent, workflow := loadThreadReplayAgentWorkflow(t, database)
	thread := models.Thread{ThreadID: "thread-owner", OwnerUserID: 2, Status: models.ThreadStatusActive, MetadataJSON: "{}"}
	if err := database.Create(&thread).Error; err != nil {
		t.Fatalf("create thread failed: %v", err)
	}
	loopID := "loop-owner"
	source := models.Run{RunID: "run-owner", ThreadID: thread.ThreadID, AgentID: agent.ID, WorkflowID: workflow.ID, UserID: 2, TriggerType: "agui", InputJSON: `{}`, Status: models.RunStatusSuccess, LoopID: &loopID}
	if err := database.Create(&source).Error; err != nil {
		t.Fatalf("create source failed: %v", err)
	}
	tests := []struct {
		name    string
		command ThreadReplayCommand
		wantErr error
	}{
		{name: "missing thread", command: ThreadReplayCommand{OwnerUserID: 2, ThreadID: "missing"}, wantErr: ErrThreadNotFound()},
		{name: "foreign owner", command: ThreadReplayCommand{OwnerUserID: 1, ThreadID: thread.ThreadID}, wantErr: ErrRunForbidden()},
		{name: "wrong source thread", command: ThreadReplayCommand{OwnerUserID: 2, ThreadID: thread.ThreadID, SourceRunID: "missing-source"}, wantErr: ErrRunNotFound()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := runtimeService.ReplayThread(context.Background(), test.command); !errors.Is(err, test.wantErr) {
				t.Fatalf("ReplayThread() error=%v, want=%v", err, test.wantErr)
			}
		})
	}

	closed := thread
	closed.ID = 0
	closed.ThreadID = "thread-closed"
	closed.Status = models.ThreadStatusClosed
	if err := database.Create(&closed).Error; err != nil {
		t.Fatalf("create closed thread failed: %v", err)
	}
	if _, err := runtimeService.ReplayThread(context.Background(), ThreadReplayCommand{OwnerUserID: 2, ThreadID: closed.ThreadID}); !errors.Is(err, ErrThreadUnavailable()) {
		t.Fatalf("expected unavailable thread error, got %v", err)
	}

	running := source
	running.ID = 0
	running.RunID = "run-running"
	running.Status = models.RunStatusRunning
	running.LoopID = nil
	if err := database.Create(&running).Error; err != nil {
		t.Fatalf("create running source failed: %v", err)
	}
	if _, err := runtimeService.ReplayThread(context.Background(), ThreadReplayCommand{OwnerUserID: 2, ThreadID: thread.ThreadID, SourceRunID: running.RunID}); !errors.Is(err, ErrRunNotReplayable()) {
		t.Fatalf("expected non-replayable error, got %v", err)
	}

	adminResult, err := runtimeService.ReplayThread(context.Background(), ThreadReplayCommand{OwnerUserID: 1, IsAdmin: true, ThreadID: thread.ThreadID, SourceRunID: source.RunID, IdempotencyKey: "admin-replay-key"})
	if err != nil {
		t.Fatalf("admin replay failed: %v", err)
	}
	if adminResult.Run.UserID != thread.OwnerUserID {
		t.Fatalf("admin replay changed run owner: got=%d want=%d", adminResult.Run.UserID, thread.OwnerUserID)
	}
	ownerResult, err := runtimeService.ReplayThread(context.Background(), ThreadReplayCommand{OwnerUserID: 2, ThreadID: thread.ThreadID, SourceRunID: source.RunID, IdempotencyKey: "admin-replay-key"})
	if err != nil {
		t.Fatalf("owner replay after admin replay failed: %v", err)
	}
	if ownerResult.IdempotentHit || ownerResult.Run.RunID == adminResult.Run.RunID {
		t.Fatalf("admin and owner unexpectedly shared idempotency mapping: admin=%+v owner=%+v", adminResult, ownerResult)
	}
}

func TestReplayThreadAcceptsEmptyHistory(t *testing.T) {
	database, runtimeService, _, publisher := setupRuntimeTestService(t)
	agent, workflow := loadThreadReplayAgentWorkflow(t, database)
	thread := models.Thread{ThreadID: "thread-empty-history", OwnerUserID: 1, Status: models.ThreadStatusActive, MetadataJSON: "{}"}
	if err := database.Create(&thread).Error; err != nil {
		t.Fatalf("create empty thread failed: %v", err)
	}
	loopID := "loop-empty-history-source"
	source := models.Run{
		RunID: "run-empty-history-source", ThreadID: thread.ThreadID, TraceID: "trace-empty-history-source", LoopID: &loopID,
		AgentID: agent.ID, WorkflowID: workflow.ID, UserID: 1, TriggerType: "agui", InputJSON: `{}`,
		Status: models.RunStatusCancelled,
	}
	if err := database.Create(&source).Error; err != nil {
		t.Fatalf("create empty history source failed: %v", err)
	}
	result, err := runtimeService.ReplayThread(context.Background(), ThreadReplayCommand{OwnerUserID: 1, ThreadID: thread.ThreadID, SourceRunID: source.RunID})
	if err != nil {
		t.Fatalf("replay empty history failed: %v", err)
	}
	var snapshot threadReplayInput
	if err := json.Unmarshal([]byte(result.Run.InputJSON), &snapshot); err != nil {
		t.Fatalf("decode empty history snapshot failed: %v", err)
	}
	if snapshot.ThreadID != thread.ThreadID || snapshot.SourceRunID != source.RunID || len(snapshot.Messages) != 0 {
		t.Fatalf("unexpected empty history snapshot: %+v", snapshot)
	}
	if len(publisher.runIDs) != 1 {
		t.Fatalf("empty history replay was not published exactly once: %v", publisher.runIDs)
	}
}

func TestReplayThreadDispatchFailureMarksRunFailed(t *testing.T) {
	database, runtimeService, _, publisher := setupRuntimeTestService(t)
	agent, workflow := loadThreadReplayAgentWorkflow(t, database)
	thread := models.Thread{ThreadID: "thread-dispatch", OwnerUserID: 1, Status: models.ThreadStatusActive, MetadataJSON: "{}"}
	if err := database.Create(&thread).Error; err != nil {
		t.Fatalf("create thread failed: %v", err)
	}
	loopID := "loop-dispatch"
	source := models.Run{RunID: "run-dispatch-source", ThreadID: thread.ThreadID, AgentID: agent.ID, WorkflowID: workflow.ID, UserID: 1, TriggerType: "agui", InputJSON: `{}`, Status: models.RunStatusSuccess, LoopID: &loopID}
	if err := database.Create(&source).Error; err != nil {
		t.Fatalf("create source failed: %v", err)
	}
	publisher.err = errors.New("kafka unavailable")
	result, err := runtimeService.ReplayThread(context.Background(), ThreadReplayCommand{OwnerUserID: 1, ThreadID: thread.ThreadID, SourceRunID: source.RunID})
	if result != nil || !errors.Is(err, ErrRunDispatchFailed()) {
		t.Fatalf("expected dispatch failure, result=%+v err=%v", result, err)
	}
	var replay models.Run
	if err := database.Where("thread_id = ? AND trigger_type = ?", thread.ThreadID, "replay").First(&replay).Error; err != nil {
		t.Fatalf("load failed replay run: %v", err)
	}
	if replay.Status != models.RunStatusFailed || len(publisher.runIDs) != 1 {
		t.Fatalf("dispatch failure did not persist failed run: %+v published=%v", replay, publisher.runIDs)
	}
}
