package services

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"GoAI/models"

	"gorm.io/gorm"
)

func setupRuntimeTestService(t *testing.T) (*gorm.DB, *RuntimeService, *RunService, *recordingRunPublisher) {
	t.Helper()
	database := openSQLiteTestDB(t)
	if err := database.AutoMigrate(
		&models.Agent{},
		&models.Workflow{},
		&models.Thread{},
		&models.Message{},
		&models.Run{},
		&models.RunStep{},
		&models.RunInterrupt{},
		&models.RunIdempotency{},
		&models.Delegation{},
		&models.DelegationGroup{},
	); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
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
	seedAgentWorkflow(t, database)
	return database, runtimeService, runService, publisher
}

func validRuntimeCommand() StartRunCommand {
	return StartRunCommand{
		OwnerUserID: 1,
		AgentCode:   "agent_test",
		TriggerType: "agui",
		Input:       json.RawMessage(`{"messages":[{"role":"user","content":"hello"}]}`),
		Messages: []IncomingMessage{{
			MessageID:    "msg-input-1",
			SenderType:   models.MessageSenderUser,
			ReceiverType: models.MessageSenderAgent,
			MessageType:  models.MessageTypeInput,
			ContentType:  "text",
			ContentJSON:  `{"text":"hello"}`,
		}},
	}
}

func TestRuntimeStartRunCreatesThreadMessageAndRun(t *testing.T) {
	database, runtimeService, _, publisher := setupRuntimeTestService(t)
	result, err := runtimeService.StartRun(context.Background(), validRuntimeCommand())
	if err != nil {
		t.Fatalf("start run failed: %v", err)
	}
	if result.Thread == nil || result.Thread.ThreadID == "" || result.Thread.Status != models.ThreadStatusActive {
		t.Fatalf("unexpected thread: %+v", result.Thread)
	}
	if result.Run == nil || result.Run.Status != models.RunStatusQueued || result.Run.TriggerType != "agui" {
		t.Fatalf("unexpected run: %+v", result.Run)
	}
	if result.Run.ThreadID != result.Thread.ThreadID {
		t.Fatalf("run/thread mismatch: run=%s thread=%s", result.Run.ThreadID, result.Thread.ThreadID)
	}
	var message models.Message
	if err := database.Where("run_id = ?", result.Run.RunID).First(&message).Error; err != nil {
		t.Fatalf("query message failed: %v", err)
	}
	if message.ThreadID != result.Thread.ThreadID || message.ContentJSON != `{"text":"hello"}` {
		t.Fatalf("unexpected message: %+v", message)
	}
	if len(publisher.runIDs) != 1 || publisher.runIDs[0] != result.Run.RunID {
		t.Fatalf("unexpected published runs: %v", publisher.runIDs)
	}
}

func TestRuntimeStartRunReusesOwnedActiveThread(t *testing.T) {
	database, runtimeService, _, _ := setupRuntimeTestService(t)
	thread := models.Thread{ThreadID: "thread-owned", OwnerUserID: 1, Status: models.ThreadStatusActive, MetadataJSON: "{}"}
	if err := database.Create(&thread).Error; err != nil {
		t.Fatalf("create thread failed: %v", err)
	}
	command := validRuntimeCommand()
	command.ThreadID = thread.ThreadID
	result, err := runtimeService.StartRun(context.Background(), command)
	if err != nil {
		t.Fatalf("start run failed: %v", err)
	}
	if result.Thread.ID != thread.ID {
		t.Fatalf("expected existing thread %d, got %d", thread.ID, result.Thread.ID)
	}
}

func TestRuntimeStartRunReusesHistoricalMessagesAcrossRuns(t *testing.T) {
	database, runtimeService, _, publisher := setupRuntimeTestService(t)
	firstCommand := validRuntimeCommand()
	firstCommand.ThreadID = "thread-history"
	first, err := runtimeService.StartRun(context.Background(), firstCommand)
	if err != nil {
		t.Fatalf("start first run failed: %v", err)
	}

	secondCommand := validRuntimeCommand()
	secondCommand.ThreadID = first.Thread.ThreadID
	secondCommand.Input = json.RawMessage(`{"messages":[{"role":"user","content":"hello"},{"role":"user","content":"continue"}]}`)
	secondCommand.Messages = append(secondCommand.Messages, IncomingMessage{
		MessageID:    "msg-input-2",
		SenderType:   models.MessageSenderUser,
		ReceiverType: models.MessageSenderAgent,
		MessageType:  models.MessageTypeInput,
		ContentType:  "text",
		ContentJSON:  `{"text":"continue"}`,
	})
	second, err := runtimeService.StartRun(context.Background(), secondCommand)
	if err != nil {
		t.Fatalf("start second run with history failed: %v", err)
	}
	if first.Run.RunID == second.Run.RunID {
		t.Fatalf("expected a new run, got %s", second.Run.RunID)
	}

	var runCount int64
	if err := database.Model(&models.Run{}).Where("thread_id = ?", first.Thread.ThreadID).Count(&runCount).Error; err != nil {
		t.Fatalf("count runs failed: %v", err)
	}
	if runCount != 2 {
		t.Fatalf("expected two runs, got %d", runCount)
	}
	var messages []models.Message
	if err := database.Where("thread_id = ?", first.Thread.ThreadID).Order("message_id ASC").Find(&messages).Error; err != nil {
		t.Fatalf("query messages failed: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected historical message to remain unique, got %d messages", len(messages))
	}
	if messages[0].MessageID != "msg-input-1" || messages[0].RunID != first.Run.RunID {
		t.Fatalf("historical message ownership changed: %+v", messages[0])
	}
	if messages[1].MessageID != "msg-input-2" || messages[1].RunID != second.Run.RunID {
		t.Fatalf("new message was not attached to second run: %+v", messages[1])
	}
	if len(publisher.runIDs) != 2 {
		t.Fatalf("expected two published runs, got %v", publisher.runIDs)
	}
}

func TestRuntimeStartRunReusesWorkerResultMessageFromAGUIHistory(t *testing.T) {
	database, runtimeService, runService, publisher := setupRuntimeTestService(t)
	firstCommand := validRuntimeCommand()
	firstCommand.ThreadID = "thread-result-history"
	first, err := runtimeService.StartRun(context.Background(), firstCommand)
	if err != nil {
		t.Fatalf("start first run failed: %v", err)
	}
	step := &models.RunStep{RunID: first.Run.RunID, StepKey: "respond", Attempt: 1}
	if err := database.Transaction(func(tx *gorm.DB) error {
		return runService.persistResultMessage(context.Background(), tx, first.Run, step, `{"message":"answer"}`)
	}); err != nil {
		t.Fatalf("persist result message failed: %v", err)
	}
	var resultMessage models.Message
	if err := database.Where("run_id = ? AND message_type = ?", first.Run.RunID, models.MessageTypeResult).First(&resultMessage).Error; err != nil {
		t.Fatalf("load result message failed: %v", err)
	}

	secondCommand := validRuntimeCommand()
	secondCommand.ThreadID = first.Thread.ThreadID
	secondCommand.Input = json.RawMessage(`{"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"answer"},{"role":"user","content":"continue"}]}`)
	secondCommand.Messages = append(secondCommand.Messages,
		IncomingMessage{
			MessageID:    resultMessage.MessageID,
			SenderType:   models.MessageSenderAgent,
			SenderID:     "agent_test",
			ReceiverType: models.MessageSenderUser,
			ReceiverID:   "1",
			MessageType:  models.MessageTypeResult,
			ContentType:  "text",
			ContentJSON:  `{"text":"answer"}`,
			MetadataJSON: `{"source":"agui_request"}`,
		},
		IncomingMessage{
			MessageID:    "msg-input-2",
			SenderType:   models.MessageSenderUser,
			ReceiverType: models.MessageSenderAgent,
			MessageType:  models.MessageTypeInput,
			ContentType:  "text",
			ContentJSON:  `{"text":"continue"}`,
		},
	)
	if _, err := runtimeService.StartRun(context.Background(), secondCommand); err != nil {
		t.Fatalf("start second run with worker result history failed: %v", err)
	}
	if len(publisher.runIDs) != 2 {
		t.Fatalf("expected two published runs, got %v", publisher.runIDs)
	}
	var messageCount int64
	if err := database.Model(&models.Message{}).Where("thread_id = ?", first.Thread.ThreadID).Count(&messageCount).Error; err != nil {
		t.Fatalf("count messages failed: %v", err)
	}
	if messageCount != 3 {
		t.Fatalf("expected one historical result and two user messages, got %d", messageCount)
	}
}
func TestRuntimeStartRunRejectsChangedHistoricalMessage(t *testing.T) {
	_, runtimeService, _, publisher := setupRuntimeTestService(t)
	command := validRuntimeCommand()
	command.ThreadID = "thread-message-conflict"
	if _, err := runtimeService.StartRun(context.Background(), command); err != nil {
		t.Fatalf("start first run failed: %v", err)
	}

	command.RequestedRunID = ""
	command.Input = json.RawMessage(`{"messages":[{"role":"user","content":"changed"}]}`)
	command.Messages[0].ContentJSON = `{"text":"changed"}`
	if _, err := runtimeService.StartRun(context.Background(), command); !errors.Is(err, ErrMessageConflict()) {
		t.Fatalf("expected message conflict, got %v", err)
	}
	if len(publisher.runIDs) != 1 {
		t.Fatalf("conflicting request should not be published: %v", publisher.runIDs)
	}
}
func TestRuntimeStartRunRejectsForeignAndInactiveThread(t *testing.T) {
	tests := []struct {
		name   string
		thread models.Thread
		want   error
	}{
		{name: "foreign", thread: models.Thread{ThreadID: "thread-foreign", OwnerUserID: 2, Status: models.ThreadStatusActive, MetadataJSON: "{}"}, want: ErrRunForbidden()},
		{name: "closed", thread: models.Thread{ThreadID: "thread-closed", OwnerUserID: 1, Status: models.ThreadStatusClosed, MetadataJSON: "{}"}, want: ErrThreadUnavailable()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, runtimeService, _, publisher := setupRuntimeTestService(t)
			if err := database.Create(&test.thread).Error; err != nil {
				t.Fatalf("create thread failed: %v", err)
			}
			command := validRuntimeCommand()
			command.ThreadID = test.thread.ThreadID
			if _, err := runtimeService.StartRun(context.Background(), command); !errors.Is(err, test.want) {
				t.Fatalf("expected %v, got %v", test.want, err)
			}
			var runCount int64
			if err := database.Model(&models.Run{}).Count(&runCount).Error; err != nil {
				t.Fatalf("count runs failed: %v", err)
			}
			if runCount != 0 || len(publisher.runIDs) != 0 {
				t.Fatalf("failed transaction leaked run or event: runs=%d published=%v", runCount, publisher.runIDs)
			}
		})
	}
}

func TestRuntimeStartRunRollsBackThreadWhenMessageInvalid(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*IncomingMessage)
	}{
		{
			name: "content",
			mutate: func(message *IncomingMessage) {
				message.ContentJSON = "not-json"
			},
		},
		{
			name: "metadata",
			mutate: func(message *IncomingMessage) {
				message.MetadataJSON = "not-json"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, runtimeService, _, publisher := setupRuntimeTestService(t)
			command := validRuntimeCommand()
			command.ThreadID = "thread-rollback-" + test.name
			test.mutate(&command.Messages[0])

			if _, err := runtimeService.StartRun(context.Background(), command); err == nil {
				t.Fatal("expected invalid message error")
			}
			for _, model := range []any{&models.Thread{}, &models.Message{}, &models.Run{}} {
				var count int64
				if err := database.Model(model).Count(&count).Error; err != nil {
					t.Fatalf("count %T failed: %v", model, err)
				}
				if count != 0 {
					t.Fatalf("transaction leaked %T rows: %d", model, count)
				}
			}
			if len(publisher.runIDs) != 0 {
				t.Fatalf("unexpected publish after rollback: %v", publisher.runIDs)
			}
		})
	}
}

func TestRuntimeStartRunRejectsInvalidMessageEnvelope(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*IncomingMessage)
		wantErr string
	}{
		{name: "missing message id", mutate: func(message *IncomingMessage) { message.MessageID = "" }, wantErr: "message_id is required"},
		{name: "message id too long", mutate: func(message *IncomingMessage) { message.MessageID = strings.Repeat("m", 65) }, wantErr: "message_id must be at most 64 characters"},
		{name: "parent message id too long", mutate: func(message *IncomingMessage) { message.ParentMessageID = strings.Repeat("p", 65) }, wantErr: "parent_message_id must be at most 64 characters"},
		{name: "invalid sender type", mutate: func(message *IncomingMessage) { message.SenderType = "invalid" }, wantErr: "sender_type is invalid"},
		{name: "sender id too long", mutate: func(message *IncomingMessage) { message.SenderID = strings.Repeat("s", 65) }, wantErr: "sender_id must be at most 64 characters"},
		{name: "invalid receiver type", mutate: func(message *IncomingMessage) { message.ReceiverType = "invalid" }, wantErr: "receiver_type is invalid"},
		{name: "receiver id too long", mutate: func(message *IncomingMessage) { message.ReceiverID = strings.Repeat("r", 65) }, wantErr: "receiver_id must be at most 64 characters"},
		{name: "invalid message type", mutate: func(message *IncomingMessage) { message.MessageType = "invalid" }, wantErr: "message_type is invalid"},
		{name: "missing content type", mutate: func(message *IncomingMessage) { message.ContentType = "" }, wantErr: "content_type is required"},
		{name: "content type too long", mutate: func(message *IncomingMessage) { message.ContentType = strings.Repeat("c", 33) }, wantErr: "content_type must be at most 32 characters"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, runtimeService, _, publisher := setupRuntimeTestService(t)
			command := validRuntimeCommand()
			command.ThreadID = "thread-invalid-message"
			test.mutate(&command.Messages[0])

			if _, err := runtimeService.StartRun(context.Background(), command); err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("expected error containing %q, got %v", test.wantErr, err)
			}
			for _, model := range []any{&models.Thread{}, &models.Message{}, &models.Run{}} {
				var count int64
				if err := database.Model(model).Count(&count).Error; err != nil {
					t.Fatalf("count %T failed: %v", model, err)
				}
				if count != 0 {
					t.Fatalf("invalid message leaked %T rows: %d", model, count)
				}
			}
			if len(publisher.runIDs) != 0 {
				t.Fatalf("invalid message published run: %v", publisher.runIDs)
			}
		})
	}
}

func TestRuntimeRequestedRunIDRejectsChangedMessageEnvelope(t *testing.T) {
	_, runtimeService, _, publisher := setupRuntimeTestService(t)
	command := validRuntimeCommand()
	command.ThreadID = "thread-requested-message-conflict"
	command.RequestedRunID = "run-requested-message-conflict"
	if _, err := runtimeService.StartRun(context.Background(), command); err != nil {
		t.Fatalf("first start failed: %v", err)
	}

	command.Messages[0].ContentJSON = `{"text":"changed outside run input"}`
	if _, err := runtimeService.StartRun(context.Background(), command); !errors.Is(err, ErrMessageConflict()) {
		t.Fatalf("expected message conflict, got %v", err)
	}
	if len(publisher.runIDs) != 1 {
		t.Fatalf("conflicting retry must not publish again: %v", publisher.runIDs)
	}
}

func TestRuntimeRequestedRunIDIsReusableOnlyForSameRequest(t *testing.T) {
	_, runtimeService, _, publisher := setupRuntimeTestService(t)
	command := validRuntimeCommand()
	command.ThreadID = "thread-requested-run"
	command.RequestedRunID = "run-from-agui"
	first, err := runtimeService.StartRun(context.Background(), command)
	if err != nil {
		t.Fatalf("first start failed: %v", err)
	}
	second, err := runtimeService.StartRun(context.Background(), command)
	if err != nil {
		t.Fatalf("second start failed: %v", err)
	}
	if !second.Reused || first.Run.RunID != second.Run.RunID || len(publisher.runIDs) != 1 {
		t.Fatalf("expected safe reuse: first=%+v second=%+v published=%v", first, second, publisher.runIDs)
	}
	command.Input = json.RawMessage(`{"messages":[{"role":"user","content":"changed"}]}`)
	if _, err := runtimeService.StartRun(context.Background(), command); !errors.Is(err, ErrRunAlreadyExists()) {
		t.Fatalf("expected run id conflict, got %v", err)
	}
}

func TestRuntimeRequestedRunIDReuseIgnoresLaterAgentAvailability(t *testing.T) {
	database, runtimeService, _, publisher := setupRuntimeTestService(t)
	command := validRuntimeCommand()
	command.ThreadID = "thread-disabled-agent-retry"
	command.RequestedRunID = "run-disabled-agent-retry"

	first, err := runtimeService.StartRun(context.Background(), command)
	if err != nil {
		t.Fatalf("first start failed: %v", err)
	}
	if err := database.Model(&models.Agent{}).
		Where("id = ?", first.Run.AgentID).
		Update("status", models.AgentStatusInactive).Error; err != nil {
		t.Fatalf("disable agent failed: %v", err)
	}

	second, err := runtimeService.StartRun(context.Background(), command)
	if err != nil {
		t.Fatalf("same requested run should remain reusable after agent disable: %v", err)
	}
	if !second.Reused || second.Run.RunID != first.Run.RunID || len(publisher.runIDs) != 1 {
		t.Fatalf("expected durable run reuse: first=%+v second=%+v published=%v", first, second, publisher.runIDs)
	}
}

func TestRuntimeRequestedRunIDReusePinsOriginallyResolvedWorkflow(t *testing.T) {
	database, runtimeService, _, publisher := setupRuntimeTestService(t)
	command := validRuntimeCommand()
	command.ThreadID = "thread-workflow-switch-retry"
	command.RequestedRunID = "run-workflow-switch-retry"

	first, err := runtimeService.StartRun(context.Background(), command)
	if err != nil {
		t.Fatalf("first start failed: %v", err)
	}
	if err := database.Model(&models.Workflow{}).
		Where("id = ?", first.Run.WorkflowID).
		Update("is_active", false).Error; err != nil {
		t.Fatalf("deactivate original workflow failed: %v", err)
	}
	workflow := models.Workflow{
		AgentID:        first.Run.AgentID,
		Version:        2,
		DefinitionJSON: `{"entry_node":"planner","nodes":[{"key":"planner","type":"planner"}]}`,
		Checksum:       "version-2",
		IsActive:       true,
		CreatedBy:      1,
	}
	if err := database.Create(&workflow).Error; err != nil {
		t.Fatalf("create replacement workflow failed: %v", err)
	}

	second, err := runtimeService.StartRun(context.Background(), command)
	if err != nil {
		t.Fatalf("same requested run should remain pinned to its original workflow: %v", err)
	}
	if !second.Reused || second.Run.WorkflowID != first.Run.WorkflowID || len(publisher.runIDs) != 1 {
		t.Fatalf("expected original workflow reuse: first=%+v second=%+v published=%v", first, second, publisher.runIDs)
	}
}
func TestRuntimeRequestedRunIDReusesGeneratedThread(t *testing.T) {
	_, runtimeService, _, publisher := setupRuntimeTestService(t)
	command := validRuntimeCommand()
	command.RequestedRunID = "run-generated-thread"

	first, err := runtimeService.StartRun(context.Background(), command)
	if err != nil {
		t.Fatalf("first start failed: %v", err)
	}
	second, err := runtimeService.StartRun(context.Background(), command)
	if err != nil {
		t.Fatalf("second start failed: %v", err)
	}
	if !second.Reused || first.Run.RunID != second.Run.RunID || first.Thread.ThreadID != second.Thread.ThreadID {
		t.Fatalf("expected generated thread reuse: first=%+v second=%+v", first, second)
	}
	if len(publisher.runIDs) != 1 {
		t.Fatalf("expected one publish, got %v", publisher.runIDs)
	}
}

func TestRuntimeRequestedRunIDRejectsDifferentExplicitThread(t *testing.T) {
	_, runtimeService, _, publisher := setupRuntimeTestService(t)
	command := validRuntimeCommand()
	command.ThreadID = "thread-one"
	command.RequestedRunID = "run-explicit-thread-conflict"

	if _, err := runtimeService.StartRun(context.Background(), command); err != nil {
		t.Fatalf("first start failed: %v", err)
	}
	command.ThreadID = "thread-two"
	if _, err := runtimeService.StartRun(context.Background(), command); !errors.Is(err, ErrRunAlreadyExists()) {
		t.Fatalf("expected run id conflict for a different explicit thread, got %v", err)
	}
	if len(publisher.runIDs) != 1 {
		t.Fatalf("expected one publish, got %v", publisher.runIDs)
	}
}
func TestRuntimeSnapshotEnforcesOwnership(t *testing.T) {
	_, runtimeService, _, _ := setupRuntimeTestService(t)
	result, err := runtimeService.StartRun(context.Background(), validRuntimeCommand())
	if err != nil {
		t.Fatalf("start run failed: %v", err)
	}
	if _, err := runtimeService.Snapshot(context.Background(), 2, result.Run.RunID); !errors.Is(err, ErrRunForbidden()) {
		t.Fatalf("expected forbidden snapshot, got %v", err)
	}
	snapshot, err := runtimeService.Snapshot(context.Background(), 1, result.Run.RunID)
	if err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}
	if snapshot.Run.RunID != result.Run.RunID || len(snapshot.Messages) != 1 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
}

func TestNewRuntimeServiceRejectsMissingDependencies(t *testing.T) {
	database, _, runService, _ := setupRuntimeTestService(t)
	if _, err := NewRuntimeService(nil, runService); err == nil {
		t.Fatal("expected nil database error")
	}
	if _, err := NewRuntimeService(database, nil); err == nil {
		t.Fatal("expected nil run service error")
	}
}
