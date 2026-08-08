package services

import (
	"context"
	"errors"
	"strings"
	"testing"

	"GoAI/models"
)

func TestRunInterruptPausesAndResumesFromSuccessor(t *testing.T) {
	database, runtimeService, runService, publisher := setupRuntimeTestService(t)
	var workflow models.Workflow
	if err := database.Where("agent_id = ?", 1).First(&workflow).Error; err != nil {
		t.Fatalf("load test workflow failed: %v", err)
	}
	workflow.DefinitionJSON = `{"entry_node":"approval","nodes":[{"key":"approval","type":"interrupt","config":{"interrupt_id":"approval","reason":"approval_required","message":"Approve this action?","response_schema":{"type":"object"},"metadata":{"source":"test"}}},{"key":"finish","type":"noop"}],"edges":[{"from":"approval","to":"finish"}]}`
	if err := database.Save(&workflow).Error; err != nil {
		t.Fatalf("update interrupt workflow failed: %v", err)
	}

	command := validRuntimeCommand()
	command.ThreadID = "thread-interrupt"
	command.Messages[0].MessageID = "msg-interrupt"
	started, err := runtimeService.StartRun(context.Background(), command)
	if err != nil {
		t.Fatalf("start interrupt run failed: %v", err)
	}
	if err := runService.HandleRunExecute(context.Background(), started.Run.RunID); err != nil {
		t.Fatalf("interrupt execution failed: %v", err)
	}

	var paused models.Run
	if err := database.Where("run_id = ?", started.Run.RunID).First(&paused).Error; err != nil {
		t.Fatalf("load paused run failed: %v", err)
	}
	if paused.Status != models.RunStatusWaitingInput {
		t.Fatalf("expected waiting_input run, got %s", paused.Status)
	}
	var interrupt models.RunInterrupt
	if err := database.Where("run_id = ? AND interrupt_id = ?", paused.RunID, "approval").First(&interrupt).Error; err != nil {
		t.Fatalf("load interrupt failed: %v", err)
	}
	if interrupt.Status != models.RunInterruptStatusPending || interrupt.ResumeNodeKey != "finish" {
		t.Fatalf("unexpected interrupt: %+v", interrupt)
	}
	var pausedStep models.RunStep
	if err := database.Where("run_id = ? AND step_key = ?", paused.RunID, "approval").First(&pausedStep).Error; err != nil {
		t.Fatalf("load paused step failed: %v", err)
	}
	if pausedStep.Status != models.RunStepStatusWaitingInput {
		t.Fatalf("expected waiting_input step, got %s", pausedStep.Status)
	}

	_, err = runtimeService.ResumeRun(context.Background(), ResumeRunCommand{
		OwnerUserID: 1,
		AgentCode:   "other-agent",
		RunID:       paused.RunID,
		Interrupts: []ResumeInterruptCommand{{
			InterruptID: "approval",
			Status:      models.RunInterruptStatusResolved,
			PayloadJSON: `{"approved":true}`,
		}},
	})
	if !errors.Is(err, ErrAgentNotFound()) {
		t.Fatalf("resume through another agent endpoint must be rejected, got %v", err)
	}

	resumed, err := runtimeService.ResumeRun(context.Background(), ResumeRunCommand{
		OwnerUserID: 1,
		AgentCode:   "agent_test",
		RunID:       paused.RunID,
		Interrupts: []ResumeInterruptCommand{{
			InterruptID: "approval",
			Status:      models.RunInterruptStatusResolved,
			PayloadJSON: `{"approved":true}`,
		}},
	})
	if err != nil {
		t.Fatalf("resume run failed: %v", err)
	}
	if resumed.Run.Status != models.RunStatusQueued || resumed.Reused {
		t.Fatalf("unexpected resumed run: %+v", resumed)
	}
	if len(publisher.runIDs) != 2 {
		t.Fatalf("expected initial and resume publications, got %v", publisher.runIDs)
	}
	if err := runService.HandleRunExecute(context.Background(), paused.RunID); err != nil {
		t.Fatalf("execute resumed run failed: %v", err)
	}

	var completed models.Run
	if err := database.Where("run_id = ?", paused.RunID).First(&completed).Error; err != nil {
		t.Fatalf("load completed run failed: %v", err)
	}
	if completed.Status != models.RunStatusSuccess {
		t.Fatalf("expected successful resumed run, got %s", completed.Status)
	}
	var finishStep models.RunStep
	if err := database.Where("run_id = ? AND step_key = ?", paused.RunID, "finish").First(&finishStep).Error; err != nil {
		t.Fatalf("load successor step failed: %v", err)
	}
	if finishStep.Status != models.RunStepStatusSuccess {
		t.Fatalf("expected successor step success, got %s", finishStep.Status)
	}

	repeated, err := runtimeService.ResumeRun(context.Background(), ResumeRunCommand{
		OwnerUserID: 1,
		AgentCode:   "agent_test",
		RunID:       paused.RunID,
		Interrupts: []ResumeInterruptCommand{{
			InterruptID: "approval",
			Status:      models.RunInterruptStatusResolved,
			PayloadJSON: `{"approved":true}`,
		}},
	})
	if err != nil {
		t.Fatalf("repeated resume should be idempotent: %v", err)
	}
	if !repeated.Reused || len(publisher.runIDs) != 2 {
		t.Fatalf("repeated resume must not publish or execute again: reused=%v published=%v", repeated.Reused, publisher.runIDs)
	}

	conflict, err := runtimeService.ResumeRun(context.Background(), ResumeRunCommand{
		OwnerUserID: 1,
		AgentCode:   "agent_test",
		RunID:       paused.RunID,
		Interrupts: []ResumeInterruptCommand{{
			InterruptID: "approval",
			Status:      models.RunInterruptStatusResolved,
			PayloadJSON: `{"approved":false}`,
		}},
	})
	if conflict != nil || !errors.Is(err, ErrRunNotWaitingInput()) {
		t.Fatalf("conflicting terminal resume must be rejected without mutation: result=%+v err=%v", conflict, err)
	}
}

func TestResumeRunRejectsInvalidEntriesBeforeDatabaseMutation(t *testing.T) {
	_, runtimeService, _, _ := setupRuntimeTestService(t)
	tests := []struct {
		name     string
		command  ResumeRunCommand
		wantErr  error
		wantText string
	}{
		{
			name:    "missing entries",
			command: ResumeRunCommand{OwnerUserID: 1, AgentCode: "agent_test", RunID: "run-1"},
			wantErr: errResumeEntriesRequired,
		},
		{
			name: "duplicate interrupt",
			command: ResumeRunCommand{OwnerUserID: 1, AgentCode: "agent_test", RunID: "run-1", Interrupts: []ResumeInterruptCommand{
				{InterruptID: "approval", Status: models.RunInterruptStatusResolved, PayloadJSON: `{}`},
				{InterruptID: "approval", Status: models.RunInterruptStatusCancelled, PayloadJSON: `{}`},
			}},
			wantText: "duplicated",
		},
		{
			name: "invalid status",
			command: ResumeRunCommand{OwnerUserID: 1, AgentCode: "agent_test", RunID: "run-1", Interrupts: []ResumeInterruptCommand{
				{InterruptID: "approval", Status: "pending", PayloadJSON: `{}`},
			}},
			wantText: "must be resolved or cancelled",
		},
		{
			name: "invalid payload",
			command: ResumeRunCommand{OwnerUserID: 1, AgentCode: "agent_test", RunID: "run-1", Interrupts: []ResumeInterruptCommand{
				{InterruptID: "approval", Status: models.RunInterruptStatusResolved, PayloadJSON: `{invalid`},
			}},
			wantText: "payload must be valid JSON",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := runtimeService.ResumeRun(context.Background(), test.command)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("error=%v want=%v", err, test.wantErr)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantText) {
				t.Fatalf("error=%v want text %q", err, test.wantText)
			}
		})
	}
}

func TestRuntimeStartRunParentLineageValidatesOwnershipAndThread(t *testing.T) {
	database, runtimeService, _, _ := setupRuntimeTestService(t)
	parentCommand := validRuntimeCommand()
	parentCommand.ThreadID = "thread-lineage"
	parentCommand.RequestedRunID = "run-parent-lineage"
	parentCommand.Messages[0].MessageID = "msg-parent-lineage"
	parent, err := runtimeService.StartRun(context.Background(), parentCommand)
	if err != nil {
		t.Fatalf("start parent run failed: %v", err)
	}

	childCommand := validRuntimeCommand()
	childCommand.ThreadID = ""
	childCommand.RequestedRunID = "run-child-lineage"
	childCommand.ParentRunID = parent.Run.RunID
	childCommand.Messages[0].MessageID = "msg-child-lineage"
	child, err := runtimeService.StartRun(context.Background(), childCommand)
	if err != nil {
		t.Fatalf("start child run failed: %v", err)
	}
	if child.Run.ThreadID != parent.Run.ThreadID || child.Thread.ThreadID != parent.Thread.ThreadID || child.Run.ParentRunID == nil || *child.Run.ParentRunID != parent.Run.RunID {
		t.Fatalf("unexpected lineage: parent=%+v child=%+v", parent.Run, child.Run)
	}

	mismatch := childCommand
	mismatch.RequestedRunID = "run-child-mismatch"
	mismatch.ThreadID = "thread-other"
	mismatch.Messages[0].MessageID = "msg-child-mismatch"
	if _, err := runtimeService.StartRun(context.Background(), mismatch); !errors.Is(err, ErrParentRunThreadMismatch()) {
		t.Fatalf("expected parent/thread mismatch, got %v", err)
	}

	foreign := childCommand
	foreign.OwnerUserID = 2
	foreign.RequestedRunID = "run-child-foreign"
	foreign.Messages[0].MessageID = "msg-child-foreign"
	if _, err := runtimeService.StartRun(context.Background(), foreign); !errors.Is(err, ErrRunForbidden()) {
		t.Fatalf("expected foreign parent to be forbidden, got %v", err)
	}

	self := childCommand
	self.RequestedRunID = parent.Run.RunID
	self.Messages[0].MessageID = "msg-child-self"
	if _, err := runtimeService.StartRun(context.Background(), self); err == nil {
		t.Fatal("expected parent run to reject self lineage")
	}
	_ = database
}
