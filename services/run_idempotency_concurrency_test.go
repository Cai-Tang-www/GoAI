package services

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"GoAI/models"
)

func TestHandleRunExecuteConcurrentDuplicateDeliveryClaimsOnce(t *testing.T) {
	database, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, database)
	run := models.Run{
		RunID:       "run_concurrent_duplicate",
		ThreadID:    "thread-concurrent-duplicate",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "api",
		InputJSON:   `{"prompt":"hello"}`,
		Status:      models.RunStatusQueued,
	}
	if err := database.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	service.stepRetryBackoffs = []time.Duration{0, 0, 0}
	var executions atomic.Int32
	firstStepStarted := make(chan struct{})
	releaseFirstStep := make(chan struct{})
	service.executeNode = func(ctx context.Context, _ *models.Run, _ WorkflowNode, _ int) (string, error) {
		if executions.Add(1) == 1 {
			close(firstStepStarted)
			select {
			case <-releaseFirstStep:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		return `{"ok":true}`, nil
	}

	results := make(chan error, 2)
	go func() {
		results <- service.HandleRunExecute(context.Background(), run.RunID)
	}()
	select {
	case <-firstStepStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first worker did not start executing a step")
	}

	go func() {
		results <- service.HandleRunExecute(context.Background(), run.RunID)
	}()
	select {
	case err := <-results:
		if err != nil {
			t.Fatalf("duplicate worker returned an error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("duplicate worker did not return while the first worker was executing")
	}

	close(releaseFirstStep)
	select {
	case err := <-results:
		if err != nil {
			t.Fatalf("claiming worker returned an error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("claiming worker did not finish")
	}

	if got := executions.Load(); got != 2 {
		t.Fatalf("duplicate delivery executed workflow %d times, want 2 workflow nodes once", got)
	}
	var stepCount int64
	if err := database.Model(&models.RunStep{}).Where("run_id = ?", run.RunID).Count(&stepCount).Error; err != nil {
		t.Fatalf("count run steps failed: %v", err)
	}
	if stepCount != 2 {
		t.Fatalf("duplicate delivery created %d steps, want 2", stepCount)
	}
	var stored models.Run
	if err := database.Where("run_id = ?", run.RunID).First(&stored).Error; err != nil {
		t.Fatalf("load run failed: %v", err)
	}
	if stored.Status != models.RunStatusSuccess {
		t.Fatalf("run status got=%s want=%s", stored.Status, models.RunStatusSuccess)
	}
}
