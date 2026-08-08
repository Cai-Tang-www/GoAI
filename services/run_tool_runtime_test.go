package services

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"GoAI/models"
)

type recordingToolInvoker struct {
	mu        sync.Mutex
	arguments []ToolInvocationRequest
	results   []any
	errors    []error
	block     <-chan struct{}
}

func (i *recordingToolInvoker) Invoke(ctx context.Context, request ToolInvocationRequest) (any, error) {
	i.mu.Lock()
	i.arguments = append(i.arguments, request)
	index := len(i.arguments) - 1
	block := i.block
	var result any
	if index < len(i.results) {
		result = i.results[index]
	}
	var invokeErr error
	if index < len(i.errors) {
		invokeErr = i.errors[index]
	}
	i.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if invokeErr != nil {
		return nil, invokeErr
	}
	return result, nil
}

func (i *recordingToolInvoker) callCount() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.arguments)
}

func TestRunServiceExecutesMCPToolChainAndPersistsAttempts(t *testing.T) {
	database, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, database)
	workflow.DefinitionJSON = `{"entry_node":"search","nodes":[{"key":"search","type":"tool","config":{"server_code":"docs","tool_name":"search","input":{"query":"go"}}},{"key":"format","type":"tool","config":{"server_code":"docs","tool_name":"format","input_from":["search"]}}],"edges":[{"from":"search","to":"format"}]}`
	if err := database.Save(&workflow).Error; err != nil {
		t.Fatalf("save tool workflow: %v", err)
	}
	invoker := &recordingToolInvoker{results: []any{
		map[string]any{"documents": []any{"one"}},
		map[string]any{"formatted": true},
	}}
	service.toolInvoker = invoker
	service.stepRetryBackoffs = []time.Duration{0, 0, 0}
	run := models.Run{
		RunID: "run-tool-chain", ThreadID: "thread-tool-chain", AgentID: agent.ID, WorkflowID: workflow.ID,
		UserID: 1, TriggerType: "manual", InputJSON: `{"prompt":"find"}`, Status: models.RunStatusQueued,
	}
	if err := database.Create(&run).Error; err != nil {
		t.Fatalf("create tool run: %v", err)
	}

	if err := service.HandleRunExecute(context.Background(), run.RunID); err != nil {
		t.Fatalf("execute tool workflow: %v", err)
	}
	if invoker.callCount() != 2 {
		t.Fatalf("tool invocation count = %d, want 2", invoker.callCount())
	}
	invoker.mu.Lock()
	first, second := invoker.arguments[0], invoker.arguments[1]
	invoker.mu.Unlock()
	if first.OwnerUserID != agent.OwnerUserID || first.ServerCode != "docs" || first.ToolName != "search" || first.Arguments["query"] != "go" {
		t.Fatalf("unexpected static tool request: %+v", first)
	}
	if second.Arguments["run_input"] == nil || second.Arguments["step_outputs"] == nil {
		t.Fatalf("input_from aggregation was not passed to successor: %+v", second.Arguments)
	}

	var stored models.Run
	if err := database.Where("run_id = ?", run.RunID).First(&stored).Error; err != nil {
		t.Fatalf("load tool run: %v", err)
	}
	if stored.Status != models.RunStatusSuccess {
		t.Fatalf("run status = %s, want success", stored.Status)
	}
	var steps []models.RunStep
	if err := database.Where("run_id = ?", run.RunID).Order("step_key ASC, attempt ASC").Find(&steps).Error; err != nil {
		t.Fatalf("load tool steps: %v", err)
	}
	if len(steps) != 2 || steps[0].StepType != "tool" || steps[1].StepType != "tool" {
		t.Fatalf("unexpected tool steps: %+v", steps)
	}
	for _, step := range steps {
		if step.Status != models.RunStepStatusSuccess || step.Attempt != 1 {
			t.Fatalf("tool step did not complete once: %+v", step)
		}
	}
}

func TestRunServiceMCPToolTimeoutFailsWithoutRetryingCanceledAttempt(t *testing.T) {
	database, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, database)
	workflow.DefinitionJSON = `{"entry_node":"slow","nodes":[{"key":"slow","type":"tool","config":{"server_code":"docs","tool_name":"slow","input":{},"timeout_ms":5}}],"edges":[]}`
	if err := database.Save(&workflow).Error; err != nil {
		t.Fatalf("save timeout workflow: %v", err)
	}
	invoker := &recordingToolInvoker{block: make(chan struct{})}
	service.toolInvoker = invoker
	service.stepRetryBackoffs = []time.Duration{0, 0, 0}
	run := models.Run{RunID: "run-tool-timeout", ThreadID: "thread-tool-timeout", AgentID: agent.ID, WorkflowID: workflow.ID, UserID: 1, TriggerType: "manual", InputJSON: `{}`, Status: models.RunStatusQueued}
	if err := database.Create(&run).Error; err != nil {
		t.Fatalf("create timeout run: %v", err)
	}

	if err := service.HandleRunExecute(context.Background(), run.RunID); err == nil {
		t.Fatal("expected timeout execution to fail")
	}
	if invoker.callCount() != 1 {
		t.Fatalf("timeout should not retry canceled invocation, got %d calls", invoker.callCount())
	}
	var stored models.Run
	if err := database.Where("run_id = ?", run.RunID).First(&stored).Error; err != nil {
		t.Fatalf("load timeout run: %v", err)
	}
	if stored.Status != models.RunStatusFailed {
		t.Fatalf("timeout run status = %s, want failed", stored.Status)
	}
}

func TestRunServiceMCPToolRetriesTransientFailureAndDeduplicatesDelivery(t *testing.T) {
	database, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, database)
	workflow.DefinitionJSON = `{"entry_node":"retry","nodes":[{"key":"retry","type":"tool","config":{"server_code":"docs","tool_name":"retry","input":{}}}],"edges":[]}`
	if err := database.Save(&workflow).Error; err != nil {
		t.Fatalf("save retry workflow: %v", err)
	}
	invoker := &recordingToolInvoker{
		results: []any{nil, nil, nil, map[string]any{"ok": true}},
		errors:  []error{errors.New("temporary 1"), errors.New("temporary 2"), errors.New("temporary 3")},
	}
	service.toolInvoker = invoker
	service.stepRetryBackoffs = []time.Duration{0, 0, 0}
	run := models.Run{RunID: "run-tool-retry", ThreadID: "thread-tool-retry", AgentID: agent.ID, WorkflowID: workflow.ID, UserID: 1, TriggerType: "manual", InputJSON: `{}`, Status: models.RunStatusQueued}
	if err := database.Create(&run).Error; err != nil {
		t.Fatalf("create retry run: %v", err)
	}
	if err := service.HandleRunExecute(context.Background(), run.RunID); err != nil {
		t.Fatalf("retry workflow should succeed: %v", err)
	}
	if invoker.callCount() != 4 {
		t.Fatalf("transient tool failure calls = %d, want 4", invoker.callCount())
	}
	if err := service.HandleRunExecute(context.Background(), run.RunID); err != nil {
		t.Fatalf("duplicate delivery should be no-op: %v", err)
	}
	if invoker.callCount() != 4 {
		t.Fatalf("duplicate delivery re-executed successful tool: %d calls", invoker.callCount())
	}
	var steps []models.RunStep
	if err := database.Where("run_id = ?", run.RunID).Order("attempt ASC").Find(&steps).Error; err != nil {
		t.Fatalf("load retry steps: %v", err)
	}
	if len(steps) != 4 || steps[3].Status != models.RunStepStatusSuccess || steps[3].Attempt != 4 {
		t.Fatalf("unexpected retry step history: %+v", steps)
	}
}
