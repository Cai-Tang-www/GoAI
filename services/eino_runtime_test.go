package services

import (
	"context"
	"encoding/json"
	"testing"

	"GoAI/models"
)

func TestHandleRunExecuteUsesEinoGraphForSerialNodeDataflow(t *testing.T) {
	database, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, database)
	workflow.DefinitionJSON = `{"entry_node":"first","nodes":[{"key":"first","type":"noop"},{"key":"second","type":"noop"}],"edges":[{"from":"first","to":"second"}]}`
	if err := database.Save(&workflow).Error; err != nil {
		t.Fatalf("update workflow: %v", err)
	}

	run := models.Run{
		RunID:       "run-eino-dataflow",
		ThreadID:    "thread-eino-dataflow",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "manual",
		InputJSON:   `{"prompt":"start"}`,
		Status:      models.RunStatusQueued,
	}
	if err := database.Create(&run).Error; err != nil {
		t.Fatalf("create run: %v", err)
	}

	var inputs []string
	service.executeNode = func(_ context.Context, run *models.Run, node WorkflowNode, _ int) (string, error) {
		inputs = append(inputs, node.Key+":"+run.InputJSON)
		encoded, err := json.Marshal(map[string]string{"node": node.Key, "input": run.InputJSON})
		return string(encoded), err
	}

	if err := service.HandleRunExecute(context.Background(), run.RunID); err != nil {
		t.Fatalf("handle run: %v", err)
	}

	wantFirst := `first:{"prompt":"start"}`
	if len(inputs) != 2 || inputs[0] != wantFirst {
		t.Fatalf("unexpected first node inputs: %#v", inputs)
	}
	var secondInput map[string]string
	if err := json.Unmarshal([]byte(inputs[1][len("second:"):]), &secondInput); err != nil {
		t.Fatalf("decode second node input: %v", err)
	}
	if secondInput["node"] != "first" {
		t.Fatalf("second node did not receive first output: %#v", secondInput)
	}

	var stored models.Run
	if err := database.Where("run_id = ?", run.RunID).First(&stored).Error; err != nil {
		t.Fatalf("load run: %v", err)
	}
	if stored.Status != models.RunStatusSuccess {
		t.Fatalf("status=%s, want success", stored.Status)
	}
}

func TestHandleRunExecuteMarksRunFailedWhenEinoRejectsGraph(t *testing.T) {
	database, service, _ := setupRunTestService(t)
	agent, workflow := seedAgentWorkflow(t, database)
	workflow.DefinitionJSON = `{"entry_node":"start","nodes":[{"key":"start","type":"noop"},{"key":"left","type":"noop"},{"key":"right","type":"noop"}],"edges":[{"from":"start","to":"left"},{"from":"start","to":"right"}]}`
	if err := database.Save(&workflow).Error; err != nil {
		t.Fatalf("update workflow: %v", err)
	}

	run := models.Run{
		RunID:       "run-eino-invalid-graph",
		ThreadID:    "thread-eino-invalid-graph",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "manual",
		InputJSON:   `{}`,
		Status:      models.RunStatusQueued,
	}
	if err := database.Create(&run).Error; err != nil {
		t.Fatalf("create run: %v", err)
	}

	if err := service.HandleRunExecute(context.Background(), run.RunID); err == nil {
		t.Fatal("expected Eino graph validation error")
	}

	var stored models.Run
	if err := database.Where("run_id = ?", run.RunID).First(&stored).Error; err != nil {
		t.Fatalf("load run: %v", err)
	}
	if stored.Status != models.RunStatusFailed {
		t.Fatalf("status=%s, want failed", stored.Status)
	}
}
