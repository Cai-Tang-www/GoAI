package services

import (
	"GoAI/db"
	"GoAI/kafka"
	"GoAI/models"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupRunTestDB(t *testing.T) {
	t.Helper()
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "_") + "?mode=memory&cache=shared"
	gdb, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := gdb.AutoMigrate(&models.User{}, &models.Agent{}, &models.Workflow{}, &models.Run{}, &models.RunStep{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	db.DB = gdb
}

func seedAgentWorkflow(t *testing.T) (models.Agent, models.Workflow) {
	t.Helper()
	agent := models.Agent{
		AgentCode:   "agent_test",
		Name:        "Agent Test",
		Description: "for test",
		OwnerUserID: 1,
		Status:      models.AgentStatusActive,
	}
	if err := db.DB.Create(&agent).Error; err != nil {
		t.Fatalf("create agent failed: %v", err)
	}
	workflow := models.Workflow{
		AgentID:        agent.ID,
		Version:        1,
		DefinitionJSON: `{"entry_node":"planner","nodes":[{"key":"planner","type":"planner"},{"key":"tool1","type":"tool"}],"edges":[{"from":"planner","to":"tool1"}]}`,
		Checksum:       "abc",
		IsActive:       true,
		CreatedBy:      1,
	}
	if err := db.DB.Create(&workflow).Error; err != nil {
		t.Fatalf("create workflow failed: %v", err)
	}
	return agent, workflow
}

func TestCreateRunSuccessAndQueued(t *testing.T) {
	setupRunTestDB(t)
	seedAgentWorkflow(t)

	origPublisher := publishRunExecuteEvent
	defer func() { publishRunExecuteEvent = origPublisher }()
	var publishedRunID string
	publishRunExecuteEvent = func(ctx context.Context, runID string) error {
		publishedRunID = runID
		return nil
	}

	run, err := CreateRun(context.Background(), 1, CreateRunRequest{
		AgentCode:   "agent_test",
		TriggerType: "api",
		Input:       []byte(`{"prompt":"hello"}`),
	})
	if err != nil {
		t.Fatalf("create run failed: %v", err)
	}
	if run.Status != models.RunStatusQueued {
		t.Fatalf("expected queued status, got %s", run.Status)
	}
	if run.RunID == "" || publishedRunID != run.RunID {
		t.Fatalf("publisher not called with expected run id: run=%s published=%s", run.RunID, publishedRunID)
	}
}

func TestCreateRunDispatchFailMarksRunFailed(t *testing.T) {
	setupRunTestDB(t)
	seedAgentWorkflow(t)

	origPublisher := publishRunExecuteEvent
	defer func() { publishRunExecuteEvent = origPublisher }()
	publishRunExecuteEvent = func(ctx context.Context, runID string) error {
		return errors.New("kafka down")
	}

	run, err := CreateRun(context.Background(), 1, CreateRunRequest{
		AgentCode:   "agent_test",
		TriggerType: "api",
		Input:       []byte(`{"prompt":"hello"}`),
	})
	if err == nil {
		t.Fatal("expected dispatch error but got nil")
	}
	var saved models.Run
	if dbErr := db.DB.Where("run_id = ?", run.RunID).First(&saved).Error; dbErr != nil {
		t.Fatalf("query run failed: %v", dbErr)
	}
	if saved.Status != models.RunStatusFailed {
		t.Fatalf("expected run failed status, got %s", saved.Status)
	}
}

func TestHandleRunExecuteMessageSuccess(t *testing.T) {
	setupRunTestDB(t)
	agent, workflow := seedAgentWorkflow(t)

	run := models.Run{
		RunID:       "run_test_success",
		ThreadID:    "thread-1",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      1,
		TriggerType: "api",
		InputJSON:   `{"prompt":"hello"}`,
		Status:      models.RunStatusQueued,
	}
	if err := db.DB.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	origBackoffs := stepRetryBackoffs
	stepRetryBackoffs = []time.Duration{0, 0, 0}
	defer func() { stepRetryBackoffs = origBackoffs }()

	err := HandleRunExecuteMessage(context.Background(), kafka.RunExecuteMessage{RunID: run.RunID})
	if err != nil {
		t.Fatalf("handle run message failed: %v", err)
	}

	var saved models.Run
	if err := db.DB.Where("run_id = ?", run.RunID).First(&saved).Error; err != nil {
		t.Fatalf("query run failed: %v", err)
	}
	if saved.Status != models.RunStatusSuccess {
		t.Fatalf("expected run success, got %s", saved.Status)
	}
	var steps []models.RunStep
	if err := db.DB.Where("run_id = ?", run.RunID).Find(&steps).Error; err != nil {
		t.Fatalf("query steps failed: %v", err)
	}
	if len(steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(steps))
	}
}
