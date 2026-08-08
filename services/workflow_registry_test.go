package services

import (
	"context"
	"errors"
	"strings"
	"testing"

	"GoAI/models"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newWorkflowRegistryTestService(t *testing.T) (*gorm.DB, *AgentRegistryService) {
	t.Helper()
	database, err := gorm.Open(sqlite.Open("file:"+strings.ReplaceAll(t.Name(), "/", "_")+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := database.AutoMigrate(&models.Agent{}, &models.Workflow{}, &models.AgentCapability{}, &models.AgentEndpoint{}); err != nil {
		t.Fatalf("migrate workflow registry models: %v", err)
	}
	service, err := NewAgentRegistryService(database, AgentCardHealthCheckerFunc(func(context.Context, AgentCardHealthCheckRequest) error { return nil }), nil, false)
	if err != nil {
		t.Fatalf("create workflow registry service: %v", err)
	}
	t.Cleanup(func() {
		sqlDB, dbErr := database.DB()
		if dbErr == nil {
			_ = sqlDB.Close()
		}
	})
	return database, service
}

func workflowDefinitionJSON() string {
	return `{"entry_node":"start","nodes":[{"key":"start","type":"noop"}],"edges":[]}`
}

func TestNormalizeWorkflowDefinitionProducesStableCanonicalChecksum(t *testing.T) {
	first, firstChecksum, err := normalizeWorkflowDefinition([]byte(`{
    "edges": [],
    "nodes": [{"type":"noop","key":" start "}],
    "entry_node": " start "
  }`))
	if err != nil {
		t.Fatalf("normalize first definition: %v", err)
	}
	second, secondChecksum, err := normalizeWorkflowDefinition([]byte(workflowDefinitionJSON()))
	if err != nil {
		t.Fatalf("normalize second definition: %v", err)
	}
	if first != second || firstChecksum != secondChecksum {
		t.Fatalf("equivalent definitions differ: first=%s/%s second=%s/%s", first, firstChecksum, second, secondChecksum)
	}
}

func TestWorkflowRegistryLifecycleAndCapabilityVersioning(t *testing.T) {
	database, service := newWorkflowRegistryTestService(t)
	ctx := context.Background()
	owner := RegistryActor{UserID: 1}
	if _, err := service.CreateAgent(ctx, owner, CreateAgentCommand{AgentCode: "writer", Name: "Writer"}); err != nil {
		t.Fatalf("create agent: %v", err)
	}

	v1, err := service.CreateWorkflow(ctx, owner, "writer", CreateWorkflowCommand{Version: 1, Definition: []byte(workflowDefinitionJSON())})
	if err != nil {
		t.Fatalf("create workflow v1: %v", err)
	}
	if v1.IsActive || v1.Checksum == "" || len(v1.Definition) == 0 {
		t.Fatalf("unexpected v1 view: %+v", v1)
	}
	if _, err := service.CreateWorkflow(ctx, owner, "writer", CreateWorkflowCommand{Version: 1, Definition: []byte(workflowDefinitionJSON())}); !errors.Is(err, ErrWorkflowAlreadyExists()) {
		t.Fatalf("duplicate workflow error = %v", err)
	}

	v2, err := service.CreateWorkflow(ctx, owner, "writer", CreateWorkflowCommand{Version: 2, Definition: []byte(workflowDefinitionJSON())})
	if err != nil {
		t.Fatalf("create workflow v2: %v", err)
	}
	updatedV2, err := service.UpdateWorkflow(ctx, owner, "writer", 2, UpdateWorkflowCommand{Definition: []byte(`{"entry_node":"finish","nodes":[{"key":"finish","type":"noop"}],"edges":[]}`)})
	if err != nil {
		t.Fatalf("update inactive workflow v2: %v", err)
	}
	if updatedV2.Checksum == v2.Checksum {
		t.Fatal("workflow checksum did not change after definition update")
	}

	if _, err := service.ActivateWorkflow(ctx, owner, "writer", 1); err != nil {
		t.Fatalf("activate workflow v1: %v", err)
	}
	if _, err := service.ActivateWorkflow(ctx, owner, "writer", 2); err != nil {
		t.Fatalf("activate workflow v2: %v", err)
	}
	var persistedV1 models.Workflow
	if err := database.Where("agent_id = ? AND version = ?", 1, 1).First(&persistedV1).Error; err != nil {
		t.Fatalf("load workflow v1: %v", err)
	}
	var persistedV2 models.Workflow
	if err := database.Where("agent_id = ? AND version = ?", 1, 2).First(&persistedV2).Error; err != nil {
		t.Fatalf("load workflow v2: %v", err)
	}

	capability := UpsertCapabilityCommand{
		CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeWorkflow,
		WorkflowID: &persistedV1.ID, Version: "1", Status: models.AgentCapabilityStatusActive,
	}
	if _, err := service.CreateCapability(ctx, owner, "writer", capability); err != nil {
		t.Fatalf("create workflow capability: %v", err)
	}
	if _, err := service.DeactivateWorkflow(ctx, owner, "writer", 1); !errors.Is(err, ErrWorkflowInvalidState()) {
		t.Fatalf("deactivate referenced workflow error = %v", err)
	}
	if _, err := service.UpdateWorkflow(ctx, owner, "writer", 1, UpdateWorkflowCommand{Definition: []byte(workflowDefinitionJSON())}); !errors.Is(err, ErrWorkflowInvalidState()) {
		t.Fatalf("update active workflow error = %v", err)
	}

	capability.WorkflowID = &persistedV2.ID
	capability.Version = "2"
	if _, err := service.UpdateCapability(ctx, owner, "writer", "write", capability); err != nil {
		t.Fatalf("rebind capability to v2: %v", err)
	}
	if _, err := service.DeactivateWorkflow(ctx, owner, "writer", 1); err != nil {
		t.Fatalf("deactivate unreferenced workflow v1: %v", err)
	}
	if _, err := service.DeactivateWorkflow(ctx, owner, "writer", 2); !errors.Is(err, ErrWorkflowInvalidState()) {
		t.Fatalf("deactivate referenced workflow v2 error = %v", err)
	}

	workflows, err := service.ListWorkflows(ctx, owner, "writer")
	if err != nil {
		t.Fatalf("list workflows: %v", err)
	}
	if len(workflows) != 2 || workflows[0].Version != 2 || workflows[1].Version != 1 {
		t.Fatalf("unexpected workflow order: %+v", workflows)
	}
	if len(workflows[0].Capabilities) != 1 || workflows[0].Capabilities[0].CapabilityCode != "write" {
		t.Fatalf("workflow capability reference missing: %+v", workflows[0].Capabilities)
	}
}

func TestWorkflowRegistryRejectsInvalidDefinitionAndEnforcesOwnership(t *testing.T) {
	_, service := newWorkflowRegistryTestService(t)
	ctx := context.Background()
	owner := RegistryActor{UserID: 1}
	admin := RegistryActor{UserID: 2, CanManage: true}
	if _, err := service.CreateAgent(ctx, owner, CreateAgentCommand{AgentCode: "writer", Name: "Writer"}); err != nil {
		t.Fatalf("create agent: %v", err)
	}
	if _, err := service.CreateWorkflow(ctx, owner, "writer", CreateWorkflowCommand{Version: 0, Definition: []byte(workflowDefinitionJSON())}); !errors.Is(err, ErrAgentRegistryValidation()) {
		t.Fatalf("invalid version error = %v", err)
	}
	if _, err := service.CreateWorkflow(ctx, owner, "writer", CreateWorkflowCommand{Version: 1, Definition: []byte(`{"entry_node":"missing","nodes":[],"edges":[]}`)}); !errors.Is(err, ErrAgentRegistryValidation()) {
		t.Fatalf("invalid definition error = %v", err)
	}
	if _, err := service.GetWorkflow(ctx, RegistryActor{UserID: 3}, "writer", 1); !errors.Is(err, ErrAgentForbidden()) {
		t.Fatalf("foreign owner error = %v", err)
	}
	if _, err := service.ListWorkflows(ctx, admin, "writer"); err != nil {
		t.Fatalf("admin workflow access: %v", err)
	}
}

func TestWorkflowActivationRejectsMismatchedInactiveCapability(t *testing.T) {
	database, service := newWorkflowRegistryTestService(t)
	ctx := context.Background()
	owner := RegistryActor{UserID: 1}
	if _, err := service.CreateAgent(ctx, owner, CreateAgentCommand{AgentCode: "writer", Name: "Writer"}); err != nil {
		t.Fatalf("create agent: %v", err)
	}
	workflow, err := service.CreateWorkflow(ctx, owner, "writer", CreateWorkflowCommand{Version: 2, Definition: []byte(workflowDefinitionJSON())})
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	if err := database.Create(&models.AgentCapability{
		AgentID: 1, CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeWorkflow,
		WorkflowID: &workflow.ID, Version: "1", Status: models.AgentCapabilityStatusInactive,
	}).Error; err != nil {
		t.Fatalf("create mismatched inactive capability: %v", err)
	}
	if _, err := service.ActivateWorkflow(ctx, owner, "writer", 2); !errors.Is(err, ErrWorkflowInvalidState()) {
		t.Fatalf("mismatched capability activation error = %v", err)
	}
}
