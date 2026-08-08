package services

import (
	"context"
	"errors"
	"testing"

	"GoAI/models"
)

func TestRegistryAgentRouterSelectsDeterministicHealthyCandidate(t *testing.T) {
	database := openSQLiteTestDB(t)
	if err := database.AutoMigrate(&models.Agent{}, &models.Workflow{}, &models.AgentCapability{}, &models.AgentEndpoint{}); err != nil {
		t.Fatalf("auto migrate router tables: %v", err)
	}
	source := models.Agent{AgentCode: "supervisor", Name: "Supervisor", OwnerUserID: 1, Status: models.AgentStatusActive}
	if err := database.Create(&source).Error; err != nil {
		t.Fatalf("create source agent: %v", err)
	}
	createCandidate := func(code, endpointCode string) models.Agent {
		t.Helper()
		agent := models.Agent{AgentCode: code, Name: code, OwnerUserID: 1, Status: models.AgentStatusActive}
		if err := database.Create(&agent).Error; err != nil {
			t.Fatalf("create candidate %s: %v", code, err)
		}
		workflow := models.Workflow{AgentID: agent.ID, Version: 3, DefinitionJSON: `{"entry_node":"worker","nodes":[{"key":"worker","type":"noop"}],"edges":[]}`, Checksum: code + "-checksum", IsActive: true, CreatedBy: 1}
		if err := database.Create(&workflow).Error; err != nil {
			t.Fatalf("create workflow %s: %v", code, err)
		}
		capability := models.AgentCapability{AgentID: agent.ID, CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeWorkflow, WorkflowID: &workflow.ID, Version: "3", Status: models.AgentCapabilityStatusActive}
		if err := database.Create(&capability).Error; err != nil {
			t.Fatalf("create capability %s: %v", code, err)
		}
		endpoint := models.AgentEndpoint{AgentID: agent.ID, EndpointCode: endpointCode, Protocol: models.AgentEndpointProtocolA2A, Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1/" + code, Status: models.AgentEndpointStatusActive}
		if err := database.Create(&endpoint).Error; err != nil {
			t.Fatalf("create endpoint %s: %v", code, err)
		}
		return agent
	}
	createCandidate("writer-z", "z-local")
	createCandidate("writer-a", "a-local")

	router, err := NewRegistryAgentRouter(database)
	if err != nil {
		t.Fatalf("create router: %v", err)
	}
	route, err := router.Route(context.Background(), AgentRouteRequest{SourceAgentID: source.ID, CapabilityCode: "write"})
	if err != nil {
		t.Fatalf("route agent: %v", err)
	}
	if route.Agent.AgentCode != "writer-a" || route.Endpoint.EndpointCode != "a-local" || route.Workflow.Version != 3 {
		t.Fatalf("unexpected deterministic route: %+v", route)
	}
	if route.SelectionReason != "registry:agent_code=writer-a;endpoint_code=a-local" {
		t.Fatalf("unexpected selection reason: %q", route.SelectionReason)
	}

	preferred, err := router.Route(context.Background(), AgentRouteRequest{SourceAgentID: source.ID, CapabilityCode: "write", PreferredAgentCode: "writer-z"})
	if err != nil {
		t.Fatalf("route preferred agent: %v", err)
	}
	if preferred.Agent.AgentCode != "writer-z" {
		t.Fatalf("preferred agent was not selected: %+v", preferred)
	}
}

func TestRegistryAgentRouterRejectsUnpublishedOrUnavailableCandidates(t *testing.T) {
	database := openSQLiteTestDB(t)
	if err := database.AutoMigrate(&models.Agent{}, &models.Workflow{}, &models.AgentCapability{}, &models.AgentEndpoint{}); err != nil {
		t.Fatalf("auto migrate router tables: %v", err)
	}
	agent := models.Agent{AgentCode: "worker", Name: "Worker", OwnerUserID: 1, Status: models.AgentStatusActive}
	if err := database.Create(&agent).Error; err != nil {
		t.Fatalf("create agent: %v", err)
	}
	workflow := models.Workflow{AgentID: agent.ID, Version: 1, DefinitionJSON: `{"entry_node":"worker","nodes":[{"key":"worker","type":"noop"}],"edges":[]}`, Checksum: "worker-checksum", IsActive: true, CreatedBy: 1}
	if err := database.Create(&workflow).Error; err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	capability := models.AgentCapability{AgentID: agent.ID, CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeWorkflow, WorkflowID: &workflow.ID, Version: "1", Status: models.AgentCapabilityStatusActive}
	if err := database.Create(&capability).Error; err != nil {
		t.Fatalf("create capability: %v", err)
	}
	endpoint := models.AgentEndpoint{AgentID: agent.ID, EndpointCode: "worker-local", Protocol: models.AgentEndpointProtocolA2A, Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1/worker", Status: models.AgentEndpointStatusInactive}
	if err := database.Create(&endpoint).Error; err != nil {
		t.Fatalf("create endpoint: %v", err)
	}
	router, err := NewRegistryAgentRouter(database)
	if err != nil {
		t.Fatalf("create router: %v", err)
	}
	if _, err := router.Route(context.Background(), AgentRouteRequest{CapabilityCode: "write"}); !errors.Is(err, ErrAgentRouteUnavailable()) {
		t.Fatalf("expected unavailable route, got %v", err)
	}
	if _, err := router.Route(context.Background(), AgentRouteRequest{CapabilityCode: "missing"}); !errors.Is(err, ErrAgentRouteNotFound()) {
		t.Fatalf("expected missing route, got %v", err)
	}
	if _, err := router.Route(context.Background(), AgentRouteRequest{}); !errors.Is(err, ErrAgentRouteInvalid()) {
		t.Fatalf("expected invalid route request, got %v", err)
	}
}

func TestRegistryAgentRouterSelectsRemoteA2ACapabilityWithoutLocalWorkflow(t *testing.T) {
	database := openSQLiteTestDB(t)
	if err := database.AutoMigrate(&models.Agent{}, &models.AgentCapability{}, &models.AgentEndpoint{}); err != nil {
		t.Fatalf("auto migrate remote router tables: %v", err)
	}
	source := models.Agent{AgentCode: "planner", Name: "Planner", OwnerUserID: 1, Status: models.AgentStatusActive}
	remote := models.Agent{AgentCode: "external-writer", Name: "External Writer", OwnerUserID: 2, Status: models.AgentStatusActive}
	if err := database.Create(&source).Error; err != nil {
		t.Fatalf("create source agent: %v", err)
	}
	if err := database.Create(&remote).Error; err != nil {
		t.Fatalf("create remote agent: %v", err)
	}
	if err := database.Create(&models.AgentCapability{
		AgentID: remote.ID, CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeRemote,
		Version: "1", Status: models.AgentCapabilityStatusActive,
	}).Error; err != nil {
		t.Fatalf("create remote capability: %v", err)
	}
	if err := database.Create(&models.AgentEndpoint{
		AgentID: remote.ID, EndpointCode: "primary", Protocol: models.AgentEndpointProtocolA2A,
		Transport: models.AgentEndpointTransportHTTPS, Address: "https://agents.example.com/a2a/agents/external-writer",
		Status: models.AgentEndpointStatusActive,
	}).Error; err != nil {
		t.Fatalf("create remote endpoint: %v", err)
	}
	router, err := NewRegistryAgentRouter(database)
	if err != nil {
		t.Fatalf("create router: %v", err)
	}
	route, err := router.Route(context.Background(), AgentRouteRequest{SourceAgentID: source.ID, CapabilityCode: "write"})
	if err != nil {
		t.Fatalf("route remote capability: %v", err)
	}
	if route.Agent.AgentCode != remote.AgentCode || route.Capability.CapabilityType != models.AgentCapabilityTypeRemote || route.Workflow.ID != 0 {
		t.Fatalf("unexpected remote route: %+v", route)
	}
}
