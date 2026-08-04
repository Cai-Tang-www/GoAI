package services

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"GoAI/a2aauth"
	"GoAI/models"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type registryHealthChecker struct {
	mu       sync.Mutex
	requests []AgentCardHealthCheckRequest
	err      error
}

func (c *registryHealthChecker) CheckAgentCard(_ context.Context, request AgentCardHealthCheckRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, request)
	return c.err
}

func (c *registryHealthChecker) setError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
}

type blockingRegistryHealthChecker struct {
	started chan struct{}
	release chan struct{}
}

func (c *blockingRegistryHealthChecker) CheckAgentCard(context.Context, AgentCardHealthCheckRequest) error {
	close(c.started)
	<-c.release
	return nil
}

func setupAgentRegistryTest(t *testing.T, authRequired bool) (*gorm.DB, *AgentRegistryService, *registryHealthChecker) {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?cache=shared&_busy_timeout=5000", filepath.ToSlash(filepath.Join(t.TempDir(), "registry.db")))
	database, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open registry database: %v", err)
	}
	if err := database.AutoMigrate(&models.Agent{}, &models.AgentCapability{}, &models.AgentEndpoint{}, &models.Workflow{}); err != nil {
		t.Fatalf("migrate registry database: %v", err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get registry sql database: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	checker := &registryHealthChecker{}
	var resolver a2aauth.CredentialResolver
	if authRequired {
		resolver, err = a2aauth.NewStaticCredentialResolver(map[string]string{
			"writer-key": "0123456789abcdef0123456789abcdef",
		})
		if err != nil {
			t.Fatalf("create credential resolver: %v", err)
		}
	}
	service, err := NewAgentRegistryService(database, checker, resolver, authRequired)
	if err != nil {
		t.Fatalf("create registry service: %v", err)
	}
	service.now = func() time.Time { return time.Date(2026, time.August, 4, 10, 0, 0, 0, time.UTC) }
	return database, service, checker
}

func createRegistryAgent(t *testing.T, service *AgentRegistryService, actor RegistryActor, code string) *AgentDetailView {
	t.Helper()
	agent, err := service.CreateAgent(context.Background(), actor, CreateAgentCommand{
		AgentCode: code, Name: "Agent " + code, Description: "test agent",
	})
	if err != nil {
		t.Fatalf("create agent %s: %v", code, err)
	}
	return agent
}

func preparePublishableAgent(t *testing.T, service *AgentRegistryService, actor RegistryActor, code string, authRequired bool) {
	t.Helper()
	createRegistryAgent(t, service, actor, code)
	var agent models.Agent
	if err := service.database.Where("agent_code = ?", code).First(&agent).Error; err != nil {
		t.Fatalf("load agent %s: %v", code, err)
	}
	workflow := models.Workflow{
		AgentID: agent.ID, Version: 1, DefinitionJSON: `{"nodes":[],"edges":[]}`,
		Checksum: code + "-v1", IsActive: true, CreatedBy: actor.UserID,
	}
	if err := service.database.Create(&workflow).Error; err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	if _, err := service.CreateCapability(context.Background(), actor, code, UpsertCapabilityCommand{
		CapabilityCode: "execute", Name: "Execute", CapabilityType: models.AgentCapabilityTypeWorkflow,
		WorkflowID: &workflow.ID, Version: "1", InputSchemaJSON: `{"type":"object"}`, OutputSchemaJSON: `{"type":"object"}`,
	}); err != nil {
		t.Fatalf("create capability: %v", err)
	}
	endpoint := UpsertEndpointCommand{
		EndpointCode: "local", Protocol: models.AgentEndpointProtocolA2A,
		Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1:8080/a2a/agents/" + code,
		AuthType: models.AgentEndpointAuthTypeNone,
	}
	if authRequired {
		endpoint.AuthType = models.AgentEndpointAuthTypeHMACSHA256
		endpoint.CredentialRef = "writer-key"
	}
	if _, err := service.CreateEndpoint(context.Background(), actor, code, endpoint); err != nil {
		t.Fatalf("create endpoint: %v", err)
	}
	if _, err := service.CheckEndpointHealth(context.Background(), actor, code, "local"); err != nil {
		t.Fatalf("check endpoint: %v", err)
	}
}

func TestAgentRegistryOwnershipAndAdminVisibility(t *testing.T) {
	_, service, _ := setupAgentRegistryTest(t, false)
	owner := RegistryActor{UserID: 1}
	other := RegistryActor{UserID: 2}
	admin := RegistryActor{UserID: 99, CanManage: true}
	createRegistryAgent(t, service, owner, "writer")
	createRegistryAgent(t, service, other, "reviewer")

	ownerAgents, err := service.ListAgents(context.Background(), owner)
	if err != nil || len(ownerAgents) != 1 || ownerAgents[0].AgentCode != "writer" {
		t.Fatalf("owner list mismatch: agents=%+v err=%v", ownerAgents, err)
	}
	adminAgents, err := service.ListAgents(context.Background(), admin)
	if err != nil || len(adminAgents) != 2 {
		t.Fatalf("admin list mismatch: agents=%+v err=%v", adminAgents, err)
	}
	if _, err := service.GetAgent(context.Background(), other, "writer"); !errors.Is(err, ErrAgentForbidden()) {
		t.Fatalf("cross-owner access error = %v, want forbidden", err)
	}
	if _, err := service.GetAgent(context.Background(), admin, "writer"); err != nil {
		t.Fatalf("admin must access owner agent: %v", err)
	}
}

func TestAgentRegistryCreateValidatesAndRejectsDuplicate(t *testing.T) {
	_, service, _ := setupAgentRegistryTest(t, false)
	actor := RegistryActor{UserID: 1}
	created := createRegistryAgent(t, service, actor, "writer")
	if created.Status != models.AgentStatusInactive || created.OwnerUserID != actor.UserID {
		t.Fatalf("created agent mismatch: %+v", created)
	}
	if _, err := service.CreateAgent(context.Background(), actor, CreateAgentCommand{AgentCode: "writer", Name: "duplicate"}); !errors.Is(err, ErrAgentAlreadyExists()) {
		t.Fatalf("duplicate error = %v", err)
	}
	if _, err := service.CreateAgent(context.Background(), actor, CreateAgentCommand{AgentCode: "bad code", Name: "bad"}); !errors.Is(err, ErrAgentRegistryValidation()) {
		t.Fatalf("validation error = %v", err)
	}
}

func TestAgentRegistryActivationRequiresCompleteAssets(t *testing.T) {
	_, service, _ := setupAgentRegistryTest(t, false)
	actor := RegistryActor{UserID: 1}
	createRegistryAgent(t, service, actor, "writer")
	if _, err := service.ActivateAgent(context.Background(), actor, "writer"); !errors.Is(err, ErrAgentPublishValidation()) {
		t.Fatalf("activation without assets error = %v", err)
	}
	if _, err := service.CreateCapability(context.Background(), actor, "writer", UpsertCapabilityCommand{
		CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeCustom, Version: "1",
	}); err != nil {
		t.Fatalf("create capability: %v", err)
	}
	if _, err := service.ActivateAgent(context.Background(), actor, "writer"); !errors.Is(err, ErrAgentPublishValidation()) {
		t.Fatalf("activation without endpoint error = %v", err)
	}
}

func TestAgentRegistryActivationRejectsNonExecutableCapabilities(t *testing.T) {
	database, service, _ := setupAgentRegistryTest(t, false)
	actor := RegistryActor{UserID: 1}
	createRegistryAgent(t, service, actor, "writer")
	if _, err := service.CreateCapability(context.Background(), actor, "writer", UpsertCapabilityCommand{
		CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeCustom, Version: "1",
	}); err != nil {
		t.Fatalf("create custom capability: %v", err)
	}
	if _, err := service.CreateEndpoint(context.Background(), actor, "writer", UpsertEndpointCommand{
		EndpointCode: "local", Protocol: models.AgentEndpointProtocolA2A,
		Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1:8080/a2a/agents/writer",
		AuthType: models.AgentEndpointAuthTypeNone,
	}); err != nil {
		t.Fatalf("create endpoint: %v", err)
	}
	if _, err := service.CheckEndpointHealth(context.Background(), actor, "writer", "local"); err != nil {
		t.Fatalf("check endpoint health: %v", err)
	}
	if _, err := service.ActivateAgent(context.Background(), actor, "writer"); !errors.Is(err, ErrAgentPublishValidation()) {
		t.Fatalf("activation with only non-executable capabilities error = %v", err)
	}
	var agent models.Agent
	if err := database.Where("agent_code = ?", "writer").First(&agent).Error; err != nil {
		t.Fatal(err)
	}
	if agent.Status != models.AgentStatusInactive {
		t.Fatalf("agent with no executable capability must remain inactive: %+v", agent)
	}
}

func TestAgentRegistryHealthAndActivationHappyPath(t *testing.T) {
	_, service, checker := setupAgentRegistryTest(t, false)
	actor := RegistryActor{UserID: 1}
	preparePublishableAgent(t, service, actor, "writer", false)
	agent, err := service.ActivateAgent(context.Background(), actor, "writer")
	if err != nil {
		t.Fatalf("activate agent: %v", err)
	}
	if agent.Status != models.AgentStatusActive || len(agent.Capabilities) != 1 || len(agent.Endpoints) != 1 {
		t.Fatalf("activated agent mismatch: %+v", agent)
	}
	if agent.Endpoints[0].Status != models.AgentEndpointStatusActive || agent.Endpoints[0].LastHealthyAt == nil {
		t.Fatalf("healthy endpoint mismatch: %+v", agent.Endpoints[0])
	}
	checker.mu.Lock()
	defer checker.mu.Unlock()
	if len(checker.requests) != 1 || checker.requests[0].AgentCode != "writer" {
		t.Fatalf("health request mismatch: %+v", checker.requests)
	}
}

func TestAgentRegistryChangesReachA2ADiscoveryImmediately(t *testing.T) {
	database, service, _ := setupAgentRegistryTest(t, false)
	actor := RegistryActor{UserID: 1}
	preparePublishableAgent(t, service, actor, "writer", false)
	if _, err := service.ActivateAgent(context.Background(), actor, "writer"); err != nil {
		t.Fatalf("activate agent: %v", err)
	}
	runService, err := NewRunService(database, RunEventPublisherFunc(func(context.Context, string) error { return nil }))
	if err != nil {
		t.Fatalf("create run service: %v", err)
	}
	runtimeService, err := NewRuntimeService(database, runService)
	if err != nil {
		t.Fatalf("create runtime service: %v", err)
	}
	descriptor, err := runtimeService.DescribeAgent(context.Background(), "writer")
	if err != nil {
		t.Fatalf("describe activated agent: %v", err)
	}
	if len(descriptor.Capabilities) != 1 || descriptor.Capabilities[0].Code != "execute" {
		t.Fatalf("initial discovery capabilities = %+v", descriptor.Capabilities)
	}

	detail, err := service.GetAgent(context.Background(), actor, "writer")
	if err != nil {
		t.Fatalf("get agent detail: %v", err)
	}
	capability := detail.Capabilities[0]
	if _, err := service.UpdateCapability(context.Background(), actor, "writer", "execute", UpsertCapabilityCommand{
		Name: "Execute v2", Description: "updated through registry",
		CapabilityType: models.AgentCapabilityTypeWorkflow, WorkflowID: capability.WorkflowID,
		Version: capability.Version, InputSchemaJSON: capability.InputSchemaJSON,
		OutputSchemaJSON: capability.OutputSchemaJSON, ConfigJSON: capability.ConfigJSON,
		Status: models.AgentCapabilityStatusActive,
	}); err != nil {
		t.Fatalf("update active capability: %v", err)
	}
	descriptor, err = runtimeService.DescribeAgent(context.Background(), "writer")
	if err != nil {
		t.Fatalf("describe updated agent: %v", err)
	}
	if len(descriptor.Capabilities) != 1 || descriptor.Capabilities[0].Name != "Execute v2" {
		t.Fatalf("updated discovery capabilities = %+v", descriptor.Capabilities)
	}
}

func TestAgentRegistryWorkflowCapabilityMustReferenceOwnActiveWorkflow(t *testing.T) {
	database, service, _ := setupAgentRegistryTest(t, false)
	actor := RegistryActor{UserID: 1}
	writer := createRegistryAgent(t, service, actor, "writer")
	reviewer := createRegistryAgent(t, service, actor, "reviewer")
	var writerModel, reviewerModel models.Agent
	if err := database.Where("agent_code = ?", writer.AgentCode).First(&writerModel).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.Where("agent_code = ?", reviewer.AgentCode).First(&reviewerModel).Error; err != nil {
		t.Fatal(err)
	}
	foreignWorkflow := models.Workflow{AgentID: reviewerModel.ID, Version: 2, DefinitionJSON: `{}`, Checksum: "foreign", IsActive: true, CreatedBy: actor.UserID}
	if err := database.Create(&foreignWorkflow).Error; err != nil {
		t.Fatal(err)
	}
	_, err := service.CreateCapability(context.Background(), actor, "writer", UpsertCapabilityCommand{
		CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeWorkflow,
		WorkflowID: &foreignWorkflow.ID, Version: "2",
	})
	if !errors.Is(err, ErrAgentRegistryValidation()) {
		t.Fatalf("foreign workflow error = %v", err)
	}
	ownWorkflow := models.Workflow{AgentID: writerModel.ID, Version: 3, DefinitionJSON: `{}`, Checksum: "own", IsActive: true, CreatedBy: actor.UserID}
	if err := database.Create(&ownWorkflow).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateCapability(context.Background(), actor, "writer", UpsertCapabilityCommand{
		CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeWorkflow,
		WorkflowID: &ownWorkflow.ID, Version: "2",
	}); !errors.Is(err, ErrAgentRegistryValidation()) {
		t.Fatalf("version mismatch error = %v", err)
	}
	if _, err := service.CreateCapability(context.Background(), actor, "writer", UpsertCapabilityCommand{
		CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeWorkflow,
		WorkflowID: &ownWorkflow.ID, Version: "3",
	}); err != nil {
		t.Fatalf("create own workflow capability: %v", err)
	}
}

func TestAgentRegistryEndpointTransportAndCredentials(t *testing.T) {
	_, service, _ := setupAgentRegistryTest(t, true)
	actor := RegistryActor{UserID: 1}
	createRegistryAgent(t, service, actor, "writer")
	tests := []UpsertEndpointCommand{
		{EndpointCode: "remote-http", Protocol: "a2a", Transport: "http", Address: "http://example.com/a2a", AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "writer-key"},
		{EndpointCode: "anonymous", Protocol: "a2a", Transport: "https", Address: "https://example.com/a2a", AuthType: models.AgentEndpointAuthTypeNone},
		{EndpointCode: "long-address", Protocol: "a2a", Transport: "https", Address: "https://example.com/" + strings.Repeat("a", 512), AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "writer-key"},
		{EndpointCode: "inline-secret", Protocol: "a2a", Transport: "https", Address: "https://example.com/a2a", AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "writer-key", ConfigJSON: `{"api_key":"do-not-store-here"}`},
		{EndpointCode: "nested-secret", Protocol: "a2a", Transport: "https", Address: "https://example.com/a2a", AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "writer-key", ConfigJSON: `{"auth":{"client_secret":"do-not-store-here"}}`},
		{EndpointCode: "aws-secret", Protocol: "a2a", Transport: "https", Address: "https://example.com/a2a", AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "writer-key", ConfigJSON: `{"aws_secret_access_key":"do-not-store-here"}`},
		{EndpointCode: "provider-key", Protocol: "a2a", Transport: "https", Address: "https://example.com/a2a", AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "writer-key", ConfigJSON: `{"openai_api_key":"do-not-store-here"}`},
		{EndpointCode: "auth-token", Protocol: "a2a", Transport: "https", Address: "https://example.com/a2a", AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "writer-key", ConfigJSON: `{"auth_token_ref":"do-not-store-here"}`},
	}
	for _, command := range tests {
		if _, err := service.CreateEndpoint(context.Background(), actor, "writer", command); !errors.Is(err, ErrAgentRegistryValidation()) {
			t.Fatalf("endpoint %s error = %v", command.EndpointCode, err)
		}
	}
	endpoint, err := service.CreateEndpoint(context.Background(), actor, "writer", UpsertEndpointCommand{
		EndpointCode: "remote", Protocol: "a2a", Transport: "https", Address: "https://example.com/a2a",
		AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "writer-key",
		ConfigJSON: `{"max_tokens":128,"timeout_ms":5000}`,
	})
	if err != nil {
		t.Fatalf("create secure endpoint: %v", err)
	}
	if endpoint.ConfigJSON != `{"max_tokens":128,"timeout_ms":5000}` {
		t.Fatalf("safe endpoint metadata was not preserved: %+v", endpoint)
	}
}

func TestAgentRegistryActiveAssetMutationRollsBack(t *testing.T) {
	database, service, _ := setupAgentRegistryTest(t, false)
	actor := RegistryActor{UserID: 1}
	preparePublishableAgent(t, service, actor, "writer", false)
	if _, err := service.ActivateAgent(context.Background(), actor, "writer"); err != nil {
		t.Fatalf("activate agent: %v", err)
	}
	if _, err := service.DeactivateCapability(context.Background(), actor, "writer", "execute"); !errors.Is(err, ErrAgentPublishValidation()) {
		t.Fatalf("deactivate sole capability error = %v", err)
	}
	var capability models.AgentCapability
	if err := database.Where("capability_code = ?", "execute").First(&capability).Error; err != nil {
		t.Fatal(err)
	}
	if capability.Status != models.AgentCapabilityStatusActive {
		t.Fatalf("capability mutation was not rolled back: %+v", capability)
	}
	if _, err := service.DeactivateEndpoint(context.Background(), actor, "writer", "local"); !errors.Is(err, ErrAgentPublishValidation()) {
		t.Fatalf("deactivate sole endpoint error = %v", err)
	}
	var endpoint models.AgentEndpoint
	if err := database.Where("endpoint_code = ?", "local").First(&endpoint).Error; err != nil {
		t.Fatal(err)
	}
	if endpoint.Status != models.AgentEndpointStatusActive {
		t.Fatalf("endpoint mutation was not rolled back: %+v", endpoint)
	}
}

func TestAgentRegistryHealthCheckDoesNotReactivateChangedEndpoint(t *testing.T) {
	database, service, _ := setupAgentRegistryTest(t, false)
	actor := RegistryActor{UserID: 1}
	preparePublishableAgent(t, service, actor, "writer", false)

	checker := &blockingRegistryHealthChecker{started: make(chan struct{}), release: make(chan struct{})}
	checkingService, err := NewAgentRegistryService(database, checker, nil, false)
	if err != nil {
		t.Fatalf("create blocking registry service: %v", err)
	}
	defer func() {
		select {
		case <-checker.release:
		default:
			close(checker.release)
		}
	}()

	result := make(chan error, 1)
	go func() {
		_, err := checkingService.CheckEndpointHealth(context.Background(), actor, "writer", "local")
		result <- err
	}()
	select {
	case <-checker.started:
	case <-time.After(2 * time.Second):
		t.Fatal("health check did not start")
	}

	if _, err := checkingService.DeactivateEndpoint(context.Background(), actor, "writer", "local"); err != nil {
		t.Fatalf("deactivate endpoint during health check: %v", err)
	}
	close(checker.release)

	select {
	case err := <-result:
		if !errors.Is(err, ErrAgentInvalidState()) {
			t.Fatalf("stale health check error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("health check did not finish")
	}

	var endpoint models.AgentEndpoint
	if err := database.Where("endpoint_code = ?", "local").First(&endpoint).Error; err != nil {
		t.Fatal(err)
	}
	if endpoint.Status != models.AgentEndpointStatusInactive {
		t.Fatalf("stale health check reactivated endpoint: %+v", endpoint)
	}
}

func TestAgentRegistryFailedHealthCheckMarksEndpointAndAgentUnavailable(t *testing.T) {
	database, service, checker := setupAgentRegistryTest(t, false)
	actor := RegistryActor{UserID: 1}
	preparePublishableAgent(t, service, actor, "writer", false)
	if _, err := service.ActivateAgent(context.Background(), actor, "writer"); err != nil {
		t.Fatalf("activate agent: %v", err)
	}
	checker.setError(errors.New("agent card unavailable"))
	if _, err := service.CheckEndpointHealth(context.Background(), actor, "writer", "local"); !errors.Is(err, ErrEndpointHealthCheckFailed()) {
		t.Fatalf("health failure error = %v", err)
	}
	var agent models.Agent
	if err := database.Where("agent_code = ?", "writer").First(&agent).Error; err != nil {
		t.Fatal(err)
	}
	var endpoint models.AgentEndpoint
	if err := database.Where("agent_id = ? AND endpoint_code = ?", agent.ID, "local").First(&endpoint).Error; err != nil {
		t.Fatal(err)
	}
	if endpoint.Status != models.AgentEndpointStatusUnhealthy || agent.Status != models.AgentStatusInactive {
		t.Fatalf("failed health state mismatch: agent=%+v endpoint=%+v", agent, endpoint)
	}
}

func TestAgentRegistryConcurrentActivationIsIdempotent(t *testing.T) {
	_, service, _ := setupAgentRegistryTest(t, false)
	actor := RegistryActor{UserID: 1}
	preparePublishableAgent(t, service, actor, "writer", false)
	const callers = 8
	errorsCh := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := service.ActivateAgent(context.Background(), actor, "writer")
			errorsCh <- err
		}()
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("concurrent activation failed: %v", err)
		}
	}
	agent, err := service.GetAgent(context.Background(), actor, "writer")
	if err != nil || agent.Status != models.AgentStatusActive {
		t.Fatalf("final agent state = %+v err=%v", agent, err)
	}
}

func TestAgentRegistryHMACPublishRequiresResolvableCredential(t *testing.T) {
	database, service, _ := setupAgentRegistryTest(t, true)
	actor := RegistryActor{UserID: 1}
	preparePublishableAgent(t, service, actor, "writer", true)
	if err := database.Model(&models.AgentEndpoint{}).Where("endpoint_code = ?", "local").Update("credential_ref", "missing-key").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := service.ActivateAgent(context.Background(), actor, "writer"); !errors.Is(err, ErrAgentPublishValidation()) {
		t.Fatalf("unresolvable credential error = %v", err)
	}
}
