package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"GoAI/a2agateway"
	"GoAI/config"
	"GoAI/db"
	"GoAI/middlewares"
	"GoAI/models"
	"GoAI/services"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type handlerRegistryChecker struct {
	mu       sync.Mutex
	err      error
	requests []services.AgentCardHealthCheckRequest
}

func (c *handlerRegistryChecker) CheckAgentCard(_ context.Context, request services.AgentCardHealthCheckRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, request)
	return c.err
}

func (c *handlerRegistryChecker) setError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
}

type registryHTTPFixture struct {
	database   *gorm.DB
	router     *gin.Engine
	owner      models.User
	other      models.User
	admin      models.User
	ownerToken string
	otherToken string
	adminToken string
	checker    *handlerRegistryChecker
}

func setupRegistryHTTPFixture(t *testing.T) registryHTTPFixture {
	t.Helper()
	database := setupRBACIntegrationDB(t)
	if err := database.AutoMigrate(&models.AgentCapability{}, &models.AgentEndpoint{}); err != nil {
		t.Fatalf("migrate registry models: %v", err)
	}
	fixture := registryHTTPFixture{
		database: database,
		owner:    models.User{Username: "registry-owner", Email: "registry-owner@example.com", Password: "x"},
		other:    models.User{Username: "registry-other", Email: "registry-other@example.com", Password: "x"},
		admin:    models.User{Username: "registry-admin", Email: "registry-admin@example.com", Password: "x"},
		checker:  &handlerRegistryChecker{},
	}
	for _, user := range []*models.User{&fixture.owner, &fixture.other, &fixture.admin} {
		if err := database.Create(user).Error; err != nil {
			t.Fatalf("create registry user %s: %v", user.Username, err)
		}
	}
	config.AppConfig = &config.Config{
		JWTSecret:                  "registry-handler-test-secret",
		RBACEnable:                 true,
		RBACBootstrapAdminUsername: fixture.admin.Username,
		ModelProviders:             map[string]config.ModelProviderConfig{},
	}
	if err := db.SeedRBAC(database, config.AppConfig); err != nil {
		t.Fatalf("seed registry RBAC: %v", err)
	}
	var err error
	fixture.ownerToken, err = middlewares.GenerateToken(fixture.owner.ID)
	if err != nil {
		t.Fatalf("generate owner token: %v", err)
	}
	fixture.otherToken, err = middlewares.GenerateToken(fixture.other.ID)
	if err != nil {
		t.Fatalf("generate other token: %v", err)
	}
	fixture.adminToken, err = middlewares.GenerateToken(fixture.admin.ID)
	if err != nil {
		t.Fatalf("generate admin token: %v", err)
	}
	fixture.router = newTestRouterWithRegistryChecker(t, database, nil, fixture.checker)
	return fixture
}

func registryRequest(t *testing.T, router *gin.Engine, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var raw []byte
	var err error
	switch value := body.(type) {
	case nil:
	case string:
		raw = []byte(value)
	default:
		raw, err = json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal registry request: %v", err)
		}
	}
	request := httptest.NewRequest(method, path, bytes.NewReader(raw))
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}

func requireRegistryResponse(t *testing.T, response *httptest.ResponseRecorder, status int, code string) apiEnvelope {
	t.Helper()
	if response.Code != status {
		t.Fatalf("unexpected registry status: got=%d want=%d body=%s", response.Code, status, response.Body.String())
	}
	envelope := decodeEnvelope(t, response)
	if envelope.Code != code {
		t.Fatalf("unexpected registry code: got=%s want=%s body=%s", envelope.Code, code, response.Body.String())
	}
	return envelope
}

func TestAgentRegistryAPICompleteLifecycle(t *testing.T) {
	fixture := setupRegistryHTTPFixture(t)

	create := registryRequest(t, fixture.router, http.MethodPost, "/api/agents", fixture.ownerToken, map[string]any{
		"agent_code": "writer", "name": "Writer", "description": "writes articles",
	})
	requireRegistryResponse(t, create, http.StatusCreated, middlewares.CodeOK)
	var writer models.Agent
	if err := fixture.database.Where("agent_code = ?", "writer").First(&writer).Error; err != nil {
		t.Fatalf("load writer agent: %v", err)
	}
	workflow := models.Workflow{
		AgentID: writer.ID, Version: 1, DefinitionJSON: `{"nodes":[],"edges":[]}`,
		Checksum: "writer-v1", IsActive: true, CreatedBy: uint64(fixture.owner.ID),
	}
	if err := fixture.database.Create(&workflow).Error; err != nil {
		t.Fatalf("create writer workflow: %v", err)
	}

	capability := registryRequest(t, fixture.router, http.MethodPost, "/api/agents/writer/capabilities", fixture.ownerToken, map[string]any{
		"capability_code": "write", "name": "Write", "capability_type": "workflow", "workflow_id": workflow.ID, "version": "1",
		"input_schema_json": "{\"type\":\"object\"}", "output_schema_json": "{\"type\":\"object\"}",
	})
	requireRegistryResponse(t, capability, http.StatusCreated, middlewares.CodeOK)

	endpoint := registryRequest(t, fixture.router, http.MethodPost, "/api/agents/writer/endpoints", fixture.ownerToken, map[string]any{
		"endpoint_code": "local", "protocol": "a2a", "transport": "http",
		"address": "http://127.0.0.1:8080/a2a/agents/writer", "auth_type": "none",
	})
	requireRegistryResponse(t, endpoint, http.StatusCreated, middlewares.CodeOK)

	activateBeforeHealth := registryRequest(t, fixture.router, http.MethodPost, "/api/agents/writer/activate", fixture.ownerToken, nil)
	requireRegistryResponse(t, activateBeforeHealth, http.StatusUnprocessableEntity, middlewares.CodeAgentPublishValidation)

	health := registryRequest(t, fixture.router, http.MethodPost, "/api/agents/writer/endpoints/local/health-check", fixture.ownerToken, nil)
	healthEnvelope := requireRegistryResponse(t, health, http.StatusOK, middlewares.CodeOK)
	if !bytes.Contains(healthEnvelope.Data, []byte(`"status":"active"`)) {
		t.Fatalf("health response should activate endpoint: %s", health.Body.String())
	}

	activate := registryRequest(t, fixture.router, http.MethodPost, "/api/agents/writer/activate", fixture.ownerToken, nil)
	activateEnvelope := requireRegistryResponse(t, activate, http.StatusOK, middlewares.CodeOK)
	if !bytes.Contains(activateEnvelope.Data, []byte(`"status":"active"`)) {
		t.Fatalf("activation response should contain active agent: %s", activate.Body.String())
	}

	runService, err := services.NewRunService(fixture.database, services.RunEventPublisherFunc(func(context.Context, string) error { return nil }))
	if err != nil {
		t.Fatalf("create run service: %v", err)
	}
	runtimeService, err := services.NewRuntimeService(fixture.database, runService)
	if err != nil {
		t.Fatalf("create runtime service: %v", err)
	}
	gateway, err := a2agateway.New(runtimeService)
	if err != nil {
		t.Fatalf("create A2A gateway: %v", err)
	}
	requestCard := func() *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/a2a/agents/writer/.well-known/agent-card.json", nil)
		gateway.ServeHTTP(response, request)
		return response
	}
	card := requestCard()
	if card.Code != http.StatusOK || !bytes.Contains(card.Body.Bytes(), []byte(`"id":"write"`)) {
		t.Fatalf("activated capability missing from Agent Card: status=%d body=%s", card.Code, card.Body.String())
	}

	update := registryRequest(t, fixture.router, http.MethodPut, "/api/agents/writer", fixture.ownerToken, map[string]any{
		"name": "Writer v2", "description": "updated",
	})
	updateEnvelope := requireRegistryResponse(t, update, http.StatusOK, middlewares.CodeOK)
	if !bytes.Contains(updateEnvelope.Data, []byte("Writer v2")) {
		t.Fatalf("updated metadata missing: %s", update.Body.String())
	}

	updateCapability := registryRequest(t, fixture.router, http.MethodPut, "/api/agents/writer/capabilities/write", fixture.ownerToken, map[string]any{
		"capability_code": "ignored", "name": "Write v2", "capability_type": "workflow",
		"workflow_id": workflow.ID, "version": "1", "status": "active",
	})
	updateCapabilityEnvelope := requireRegistryResponse(t, updateCapability, http.StatusOK, middlewares.CodeOK)
	if bytes.Contains(updateCapabilityEnvelope.Data, []byte(`"capability_code":"ignored"`)) || !bytes.Contains(updateCapabilityEnvelope.Data, []byte(`"capability_code":"write"`)) {
		t.Fatalf("capability path identity was not authoritative: %s", updateCapability.Body.String())
	}
	card = requestCard()
	if card.Code != http.StatusOK || !bytes.Contains(card.Body.Bytes(), []byte(`"name":"Write v2"`)) {
		t.Fatalf("Agent Card did not reflect Registry capability update: status=%d body=%s", card.Code, card.Body.String())
	}

	list := registryRequest(t, fixture.router, http.MethodGet, "/api/agents", fixture.ownerToken, nil)
	listEnvelope := requireRegistryResponse(t, list, http.StatusOK, middlewares.CodeOK)
	if !bytes.Contains(listEnvelope.Data, []byte(`"agent_code":"writer"`)) {
		t.Fatalf("agent list missing writer: %s", list.Body.String())
	}

	deactivate := registryRequest(t, fixture.router, http.MethodPost, "/api/agents/writer/deactivate", fixture.ownerToken, nil)
	requireRegistryResponse(t, deactivate, http.StatusOK, middlewares.CodeOK)

	updateEndpoint := registryRequest(t, fixture.router, http.MethodPut, "/api/agents/writer/endpoints/local", fixture.ownerToken, map[string]any{
		"endpoint_code": "ignored", "protocol": "a2a", "transport": "http",
		"address": "http://localhost:8081/a2a/agents/writer", "auth_type": "none",
	})
	updateEndpointEnvelope := requireRegistryResponse(t, updateEndpoint, http.StatusOK, middlewares.CodeOK)
	if bytes.Contains(updateEndpointEnvelope.Data, []byte(`"endpoint_code":"ignored"`)) || !bytes.Contains(updateEndpointEnvelope.Data, []byte(`"endpoint_code":"local"`)) {
		t.Fatalf("endpoint path identity was not authoritative: %s", updateEndpoint.Body.String())
	}
}

func TestAgentRegistryAPIAuthorizationAndStableErrors(t *testing.T) {
	fixture := setupRegistryHTTPFixture(t)

	invalid := registryRequest(t, fixture.router, http.MethodPost, "/api/agents", fixture.ownerToken, "{")
	requireRegistryResponse(t, invalid, http.StatusBadRequest, middlewares.CodeValidationFailed)

	createBody := map[string]any{"agent_code": "reviewer", "name": "Reviewer"}
	requireRegistryResponse(t, registryRequest(t, fixture.router, http.MethodPost, "/api/agents", fixture.ownerToken, createBody), http.StatusCreated, middlewares.CodeOK)
	requireRegistryResponse(t, registryRequest(t, fixture.router, http.MethodPost, "/api/agents", fixture.ownerToken, createBody), http.StatusConflict, middlewares.CodeAgentAlreadyExists)

	forbidden := registryRequest(t, fixture.router, http.MethodGet, "/api/agents/reviewer", fixture.otherToken, nil)
	requireRegistryResponse(t, forbidden, http.StatusForbidden, middlewares.CodeAuthForbidden)
	admin := registryRequest(t, fixture.router, http.MethodGet, "/api/agents/reviewer", fixture.adminToken, nil)
	requireRegistryResponse(t, admin, http.StatusOK, middlewares.CodeOK)
	missing := registryRequest(t, fixture.router, http.MethodGet, "/api/agents/missing", fixture.ownerToken, nil)
	requireRegistryResponse(t, missing, http.StatusNotFound, middlewares.CodeAgentNotFound)

	capabilityBody := map[string]any{"capability_code": "review", "name": "Review", "capability_type": "custom", "version": "1"}
	requireRegistryResponse(t, registryRequest(t, fixture.router, http.MethodPost, "/api/agents/reviewer/capabilities", fixture.ownerToken, capabilityBody), http.StatusCreated, middlewares.CodeOK)
	requireRegistryResponse(t, registryRequest(t, fixture.router, http.MethodPost, "/api/agents/reviewer/capabilities", fixture.ownerToken, capabilityBody), http.StatusConflict, middlewares.CodeCapabilityAlreadyExists)
	requireRegistryResponse(t, registryRequest(t, fixture.router, http.MethodPut, "/api/agents/reviewer/capabilities/missing", fixture.ownerToken, capabilityBody), http.StatusNotFound, middlewares.CodeCapabilityNotFound)

	endpointBody := map[string]any{
		"endpoint_code": "local", "protocol": "a2a", "transport": "http",
		"address": "http://127.0.0.1:8080/a2a/agents/reviewer", "auth_type": "none",
	}
	requireRegistryResponse(t, registryRequest(t, fixture.router, http.MethodPost, "/api/agents/reviewer/endpoints", fixture.ownerToken, endpointBody), http.StatusCreated, middlewares.CodeOK)
	requireRegistryResponse(t, registryRequest(t, fixture.router, http.MethodPost, "/api/agents/reviewer/endpoints", fixture.ownerToken, endpointBody), http.StatusConflict, middlewares.CodeEndpointAlreadyExists)
	requireRegistryResponse(t, registryRequest(t, fixture.router, http.MethodPut, "/api/agents/reviewer/endpoints/missing", fixture.ownerToken, endpointBody), http.StatusNotFound, middlewares.CodeEndpointNotFound)
	requireRegistryResponse(t, registryRequest(t, fixture.router, http.MethodPost, "/api/agents/reviewer/endpoints/missing/health-check", fixture.ownerToken, nil), http.StatusNotFound, middlewares.CodeEndpointNotFound)

	fixture.checker.setError(errors.New("agent card unavailable"))
	healthFailure := registryRequest(t, fixture.router, http.MethodPost, "/api/agents/reviewer/endpoints/local/health-check", fixture.ownerToken, nil)
	requireRegistryResponse(t, healthFailure, http.StatusBadGateway, middlewares.CodeEndpointHealthCheckFailed)

	var reviewer models.Agent
	if err := fixture.database.Where("agent_code = ?", "reviewer").First(&reviewer).Error; err != nil {
		t.Fatalf("load reviewer: %v", err)
	}
	const secretMarker = "0123456789abcdef0123456789abcdef"
	if err := fixture.database.Create(&models.AgentEndpoint{
		AgentID: reviewer.ID, EndpointCode: "secure", Protocol: models.AgentEndpointProtocolA2A,
		Transport: models.AgentEndpointTransportHTTPS, Address: "https://agents.example.com/a2a/agents/reviewer",
		AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "reviewer-key",
		ConfigJSON: `{"aws_secret_access_key":"` + secretMarker + `"}`, Status: models.AgentEndpointStatusInactive,
	}).Error; err != nil {
		t.Fatalf("create secure endpoint fixture: %v", err)
	}
	get := registryRequest(t, fixture.router, http.MethodGet, "/api/agents/reviewer", fixture.ownerToken, nil)
	requireRegistryResponse(t, get, http.StatusOK, middlewares.CodeOK)
	if strings.Contains(get.Body.String(), secretMarker) {
		t.Fatalf("registry response leaked credential secret: %s", get.Body.String())
	}
	if !strings.Contains(get.Body.String(), `"credential_ref":"reviewer-key"`) {
		t.Fatalf("registry response should expose only credential_ref: %s", get.Body.String())
	}
	if !strings.Contains(get.Body.String(), `"config_json":"{}"`) {
		t.Fatalf("registry response should redact unsafe legacy config: %s", get.Body.String())
	}
}
