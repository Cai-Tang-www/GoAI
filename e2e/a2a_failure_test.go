package e2e_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"GoAI/a2aclient"
	"GoAI/a2agateway"
	"GoAI/a2aprotocol"
	"GoAI/models"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"gorm.io/gorm"
)

type a2aTargetFixture struct {
	database *gorm.DB
	agent    models.Agent
	gateway  *a2agateway.Gateway
}

func TestA2AHTTPFailureMatrix(t *testing.T) {
	t.Run("target agent does not exist", func(t *testing.T) {
		fixture := newA2ATargetFixture(t, `{"entry_node":"write","nodes":[{"key":"write","type":"noop"}],"edges":[]}`)
		server := httptest.NewServer(fixture.gateway)
		t.Cleanup(server.Close)

		client := newA2AClient(t, server.Client(), time.Second)
		request := a2aInvocationRequest(server.URL+"/a2a/agents/missing", models.AgentEndpointTransportHTTP)
		request.TargetAgentCode = "missing"
		_, err := client.Invoke(context.Background(), request)
		if err == nil || !strings.Contains(err.Error(), "discovering target agent card") {
			t.Fatalf("expected missing target discovery error, got %v", err)
		}
		assertRetryableInvocationError(t, err, true)
	})

	t.Run("target capability is not supported", func(t *testing.T) {
		fixture := newA2ATargetFixture(t, `{"entry_node":"write","nodes":[{"key":"write","type":"noop"}],"edges":[]}`)
		server := httptest.NewServer(fixture.gateway)
		t.Cleanup(server.Close)
		seedProtocolEndpoint(t, fixture.database, fixture.agent, server.URL+"/a2a/agents/writer", models.AgentEndpointTransportHTTP)

		client := newA2AClient(t, server.Client(), time.Second)
		request := a2aInvocationRequest(server.URL+"/a2a/agents/writer", models.AgentEndpointTransportHTTP)
		request.CapabilityCode = "translate"
		_, err := client.Invoke(context.Background(), request)
		if err == nil || !strings.Contains(err.Error(), `does not expose capability "translate"`) {
			t.Fatalf("expected unsupported capability error, got %v", err)
		}
		assertRetryableInvocationError(t, err, false)
		assertRowCount(t, fixture.database, &models.Run{}, 0)
		assertRowCount(t, fixture.database, &models.Delegation{}, 0)
	})

	t.Run("child run fails output contract", func(t *testing.T) {
		fixture := newA2ATargetFixture(t, `{"entry_node":"write","nodes":[{"key":"write","type":"noop"}],"edges":[]}`)
		if err := fixture.database.Model(&models.AgentCapability{}).
			Where("agent_id = ? AND capability_code = ?", fixture.agent.ID, "write").
			Update("output_schema_json", `{"type":"object","required":["approved"],"properties":{"approved":{"type":"boolean"}}}`).Error; err != nil {
			t.Fatalf("configure output contract: %v", err)
		}
		server := httptest.NewServer(fixture.gateway)
		t.Cleanup(server.Close)
		seedProtocolEndpoint(t, fixture.database, fixture.agent, server.URL+"/a2a/agents/writer", models.AgentEndpointTransportHTTP)

		client := newA2AClient(t, server.Client(), time.Second)
		request := a2aInvocationRequest(server.URL+"/a2a/agents/writer", models.AgentEndpointTransportHTTP)
		_, err := client.Invoke(context.Background(), request)
		if err == nil || !strings.Contains(err.Error(), "target agent execution failed") {
			t.Fatalf("expected failed child task, got %v", err)
		}
		assertRetryableInvocationError(t, err, false)

		var run models.Run
		if err := fixture.database.Where("run_id = ?", request.TaskID).First(&run).Error; err != nil {
			t.Fatalf("load failed child run: %v", err)
		}
		if run.Status != models.RunStatusFailed || !strings.Contains(run.ErrorMessage, "child output violates capability write contract") {
			t.Fatalf("child run did not preserve output contract failure: %+v", run)
		}
		var delegation models.Delegation
		if err := fixture.database.Where("child_run_id = ?", request.TaskID).First(&delegation).Error; err != nil {
			t.Fatalf("load failed delegation: %v", err)
		}
		if delegation.Status != models.DelegationStatusFailed {
			t.Fatalf("delegation status=%s, want %s", delegation.Status, models.DelegationStatusFailed)
		}
		var steps []models.RunStep
		if err := fixture.database.Where("run_id = ?", request.TaskID).Find(&steps).Error; err != nil {
			t.Fatalf("load child steps: %v", err)
		}
		if len(steps) != 1 || steps[0].Status != models.RunStepStatusSuccess {
			t.Fatalf("Eino node should complete before output contract failure: %+v", steps)
		}
	})

	t.Run("child run fails malformed output contract", func(t *testing.T) {
		fixture := newA2ATargetFixture(t, `{"entry_node":"write","nodes":[{"key":"write","type":"noop"}],"edges":[]}`)
		if err := fixture.database.Model(&models.AgentCapability{}).
			Where("agent_id = ? AND capability_code = ?", fixture.agent.ID, "write").
			Update("output_schema_json", `{"type":"object","required":"approved"}`).Error; err != nil {
			t.Fatalf("configure malformed output contract: %v", err)
		}
		server := httptest.NewServer(fixture.gateway)
		t.Cleanup(server.Close)
		seedProtocolEndpoint(t, fixture.database, fixture.agent, server.URL+"/a2a/agents/writer", models.AgentEndpointTransportHTTP)

		client := newA2AClient(t, server.Client(), time.Second)
		request := a2aInvocationRequest(server.URL+"/a2a/agents/writer", models.AgentEndpointTransportHTTP)
		_, err := client.Invoke(context.Background(), request)
		if err == nil || !strings.Contains(err.Error(), "target agent execution failed") {
			t.Fatalf("expected failed child task, got %v", err)
		}
		assertRetryableInvocationError(t, err, false)

		var run models.Run
		if err := fixture.database.Where("run_id = ?", request.TaskID).First(&run).Error; err != nil {
			t.Fatalf("load failed child run: %v", err)
		}
		if run.Status != models.RunStatusFailed || !strings.Contains(run.ErrorMessage, "schema required at $ must be an array") {
			t.Fatalf("child run did not preserve malformed output contract failure: %+v", run)
		}
		var delegation models.Delegation
		if err := fixture.database.Where("child_run_id = ?", request.TaskID).First(&delegation).Error; err != nil {
			t.Fatalf("load failed delegation: %v", err)
		}
		if delegation.Status != models.DelegationStatusFailed {
			t.Fatalf("delegation status=%s, want %s", delegation.Status, models.DelegationStatusFailed)
		}
		var steps []models.RunStep
		if err := fixture.database.Where("run_id = ?", request.TaskID).Find(&steps).Error; err != nil {
			t.Fatalf("load child steps: %v", err)
		}
		if len(steps) != 1 || steps[0].Status != models.RunStepStatusSuccess {
			t.Fatalf("Eino node should complete before malformed output contract failure: %+v", steps)
		}
	})
	t.Run("A2A HTTP request times out", func(t *testing.T) {
		fixture := newA2ATargetFixture(t, `{"entry_node":"write","nodes":[{"key":"write","type":"noop"}],"edges":[]}`)
		delayed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/message:send") {
				select {
				case <-r.Context().Done():
					return
				case <-time.After(200 * time.Millisecond):
				}
			}
			fixture.gateway.ServeHTTP(w, r)
		})
		server := httptest.NewServer(delayed)
		t.Cleanup(server.Close)
		seedProtocolEndpoint(t, fixture.database, fixture.agent, server.URL+"/a2a/agents/writer", models.AgentEndpointTransportHTTP)

		client := newA2AClient(t, server.Client(), 50*time.Millisecond)
		request := a2aInvocationRequest(server.URL+"/a2a/agents/writer", models.AgentEndpointTransportHTTP)
		startedAt := time.Now()
		_, err := client.Invoke(context.Background(), request)
		if err == nil || !strings.Contains(err.Error(), "sending A2A message") {
			t.Fatalf("expected A2A send timeout, got %v", err)
		}
		if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
			t.Fatalf("A2A timeout took too long: %s", elapsed)
		}
		assertRetryableInvocationError(t, err, true)
		assertRowCount(t, fixture.database, &models.Run{}, 0)
	})

	t.Run("duplicate A2A request reuses child run", func(t *testing.T) {
		fixture := newA2ATargetFixture(t, `{"entry_node":"write","nodes":[{"key":"write","type":"noop"}],"edges":[]}`)
		server := httptest.NewServer(fixture.gateway)
		t.Cleanup(server.Close)
		seedProtocolEndpoint(t, fixture.database, fixture.agent, server.URL+"/a2a/agents/writer", models.AgentEndpointTransportHTTP)

		client := newA2AClient(t, server.Client(), time.Second)
		request := a2aInvocationRequest(server.URL+"/a2a/agents/writer", models.AgentEndpointTransportHTTP)
		first, err := client.Invoke(context.Background(), request)
		if err != nil {
			t.Fatalf("first A2A invocation failed: %v", err)
		}
		second, err := client.Invoke(context.Background(), request)
		if err != nil {
			t.Fatalf("duplicate A2A invocation failed: %v", err)
		}
		if first.TaskID != request.TaskID || second.TaskID != request.TaskID || first.OutputJSON != second.OutputJSON {
			t.Fatalf("duplicate invocation did not reuse result: first=%+v second=%+v", first, second)
		}

		assertRowCount(t, fixture.database, &models.Run{}, 1)
		assertRowCount(t, fixture.database, &models.Delegation{}, 1)
		assertRowCount(t, fixture.database, &models.RunStep{}, 1)
		assertRowCount(t, fixture.database, &models.Message{}, 2)
	})
}

func TestA2AHTTPSGatewayPublishesContractsAndUsesSameProtocolSemantics(t *testing.T) {
	fixture := newA2ATargetFixture(t, `{"entry_node":"write","nodes":[{"key":"write","type":"noop"}],"edges":[]}`)
	inputSchema := `{"type":"object","required":["prompt"],"properties":{"prompt":{"type":"string"}},"additionalProperties":false}`
	outputSchema := `{"type":"object","required":["result"],"properties":{"result":{"type":"string"}}}`
	if err := fixture.database.Model(&models.AgentCapability{}).
		Where("agent_id = ? AND capability_code = ?", fixture.agent.ID, "write").
		Updates(map[string]any{"input_schema_json": inputSchema, "output_schema_json": outputSchema}).Error; err != nil {
		t.Fatalf("configure capability contracts: %v", err)
	}

	server := httptest.NewTLSServer(fixture.gateway)
	t.Cleanup(server.Close)
	seedProtocolEndpoint(t, fixture.database, fixture.agent, server.URL+"/a2a/agents/writer", models.AgentEndpointTransportHTTPS)

	response, err := server.Client().Get(server.URL + "/a2a/agents/writer/.well-known/agent-card.json")
	if err != nil {
		t.Fatalf("load HTTPS agent card: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("HTTPS agent card status=%d", response.StatusCode)
	}
	var card a2a.AgentCard
	if err := json.NewDecoder(response.Body).Decode(&card); err != nil {
		t.Fatalf("decode HTTPS agent card: %v", err)
	}
	assertAgentCardContract(t, &card, "write")

	client := newA2AClient(t, server.Client(), time.Second)
	request := a2aInvocationRequest(server.URL+"/a2a/agents/writer", models.AgentEndpointTransportHTTPS)
	result, err := client.Invoke(context.Background(), request)
	if err != nil {
		t.Fatalf("invoke HTTPS A2A gateway: %v", err)
	}
	if result.TaskID != request.TaskID || result.State != services.AgentInvocationStateCompleted || !strings.Contains(result.OutputJSON, `"result":"ok"`) {
		t.Fatalf("unexpected HTTPS A2A result: %+v", result)
	}
}

func newA2ATargetFixture(t *testing.T, definition string) *a2aTargetFixture {
	t.Helper()
	database := openE2EDB(t, "target")
	migrateE2ADB(t, database)
	seedAgent(t, database, "planner", "Planner Agent", 11, `{"entry_node":"plan","nodes":[{"key":"plan","type":"noop"}],"edges":[]}`)
	target, workflow := seedAgent(t, database, "writer", "Writer Agent", 22, definition)
	seedCapability(t, database, target, "write", workflow)

	var runService *services.RunService
	publisher := syncRunPublisher{execute: func(ctx context.Context, runID string) error {
		if runService == nil {
			return errors.New("target run service is not initialized")
		}
		// Kafka dispatch success is independent from the Child Run terminal state.
		_ = runService.HandleRunExecute(ctx, runID)
		return nil
	}}
	var err error
	runService, err = services.NewRunService(database, publisher)
	if err != nil {
		t.Fatalf("create target run service: %v", err)
	}
	runtime, err := services.NewRuntimeService(database, runService)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	gateway, err := a2agateway.New(runtime)
	if err != nil {
		t.Fatalf("create target A2A gateway: %v", err)
	}
	return &a2aTargetFixture{database: database, agent: target, gateway: gateway}
}

func newA2AClient(t *testing.T, httpClient *http.Client, timeout time.Duration) *a2aclient.Client {
	t.Helper()
	client, err := a2aclient.New(httpClient, timeout, a2aclient.WithCallbackBaseURL("http://127.0.0.1"))
	if err != nil {
		t.Fatalf("create A2A client: %v", err)
	}
	return client
}

func a2aInvocationRequest(address, transport string) services.AgentInvocationRequest {
	return services.AgentInvocationRequest{
		SourceAgentCode: "planner",
		TargetAgentCode: "writer",
		CapabilityCode:  "write",
		ParentRunID:     "run-parent-http",
		TraceID:         "trace-http-e2e",
		DelegationID:    "delegation-http-e2e",
		ThreadID:        "thread-http-e2e",
		TaskID:          "run-child-http-e2e",
		MessageID:       "message-http-e2e",
		InputJSON:       `{"prompt":"draft a release note"}`,
		Endpoints: []services.AgentInvocationEndpoint{{
			Address:   address,
			Transport: transport,
		}},
	}
}

func assertRetryableInvocationError(t *testing.T, err error, want bool) {
	t.Helper()
	var retryable interface{ Retryable() bool }
	if !errors.As(err, &retryable) {
		t.Fatalf("A2A error does not expose retryability: %T %v", err, err)
	}
	if retryable.Retryable() != want {
		t.Fatalf("retryable=%v, want %v: %v", retryable.Retryable(), want, err)
	}
}

func assertRowCount(t *testing.T, database *gorm.DB, model any, want int64) {
	t.Helper()
	var count int64
	if err := database.Model(model).Count(&count).Error; err != nil {
		t.Fatalf("count %T rows: %v", model, err)
	}
	if count != want {
		t.Fatalf("%T row count=%d, want %d", model, count, want)
	}
}

func assertAgentCardContract(t *testing.T, card *a2a.AgentCard, capabilityCode string) {
	t.Helper()
	if card == nil {
		t.Fatal("agent card is nil")
	}
	var extension *a2a.AgentExtension
	for index := range card.Capabilities.Extensions {
		candidate := &card.Capabilities.Extensions[index]
		if candidate.URI == a2aprotocol.DelegationExtensionURI {
			extension = candidate
			break
		}
	}
	if extension == nil || !extension.Required {
		t.Fatalf("delegation extension missing or optional: %+v", card.Capabilities.Extensions)
	}
	capabilities, ok := extension.Params["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("capability contracts missing from extension params: %#v", extension.Params)
	}
	contract, ok := capabilities[capabilityCode].(map[string]any)
	if !ok {
		t.Fatalf("capability %s contract missing: %#v", capabilityCode, capabilities)
	}
	if contract["version"] != "1" {
		t.Fatalf("capability version=%v, want 1", contract["version"])
	}
	if _, ok := contract["inputSchema"].(map[string]any); !ok {
		t.Fatalf("input schema missing from contract: %#v", contract)
	}
	if _, ok := contract["outputSchema"].(map[string]any); !ok {
		t.Fatalf("output schema missing from contract: %#v", contract)
	}
}

func seedProtocolEndpoint(t *testing.T, database *gorm.DB, agent models.Agent, address, transport string) {
	t.Helper()
	endpoint := models.AgentEndpoint{
		AgentID:      agent.ID,
		EndpointCode: "primary",
		Protocol:     models.AgentEndpointProtocolA2A,
		Transport:    transport,
		Address:      address,
		Status:       models.AgentEndpointStatusActive,
	}
	if err := database.Create(&endpoint).Error; err != nil {
		t.Fatalf("create %s endpoint for %s: %v", transport, agent.AgentCode, err)
	}
}
