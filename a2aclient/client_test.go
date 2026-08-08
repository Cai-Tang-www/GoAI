package a2aclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"GoAI/a2aauth"
	"GoAI/a2aprotocol"
	"GoAI/observability"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

func TestClientInvokeReturnsAcceptedTaskWithoutPolling(t *testing.T) {
	var sendCount atomic.Int32
	var pollCount atomic.Int32
	var transportCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/a2a/agents/writer/.well-known/agent-card.json":
			card := testCard(serverBaseURL(r), "write")
			_ = json.NewEncoder(w).Encode(card)
		case "/a2a/agents/writer/message:send":
			sendCount.Add(1)
			var request a2a.SendMessageRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode send request: %v", err)
			}
			if request.Message == nil || request.Message.Role != a2a.MessageRoleUser {
				t.Fatalf("unexpected A2A message: %+v", request.Message)
			}
			metadata, ok := request.Message.Metadata[a2aprotocol.DelegationExtensionURI].(map[string]any)
			if !ok || metadata["traceId"] != "trace-parent" ||
				metadata["delegationId"] != "dlg-parent" ||
				metadata["parentRunId"] != "run-parent" ||
				metadata["delegationGroupId"] != "group-parent" ||
				metadata["groupMemberKey"] != "security" {
				t.Fatalf("A2A delegation metadata not propagated: %#v", request.Message.Metadata)
			}
			if request.Config == nil || !request.Config.ReturnImmediately || request.Config.PushConfig == nil {
				t.Fatalf("A2A asynchronous configuration missing: %+v", request.Config)
			}
			push := request.Config.PushConfig
			if string(push.TaskID) != "a2a_task_123" || push.ID != "dlg-parent" || push.URL != "http://127.0.0.1/a2a/agents/planner/callbacks/tasks/a2a_task_123" {
				t.Fatalf("unexpected A2A push config: %+v", push)
			}
			response := &a2a.StreamResponse{Event: &a2a.Task{
				ID:        request.Message.TaskID,
				ContextID: request.Message.ContextID,
				Status:    a2a.TaskStatus{State: a2a.TaskStateWorking},
			}}
			_ = json.NewEncoder(w).Encode(response)
		case "/a2a/agents/writer/tasks/a2a_task_123":
			pollCount.Add(1)
			_ = json.NewEncoder(w).Encode(&a2a.Task{
				ID:        "a2a_task_123",
				ContextID: "thread-1",
				Status:    a2a.TaskStatus{State: a2a.TaskStateCompleted},
				Artifacts: []*a2a.Artifact{{Parts: a2a.ContentParts{a2a.NewDataPart(map[string]any{"answer": "ok"})}}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	serverClient := server.Client()
	injectedClient := &http.Client{
		Transport: countingRoundTripper{
			base:  serverClient.Transport,
			calls: &transportCount,
		},
	}
	client, err := New(injectedClient, time.Second, WithCallbackBaseURL("http://127.0.0.1"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	result, err := client.Invoke(context.Background(), services.AgentInvocationRequest{
		SourceAgentCode:   "planner",
		TargetAgentCode:   "writer",
		CapabilityCode:    "write",
		ParentRunID:       "run-parent",
		TraceID:           "trace-parent",
		DelegationID:      "dlg-parent",
		ThreadID:          "thread-1",
		TaskID:            "a2a_task_123",
		MessageID:         "a2a_message_123",
		DelegationGroupID: "group-parent",
		GroupMemberKey:    "security",
		InputJSON:         `{"prompt":"write"}`,
		Endpoints:         []services.AgentInvocationEndpoint{{Address: server.URL + "/a2a/agents/writer", Transport: "http"}},
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if result.State != services.AgentInvocationStateAccepted || result.OutputJSON != "{}" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if sendCount.Load() != 1 || pollCount.Load() != 0 {
		t.Fatalf("unexpected request counts: send=%d poll=%d", sendCount.Load(), pollCount.Load())
	}
	if transportCount.Load() != 2 {
		t.Fatalf("injected transport calls = %d, want 2", transportCount.Load())
	}
}

func TestClientCancelTaskUsesA2ACancelEndpoint(t *testing.T) {
	var cancelCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/a2a/agents/writer/.well-known/agent-card.json":
			_ = json.NewEncoder(w).Encode(testCard(serverBaseURL(r), "write"))
		case "/a2a/agents/writer/tasks/task-to-cancel:cancel":
			cancelCount.Add(1)
			if r.Body != http.NoBody {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Fatalf("read cancel request: %v", err)
				}
				if len(body) != 0 {
					t.Fatalf("cancel request unexpectedly contained a body: %s", body)
				}
			}
			_ = json.NewEncoder(w).Encode(&a2a.Task{
				ID: "task-to-cancel", ContextID: "thread-cancel",
				Status: a2a.TaskStatus{State: a2a.TaskStateCanceled},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.Client(), time.Second, WithCallbackBaseURL("http://127.0.0.1"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if err := client.CancelTask(context.Background(), services.AgentTaskCancellationRequest{
		SourceAgentCode: "planner", TargetAgentCode: "writer", TaskID: "task-to-cancel",
		Endpoints: []services.AgentInvocationEndpoint{{Address: server.URL + "/a2a/agents/writer", Transport: "http"}},
	}); err != nil {
		t.Fatalf("cancel task failed: %v", err)
	}
	if cancelCount.Load() != 1 {
		t.Fatalf("cancel endpoint calls=%d want 1", cancelCount.Load())
	}
}

type countingRoundTripper struct {
	base  http.RoundTripper
	calls *atomic.Int32
}

func (t countingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return t.base.RoundTrip(req)
}

func TestClientRejectsRemoteHTTPEndpoint(t *testing.T) {
	client, err := New(nil, time.Second, WithCallbackBaseURL("http://127.0.0.1"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.Invoke(context.Background(), services.AgentInvocationRequest{
		SourceAgentCode: "planner",
		TargetAgentCode: "writer",
		CapabilityCode:  "write",
		ParentRunID:     "run-parent",
		TaskID:          "task",
		MessageID:       "message",
		InputJSON:       `{}`,
		Endpoints:       []services.AgentInvocationEndpoint{{Address: "http://example.com/a2a", Transport: "http"}},
	})
	if err == nil || !contains(err.Error(), "remote A2A endpoint must use HTTPS") {
		t.Fatalf("expected remote HTTP rejection, got %v", err)
	}
}

func TestClientInvokesStandardAgentWithoutGoAIDelegationExtension(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/a2a/agents/writer/.well-known/agent-card.json":
			card := testCard(serverBaseURL(r), "write")
			card.Capabilities.Extensions = nil
			_ = json.NewEncoder(w).Encode(card)
		case "/a2a/agents/writer/message:send":
			var request a2a.SendMessageRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode standard A2A request: %v", err)
			}
			if len(request.Message.Extensions) != 0 || len(request.Message.Metadata) != 0 {
				t.Fatalf("standard peer received GoAI extension data: %+v", request.Message)
			}
			_ = json.NewEncoder(w).Encode(&a2a.StreamResponse{Event: &a2a.Task{
				ID: request.Message.TaskID, ContextID: request.Message.ContextID,
				Status: a2a.TaskStatus{State: a2a.TaskStateWorking},
			}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, _ := New(server.Client(), time.Second, WithCallbackBaseURL("http://127.0.0.1"))
	result, err := client.Invoke(context.Background(), services.AgentInvocationRequest{
		SourceAgentCode: "planner", TargetAgentCode: "writer", CapabilityCode: "write", ParentRunID: "run-parent",
		TaskID: "task", MessageID: "message", InputJSON: `{}`,
		Endpoints: []services.AgentInvocationEndpoint{{Address: server.URL + "/a2a/agents/writer", Transport: "http"}},
	})
	if err != nil {
		t.Fatalf("standard A2A invocation failed: %v", err)
	}
	if result.State != services.AgentInvocationStateAccepted {
		t.Fatalf("unexpected standard A2A result: %+v", result)
	}
}

func TestClientAuthenticationSignsBusinessRequestsWithFreshNonces(t *testing.T) {
	const secret = "test-only-a2a-secret-at-least-32-bytes-long"
	resolver, err := a2aauth.NewStaticCredentialResolver(map[string]string{"planner-key": secret})
	if err != nil {
		t.Fatalf("create credential resolver: %v", err)
	}
	verifier, err := a2aauth.NewVerifier(resolver, a2aauth.NewMemoryNonceStore(), time.Minute)
	if err != nil {
		t.Fatalf("create verifier: %v", err)
	}

	var mu sync.Mutex
	var discoveryHeaders http.Header
	var businessNonces []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/a2a/agents/writer/.well-known/agent-card.json":
			mu.Lock()
			discoveryHeaders = r.Header.Clone()
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(testSecuredCard(serverBaseURL(r), "write"))
		case "/a2a/agents/writer/message:send":
			if agent, verifyErr := verifier.Verify(r, "planner-key"); verifyErr != nil || agent != "planner" {
				http.Error(w, "invalid authentication", http.StatusUnauthorized)
				return
			}
			mu.Lock()
			businessNonces = append(businessNonces, r.Header.Get(a2aauth.HeaderNonce))
			mu.Unlock()
			var request a2a.SendMessageRequest
			if decodeErr := json.NewDecoder(r.Body).Decode(&request); decodeErr != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(&a2a.StreamResponse{Event: &a2a.Task{
				ID: request.Message.TaskID, ContextID: request.Message.ContextID,
				Status: a2a.TaskStatus{State: a2a.TaskStateWorking},
			}})
		case "/a2a/agents/writer/tasks/secured-task":
			if agent, verifyErr := verifier.Verify(r, "planner-key"); verifyErr != nil || agent != "planner" {
				http.Error(w, "invalid authentication", http.StatusUnauthorized)
				return
			}
			mu.Lock()
			businessNonces = append(businessNonces, r.Header.Get(a2aauth.HeaderNonce))
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(&a2a.Task{
				ID: "secured-task", ContextID: "thread-secured",
				Status:    a2a.TaskStatus{State: a2a.TaskStateCompleted},
				Artifacts: []*a2a.Artifact{{Parts: a2a.ContentParts{a2a.NewDataPart(map[string]any{"answer": "signed"})}}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := New(server.Client(), time.Second, WithCallbackBaseURL("http://127.0.0.1"), WithAuthentication(resolver, true))
	if err != nil {
		t.Fatalf("new authenticated client: %v", err)
	}
	result, err := client.Invoke(context.Background(), services.AgentInvocationRequest{
		SourceAgentCode: "planner", SourceAuthType: a2aauth.AuthTypeHMACSHA256, SourceCredentialRef: "planner-key",
		TargetAgentCode: "writer", CapabilityCode: "write", ParentRunID: "parent-secured",
		ThreadID: "thread-secured", TaskID: "secured-task", MessageID: "message-secured", InputJSON: `{}`,
		Endpoints: []services.AgentInvocationEndpoint{{Address: server.URL + "/a2a/agents/writer", Transport: "http"}},
	})
	if err != nil {
		t.Fatalf("invoke authenticated endpoint: %v", err)
	}
	if result.State != services.AgentInvocationStateAccepted || result.OutputJSON != "{}" {
		t.Fatalf("unexpected authenticated result: %+v", result)
	}

	mu.Lock()
	defer mu.Unlock()
	if discoveryHeaders.Get("Authorization") != "" || discoveryHeaders.Get(a2aauth.HeaderAgentCode) != "" {
		t.Fatalf("public discovery carried authentication headers: %v", discoveryHeaders)
	}
	if len(businessNonces) != 1 || businessNonces[0] == "" {
		t.Fatalf("business request must be signed with a nonce: %v", businessNonces)
	}
}

func TestClientAuthenticationRejectsMissingSourceConfigAndUnsecuredCard(t *testing.T) {
	const secret = "test-only-a2a-secret-at-least-32-bytes-long"
	resolver, err := a2aauth.NewStaticCredentialResolver(map[string]string{"planner-key": secret})
	if err != nil {
		t.Fatalf("create credential resolver: %v", err)
	}
	tests := []struct {
		name          string
		secureCard    bool
		request       services.AgentInvocationRequest
		wantErrorText string
	}{
		{
			name: "missing source authentication", secureCard: true,
			request:       services.AgentInvocationRequest{SourceAgentCode: "planner"},
			wantErrorText: "source agent A2A authentication is not configured",
		},
		{
			name: "target card omits authentication", secureCard: false,
			request:       services.AgentInvocationRequest{SourceAgentCode: "planner", SourceAuthType: a2aauth.AuthTypeHMACSHA256, SourceCredentialRef: "planner-key"},
			wantErrorText: "target agent card does not declare GoAI HMAC authentication",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var businessCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/a2a/agents/writer/.well-known/agent-card.json" {
					card := testCard(serverBaseURL(r), "write")
					if test.secureCard {
						card = testSecuredCard(serverBaseURL(r), "write")
					}
					_ = json.NewEncoder(w).Encode(card)
					return
				}
				businessCalls.Add(1)
				http.Error(w, "unexpected business request", http.StatusInternalServerError)
			}))
			defer server.Close()

			client, newErr := New(server.Client(), time.Second, WithCallbackBaseURL("http://127.0.0.1"), WithAuthentication(resolver, true))
			if newErr != nil {
				t.Fatalf("new authenticated client: %v", newErr)
			}
			test.request.TargetAgentCode = "writer"
			test.request.CapabilityCode = "write"
			test.request.ParentRunID = "parent"
			test.request.TaskID = "task"
			test.request.MessageID = "message"
			test.request.InputJSON = `{}`
			test.request.Endpoints = []services.AgentInvocationEndpoint{{Address: server.URL + "/a2a/agents/writer", Transport: "http"}}
			if _, invokeErr := client.Invoke(context.Background(), test.request); invokeErr == nil || !contains(invokeErr.Error(), test.wantErrorText) {
				t.Fatalf("got %v, want error containing %q", invokeErr, test.wantErrorText)
			}
			if businessCalls.Load() != 0 {
				t.Fatalf("invalid authentication configuration reached business endpoint: %d", businessCalls.Load())
			}
		})
	}
}
func testCard(baseURL, capability string) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name: "writer", Description: "writer agent", Version: "1.0",
		SupportedInterfaces: []*a2a.AgentInterface{a2a.NewAgentInterface(baseURL, a2a.TransportProtocolHTTPJSON)},
		Capabilities:        a2a.AgentCapabilities{Extensions: []a2a.AgentExtension{{URI: a2aprotocol.DelegationExtensionURI, Params: map[string]any{"agentCode": "writer"}}}},
		Skills:              []a2a.AgentSkill{{ID: capability, Name: capability}},
		DefaultInputModes:   []string{"application/json"}, DefaultOutputModes: []string{"application/json"},
	}
}

func testSecuredCard(baseURL, capability string) *a2a.AgentCard {
	card := testCard(baseURL, capability)
	const schemeName a2a.SecuritySchemeName = "goaiHMACSHA256"
	card.SecuritySchemes = a2a.NamedSecuritySchemes{
		schemeName: a2a.HTTPAuthSecurityScheme{Scheme: a2aauth.AuthorizationScheme},
	}
	card.SecurityRequirements = a2a.SecurityRequirementsOptions{
		a2a.SecurityRequirements{schemeName: a2a.SecuritySchemeScopes{}},
	}
	return card
}
func serverBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + "/a2a/agents/writer"
}

func contains(value, target string) bool {
	for index := 0; index+len(target) <= len(value); index++ {
		if value[index:index+len(target)] == target {
			return true
		}
	}
	return false
}

func TestClientRejectsTaskIDMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/a2a/agents/writer/.well-known/agent-card.json":
			_ = json.NewEncoder(w).Encode(testCard(serverBaseURL(r), "write"))
		case "/a2a/agents/writer/message:send":
			_ = json.NewEncoder(w).Encode(&a2a.StreamResponse{Event: &a2a.Task{ID: "different-task", Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.Client(), time.Second, WithCallbackBaseURL("http://127.0.0.1"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.Invoke(context.Background(), services.AgentInvocationRequest{
		SourceAgentCode: "planner", TargetAgentCode: "writer", CapabilityCode: "write", ParentRunID: "parent",
		TaskID: "expected-task", MessageID: "message", InputJSON: `{}`, Endpoints: []services.AgentInvocationEndpoint{{Address: server.URL + "/a2a/agents/writer", Transport: "http"}},
	})
	if err == nil || !contains(err.Error(), "task id mismatch") {
		t.Fatalf("expected task mismatch error, got %v", err)
	}
}

func TestClientReturnsNonRetryableFailedTask(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/a2a/agents/writer/.well-known/agent-card.json":
			_ = json.NewEncoder(w).Encode(testCard(serverBaseURL(r), "write"))
		case "/a2a/agents/writer/message:send":
			failed := &a2a.Task{ID: "failed-task", Status: a2a.TaskStatus{State: a2a.TaskStateFailed, Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("target rejected"))}}
			_ = json.NewEncoder(w).Encode(&a2a.StreamResponse{Event: failed})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.Client(), time.Second, WithCallbackBaseURL("http://127.0.0.1"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.Invoke(context.Background(), services.AgentInvocationRequest{
		SourceAgentCode: "planner", TargetAgentCode: "writer", CapabilityCode: "write", ParentRunID: "parent",
		TaskID: "failed-task", MessageID: "message", InputJSON: `{}`, Endpoints: []services.AgentInvocationEndpoint{{Address: server.URL + "/a2a/agents/writer", Transport: "http"}},
	})
	if err == nil || !contains(err.Error(), "target rejected") {
		t.Fatalf("expected failed task error, got %v", err)
	}
	if isRetryable(err) {
		t.Fatalf("failed task should be non-retryable: %v", err)
	}
}

func TestClientReturnsAcceptedTaskBeforeCallerContextEnds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/a2a/agents/writer/.well-known/agent-card.json":
			_ = json.NewEncoder(w).Encode(testCard(serverBaseURL(r), "write"))
		case "/a2a/agents/writer/message:send":
			_ = json.NewEncoder(w).Encode(&a2a.StreamResponse{Event: &a2a.Task{ID: "working-task", Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.Client(), time.Second, WithCallbackBaseURL("http://127.0.0.1"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := client.Invoke(ctx, services.AgentInvocationRequest{
		SourceAgentCode: "planner", TargetAgentCode: "writer", CapabilityCode: "write", ParentRunID: "parent",
		TaskID: "working-task", MessageID: "message", InputJSON: `{}`, Endpoints: []services.AgentInvocationEndpoint{{Address: server.URL + "/a2a/agents/writer", Transport: "http"}},
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if result.State != services.AgentInvocationStateAccepted {
		t.Fatalf("state=%s, want accepted", result.State)
	}
}

func TestClientRejectsUnsafeRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/a2a/agents/writer/.well-known/agent-card.json" {
			http.Redirect(w, r, "http://example.com/agent-card.json", http.StatusFound)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client, err := New(server.Client(), time.Second, WithCallbackBaseURL("http://127.0.0.1"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	_, err = client.Invoke(context.Background(), services.AgentInvocationRequest{
		SourceAgentCode: "planner", TargetAgentCode: "writer", CapabilityCode: "write", ParentRunID: "parent",
		TaskID: "task", MessageID: "message", InputJSON: `{}`, Endpoints: []services.AgentInvocationEndpoint{{Address: server.URL + "/a2a/agents/writer", Transport: "http"}},
	})
	if err == nil || !contains(err.Error(), "rejecting unsafe A2A redirect") {
		t.Fatalf("expected unsafe redirect rejection, got %v", err)
	}
}

func TestClientSupportsHTTPSAgentEndpoint(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/a2a/agents/writer/.well-known/agent-card.json":
			_ = json.NewEncoder(w).Encode(testCard(serverBaseURL(r), "write"))
		case "/a2a/agents/writer/message:send":
			var request a2a.SendMessageRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode send request: %v", err)
			}
			_ = json.NewEncoder(w).Encode(&a2a.StreamResponse{Event: &a2a.Message{
				ID: request.Message.ID, Role: a2a.MessageRoleAgent,
				Parts: a2a.ContentParts{a2a.NewDataPart(map[string]any{"answer": "https-ok"})},
			}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.Client(), time.Second, WithCallbackBaseURL("http://127.0.0.1"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	result, err := client.Invoke(context.Background(), services.AgentInvocationRequest{
		SourceAgentCode: "planner", TargetAgentCode: "writer", CapabilityCode: "write", ParentRunID: "parent",
		TaskID: "https-task", MessageID: "https-message", InputJSON: `{}`, Endpoints: []services.AgentInvocationEndpoint{{Address: server.URL + "/a2a/agents/writer", Transport: "https"}},
	})
	if err != nil {
		t.Fatalf("invoke HTTPS endpoint: %v", err)
	}
	if result.OutputJSON != `{"answer":"https-ok"}` {
		t.Fatalf("unexpected HTTPS result: %+v", result)
	}
}

func TestClientFallsBackToNextEndpointAndRecordsOverallSuccess(t *testing.T) {
	failingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer failingServer.Close()

	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/a2a/agents/writer/.well-known/agent-card.json":
			_ = json.NewEncoder(w).Encode(testCard(serverBaseURL(r), "write"))
		case "/a2a/agents/writer/message:send":
			var request a2a.SendMessageRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode send request: %v", err)
			}
			_ = json.NewEncoder(w).Encode(&a2a.StreamResponse{Event: &a2a.Message{
				ID: request.Message.ID, Role: a2a.MessageRoleAgent,
				Parts: a2a.ContentParts{a2a.NewDataPart(map[string]any{"answer": "fallback-ok"})},
			}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer successServer.Close()

	bundle := observability.NewNoop()
	client, err := New(successServer.Client(), time.Second, WithCallbackBaseURL("http://127.0.0.1"), WithObservability(bundle))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	result, err := client.Invoke(context.Background(), services.AgentInvocationRequest{
		SourceAgentCode: "planner", TargetAgentCode: "writer", CapabilityCode: "write", ParentRunID: "parent",
		TaskID: "fallback-task", MessageID: "fallback-message", InputJSON: `{}`,
		Endpoints: []services.AgentInvocationEndpoint{
			{Address: failingServer.URL + "/a2a/agents/writer", Transport: "http"},
			{Address: successServer.URL + "/a2a/agents/writer", Transport: "http"},
		},
	})
	if err != nil {
		t.Fatalf("invoke with endpoint fallback: %v", err)
	}
	if result.OutputJSON != `{"answer":"fallback-ok"}` {
		t.Fatalf("unexpected fallback result: %+v", result)
	}

	response := httptest.NewRecorder()
	bundle.Metrics.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	metrics := response.Body.String()
	if !contains(metrics, `goai_a2a_requests_total{operation="invoke",status="success"} 1`) {
		t.Fatalf("overall success metric missing:\n%s", metrics)
	}
	if contains(metrics, `goai_a2a_requests_total{operation="invoke",status="error"}`) {
		t.Fatalf("retryable endpoint failure leaked into overall invocation status:\n%s", metrics)
	}
}

func TestNilClientInvokeReturnsError(t *testing.T) {
	var client *Client
	_, err := client.Invoke(context.Background(), services.AgentInvocationRequest{})
	if err == nil || !contains(err.Error(), "A2A client is nil") {
		t.Fatalf("expected nil client error, got %v", err)
	}
}

func TestWithCallbackBaseURLRejectsQueryAndFragment(t *testing.T) {
	for _, rawURL := range []string{"https://agents.example.com?token=secret", "https://agents.example.com#callback"} {
		if _, err := New(nil, time.Second, WithCallbackBaseURL(rawURL)); err == nil || !contains(err.Error(), "query and fragment are not allowed") {
			t.Fatalf("callback base URL %q error=%v", rawURL, err)
		}
	}
}

func TestNewDoesNotOverrideManagedTransportTimeout(t *testing.T) {
	client, err := New(&http.Client{Transport: managedTimeoutRoundTripper{}}, time.Second, WithCallbackBaseURL("http://127.0.0.1"))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if client.httpClient.Timeout != 0 {
		t.Fatalf("managed transport client timeout = %s, want 0", client.httpClient.Timeout)
	}
}

type managedTimeoutRoundTripper struct{}

func (managedTimeoutRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (managedTimeoutRoundTripper) DownstreamTimeoutManaged() {}
