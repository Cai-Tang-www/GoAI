package a2aclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"GoAI/a2aprotocol"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

func TestClientInvokeDiscoversSendsAndPollsTask(t *testing.T) {
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
	client, err := New(injectedClient, time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	result, err := client.Invoke(context.Background(), services.AgentInvocationRequest{
		SourceAgentCode: "planner",
		TargetAgentCode: "writer",
		CapabilityCode:  "write",
		ParentRunID:     "run-parent",
		ThreadID:        "thread-1",
		TaskID:          "a2a_task_123",
		MessageID:       "a2a_message_123",
		InputJSON:       `{"prompt":"write"}`,
		Endpoints:       []services.AgentInvocationEndpoint{{Address: server.URL + "/a2a/agents/writer", Transport: "http"}},
	})
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if result.State != string(a2a.TaskStateCompleted) || result.OutputJSON != `{"answer":"ok"}` {
		t.Fatalf("unexpected result: %+v", result)
	}
	if sendCount.Load() != 1 || pollCount.Load() != 1 {
		t.Fatalf("unexpected request counts: send=%d poll=%d", sendCount.Load(), pollCount.Load())
	}
	if transportCount.Load() != 3 {
		t.Fatalf("injected transport calls = %d, want 3", transportCount.Load())
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
	client, err := New(nil, time.Second, time.Millisecond)
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

func TestClientRejectsMissingDelegationExtension(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/a2a/agents/writer/.well-known/agent-card.json" {
			card := testCard(serverBaseURL(r), "write")
			card.Capabilities.Extensions = nil
			_ = json.NewEncoder(w).Encode(card)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client, _ := New(server.Client(), time.Second, time.Millisecond)
	_, err := client.Invoke(context.Background(), services.AgentInvocationRequest{
		SourceAgentCode: "planner", TargetAgentCode: "writer", CapabilityCode: "write", ParentRunID: "run-parent",
		TaskID: "task", MessageID: "message", InputJSON: `{}`,
		Endpoints: []services.AgentInvocationEndpoint{{Address: server.URL + "/a2a/agents/writer", Transport: "http"}},
	})
	if err == nil || !contains(err.Error(), "does not support GoAI delegation extension") {
		t.Fatalf("expected extension rejection, got %v", err)
	}
}

func testCard(baseURL, capability string) *a2a.AgentCard {
	return &a2a.AgentCard{
		Name: "writer", Description: "writer agent", Version: "1.0",
		SupportedInterfaces: []*a2a.AgentInterface{a2a.NewAgentInterface(baseURL, a2a.TransportProtocolHTTPJSON)},
		Capabilities:        a2a.AgentCapabilities{Extensions: []a2a.AgentExtension{{URI: a2aprotocol.DelegationExtensionURI}}},
		Skills:              []a2a.AgentSkill{{ID: capability, Name: capability}},
		DefaultInputModes:   []string{"application/json"}, DefaultOutputModes: []string{"application/json"},
	}
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
	client, err := New(server.Client(), time.Second, time.Millisecond)
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
	client, err := New(server.Client(), time.Second, time.Millisecond)
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

func TestClientStopsPollingWhenContextIsCanceled(t *testing.T) {
	serverReady := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/a2a/agents/writer/.well-known/agent-card.json":
			_ = json.NewEncoder(w).Encode(testCard(serverBaseURL(r), "write"))
		case "/a2a/agents/writer/message:send":
			close(serverReady)
			_ = json.NewEncoder(w).Encode(&a2a.StreamResponse{Event: &a2a.Task{ID: "working-task", Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}})
		case "/a2a/agents/writer/tasks/working-task":
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := New(server.Client(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultCh := make(chan error, 1)
	go func() {
		_, invokeErr := client.Invoke(ctx, services.AgentInvocationRequest{
			SourceAgentCode: "planner", TargetAgentCode: "writer", CapabilityCode: "write", ParentRunID: "parent",
			TaskID: "working-task", MessageID: "message", InputJSON: `{}`, Endpoints: []services.AgentInvocationEndpoint{{Address: server.URL + "/a2a/agents/writer", Transport: "http"}},
		})
		resultCh <- invokeErr
	}()
	select {
	case <-serverReady:
	case <-time.After(time.Second):
		t.Fatal("A2A send request was not received")
	}
	cancel()
	select {
	case invokeErr := <-resultCh:
		if !errors.Is(invokeErr, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", invokeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("invoke did not stop after context cancellation")
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
	client, err := New(server.Client(), time.Second, time.Millisecond)
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
	client, err := New(server.Client(), time.Second, time.Millisecond)
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

func TestNewDoesNotOverrideManagedTransportTimeout(t *testing.T) {
	client, err := New(&http.Client{Transport: managedTimeoutRoundTripper{}}, time.Second, time.Millisecond)
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
