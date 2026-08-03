package a2agateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"GoAI/a2aauth"
	"GoAI/models"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

func TestCallbackCommandMapsTerminalTaskEvents(t *testing.T) {
	completed := &a2a.Task{
		ID: "task-1", Status: a2a.TaskStatus{State: a2a.TaskStateCompleted},
		Artifacts: []*a2a.Artifact{{ID: "result", Parts: a2a.ContentParts{a2a.NewDataPart(map[string]any{"answer": "done"})}}},
	}
	command, err := callbackCommand("planner", "writer", "task-1", "token", []byte(`{"event":"completed"}`), completed)
	if err != nil {
		t.Fatalf("map completed callback failed: %v", err)
	}
	if command.State != services.DelegationCallbackStateSucceeded || command.OutputJSON != `{"answer":"done"}` {
		t.Fatalf("unexpected completed command: %+v", command)
	}

	for _, test := range []struct {
		name  string
		state a2a.TaskState
		want  string
	}{
		{name: "failed", state: a2a.TaskStateFailed, want: services.DelegationCallbackStateFailed},
		{name: "rejected", state: a2a.TaskStateRejected, want: services.DelegationCallbackStateFailed},
		{name: "cancelled", state: a2a.TaskStateCanceled, want: services.DelegationCallbackStateCancelled},
	} {
		t.Run(test.name, func(t *testing.T) {
			event := &a2a.TaskStatusUpdateEvent{TaskID: "task-1", Status: a2a.TaskStatus{
				State:   test.state,
				Message: &a2a.Message{Parts: a2a.ContentParts{a2a.NewTextPart("child terminal message")}},
			}}
			command, err := callbackCommand("planner", "writer", "task-1", "token", []byte(`{"event":"terminal"}`), event)
			if err != nil {
				t.Fatalf("map terminal callback failed: %v", err)
			}
			if command.State != test.want || command.ErrorMessage != "child terminal message" {
				t.Fatalf("unexpected terminal command: %+v", command)
			}
		})
	}
}

func TestCallbackCommandRejectsInvalidOrNonTerminalEvents(t *testing.T) {
	tests := []struct {
		name  string
		task  string
		event a2a.Event
	}{
		{name: "task id mismatch", task: "path-task", event: &a2a.Task{ID: "body-task", Status: a2a.TaskStatus{State: a2a.TaskStateCompleted}}},
		{name: "non terminal", task: "task-1", event: &a2a.Task{ID: "task-1", Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}},
		{name: "unsupported", task: "task-1", event: &a2a.Message{ID: "message-1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := callbackCommand("planner", "writer", test.task, "token", []byte(`{}`), test.event); err == nil {
				t.Fatal("expected callback command validation error")
			}
		})
	}
}

func TestGatewayAcceptsAuthenticatedCallbackAndRejectsReplay(t *testing.T) {
	const secret = "callback-gateway-test-secret-at-least-32-bytes"
	resolver, err := a2aauth.NewStaticCredentialResolver(map[string]string{"writer-key": secret})
	if err != nil {
		t.Fatalf("create credential resolver failed: %v", err)
	}
	verifier, err := a2aauth.NewVerifier(resolver, a2aauth.NewMemoryNonceStore(), time.Minute)
	if err != nil {
		t.Fatalf("create verifier failed: %v", err)
	}
	runtime := &fakeDelegationRuntime{descriptors: map[string]*services.AgentDescriptor{
		"writer": {
			Code: "writer",
			Endpoints: []services.AgentEndpointDescriptor{{
				Code: "callback", Transport: models.AgentEndpointTransportHTTP,
				Address: "http://127.0.0.1/a2a/agents/writer", AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "writer-key",
			}},
		},
	}}
	gateway, err := New(runtime, WithAuthentication(verifier, true))
	if err != nil {
		t.Fatalf("create gateway failed: %v", err)
	}
	server := httptest.NewServer(gateway)
	defer server.Close()

	event := a2a.StreamResponse{Event: &a2a.Task{ID: "task-1", Status: a2a.TaskStatus{State: a2a.TaskStateCompleted}, Artifacts: []*a2a.Artifact{{ID: "result", Parts: a2a.ContentParts{a2a.NewDataPart(map[string]any{"answer": "ok"})}}}}}
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal callback event failed: %v", err)
	}
	callbackURL := server.URL + "/a2a/agents/planner/callbacks/tasks/task-1"
	unsigned, err := http.Post(callbackURL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("send unsigned callback failed: %v", err)
	}
	unsigned.Body.Close()
	if unsigned.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unsigned callback status=%d want=%d", unsigned.StatusCode, http.StatusUnauthorized)
	}

	signer, err := a2aauth.NewSigner(server.Client().Transport, resolver, "writer", "writer-key", a2aauth.WithNonceGenerator(func() (string, error) {
		return "callback-replay-nonce", nil
	}))
	if err != nil {
		t.Fatalf("create callback signer failed: %v", err)
	}
	client := *server.Client()
	client.Transport = signer
	send := func() *http.Response {
		request, requestErr := http.NewRequestWithContext(context.Background(), http.MethodPost, callbackURL, bytes.NewReader(body))
		if requestErr != nil {
			t.Fatalf("create callback request failed: %v", requestErr)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(notificationTokenHeader, "notification-token")
		response, requestErr := client.Do(request)
		if requestErr != nil {
			t.Fatalf("send callback request failed: %v", requestErr)
		}
		return response
	}
	first := send()
	defer first.Body.Close()
	if first.StatusCode != http.StatusAccepted {
		t.Fatalf("signed callback status=%d want=%d", first.StatusCode, http.StatusAccepted)
	}
	var accepted map[string]bool
	if err := json.NewDecoder(first.Body).Decode(&accepted); err != nil || !accepted["accepted"] {
		t.Fatalf("decode callback success response failed: payload=%v err=%v", accepted, err)
	}
	if runtime.callbackCalls != 1 || runtime.callbackCommand.SourceAgentCode != "planner" || runtime.callbackCommand.TargetAgentCode != "writer" || runtime.callbackCommand.NotificationToken != "notification-token" {
		t.Fatalf("unexpected callback runtime command: calls=%d command=%+v", runtime.callbackCalls, runtime.callbackCommand)
	}

	replayed := send()
	defer replayed.Body.Close()
	if replayed.StatusCode != http.StatusUnauthorized {
		t.Fatalf("replayed callback status=%d want=%d", replayed.StatusCode, http.StatusUnauthorized)
	}
	if runtime.callbackCalls != 1 {
		t.Fatalf("replayed callback reached runtime: calls=%d", runtime.callbackCalls)
	}
}
