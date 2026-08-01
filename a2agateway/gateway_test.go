package a2agateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"GoAI/models"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

type fakeDelegationRuntime struct {
	mu              sync.Mutex
	descriptor      *services.AgentDescriptor
	describeErr     error
	acceptResult    *services.DelegationResult
	acceptErr       error
	snapshot        *services.DelegationSnapshot
	snapshotErr     error
	acceptedCommand services.AcceptDelegationCommand
	acceptCalls     int
	snapshotCalls   int
}

func (f *fakeDelegationRuntime) DescribeAgent(context.Context, string) (*services.AgentDescriptor, error) {
	return f.descriptor, f.describeErr
}

func (f *fakeDelegationRuntime) AcceptDelegation(_ context.Context, command services.AcceptDelegationCommand) (*services.DelegationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acceptCalls++
	f.acceptedCommand = command
	return f.acceptResult, f.acceptErr
}

func (f *fakeDelegationRuntime) DelegationSnapshot(context.Context, string, string) (*services.DelegationSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshotCalls++
	return f.snapshot, f.snapshotErr
}

func (*fakeDelegationRuntime) ReconcileDelegation(context.Context, string) error { return nil }

func validDescriptor() *services.AgentDescriptor {
	return &services.AgentDescriptor{
		Code: "writer", Name: "Writer Agent", Description: "Writes reports",
		Capabilities: []services.AgentCapabilityDescriptor{{
			Code: "write", Name: "Write", Description: "Write a report", Type: models.AgentCapabilityTypeWorkflow, Version: "1",
		}},
		Endpoints: []services.AgentEndpointDescriptor{{
			Code: "local", Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1:8080/a2a/agents/writer",
		}},
	}
}

func validSnapshot(status string) *services.DelegationSnapshot {
	now := time.Now().UTC()
	return &services.DelegationSnapshot{
		Delegation: models.Delegation{
			DelegationID: "dlg_1", ParentRunID: "run_parent", ChildRunID: "run_child", ThreadID: "thread_1",
			CapabilityCode: "write", RequestMessageID: "msg_1", ResultMessageID: "msg_result",
		},
		Run: models.Run{RunID: "run_child", ThreadID: "thread_1", Status: status, UpdatedAt: now},
		Messages: []models.Message{{
			MessageID: "msg_1", ThreadID: "thread_1", RunID: "run_child", DelegationID: "dlg_1",
			SenderType: models.MessageSenderAgent, SenderID: "planner", ReceiverType: models.MessageSenderAgent,
			ReceiverID: "writer", MessageType: models.MessageTypeDelegation, ContentType: "application/json",
			ContentJSON: `{"messages":[{"role":"user","content":"draft"}]}`, MetadataJSON: `{}`,
		}, {
			MessageID: "msg_result", ThreadID: "thread_1", RunID: "run_child", DelegationID: "dlg_1",
			ParentMessageID: "msg_1", SenderType: models.MessageSenderAgent, SenderID: "writer",
			ReceiverType: models.MessageSenderAgent, ReceiverID: "planner", MessageType: models.MessageTypeResult,
			ContentType: "application/json", ContentJSON: `{"answer":"ok"}`, MetadataJSON: `{}`,
		}},
		SourceAgent: models.Agent{AgentCode: "planner"},
		TargetAgent: models.Agent{AgentCode: "writer"},
	}
}

func validSendRequest() *a2a.SendMessageRequest {
	return &a2a.SendMessageRequest{Message: &a2a.Message{
		ID: "msg_1", ContextID: "thread_1", TaskID: "run_child", Role: a2a.MessageRoleUser,
		Extensions: []string{DelegationExtensionURI},
		Metadata: map[string]any{DelegationExtensionURI: map[string]any{
			"sourceAgentCode": "planner", "capabilityCode": "write", "parentRunId": "run_parent",
		}},
		Parts: a2a.ContentParts{a2a.NewTextPart("draft")},
	}}
}

func TestNewRejectsNilRuntime(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("expected nil runtime error")
	}
}

func TestBuildAgentCardValidatesTransportBoundary(t *testing.T) {
	tests := []struct {
		name      string
		transport string
		address   string
		wantErr   bool
	}{
		{name: "loopback IPv4", transport: models.AgentEndpointTransportHTTP, address: "http://127.0.0.1:8080/a2a/agents/writer"},
		{name: "loopback localhost", transport: models.AgentEndpointTransportHTTP, address: "http://localhost:8080/a2a/agents/writer"},
		{name: "remote HTTPS", transport: models.AgentEndpointTransportHTTPS, address: "https://agents.example.com/a2a/agents/writer"},
		{name: "remote HTTP rejected", transport: models.AgentEndpointTransportHTTP, address: "http://agents.example.com/a2a/agents/writer", wantErr: true},
		{name: "HTTPS profile requires HTTPS", transport: models.AgentEndpointTransportHTTPS, address: "http://127.0.0.1:8080/a2a/agents/writer", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			descriptor := validDescriptor()
			descriptor.Endpoints[0].Transport = test.transport
			descriptor.Endpoints[0].Address = test.address
			card, err := buildAgentCard(descriptor)
			if test.wantErr {
				if err == nil {
					t.Fatal("expected endpoint validation error")
				}
				return
			}
			if err != nil {
				t.Fatalf("build agent card failed: %v", err)
			}
			if card.Capabilities.Streaming || card.Capabilities.PushNotifications {
				t.Fatalf("unsupported capabilities must not be advertised: %+v", card.Capabilities)
			}
			if len(card.Capabilities.Extensions) != 1 || card.Capabilities.Extensions[0].URI != DelegationExtensionURI {
				t.Fatalf("delegation extension missing: %+v", card.Capabilities.Extensions)
			}
			if len(card.Skills) != 1 || card.Skills[0].ID != "write" {
				t.Fatalf("agent skills mismatch: %+v", card.Skills)
			}
		})
	}
}

func TestCommandFromRequestMapsDelegationMetadataAndStableIDs(t *testing.T) {
	request := validSendRequest()
	request.Message.TaskID = ""
	request.Message.ContextID = ""
	command, err := commandFromRequest("writer", request)
	if err != nil {
		t.Fatalf("map request failed: %v", err)
	}
	if command.SourceAgentCode != "planner" || command.TargetAgentCode != "writer" || command.CapabilityCode != "write" {
		t.Fatalf("unexpected command routing: %+v", command)
	}
	if command.RequestedChildRunID != stableID("a2a", "msg_1") || command.ThreadID != stableID("thread", "msg_1") {
		t.Fatalf("stable IDs mismatch: %+v", command)
	}
	if string(command.Input) != `{"messages":[{"content":"draft","role":"user"}]}` {
		t.Fatalf("unexpected text mapping: %s", command.Input)
	}

	again, err := commandFromRequest("writer", request)
	if err != nil {
		t.Fatalf("repeat mapping failed: %v", err)
	}
	if again.RequestedChildRunID != command.RequestedChildRunID || again.ThreadID != command.ThreadID {
		t.Fatalf("same A2A message did not produce stable IDs: first=%+v second=%+v", command, again)
	}
}

func TestInputFromPartsSupportsTextOrSingleDataPart(t *testing.T) {
	text, err := inputFromParts(a2a.ContentParts{a2a.NewTextPart("first"), a2a.NewTextPart("second")})
	if err != nil {
		t.Fatalf("text mapping failed: %v", err)
	}
	var textPayload struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(text, &textPayload); err != nil {
		t.Fatalf("decode text input failed: %v", err)
	}
	if len(textPayload.Messages) != 1 || textPayload.Messages[0].Role != "user" || textPayload.Messages[0].Content != "first\nsecond" {
		t.Fatalf("unexpected text input: %+v", textPayload)
	}
	data, err := inputFromParts(a2a.ContentParts{a2a.NewDataPart(map[string]any{"topic": "Go"})})
	if err != nil {
		t.Fatalf("data mapping failed: %v", err)
	}
	if string(data) != `{"topic":"Go"}` {
		t.Fatalf("unexpected data input: %s", data)
	}
}

func TestInputFromPartsRejectsUnsupportedOrMixedContent(t *testing.T) {
	tests := []struct {
		name  string
		parts a2a.ContentParts
	}{
		{name: "raw", parts: a2a.ContentParts{a2a.NewRawPart([]byte("raw"))}},
		{name: "URL", parts: a2a.ContentParts{a2a.NewFileURLPart("https://example.com/file", "text/plain")}},
		{name: "mixed", parts: a2a.ContentParts{a2a.NewTextPart("text"), a2a.NewDataPart(map[string]any{"a": 1})}},
		{name: "multiple data", parts: a2a.ContentParts{a2a.NewDataPart(map[string]any{"a": 1}), a2a.NewDataPart(map[string]any{"b": 2})}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := inputFromParts(test.parts)
			if !errors.Is(err, a2a.ErrUnsupportedContentType) {
				t.Fatalf("got %v, want unsupported content", err)
			}
		})
	}
}

func TestCommandFromRequestRequiresDelegationExtension(t *testing.T) {
	request := validSendRequest()
	request.Message.Extensions = nil
	if _, err := commandFromRequest("writer", request); !errors.Is(err, a2a.ErrExtensionSupportRequired) {
		t.Fatalf("got %v, want extension required", err)
	}
	request = validSendRequest()
	request.Message.Metadata = map[string]any{DelegationExtensionURI: map[string]any{"sourceAgentCode": "planner"}}
	if _, err := commandFromRequest("writer", request); !errors.Is(err, a2a.ErrInvalidParams) {
		t.Fatalf("got %v, want invalid params", err)
	}
}

func TestTaskFromSnapshotMapsStateHistoryAndArtifact(t *testing.T) {
	snapshot := validSnapshot(models.RunStatusSuccess)
	task, err := taskFromSnapshot(snapshot, nil)
	if err != nil {
		t.Fatalf("map successful task failed: %v", err)
	}
	if task.Status.State != a2a.TaskStateCompleted || len(task.History) != 2 || len(task.Artifacts) != 1 {
		t.Fatalf("unexpected successful task: %+v", task)
	}
	if got := task.Artifacts[0].Parts[0].Data(); got == nil {
		t.Fatalf("result artifact data is empty: %+v", task.Artifacts[0])
	}
	historyLength := 1
	trimmed, err := taskFromSnapshot(snapshot, &historyLength)
	if err != nil {
		t.Fatalf("map trimmed task failed: %v", err)
	}
	if len(trimmed.History) != 1 || trimmed.History[0].ID != "msg_result" {
		t.Fatalf("history trimming mismatch: %+v", trimmed.History)
	}
}

func TestTaskFromSnapshotDoesNotExposeRuntimeErrors(t *testing.T) {
	snapshot := validSnapshot(models.RunStatusFailed)
	snapshot.Run.ErrorMessage = "provider key secret-value was rejected"
	task, err := taskFromSnapshot(snapshot, nil)
	if err != nil {
		t.Fatalf("map failed task failed: %v", err)
	}
	if task.Status.State != a2a.TaskStateFailed || task.Status.Message == nil {
		t.Fatalf("failed task status mismatch: %+v", task.Status)
	}
	encoded, err := json.Marshal(task)
	if err != nil {
		t.Fatalf("marshal failed task failed: %v", err)
	}
	if strings.Contains(string(encoded), "secret-value") {
		t.Fatalf("raw runtime error leaked: %s", encoded)
	}
	if len(task.History) != 1 || task.History[0].ID != "msg_1" {
		t.Fatalf("failed task should omit stored result payload: %+v", task.History)
	}
}

func TestGatewayServesAgentCardSendAndTaskRoutes(t *testing.T) {
	runtime := &fakeDelegationRuntime{
		descriptor:   validDescriptor(),
		acceptResult: &services.DelegationResult{Run: &models.Run{RunID: "run_child"}, Delegation: &models.Delegation{DelegationID: "dlg_1"}},
		snapshot:     validSnapshot(models.RunStatusQueued),
	}
	gateway, err := New(runtime)
	if err != nil {
		t.Fatalf("create gateway failed: %v", err)
	}
	server := httptest.NewServer(gateway)
	defer server.Close()

	response, err := server.Client().Get(server.URL + "/a2a/agents/writer/.well-known/agent-card.json")
	if err != nil {
		t.Fatalf("get agent card failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("agent card status=%d body=%s", response.StatusCode, body)
	}
	var card a2a.AgentCard
	if err := json.NewDecoder(response.Body).Decode(&card); err != nil {
		t.Fatalf("decode agent card failed: %v", err)
	}
	if card.Name != "Writer Agent" {
		t.Fatalf("unexpected agent card: %+v", card)
	}

	body, err := json.Marshal(validSendRequest())
	if err != nil {
		t.Fatalf("marshal send request failed: %v", err)
	}
	response, err = server.Client().Post(server.URL+"/a2a/agents/writer/message:send", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("send message failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("send status=%d body=%s", response.StatusCode, payload)
	}
	var sendResponse a2a.StreamResponse
	if err := json.NewDecoder(response.Body).Decode(&sendResponse); err != nil {
		t.Fatalf("decode send result failed: %v", err)
	}
	task, ok := sendResponse.Event.(*a2a.Task)
	if !ok || task.ID != "run_child" || task.Status.State != a2a.TaskStateSubmitted {
		t.Fatalf("unexpected send result: %#v", sendResponse.Event)
	}
	if runtime.acceptedCommand.TargetAgentCode != "writer" || runtime.acceptedCommand.RequestedChildRunID != "run_child" {
		t.Fatalf("gateway command mismatch: %+v", runtime.acceptedCommand)
	}

	response, err = server.Client().Get(server.URL + "/a2a/agents/writer/tasks/run_child?historyLength=1")
	if err != nil {
		t.Fatalf("get task failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(response.Body)
		t.Fatalf("get task status=%d body=%s", response.StatusCode, payload)
	}
	var fetchedTask a2a.Task
	if err := json.NewDecoder(response.Body).Decode(&fetchedTask); err != nil {
		t.Fatalf("decode task failed: %v", err)
	}
	if len(fetchedTask.History) != 1 {
		t.Fatalf("historyLength was not forwarded: %+v", fetchedTask.History)
	}
}

func TestRequestHandlerDuplicateSendReturnsCurrentTask(t *testing.T) {
	runtime := &fakeDelegationRuntime{
		acceptResult: &services.DelegationResult{Run: &models.Run{RunID: "run_child"}, Delegation: &models.Delegation{DelegationID: "dlg_1"}, Reused: true},
		snapshot:     validSnapshot(models.RunStatusRunning),
	}
	handler := &requestHandler{runtime: runtime}
	ctx := context.WithValue(context.Background(), targetAgentContextKey{}, "writer")
	for range 2 {
		result, err := handler.SendMessage(ctx, validSendRequest())
		if err != nil {
			t.Fatalf("duplicate send failed: %v", err)
		}
		task, ok := result.(*a2a.Task)
		if !ok || task.Status.State != a2a.TaskStateWorking {
			t.Fatalf("unexpected duplicate result: %#v", result)
		}
	}
	if runtime.acceptCalls != 2 || runtime.snapshotCalls != 2 {
		t.Fatalf("unexpected runtime calls: accept=%d snapshot=%d", runtime.acceptCalls, runtime.snapshotCalls)
	}
}

func TestRequestHandlerMapsRuntimeErrorsAndUnsupportedOperations(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "task not found", err: services.ErrDelegationNotFound(), want: a2a.ErrTaskNotFound},
		{name: "agent", err: services.ErrAgentNotFound(), want: a2a.ErrInvalidParams},
		{name: "capability", err: services.ErrCapabilityNotFound(), want: a2a.ErrInvalidParams},
		{name: "invalid delegation", err: services.ErrInvalidDelegation(), want: a2a.ErrInvalidParams},
		{name: "conflict", err: services.ErrDelegationConflict(), want: a2a.ErrInvalidRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := mapRuntimeError(test.err); !errors.Is(got, test.want) {
				t.Fatalf("got %v, want %v", got, test.want)
			}
		})
	}

	handler := &requestHandler{}
	if _, err := handler.CancelTask(context.Background(), nil); !errors.Is(err, a2a.ErrTaskNotCancelable) {
		t.Fatalf("cancel error got=%v", err)
	}
	if _, err := handler.GetTaskPushConfig(context.Background(), nil); !errors.Is(err, a2a.ErrPushNotificationNotSupported) {
		t.Fatalf("push error got=%v", err)
	}
	sequence := handler.SendStreamingMessage(context.Background(), nil)
	seen := false
	for _, err := range sequence {
		seen = true
		if !errors.Is(err, a2a.ErrUnsupportedOperation) {
			t.Fatalf("stream error got=%v", err)
		}
	}
	if !seen {
		t.Fatal("unsupported stream returned no error event")
	}
}
