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
	"unicode/utf8"

	"GoAI/a2aauth"
	"GoAI/models"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

type fakeDelegationRuntime struct {
	mu              sync.Mutex
	descriptor      *services.AgentDescriptor
	descriptors     map[string]*services.AgentDescriptor
	describeErr     error
	acceptResult    *services.DelegationResult
	acceptErr       error
	snapshot        *services.DelegationSnapshot
	snapshotErr     error
	acceptedCommand services.AcceptDelegationCommand
	acceptCalls     int
	snapshotCalls   int
	pushConfigs     map[string]services.DelegationPushConfig
	pushErr         error
	callbackCommand services.DelegationCallbackCommand
	callbackErr     error
	callbackCalls   int
}

func (f *fakeDelegationRuntime) DescribeAgent(_ context.Context, code string) (*services.AgentDescriptor, error) {
	if f.descriptors != nil {
		return f.descriptors[code], f.describeErr
	}
	return f.descriptor, f.describeErr
}

func (f *fakeDelegationRuntime) AcceptDelegation(_ context.Context, command services.AcceptDelegationCommand) (*services.DelegationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acceptCalls++
	f.acceptedCommand = command
	return f.acceptResult, f.acceptErr
}

func (f *fakeDelegationRuntime) DelegationSnapshot(context.Context, string, string, string) (*services.DelegationSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshotCalls++
	return f.snapshot, f.snapshotErr
}

func (f *fakeDelegationRuntime) CreateDelegationPushConfig(_ context.Context, _, _ string, config services.DelegationPushConfig) (*services.DelegationPushConfig, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pushErr != nil {
		return nil, f.pushErr
	}
	if config.ConfigID == "" {
		config.ConfigID = "push_generated"
	}
	if f.pushConfigs == nil {
		f.pushConfigs = make(map[string]services.DelegationPushConfig)
	}
	f.pushConfigs[config.TaskID+"\x00"+config.ConfigID] = config
	stored := config
	return &stored, nil
}

func (f *fakeDelegationRuntime) GetDelegationPushConfig(_ context.Context, _, _, taskID, configID string) (*services.DelegationPushConfig, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pushErr != nil {
		return nil, f.pushErr
	}
	config, ok := f.pushConfigs[taskID+"\x00"+configID]
	if !ok {
		return nil, services.ErrPushConfigNotFound()
	}
	return &config, nil
}

func (f *fakeDelegationRuntime) ListDelegationPushConfigs(_ context.Context, _, _, taskID string) ([]services.DelegationPushConfig, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pushErr != nil {
		return nil, f.pushErr
	}
	configs := make([]services.DelegationPushConfig, 0)
	for _, config := range f.pushConfigs {
		if config.TaskID == taskID {
			configs = append(configs, config)
		}
	}
	return configs, nil
}

func (f *fakeDelegationRuntime) DeleteDelegationPushConfig(_ context.Context, _, _, taskID, configID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pushErr != nil {
		return f.pushErr
	}
	key := taskID + "\x00" + configID
	if _, ok := f.pushConfigs[key]; !ok {
		return services.ErrPushConfigNotFound()
	}
	delete(f.pushConfigs, key)
	return nil
}

func (f *fakeDelegationRuntime) AcceptDelegationCallback(_ context.Context, command services.DelegationCallbackCommand) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callbackCalls++
	f.callbackCommand = command
	return f.callbackErr
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
			"traceId": "trace_parent", "delegationId": "dlg_1",
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
			if card.Capabilities.Streaming || !card.Capabilities.PushNotifications {
				t.Fatalf("push notifications must be advertised without claiming streaming: %+v", card.Capabilities)
			}
			if len(card.Capabilities.Extensions) != 1 || card.Capabilities.Extensions[0].URI != DelegationExtensionURI {
				t.Fatalf("delegation extension missing: %+v", card.Capabilities.Extensions)
			}
			if card.Capabilities.Extensions[0].Params["agentCode"] != descriptor.Code {
				t.Fatalf("delegation extension agentCode mismatch: %+v", card.Capabilities.Extensions[0].Params)
			}
			if len(card.Skills) != 1 || card.Skills[0].ID != "write" {
				t.Fatalf("agent skills mismatch: %+v", card.Skills)
			}
		})
	}
}

func TestBuildAgentCardRejectsAgentWithoutExecutableCapability(t *testing.T) {
	descriptor := validDescriptor()
	descriptor.Capabilities = nil
	if _, err := buildAgentCard(descriptor); err == nil || !strings.Contains(err.Error(), "no active executable capability") {
		t.Fatalf("build card error = %v", err)
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
	if command.TraceID != "trace_parent" || command.RequestedDelegationID != "dlg_1" {
		t.Fatalf("unexpected delegation tracing metadata: %+v", command)
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

func TestCommandFromRequestRejectsOversizedCorrelationMetadata(t *testing.T) {
	tests := []struct {
		name  string
		field string
		value string
	}{
		{name: "trace id", field: "traceId", value: strings.Repeat("t", 129)},
		{name: "delegation id", field: "delegationId", value: strings.Repeat("d", 65)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validSendRequest()
			metadata := request.Message.Metadata[DelegationExtensionURI].(map[string]any)
			metadata[test.field] = test.value

			if _, err := commandFromRequest("writer", request); !errors.Is(err, a2a.ErrInvalidParams) {
				t.Fatalf("got %v, want invalid params", err)
			}
		})
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

func TestPartsFromJSONMapsInternalTextMessageToA2ATextPart(t *testing.T) {
	parts, err := partsFromJSON(`{"text":"release note ready"}`)
	if err != nil {
		t.Fatalf("map internal text message: %v", err)
	}
	if len(parts) != 1 || parts[0].Text() != "release note ready" {
		t.Fatalf("unexpected A2A text parts: %+v", parts)
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

	handler := &requestHandler{runtime: &fakeDelegationRuntime{}}
	if _, err := handler.CancelTask(context.Background(), nil); !errors.Is(err, a2a.ErrTaskNotCancelable) {
		t.Fatalf("cancel error got=%v", err)
	}
	pushContext := context.WithValue(context.Background(), targetAgentContextKey{}, "writer")
	if _, err := handler.GetTaskPushConfig(pushContext, nil); !errors.Is(err, a2a.ErrInvalidParams) {
		t.Fatalf("push validation error got=%v", err)
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

func TestGatewayRequiresSignedAgentIdentityAndRejectsReplay(t *testing.T) {
	const secret = "test-only-a2a-secret-at-least-32-bytes-long"
	resolver, err := a2aauth.NewStaticCredentialResolver(map[string]string{"planner-key": secret})
	if err != nil {
		t.Fatalf("create resolver failed: %v", err)
	}
	verifier, err := a2aauth.NewVerifier(resolver, a2aauth.NewMemoryNonceStore(), time.Minute)
	if err != nil {
		t.Fatalf("create verifier failed: %v", err)
	}
	writer := validDescriptor()
	planner := &services.AgentDescriptor{Code: "planner", Endpoints: []services.AgentEndpointDescriptor{{
		Code: "local", Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1/a2a/agents/planner",
		AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "planner-key",
	}}}
	runtime := &fakeDelegationRuntime{
		descriptors: map[string]*services.AgentDescriptor{
			"planner":  planner,
			"writer":   writer,
			"inactive": {Code: "inactive"},
		},
		acceptResult: &services.DelegationResult{Run: &models.Run{RunID: "run_child"}, Delegation: &models.Delegation{DelegationID: "dlg_1"}},
		snapshot:     validSnapshot(models.RunStatusQueued),
	}
	gateway, err := New(runtime, WithAuthentication(verifier, true))
	if err != nil {
		t.Fatalf("create authenticated gateway failed: %v", err)
	}
	server := httptest.NewServer(gateway)
	defer server.Close()

	cardResponse, err := server.Client().Get(server.URL + "/a2a/agents/writer/.well-known/agent-card.json")
	if err != nil {
		t.Fatalf("public discovery failed: %v", err)
	}
	defer cardResponse.Body.Close()
	var card a2a.AgentCard
	if err := json.NewDecoder(cardResponse.Body).Decode(&card); err != nil {
		t.Fatalf("decode card failed: %v", err)
	}
	if len(card.SecurityRequirements) != 1 || len(card.SecuritySchemes) != 1 {
		t.Fatalf("agent card did not declare HMAC security: %+v", card)
	}
	encodedCard, _ := json.Marshal(card)
	if bytes.Contains(encodedCard, []byte("planner-key")) || bytes.Contains(encodedCard, []byte(secret)) {
		t.Fatal("agent card leaked credential material")
	}

	body, _ := json.Marshal(validSendRequest())
	unsignedResponse, err := server.Client().Post(server.URL+"/a2a/agents/writer/message:send", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("unsigned request failed: %v", err)
	}
	unsignedResponse.Body.Close()
	if unsignedResponse.StatusCode != http.StatusUnauthorized || runtime.acceptCalls != 0 {
		t.Fatalf("unsigned status=%d acceptCalls=%d", unsignedResponse.StatusCode, runtime.acceptCalls)
	}

	now := time.Now().UTC()
	signer, err := a2aauth.NewSigner(server.Client().Transport, resolver, "planner", "planner-key",
		a2aauth.WithSignerClock(func() time.Time { return now }),
		a2aauth.WithNonceGenerator(func() (string, error) { return "fixed-replay-nonce", nil }),
	)
	if err != nil {
		t.Fatalf("create signer failed: %v", err)
	}
	signedClient := *server.Client()
	signedClient.Transport = signer
	for attempt, expected := range []int{http.StatusOK, http.StatusUnauthorized} {
		response, err := signedClient.Post(server.URL+"/a2a/agents/writer/message:send", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("signed attempt %d failed: %v", attempt+1, err)
		}
		response.Body.Close()
		if response.StatusCode != expected {
			t.Fatalf("signed attempt %d status=%d want=%d", attempt+1, response.StatusCode, expected)
		}
	}
	if runtime.acceptCalls != 1 {
		t.Fatalf("replayed request reached runtime: acceptCalls=%d", runtime.acceptCalls)
	}

	mismatchedRequest := validSendRequest()
	metadata := mismatchedRequest.Message.Metadata[DelegationExtensionURI].(map[string]any)
	metadata["sourceAgentCode"] = "intruder"
	mismatchedBody, _ := json.Marshal(mismatchedRequest)
	mismatchSigner, err := a2aauth.NewSigner(server.Client().Transport, resolver, "planner", "planner-key",
		a2aauth.WithNonceGenerator(func() (string, error) { return "metadata-mismatch-nonce", nil }),
	)
	if err != nil {
		t.Fatalf("create mismatch signer failed: %v", err)
	}
	mismatchClient := *server.Client()
	mismatchClient.Transport = mismatchSigner
	mismatchResponse, err := mismatchClient.Post(server.URL+"/a2a/agents/writer/message:send", "application/json", bytes.NewReader(mismatchedBody))
	if err != nil {
		t.Fatalf("send source mismatch request failed: %v", err)
	}
	mismatchResponse.Body.Close()
	if mismatchResponse.StatusCode != http.StatusForbidden || runtime.acceptCalls != 1 {
		t.Fatalf("source mismatch status=%d acceptCalls=%d", mismatchResponse.StatusCode, runtime.acceptCalls)
	}

	for _, source := range []string{"ghost", "inactive"} {
		sourceSigner, signerErr := a2aauth.NewSigner(server.Client().Transport, resolver, source, "planner-key")
		if signerErr != nil {
			t.Fatalf("create %s signer failed: %v", source, signerErr)
		}
		sourceClient := *server.Client()
		sourceClient.Transport = sourceSigner
		response, requestErr := sourceClient.Post(server.URL+"/a2a/agents/writer/message:send", "application/json", bytes.NewReader(body))
		if requestErr != nil {
			t.Fatalf("send %s source request failed: %v", source, requestErr)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized || runtime.acceptCalls != 1 {
			t.Fatalf("source=%s status=%d acceptCalls=%d", source, response.StatusCode, runtime.acceptCalls)
		}
	}
}

func TestAuthenticatedSourceMustMatchDelegationMetadata(t *testing.T) {
	runtime := &fakeDelegationRuntime{}
	handler := &requestHandler{runtime: runtime, authRequired: true}
	ctx := context.WithValue(context.Background(), targetAgentContextKey{}, "writer")
	ctx = a2aauth.WithAuthenticatedAgent(ctx, "intruder")
	if _, err := handler.SendMessage(ctx, validSendRequest()); !errors.Is(err, a2a.ErrUnauthorized) {
		t.Fatalf("expected unauthorized mismatch, got %v", err)
	}
	if runtime.acceptCalls != 0 {
		t.Fatalf("identity mismatch reached runtime: %d", runtime.acceptCalls)
	}
}

func TestSafeAuditValuePreservesUTF8(t *testing.T) {
	input := strings.Repeat("协", 129)
	got := safeAuditValue(input)
	if !utf8.ValidString(got) {
		t.Fatal("safeAuditValue returned invalid UTF-8")
	}
	if runeCount := utf8.RuneCountInString(got); runeCount != 128 {
		t.Fatalf("rune count=%d, want 128", runeCount)
	}
}
