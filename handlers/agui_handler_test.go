package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"GoAI/middlewares"
	"GoAI/models"
	"GoAI/services"

	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"
	"github.com/gin-gonic/gin"
)

type fakeAGUIRuntime struct {
	startCommand services.StartRunCommand
	startResult  *services.StartRunResult
	startErr     error
	snapshotFunc func(context.Context, uint64, string) (*services.RunSnapshot, error)
}

func (f *fakeAGUIRuntime) StartRun(_ context.Context, command services.StartRunCommand) (*services.StartRunResult, error) {
	f.startCommand = command
	return f.startResult, f.startErr
}

func (f *fakeAGUIRuntime) Snapshot(ctx context.Context, ownerUserID uint64, runID string) (*services.RunSnapshot, error) {
	if f.snapshotFunc == nil {
		return nil, errors.New("snapshot function is not configured")
	}
	return f.snapshotFunc(ctx, ownerUserID, runID)
}

func TestNewAGUIHandlerRejectsNilRuntime(t *testing.T) {
	if _, err := NewAGUIHandler(nil); err == nil {
		t.Fatal("expected nil runtime to be rejected")
	}
}

func TestAGUIRunAgentStreamsOfficialEvents(t *testing.T) {
	runtime := &fakeAGUIRuntime{
		startResult: &services.StartRunResult{
			Thread: &models.Thread{ThreadID: "thread-1"},
			Run:    &models.Run{RunID: "run-1", ThreadID: "thread-1", Status: models.RunStatusQueued},
		},
		snapshotFunc: func(_ context.Context, ownerUserID uint64, runID string) (*services.RunSnapshot, error) {
			if ownerUserID != 42 || runID != "run-1" {
				t.Fatalf("unexpected snapshot lookup owner=%d run=%s", ownerUserID, runID)
			}
			return &services.RunSnapshot{
				Run: models.Run{RunID: "run-1", ThreadID: "thread-1", Status: models.RunStatusSuccess},
				Steps: []models.RunStep{{
					ID:      1,
					RunID:   "run-1",
					StepKey: "answer",
					Status:  models.RunStepStatusSuccess,
				}},
				Messages: []models.Message{
					{
						ID:           1,
						MessageID:    "history-assistant",
						MessageType:  models.MessageTypeResult,
						ContentJSON:  `{"text":"old answer"}`,
						MetadataJSON: `{"source":"agui_request"}`,
					},
					{
						ID:           2,
						MessageID:    "result-1",
						MessageType:  models.MessageTypeResult,
						ContentJSON:  `{"text":"answer"}`,
						MetadataJSON: `{}`,
					},
				},
			}, nil
		},
	}
	handler, err := NewAGUIHandler(runtime)
	if err != nil {
		t.Fatalf("create AG-UI handler failed: %v", err)
	}

	router := newAGUITestRouter(handler, true)
	body := `{"threadId":"thread-1","runId":"run-1","state":{},"messages":[{"id":"input-1","role":"user","content":"hello"}],"tools":[],"context":[]}`
	req := httptest.NewRequest(http.MethodPost, "/api/agents/planner/agui", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Trace-ID", "trace-agui-1")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, req)

	if response.Code != http.StatusOK {
		t.Fatalf("unexpected status %d body=%s", response.Code, response.Body.String())
	}
	if contentType := response.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/event-stream") {
		t.Fatalf("unexpected content type %q", contentType)
	}
	if traceID := response.Header().Get("X-Trace-ID"); traceID != "trace-agui-1" {
		t.Fatalf("unexpected trace id %q", traceID)
	}

	events := decodeAGUIEvents(t, response.Body.String())
	wantTypes := []string{
		"RUN_STARTED",
		"STEP_STARTED",
		"STEP_FINISHED",
		"TEXT_MESSAGE_START",
		"TEXT_MESSAGE_CONTENT",
		"TEXT_MESSAGE_END",
		"RUN_FINISHED",
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("unexpected event count %d events=%v", len(events), events)
	}
	for i, want := range wantTypes {
		if got, _ := events[i]["type"].(string); got != want {
			t.Fatalf("event %d type=%q want=%q events=%v", i, got, want, events)
		}
	}
	if delta, _ := events[4]["delta"].(string); delta != "answer" {
		t.Fatalf("unexpected text delta %q", delta)
	}
	if strings.Contains(response.Body.String(), "old answer") {
		t.Fatalf("historical AG-UI assistant message must not be emitted: %s", response.Body.String())
	}

	command := runtime.startCommand
	if command.OwnerUserID != 42 || command.AgentCode != "planner" || command.ThreadID != "thread-1" || command.RequestedRunID != "run-1" {
		t.Fatalf("unexpected start command: %+v", command)
	}
	if command.TriggerType != "agui" || len(command.Messages) != 1 {
		t.Fatalf("unexpected protocol mapping: %+v", command)
	}
	message := command.Messages[0]
	if message.MessageID != "input-1" || message.SenderType != models.MessageSenderUser || message.ReceiverID != "planner" || message.MessageType != models.MessageTypeInput {
		t.Fatalf("unexpected message mapping: %+v", message)
	}
}

func TestAGUIRunAgentReturnsValidationEnvelopeBeforeStream(t *testing.T) {
	runtime := &fakeAGUIRuntime{}
	handler, err := NewAGUIHandler(runtime)
	if err != nil {
		t.Fatalf("create AG-UI handler failed: %v", err)
	}
	router := newAGUITestRouter(handler, true)
	req := httptest.NewRequest(http.MethodPost, "/api/agents/planner/agui", strings.NewReader(`{"messages":`))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, req)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status %d body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Code    string `json:"code"`
		TraceID string `json:"trace_id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope failed: %v body=%s", err, response.Body.String())
	}
	if envelope.Code != middlewares.CodeValidationFailed || strings.TrimSpace(envelope.TraceID) == "" {
		t.Fatalf("unexpected error envelope: %+v", envelope)
	}
	if strings.HasPrefix(response.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("pre-stream validation error must remain JSON: %s", response.Body.String())
	}
}

func TestAGUIRunAgentRejectsMissingPrincipal(t *testing.T) {
	runtime := &fakeAGUIRuntime{}
	handler, err := NewAGUIHandler(runtime)
	if err != nil {
		t.Fatalf("create AG-UI handler failed: %v", err)
	}
	router := newAGUITestRouter(handler, false)
	body := `{"messages":[{"id":"input-1","role":"user","content":"hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/agents/planner/agui", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, req)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status %d body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope failed: %v", err)
	}
	if envelope.Code != middlewares.CodeAuthInvalidToken {
		t.Fatalf("unexpected error code %q", envelope.Code)
	}
}

func TestAGUIRunAgentMapsMissingAgentBeforeStartingStream(t *testing.T) {
	runtime := &fakeAGUIRuntime{startErr: services.ErrAgentNotFound()}
	handler, err := NewAGUIHandler(runtime)
	if err != nil {
		t.Fatalf("create AG-UI handler failed: %v", err)
	}
	router := newAGUITestRouter(handler, true)
	body := `{"messages":[{"id":"input-1","role":"user","content":"hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/agents/missing/agui", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, req)

	if response.Code != http.StatusNotFound {
		t.Fatalf("unexpected status %d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("missing agent must fail before SSE starts: headers=%v", response.Header())
	}
	var envelope struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response failed: %v body=%s", err, response.Body.String())
	}
	if envelope.Code != middlewares.CodeAgentNotFound {
		t.Fatalf("unexpected code %q body=%s", envelope.Code, response.Body.String())
	}
}
func TestAGUIRunAgentFailedRunDoesNotLeakInternalError(t *testing.T) {
	runtime := &fakeAGUIRuntime{
		startResult: &services.StartRunResult{
			Thread: &models.Thread{ThreadID: "thread-1"},
			Run:    &models.Run{RunID: "run-1", ThreadID: "thread-1"},
		},
		snapshotFunc: func(context.Context, uint64, string) (*services.RunSnapshot, error) {
			return &services.RunSnapshot{Run: models.Run{
				RunID:        "run-1",
				ThreadID:     "thread-1",
				Status:       models.RunStatusFailed,
				ErrorMessage: "provider secret failure detail",
			}}, nil
		},
	}
	handler, err := NewAGUIHandler(runtime)
	if err != nil {
		t.Fatalf("create AG-UI handler failed: %v", err)
	}
	router := newAGUITestRouter(handler, true)
	body := `{"messages":[{"id":"input-1","role":"user","content":"hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/agents/planner/agui", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, req)

	events := decodeAGUIEvents(t, response.Body.String())
	if len(events) != 2 || events[1]["type"] != "RUN_ERROR" {
		t.Fatalf("unexpected events: %v", events)
	}
	if strings.Contains(response.Body.String(), "provider secret failure detail") {
		t.Fatalf("internal run error leaked to client: %s", response.Body.String())
	}
	if message, _ := events[1]["message"].(string); message != "run did not complete successfully" {
		t.Fatalf("unexpected public error message %q", message)
	}
}

func TestAGUIRunAgentSnapshotFailureEmitsRunError(t *testing.T) {
	runtime := &fakeAGUIRuntime{
		startResult: &services.StartRunResult{
			Thread: &models.Thread{ThreadID: "thread-1"},
			Run:    &models.Run{RunID: "run-1", ThreadID: "thread-1"},
		},
		snapshotFunc: func(context.Context, uint64, string) (*services.RunSnapshot, error) {
			return nil, errors.New("database unavailable")
		},
	}
	handler, err := NewAGUIHandler(runtime)
	if err != nil {
		t.Fatalf("create AG-UI handler failed: %v", err)
	}
	router := newAGUITestRouter(handler, true)
	body := `{"messages":[{"id":"input-1","role":"user","content":"hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/agents/planner/agui", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, req)

	events := decodeAGUIEvents(t, response.Body.String())
	if len(events) != 2 || events[1]["type"] != "RUN_ERROR" || events[1]["message"] != "run observation failed" {
		t.Fatalf("unexpected events: %v", events)
	}
	if strings.Contains(response.Body.String(), "database unavailable") {
		t.Fatalf("snapshot error leaked to client: %s", response.Body.String())
	}
}

func TestAGUIRunAgentStopsWhenRequestIsCancelled(t *testing.T) {
	firstSnapshot := make(chan struct{})
	var calls atomic.Int32
	runtime := &fakeAGUIRuntime{
		startResult: &services.StartRunResult{
			Thread: &models.Thread{ThreadID: "thread-1"},
			Run:    &models.Run{RunID: "run-1", ThreadID: "thread-1"},
		},
		snapshotFunc: func(context.Context, uint64, string) (*services.RunSnapshot, error) {
			if calls.Add(1) == 1 {
				close(firstSnapshot)
			}
			return &services.RunSnapshot{Run: models.Run{RunID: "run-1", ThreadID: "thread-1", Status: models.RunStatusRunning}}, nil
		},
	}
	handler, err := NewAGUIHandler(runtime)
	if err != nil {
		t.Fatalf("create AG-UI handler failed: %v", err)
	}
	handler.pollInterval = time.Hour
	router := newAGUITestRouter(handler, true)
	ctx, cancel := context.WithCancel(context.Background())
	body := `{"messages":[{"id":"input-1","role":"user","content":"hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/agents/planner/agui", strings.NewReader(body)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		router.ServeHTTP(response, req)
	}()

	select {
	case <-firstSnapshot:
	case <-time.After(time.Second):
		t.Fatal("handler did not observe the run")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("handler did not stop after request cancellation")
	}
	if calls.Load() != 1 {
		t.Fatalf("unexpected snapshot calls after cancellation: %d", calls.Load())
	}
}

func TestAGUIRunAgentDoesNotEmitRunErrorWhenSnapshotIsCancelled(t *testing.T) {
	snapshotStarted := make(chan struct{})
	runtime := &fakeAGUIRuntime{
		startResult: &services.StartRunResult{
			Thread: &models.Thread{ThreadID: "thread-1"},
			Run:    &models.Run{RunID: "run-1", ThreadID: "thread-1"},
		},
		snapshotFunc: func(ctx context.Context, _ uint64, _ string) (*services.RunSnapshot, error) {
			close(snapshotStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	handler, err := NewAGUIHandler(runtime)
	if err != nil {
		t.Fatalf("create AG-UI handler failed: %v", err)
	}
	router := newAGUITestRouter(handler, true)
	ctx, cancel := context.WithCancel(context.Background())
	body := `{"messages":[{"id":"input-1","role":"user","content":"hello"}]}`
	req := httptest.NewRequest(http.MethodPost, "/api/agents/planner/agui", strings.NewReader(body)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		router.ServeHTTP(response, req)
	}()

	select {
	case <-snapshotStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start snapshot lookup")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("handler did not stop after snapshot cancellation")
	}

	events := decodeAGUIEvents(t, response.Body.String())
	if len(events) != 1 || events[0]["type"] != "RUN_STARTED" {
		t.Fatalf("request cancellation must not be reported as run error: %v", events)
	}
}

func TestBuildAGUIStartRunCommandRejectsUnsupportedContent(t *testing.T) {
	tests := []struct {
		name    string
		input   aguitypes.RunAgentInput
		wantErr string
	}{
		{name: "missing messages", input: aguitypes.RunAgentInput{}, wantErr: "messages are required"},
		{
			name: "thread id too long",
			input: aguitypes.RunAgentInput{
				ThreadID: strings.Repeat("t", 65),
				Messages: []aguitypes.Message{{ID: "message-1", Role: aguitypes.RoleUser, Content: "hello"}},
			},
			wantErr: "threadId must be at most 64 characters",
		},
		{
			name: "run id too long",
			input: aguitypes.RunAgentInput{
				RunID:    strings.Repeat("r", 65),
				Messages: []aguitypes.Message{{ID: "message-1", Role: aguitypes.RoleUser, Content: "hello"}},
			},
			wantErr: "runId must be at most 64 characters",
		},
		{
			name: "missing message id",
			input: aguitypes.RunAgentInput{Messages: []aguitypes.Message{{
				Role:    aguitypes.RoleUser,
				Content: "hello",
			}}},
			wantErr: "message id is required",
		},
		{
			name:    "empty text",
			input:   aguitypes.RunAgentInput{Messages: []aguitypes.Message{{ID: "message-1", Role: aguitypes.RoleUser, Content: " "}}},
			wantErr: "message content must not be empty",
		},
		{
			name: "multimodal",
			input: aguitypes.RunAgentInput{Messages: []aguitypes.Message{{
				ID:   "message-1",
				Role: aguitypes.RoleUser,
				Content: []aguitypes.InputContent{{
					Type: aguitypes.InputContentTypeBinary,
					Data: "aGVsbG8=",
				}},
			}}},
			wantErr: "multimodal message content is not supported in V1",
		},
		{
			name: "tool role",
			input: aguitypes.RunAgentInput{Messages: []aguitypes.Message{{
				ID:         "message-1",
				Role:       aguitypes.RoleTool,
				Content:    "result",
				ToolCallID: "tool-call-1",
			}}},
			wantErr: "tool, activity, and reasoning messages are not supported in V1",
		},
		{
			name: "assistant tool calls",
			input: aguitypes.RunAgentInput{Messages: []aguitypes.Message{{
				ID:      "message-1",
				Role:    aguitypes.RoleAssistant,
				Content: "calling tool",
				ToolCalls: []aguitypes.ToolCall{{
					ID:   "tool-call-1",
					Type: aguitypes.ToolCallTypeFunction,
				}},
			}}},
			wantErr: "tool call message fields are not supported in V1",
		},
		{
			name: "named message",
			input: aguitypes.RunAgentInput{Messages: []aguitypes.Message{{
				ID:      "message-1",
				Role:    aguitypes.RoleUser,
				Content: "hello",
				Name:    "operator",
			}}},
			wantErr: "named messages are not supported in V1",
		},
		{
			name: "encrypted content",
			input: aguitypes.RunAgentInput{Messages: []aguitypes.Message{{
				ID:               "message-1",
				Role:             aguitypes.RoleUser,
				Content:          "hello",
				EncryptedContent: "ciphertext",
			}}},
			wantErr: "encrypted message fields are not supported in V1",
		},
		{
			name: "activity fields",
			input: aguitypes.RunAgentInput{Messages: []aguitypes.Message{{
				ID:           "message-1",
				Role:         aguitypes.RoleUser,
				Content:      "hello",
				ActivityType: "PLAN",
			}}},
			wantErr: "activity message fields are not supported in V1",
		},
		{
			name: "state",
			input: aguitypes.RunAgentInput{
				State:    map[string]any{"stage": "draft"},
				Messages: []aguitypes.Message{{ID: "message-1", Role: aguitypes.RoleUser, Content: "hello"}},
			},
			wantErr: "state is not supported in V1",
		},
		{
			name: "client tools",
			input: aguitypes.RunAgentInput{
				Tools:    []aguitypes.Tool{{}},
				Messages: []aguitypes.Message{{ID: "message-1", Role: aguitypes.RoleUser, Content: "hello"}},
			},
			wantErr: "client-provided tools are not supported in V1",
		},
		{
			name: "context entries",
			input: aguitypes.RunAgentInput{
				Context:  []aguitypes.Context{{}},
				Messages: []aguitypes.Message{{ID: "message-1", Role: aguitypes.RoleUser, Content: "hello"}},
			},
			wantErr: "context entries are not supported in V1",
		},
		{
			name: "forwarded props",
			input: aguitypes.RunAgentInput{
				ForwardedProps: map[string]any{"tenant": "demo"},
				Messages:       []aguitypes.Message{{ID: "message-1", Role: aguitypes.RoleUser, Content: "hello"}},
			},
			wantErr: "forwardedProps is not supported in V1",
		},
		{
			name: "parent run",
			input: aguitypes.RunAgentInput{
				ParentRunID: stringPointer("parent-run-1"),
				Messages:    []aguitypes.Message{{ID: "message-1", Role: aguitypes.RoleUser, Content: "hello"}},
			},
			wantErr: "parentRunId branching is not supported in V1",
		},
		{
			name: "resume",
			input: aguitypes.RunAgentInput{
				Resume:   []aguitypes.ResumeEntry{{InterruptID: "interrupt-1", Status: aguitypes.ResumeStatusResolved}},
				Messages: []aguitypes.Message{{ID: "message-1", Role: aguitypes.RoleUser, Content: "hello"}},
			},
			wantErr: "resume is not supported in V1",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := buildAGUIStartRunCommand(42, "planner", test.input)
			if err == nil || err.Error() != test.wantErr {
				t.Fatalf("unexpected error %v want=%q", err, test.wantErr)
			}
		})
	}
}

func TestBuildAGUIStartRunCommandAllowsEmptyAdvancedFields(t *testing.T) {
	command, err := buildAGUIStartRunCommand(42, "planner", aguitypes.RunAgentInput{
		State:          map[string]any{},
		Tools:          []aguitypes.Tool{},
		Context:        []aguitypes.Context{},
		ForwardedProps: map[string]any{},
		Messages:       []aguitypes.Message{{ID: "message-1", Role: aguitypes.RoleUser, Content: "hello"}},
	})
	if err != nil {
		t.Fatalf("empty advanced fields should be accepted: %v", err)
	}
	var input map[string]json.RawMessage
	if err := json.Unmarshal(command.Input, &input); err != nil {
		t.Fatalf("decode runtime input failed: %v", err)
	}
	if len(input) != 1 || input["messages"] == nil {
		t.Fatalf("unsupported fields leaked into runtime input: %s", command.Input)
	}
}
func stringPointer(value string) *string {
	return &value
}

func newAGUITestRouter(handler *AGUIHandler, authenticated bool) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(middlewares.TraceMiddleware(), middlewares.ErrorHandlingMiddleware())
	if authenticated {
		router.Use(func(c *gin.Context) {
			c.Set("user_id", uint(42))
			c.Next()
		})
	}
	router.POST("/api/agents/:agent_code/agui", handler.RunAgent)
	return router
}

func decodeAGUIEvents(t *testing.T, body string) []map[string]any {
	t.Helper()
	var decoded []map[string]any
	for _, frame := range strings.Split(body, "\n\n") {
		for _, line := range strings.Split(frame, "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var event map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
				t.Fatalf("decode AG-UI event failed: %v frame=%s", err, frame)
			}
			decoded = append(decoded, event)
		}
	}
	return decoded
}
