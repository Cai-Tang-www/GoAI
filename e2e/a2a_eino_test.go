package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"GoAI/a2aclient"
	"GoAI/a2agateway"
	"GoAI/ai"
	"GoAI/config"
	"GoAI/handlers"
	"GoAI/middlewares"
	"GoAI/models"
	"GoAI/services"

	"github.com/gin-gonic/gin"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type syncRunPublisher struct {
	execute func(context.Context, string) error
}

func (p syncRunPublisher) PublishRunExecute(ctx context.Context, runID string) error {
	if p.execute == nil {
		return nil
	}
	return p.execute(ctx, runID)
}

func TestAGUIRequestDelegatesThroughA2AToEinoAgentAndStreamsResult(t *testing.T) {
	databaseA := openE2EDB(t, "agent_a")
	databaseB := openE2EDB(t, "agent_b")
	migrateE2ADB(t, databaseA)
	migrateE2ADB(t, databaseB)

	agentA, _ := seedAgent(t, databaseA, "planner", "Planner Agent", 11, `{"entry_node":"delegate","nodes":[{"key":"delegate","type":"agent","config":{"target_agent":"writer","capability":"write","timeout_ms":5000}}],"edges":[]}`)
	agentBInA, workflowBInA := seedAgent(t, databaseA, "writer", "Writer Agent", 22, `{"entry_node":"write","nodes":[{"key":"write","type":"llm"}],"edges":[]}`)
	seedCapability(t, databaseA, agentBInA, "write", workflowBInA)

	seedAgent(t, databaseB, "planner", "Planner Agent", 11, `{"entry_node":"write","nodes":[{"key":"write","type":"tool"}],"edges":[]}`)
	agentB, workflowB := seedAgent(t, databaseB, "writer", "Writer Agent", 22, `{"entry_node":"write","nodes":[{"key":"write","type":"llm"}],"edges":[]}`)
	seedCapability(t, databaseB, agentB, "write", workflowB)

	var providerCalls atomic.Int32
	providerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls.Add(1)
		if r.URL.Path != "/chat/completions" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"release note ready\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(providerServer.Close)
	chatService, err := services.NewChatService(&config.Config{
		ModelProviderDefault: "test",
		ModelProviders: map[string]config.ModelProviderConfig{
			"test": {
				Driver:       ai.DriverOpenAICompatible,
				BaseURL:      providerServer.URL,
				APIKey:       "test-key",
				DefaultModel: "test-model",
				EndpointPath: "/chat/completions",
			},
		},
	}, providerServer.Client())
	if err != nil {
		t.Fatalf("create child chat service: %v", err)
	}

	var childService *services.RunService
	childPublisher := syncRunPublisher{execute: func(ctx context.Context, runID string) error {
		if childService == nil {
			return fmt.Errorf("child run service is not initialized")
		}
		return childService.HandleRunExecute(ctx, runID)
	}}
	childService, err = services.NewRunService(databaseB, childPublisher, services.WithChatService(chatService))
	if err != nil {
		t.Fatalf("create child run service: %v", err)
	}
	runtimeB, err := services.NewRuntimeService(databaseB, childService)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	gatewayB, err := a2agateway.New(runtimeB)
	if err != nil {
		t.Fatalf("create target gateway: %v", err)
	}

	var pathsMu sync.Mutex
	var paths []string
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pathsMu.Lock()
		paths = append(paths, r.URL.Path)
		pathsMu.Unlock()
		gatewayB.ServeHTTP(w, r)
	}))
	t.Cleanup(serverB.Close)

	seedEndpoint(t, databaseB, agentB, serverB.URL+"/a2a/agents/writer")
	seedEndpoint(t, databaseA, agentBInA, serverB.URL+"/a2a/agents/writer")

	outbound, err := a2aclient.New(serverB.Client(), 5*time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("create A2A client: %v", err)
	}
	var parentService *services.RunService
	parentPublisher := syncRunPublisher{execute: func(ctx context.Context, runID string) error {
		if parentService == nil {
			return fmt.Errorf("parent run service is not initialized")
		}
		return parentService.HandleRunExecute(ctx, runID)
	}}
	parentService, err = services.NewRunService(databaseA, parentPublisher, services.WithAgentInvoker(outbound))
	if err != nil {
		t.Fatalf("create parent run service: %v", err)
	}
	runtimeA, err := services.NewRuntimeService(databaseA, parentService)
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	aguiHandler, err := handlers.NewAGUIHandler(runtimeA)
	if err != nil {
		t.Fatalf("create AG-UI gateway: %v", err)
	}
	gin.SetMode(gin.TestMode)
	routerA := gin.New()
	routerA.Use(middlewares.TraceMiddleware(), middlewares.ErrorHandlingMiddleware())
	routerA.Use(func(c *gin.Context) {
		c.Set("user_id", uint(42))
		c.Next()
	})
	routerA.POST("/api/agents/:agent_code/agui", aguiHandler.RunAgent)
	serverA := httptest.NewServer(routerA)
	t.Cleanup(serverA.Close)

	requestBody := `{"threadId":"thread-e2e","runId":"run-parent-e2e","state":{},"messages":[{"id":"message-user-e2e","role":"user","content":"draft a release note"}],"tools":[],"context":[]}`
	request, err := http.NewRequest(http.MethodPost, serverA.URL+"/api/agents/planner/agui", strings.NewReader(requestBody))
	if err != nil {
		t.Fatalf("create AG-UI request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Trace-ID", "trace-e2e")
	response, err := serverA.Client().Do(request)
	if err != nil {
		t.Fatalf("send AG-UI request: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read AG-UI stream: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("AG-UI status=%d body=%s", response.StatusCode, body)
	}
	if contentType := response.Header.Get("Content-Type"); !strings.HasPrefix(contentType, "text/event-stream") {
		t.Fatalf("unexpected AG-UI content type %q", contentType)
	}
	if traceID := response.Header.Get("X-Trace-ID"); traceID != "trace-e2e" {
		t.Fatalf("unexpected AG-UI trace id %q", traceID)
	}
	assertAGUIResultStream(t, string(body), "release note ready")
	if providerCalls.Load() != 1 {
		t.Fatalf("Agent B Eino LLM node provider calls=%d, want 1", providerCalls.Load())
	}

	var storedParent models.Run
	if err := databaseA.Where("run_id = ?", "run-parent-e2e").First(&storedParent).Error; err != nil {
		t.Fatalf("load parent run: %v", err)
	}
	if storedParent.Status != models.RunStatusSuccess || storedParent.AgentID != agentA.ID || storedParent.TraceID != "trace-e2e" {
		t.Fatalf("unexpected parent run: %+v", storedParent)
	}
	var parentStep models.RunStep
	if err := databaseA.Where("run_id = ? AND step_key = ?", storedParent.RunID, "delegate").First(&parentStep).Error; err != nil {
		t.Fatalf("load parent delegation step: %v", err)
	}
	if parentStep.Status != models.RunStepStatusSuccess || !strings.Contains(parentStep.OutputJSON, "release note ready") {
		t.Fatalf("unexpected parent step: %+v", parentStep)
	}

	var delegation models.Delegation
	if err := databaseB.Where("parent_run_id = ?", storedParent.RunID).First(&delegation).Error; err != nil {
		t.Fatalf("load delegation: %v", err)
	}
	if delegation.Status != models.DelegationStatusSucceeded || delegation.TraceID != "trace-e2e" || delegation.ThreadID != "thread-e2e" {
		t.Fatalf("unexpected delegation: %+v", delegation)
	}
	if delegation.SourceAgentID == 0 || delegation.TargetAgentID != agentB.ID {
		t.Fatalf("delegation agent links missing: %+v", delegation)
	}

	var childRun models.Run
	if err := databaseB.Where("run_id = ?", delegation.ChildRunID).First(&childRun).Error; err != nil {
		t.Fatalf("load child run: %v", err)
	}
	if childRun.Status != models.RunStatusSuccess || childRun.AgentID != agentB.ID || childRun.WorkflowID != workflowB.ID || childRun.TraceID != "trace-e2e" {
		t.Fatalf("unexpected child run: %+v", childRun)
	}
	var childSteps []models.RunStep
	if err := databaseB.Where("run_id = ?", childRun.RunID).Find(&childSteps).Error; err != nil {
		t.Fatalf("load child steps: %v", err)
	}
	if len(childSteps) != 1 || childSteps[0].Status != models.RunStepStatusSuccess || childSteps[0].StepKey != "write" || !strings.Contains(childSteps[0].OutputJSON, "release note ready") {
		t.Fatalf("unexpected child steps: %+v", childSteps)
	}

	var childMessages []models.Message
	if err := databaseB.Where("delegation_id = ?", delegation.DelegationID).Order("id ASC").Find(&childMessages).Error; err != nil {
		t.Fatalf("load delegation messages: %v", err)
	}
	if len(childMessages) != 2 || childMessages[0].RunID != childRun.RunID || childMessages[1].RunID != childRun.RunID {
		t.Fatalf("unexpected delegation messages: %+v", childMessages)
	}
	if !strings.Contains(childMessages[0].MetadataJSON, "traceId") || !strings.Contains(childMessages[0].MetadataJSON, "delegationId") || !strings.Contains(childMessages[1].ContentJSON, "release note ready") {
		t.Fatalf("delegation correlation or result missing: %+v", childMessages)
	}

	var parentMessages []models.Message
	if err := databaseA.Where("run_id = ?", storedParent.RunID).Order("id ASC").Find(&parentMessages).Error; err != nil {
		t.Fatalf("load parent messages: %v", err)
	}
	if len(parentMessages) != 2 || parentMessages[0].MessageType != models.MessageTypeInput || parentMessages[1].MessageType != models.MessageTypeResult || !strings.Contains(parentMessages[1].ContentJSON, "release note ready") {
		t.Fatalf("unexpected parent messages: %+v", parentMessages)
	}

	pathsMu.Lock()
	seenPaths := append([]string(nil), paths...)
	pathsMu.Unlock()
	if !containsPath(seenPaths, "/a2a/agents/writer/.well-known/agent-card.json") {
		t.Fatalf("A2A Agent Card discovery did not reach target gateway: %v", seenPaths)
	}
	if !containsPath(seenPaths, "/a2a/agents/writer/message:send") {
		t.Fatalf("A2A message:send did not reach target gateway: %v", seenPaths)
	}
}

func assertAGUIResultStream(t *testing.T, body, wantText string) {
	t.Helper()
	var eventTypes []string
	var text string
	for _, frame := range strings.Split(body, "\n\n") {
		for _, line := range strings.Split(frame, "\n") {
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var event map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
				t.Fatalf("decode AG-UI event: %v frame=%s", err, frame)
			}
			eventType, _ := event["type"].(string)
			eventTypes = append(eventTypes, eventType)
			if eventType == "TEXT_MESSAGE_CONTENT" {
				text += event["delta"].(string)
			}
		}
	}
	for _, required := range []string{"RUN_STARTED", "STEP_STARTED", "STEP_FINISHED", "TEXT_MESSAGE_START", "TEXT_MESSAGE_CONTENT", "TEXT_MESSAGE_END", "RUN_FINISHED"} {
		if !containsPath(eventTypes, required) {
			t.Fatalf("AG-UI event %s missing from %v body=%s", required, eventTypes, body)
		}
	}
	if text != wantText {
		t.Fatalf("AG-UI streamed text=%q, want %q body=%s", text, wantText, body)
	}
}

func openE2EDB(t *testing.T, suffix string) *gorm.DB {
	t.Helper()
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:e2e_%s_%d?mode=memory&cache=shared", suffix, time.Now().UnixNano())), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite %s: %v", suffix, err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get sqlite %s: %v", suffix, err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return database
}

func migrateE2ADB(t *testing.T, database *gorm.DB) {
	t.Helper()
	if err := database.AutoMigrate(
		&models.Agent{},
		&models.AgentEndpoint{},
		&models.AgentCapability{},
		&models.Workflow{},
		&models.Thread{},
		&models.Message{},
		&models.Delegation{},
		&models.Run{},
		&models.RunStep{},
		&models.RunIdempotency{},
	); err != nil {
		t.Fatalf("migrate e2e database: %v", err)
	}
}

func seedAgent(t *testing.T, database *gorm.DB, code, name string, owner uint64, definition string) (models.Agent, models.Workflow) {
	t.Helper()
	agent := models.Agent{AgentCode: code, Name: name, OwnerUserID: owner, Status: models.AgentStatusActive}
	if err := database.Create(&agent).Error; err != nil {
		t.Fatalf("create agent %s: %v", code, err)
	}
	workflow := models.Workflow{AgentID: agent.ID, Version: 1, DefinitionJSON: definition, Checksum: code + "-v1", IsActive: true, CreatedBy: owner}
	if err := database.Create(&workflow).Error; err != nil {
		t.Fatalf("create workflow %s: %v", code, err)
	}
	return agent, workflow
}

func seedCapability(t *testing.T, database *gorm.DB, agent models.Agent, code string, workflow models.Workflow) {
	t.Helper()
	capability := models.AgentCapability{
		AgentID: agent.ID, CapabilityCode: code, Name: code, CapabilityType: models.AgentCapabilityTypeWorkflow,
		WorkflowID: &workflow.ID, Version: "1", Status: models.AgentCapabilityStatusActive,
	}
	if err := database.Create(&capability).Error; err != nil {
		t.Fatalf("create capability %s: %v", code, err)
	}
}

func seedEndpoint(t *testing.T, database *gorm.DB, agent models.Agent, address string) {
	t.Helper()
	endpoint := models.AgentEndpoint{
		AgentID: agent.ID, EndpointCode: "primary", Protocol: models.AgentEndpointProtocolA2A,
		Transport: models.AgentEndpointTransportHTTP, Address: address, Status: models.AgentEndpointStatusActive,
	}
	if err := database.Create(&endpoint).Error; err != nil {
		t.Fatalf("create endpoint for %s: %v", agent.AgentCode, err)
	}
}

func containsPath(paths []string, want string) bool {
	for _, path := range paths {
		if path == want {
			return true
		}
	}
	return false
}
