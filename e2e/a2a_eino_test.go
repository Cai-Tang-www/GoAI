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

	"GoAI/a2aauth"
	"GoAI/a2aclient"
	"GoAI/a2agateway"
	"GoAI/ai"
	"GoAI/config"
	"GoAI/handlers"
	"GoAI/middlewares"
	"GoAI/models"
	"GoAI/requestctx"
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

type queuedRunPublisher struct {
	runIDs chan string
}

func (p queuedRunPublisher) PublishRunExecute(ctx context.Context, runID string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.runIDs <- runID:
		return nil
	}
}

type runResumePublisherFunc func(context.Context, string, string) error

type httpHandlerHolder struct {
	handler http.Handler
}

func (f runResumePublisherFunc) PublishRunResume(ctx context.Context, runID, delegationID string) error {
	return f(ctx, runID, delegationID)
}

func TestAGUIRequestDelegatesThroughA2AToEinoAgentAndStreamsResult(t *testing.T) {
	databaseA := openE2EDB(t, "agent_a")
	databaseB := openE2EDB(t, "agent_b")
	migrateE2ADB(t, databaseA)
	migrateE2ADB(t, databaseB)

	agentA, _ := seedAgent(t, databaseA, "planner", "Planner Agent", 11, `{"entry_node":"delegate","nodes":[{"key":"delegate","type":"agent","config":{"capability":"write","routing_policy":"registry","timeout_ms":5000}}],"edges":[]}`)
	agentBInA, workflowBInA := seedAgent(t, databaseA, "writer", "Writer Agent", 22, `{"entry_node":"write","nodes":[{"key":"write","type":"llm"}],"edges":[]}`)
	seedCapability(t, databaseA, agentBInA, "write", workflowBInA)

	plannerB, _ := seedAgent(t, databaseB, "planner", "Planner Agent", 11, `{"entry_node":"write","nodes":[{"key":"write","type":"noop"}],"edges":[]}`)
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

	credentialResolver, err := a2aauth.NewStaticCredentialResolver(map[string]string{
		"planner-key": "test-only-planner-a2a-secret-at-least-32-bytes",
		"writer-key":  "test-only-writer-a2a-secret-at-least-32-bytes",
	})
	if err != nil {
		t.Fatalf("create A2A credential resolver: %v", err)
	}
	verifier, err := a2aauth.NewVerifier(credentialResolver, a2aauth.NewMemoryNonceStore(), time.Minute)
	if err != nil {
		t.Fatalf("create A2A verifier: %v", err)
	}

	var sourceHandler atomic.Value
	sourceHandler.Store(httpHandlerHolder{handler: http.NotFoundHandler()})
	var sourcePathsMu sync.Mutex
	var sourcePaths []string
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourcePathsMu.Lock()
		sourcePaths = append(sourcePaths, r.URL.Path)
		sourcePathsMu.Unlock()
		sourceHandler.Load().(httpHandlerHolder).handler.ServeHTTP(w, r)
	}))
	t.Cleanup(serverA.Close)

	callbackSender, err := a2aclient.NewCallbackSender(serverA.Client(), credentialResolver, true)
	if err != nil {
		t.Fatalf("create A2A callback sender: %v", err)
	}
	childRunIDs := make(chan string, 1)
	childService, err := services.NewRunService(databaseB, queuedRunPublisher{runIDs: childRunIDs}, services.WithChatService(chatService))
	if err != nil {
		t.Fatalf("create child run service: %v", err)
	}
	runtimeB, err := services.NewRuntimeService(databaseB, childService, services.WithDelegationCallbackSender(callbackSender))
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	gatewayB, err := a2agateway.New(runtimeB, a2agateway.WithAuthentication(verifier, true))
	if err != nil {
		t.Fatalf("create target gateway: %v", err)
	}

	var targetPathsMu sync.Mutex
	var targetPaths []string
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetPathsMu.Lock()
		targetPaths = append(targetPaths, r.URL.Path)
		targetPathsMu.Unlock()
		gatewayB.ServeHTTP(w, r)
	}))
	t.Cleanup(serverB.Close)

	seedEndpoint(t, databaseB, agentB, serverB.URL+"/a2a/agents/writer", "writer-key")
	seedEndpoint(t, databaseB, plannerB, serverA.URL+"/a2a/agents/planner", "planner-key")
	seedEndpoint(t, databaseA, agentBInA, serverB.URL+"/a2a/agents/writer", "writer-key")
	seedEndpoint(t, databaseA, agentA, serverA.URL+"/a2a/agents/planner", "planner-key")

	outbound, err := a2aclient.New(serverB.Client(), 5*time.Second, a2aclient.WithCallbackBaseURL(serverA.URL), a2aclient.WithAuthentication(credentialResolver, true))
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
	resumePublisher := runResumePublisherFunc(func(ctx context.Context, runID, delegationID string) error {
		return parentService.HandleRunResume(ctx, runID, delegationID)
	})
	runtimeA, err := services.NewRuntimeService(databaseA, parentService, services.WithRunResumePublisher(resumePublisher))
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	gatewayA, err := a2agateway.New(runtimeA, a2agateway.WithAuthentication(verifier, true))
	if err != nil {
		t.Fatalf("create source gateway: %v", err)
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
	muxA := http.NewServeMux()
	muxA.Handle("/api/", routerA)
	muxA.Handle("/a2a/", gatewayA)
	sourceHandler.Store(httpHandlerHolder{handler: muxA})

	type aguiResult struct {
		status int
		header http.Header
		body   []byte
		err    error
	}
	responseCh := make(chan aguiResult, 1)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	go func() {
		requestBody := `{"threadId":"thread-e2e","runId":"run-parent-e2e","state":{},"messages":[{"id":"message-user-e2e","role":"user","content":"draft a release note"}],"tools":[],"context":[]}`
		request, requestErr := http.NewRequestWithContext(requestCtx, http.MethodPost, serverA.URL+"/api/agents/planner/agui", strings.NewReader(requestBody))
		if requestErr != nil {
			responseCh <- aguiResult{err: requestErr}
			return
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Trace-ID", "trace-e2e")
		response, requestErr := serverA.Client().Do(request)
		if requestErr != nil {
			responseCh <- aguiResult{err: requestErr}
			return
		}
		defer response.Body.Close()
		body, readErr := io.ReadAll(response.Body)
		responseCh <- aguiResult{status: response.StatusCode, header: response.Header.Clone(), body: body, err: readErr}
	}()

	var childRunID string
	select {
	case childRunID = <-childRunIDs:
	case <-time.After(3 * time.Second):
		t.Fatal("target A2A gateway did not enqueue child run")
	}
	waitForRunStatus(t, databaseA, "run-parent-e2e", models.RunStatusWaitingExternal)
	var waitingStep models.RunStep
	if err := databaseA.Where("run_id = ? AND step_key = ?", "run-parent-e2e", "delegate").First(&waitingStep).Error; err != nil {
		t.Fatalf("load waiting parent step: %v", err)
	}
	if waitingStep.Status != models.RunStepStatusWaitingExternal {
		t.Fatalf("parent step status before callback=%s want=%s", waitingStep.Status, models.RunStepStatusWaitingExternal)
	}

	if err := childService.HandleRunExecute(context.Background(), childRunID); err != nil {
		t.Fatalf("execute child run: %v", err)
	}
	if err := runtimeB.ReconcileDelegation(context.Background(), childRunID); err != nil {
		t.Fatalf("reconcile child delegation and send callback: %v", err)
	}

	var agui aguiResult
	select {
	case agui = <-responseCh:
	case <-time.After(3 * time.Second):
		t.Fatal("AG-UI stream did not finish after A2A callback")
	}
	if agui.err != nil {
		t.Fatalf("AG-UI request failed: %v", agui.err)
	}
	if agui.status != http.StatusOK {
		t.Fatalf("AG-UI status=%d body=%s", agui.status, agui.body)
	}
	if contentType := agui.header.Get("Content-Type"); !strings.HasPrefix(contentType, "text/event-stream") {
		t.Fatalf("unexpected AG-UI content type %q", contentType)
	}
	if traceID := agui.header.Get("X-Trace-ID"); traceID != "trace-e2e" {
		t.Fatalf("unexpected AG-UI trace id %q", traceID)
	}
	assertAGUIResultStream(t, string(agui.body), "release note ready")
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

	var sourceDelegation models.Delegation
	if err := databaseA.Where("parent_run_id = ?", storedParent.RunID).First(&sourceDelegation).Error; err != nil {
		t.Fatalf("load source delegation: %v", err)
	}
	if sourceDelegation.Status != models.DelegationStatusSucceeded || sourceDelegation.ResumeStatus != models.DelegationResumeStatusCompleted || sourceDelegation.CallbackEventHash == "" {
		t.Fatalf("source callback did not complete resume: %+v", sourceDelegation)
	}

	var targetDelegation models.Delegation
	if err := databaseB.Where("child_run_id = ?", childRunID).First(&targetDelegation).Error; err != nil {
		t.Fatalf("load target delegation: %v", err)
	}
	if targetDelegation.Status != models.DelegationStatusSucceeded || targetDelegation.TraceID != "trace-e2e" || targetDelegation.ThreadID != "thread-e2e" {
		t.Fatalf("unexpected target delegation: %+v", targetDelegation)
	}

	var childRun models.Run
	if err := databaseB.Where("run_id = ?", childRunID).First(&childRun).Error; err != nil {
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

	var targetMessages []models.Message
	if err := databaseB.Where("delegation_id = ?", targetDelegation.DelegationID).Order("id ASC").Find(&targetMessages).Error; err != nil {
		t.Fatalf("load target delegation messages: %v", err)
	}
	if len(targetMessages) != 2 || targetMessages[0].RunID != childRun.RunID || targetMessages[1].RunID != storedParent.RunID {
		t.Fatalf("unexpected target delegation messages: %+v", targetMessages)
	}
	var sourceResult models.Message
	if err := databaseA.Where("delegation_id = ? AND message_type = ?", sourceDelegation.DelegationID, models.MessageTypeResult).First(&sourceResult).Error; err != nil {
		t.Fatalf("load source callback result message: %v", err)
	}
	if sourceResult.MessageID != targetMessages[1].MessageID || sourceResult.RunID != storedParent.RunID || !strings.Contains(sourceResult.ContentJSON, "release note ready") {
		t.Fatalf("callback result message is not correlated across runtimes: source=%+v target=%+v", sourceResult, targetMessages[1])
	}

	targetPathsMu.Lock()
	seenTargetPaths := append([]string(nil), targetPaths...)
	targetPathsMu.Unlock()
	if !containsPath(seenTargetPaths, "/a2a/agents/writer/.well-known/agent-card.json") {
		t.Fatalf("A2A Agent Card discovery did not reach target gateway: %v", seenTargetPaths)
	}
	if !containsPath(seenTargetPaths, "/a2a/agents/writer/message:send") {
		t.Fatalf("A2A message:send did not reach target gateway: %v", seenTargetPaths)
	}
	sourcePathsMu.Lock()
	seenSourcePaths := append([]string(nil), sourcePaths...)
	sourcePathsMu.Unlock()
	callbackPath := "/a2a/agents/planner/callbacks/tasks/" + sourceDelegation.ChildRunID
	if !containsPath(seenSourcePaths, callbackPath) {
		t.Fatalf("A2A callback did not reach source gateway: want=%s paths=%v", callbackPath, seenSourcePaths)
	}
}

func waitForRunStatus(t *testing.T, database *gorm.DB, runID, status string) models.Run {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var run models.Run
		if err := database.Where("run_id = ?", runID).First(&run).Error; err == nil && run.Status == status {
			return run
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach status %s", runID, status)
		}
		time.Sleep(10 * time.Millisecond)
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
		&models.DelegationGroup{},
		&models.A2APushConfig{},
		&models.Run{},
		&models.RunStep{},
		&models.RunInterrupt{},
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

func seedEndpoint(t *testing.T, database *gorm.DB, agent models.Agent, address, credentialRef string) {
	t.Helper()
	endpoint := models.AgentEndpoint{
		AgentID: agent.ID, EndpointCode: "primary", Protocol: models.AgentEndpointProtocolA2A,
		Transport: models.AgentEndpointTransportHTTP, Address: address,
		AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: credentialRef,
		Status: models.AgentEndpointStatusActive,
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

func TestAgentGroupDelegatesThroughA2AHTTPToTwoChildRuns(t *testing.T) {
	databaseA := openE2EDB(t, "agent_group_source")
	databaseSecurity := openE2EDB(t, "agent_group_security")
	databaseQuality := openE2EDB(t, "agent_group_quality")
	migrateE2ADB(t, databaseA)
	migrateE2ADB(t, databaseSecurity)
	migrateE2ADB(t, databaseQuality)

	parentDefinition := `{
		"entry_node":"parallel_review",
		"nodes":[
			{"key":"parallel_review","type":"agent_group","config":{
				"strategy":"all",
				"members":[
					{"key":"security","target_agent":"security-reviewer","capability":"review","timeout_ms":5000},
					{"key":"quality","target_agent":"quality-reviewer","capability":"review","timeout_ms":5000}
				]
			}},
			{"key":"finalize","type":"noop","config":{"input_from":["parallel_review"]}}
		],
		"edges":[{"from":"parallel_review","to":"finalize"}]
	}`
	plannerA, _ := seedAgent(t, databaseA, "planner-group", "Planner Group Agent", 11, parentDefinition)
	securityInA, securityWorkflowInA := seedAgent(t, databaseA, "security-reviewer", "Security Reviewer", 22, `{"entry_node":"review","nodes":[{"key":"review","type":"llm"}],"edges":[]}`)
	qualityInA, qualityWorkflowInA := seedAgent(t, databaseA, "quality-reviewer", "Quality Reviewer", 23, `{"entry_node":"review","nodes":[{"key":"review","type":"llm"}],"edges":[]}`)
	seedCapability(t, databaseA, securityInA, "review", securityWorkflowInA)
	seedCapability(t, databaseA, qualityInA, "review", qualityWorkflowInA)

	plannerSecurity, _ := seedAgent(t, databaseSecurity, "planner-group", "Planner Group Agent", 11, `{"entry_node":"noop","nodes":[{"key":"noop","type":"noop"}],"edges":[]}`)
	securityAgent, securityWorkflow := seedAgent(t, databaseSecurity, "security-reviewer", "Security Reviewer", 22, `{"entry_node":"review","nodes":[{"key":"review","type":"llm"}],"edges":[]}`)
	seedCapability(t, databaseSecurity, securityAgent, "review", securityWorkflow)
	plannerQuality, _ := seedAgent(t, databaseQuality, "planner-group", "Planner Group Agent", 11, `{"entry_node":"noop","nodes":[{"key":"noop","type":"noop"}],"edges":[]}`)
	qualityAgent, qualityWorkflow := seedAgent(t, databaseQuality, "quality-reviewer", "Quality Reviewer", 23, `{"entry_node":"review","nodes":[{"key":"review","type":"llm"}],"edges":[]}`)
	seedCapability(t, databaseQuality, qualityAgent, "review", qualityWorkflow)

	var providerCalls atomic.Int32
	providerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls.Add(1)
		if r.URL.Path != "/chat/completions" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"review complete\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(providerServer.Close)
	chatService, err := services.NewChatService(&config.Config{
		ModelProviderDefault: "test",
		ModelProviders: map[string]config.ModelProviderConfig{
			"test": {
				Driver: ai.DriverOpenAICompatible, BaseURL: providerServer.URL, APIKey: "test-key",
				DefaultModel: "test-model", EndpointPath: "/chat/completions",
			},
		},
	}, providerServer.Client())
	if err != nil {
		t.Fatalf("create shared chat service: %v", err)
	}

	credentialResolver, err := a2aauth.NewStaticCredentialResolver(map[string]string{
		"planner-group-key": "test-only-planner-group-secret-at-least-32-bytes",
		"security-key":      "test-only-security-secret-at-least-32-bytes",
		"quality-key":       "test-only-quality-secret-at-least-32-bytes",
	})
	if err != nil {
		t.Fatalf("create group A2A credential resolver: %v", err)
	}
	verifier, err := a2aauth.NewVerifier(credentialResolver, a2aauth.NewMemoryNonceStore(), time.Minute)
	if err != nil {
		t.Fatalf("create group A2A verifier: %v", err)
	}

	var sourceHandler atomic.Value
	sourceHandler.Store(httpHandlerHolder{handler: http.NotFoundHandler()})
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceHandler.Load().(httpHandlerHolder).handler.ServeHTTP(w, r)
	}))
	t.Cleanup(serverA.Close)
	callbackSender, err := a2aclient.NewCallbackSender(serverA.Client(), credentialResolver, true)
	if err != nil {
		t.Fatalf("create group callback sender: %v", err)
	}

	securityRunIDs := make(chan string, 1)
	securityService, err := services.NewRunService(databaseSecurity, queuedRunPublisher{runIDs: securityRunIDs}, services.WithChatService(chatService))
	if err != nil {
		t.Fatalf("create security run service: %v", err)
	}
	securityRuntime, err := services.NewRuntimeService(databaseSecurity, securityService, services.WithDelegationCallbackSender(callbackSender))
	if err != nil {
		t.Fatalf("create security runtime: %v", err)
	}
	securityGateway, err := a2agateway.New(securityRuntime, a2agateway.WithAuthentication(verifier, true))
	if err != nil {
		t.Fatalf("create security gateway: %v", err)
	}
	var securityPathsMu sync.Mutex
	var securityPaths []string
	securityServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		securityPathsMu.Lock()
		securityPaths = append(securityPaths, r.URL.Path)
		securityPathsMu.Unlock()
		securityGateway.ServeHTTP(w, r)
	}))
	t.Cleanup(securityServer.Close)

	qualityRunIDs := make(chan string, 1)
	qualityService, err := services.NewRunService(databaseQuality, queuedRunPublisher{runIDs: qualityRunIDs}, services.WithChatService(chatService))
	if err != nil {
		t.Fatalf("create quality run service: %v", err)
	}
	qualityRuntime, err := services.NewRuntimeService(databaseQuality, qualityService, services.WithDelegationCallbackSender(callbackSender))
	if err != nil {
		t.Fatalf("create quality runtime: %v", err)
	}
	qualityGateway, err := a2agateway.New(qualityRuntime, a2agateway.WithAuthentication(verifier, true))
	if err != nil {
		t.Fatalf("create quality gateway: %v", err)
	}
	var qualityPathsMu sync.Mutex
	var qualityPaths []string
	qualityServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		qualityPathsMu.Lock()
		qualityPaths = append(qualityPaths, r.URL.Path)
		qualityPathsMu.Unlock()
		qualityGateway.ServeHTTP(w, r)
	}))
	t.Cleanup(qualityServer.Close)

	seedEndpoint(t, databaseSecurity, securityAgent, securityServer.URL+"/a2a/agents/security-reviewer", "security-key")
	seedEndpoint(t, databaseSecurity, plannerSecurity, serverA.URL+"/a2a/agents/planner-group", "planner-group-key")
	seedEndpoint(t, databaseQuality, qualityAgent, qualityServer.URL+"/a2a/agents/quality-reviewer", "quality-key")
	seedEndpoint(t, databaseQuality, plannerQuality, serverA.URL+"/a2a/agents/planner-group", "planner-group-key")
	seedEndpoint(t, databaseA, securityInA, securityServer.URL+"/a2a/agents/security-reviewer", "security-key")
	seedEndpoint(t, databaseA, qualityInA, qualityServer.URL+"/a2a/agents/quality-reviewer", "quality-key")
	seedEndpoint(t, databaseA, plannerA, serverA.URL+"/a2a/agents/planner-group", "planner-group-key")

	outbound, err := a2aclient.New(&http.Client{}, 5*time.Second, a2aclient.WithCallbackBaseURL(serverA.URL), a2aclient.WithAuthentication(credentialResolver, true))
	if err != nil {
		t.Fatalf("create group A2A client: %v", err)
	}
	var parentService *services.RunService
	parentPublisher := syncRunPublisher{execute: func(ctx context.Context, runID string) error {
		return parentService.HandleRunExecute(ctx, runID)
	}}
	parentService, err = services.NewRunService(databaseA, parentPublisher, services.WithAgentInvoker(outbound), services.WithChatService(chatService))
	if err != nil {
		t.Fatalf("create group parent service: %v", err)
	}
	var resumeCalls atomic.Int32
	resumePublisher := runResumePublisherFunc(func(ctx context.Context, runID, delegationID string) error {
		resumeCalls.Add(1)
		return parentService.HandleRunResume(ctx, runID, delegationID)
	})
	runtimeA, err := services.NewRuntimeService(databaseA, parentService, services.WithRunResumePublisher(resumePublisher))
	if err != nil {
		t.Fatalf("create group source runtime: %v", err)
	}
	gatewayA, err := a2agateway.New(runtimeA, a2agateway.WithAuthentication(verifier, true))
	if err != nil {
		t.Fatalf("create group source gateway: %v", err)
	}
	muxA := http.NewServeMux()
	muxA.Handle("/a2a/", gatewayA)
	sourceHandler.Store(httpHandlerHolder{handler: muxA})

	ctx := requestctx.WithTraceID(context.Background(), "trace-agent-group-e2e")
	result, err := parentService.CreateRun(ctx, 42, services.CreateRunRequest{
		AgentCode: "planner-group", ThreadID: "thread-agent-group-e2e", TriggerType: "api",
		Input: json.RawMessage(`{"prompt":"review this release"}`),
	})
	if err != nil {
		t.Fatalf("create parent agent group run: %v", err)
	}
	parentRunID := result.Run.RunID
	waitForRunStatus(t, databaseA, parentRunID, models.RunStatusWaitingExternal)

	var securityChildID, qualityChildID string
	select {
	case securityChildID = <-securityRunIDs:
	case <-time.After(3 * time.Second):
		t.Fatal("security A2A gateway did not enqueue a child run")
	}
	select {
	case qualityChildID = <-qualityRunIDs:
	case <-time.After(3 * time.Second):
		t.Fatal("quality A2A gateway did not enqueue a child run")
	}
	if securityChildID == qualityChildID {
		t.Fatalf("agent group reused child run id %s", securityChildID)
	}

	if err := securityService.HandleRunExecute(context.Background(), securityChildID); err != nil {
		t.Fatalf("execute security child run: %v", err)
	}
	if err := securityRuntime.ReconcileDelegation(context.Background(), securityChildID); err != nil {
		t.Fatalf("reconcile security child delegation: %v", err)
	}
	waitForRunStatus(t, databaseA, parentRunID, models.RunStatusWaitingExternal)
	if err := qualityService.HandleRunExecute(context.Background(), qualityChildID); err != nil {
		t.Fatalf("execute quality child run: %v", err)
	}
	if err := qualityRuntime.ReconcileDelegation(context.Background(), qualityChildID); err != nil {
		t.Fatalf("reconcile quality child delegation: %v", err)
	}

	parentRun := waitForRunStatus(t, databaseA, parentRunID, models.RunStatusSuccess)
	if parentRun.TraceID != "trace-agent-group-e2e" {
		t.Fatalf("parent trace id=%q", parentRun.TraceID)
	}
	if resumeCalls.Load() != 1 {
		t.Fatalf("group resume calls=%d want=1", resumeCalls.Load())
	}
	if providerCalls.Load() != 2 {
		t.Fatalf("provider calls=%d want=2 (one call per child Agent)", providerCalls.Load())
	}

	var parentSteps []models.RunStep
	if err := databaseA.Where("run_id = ?", parentRunID).Order("id ASC").Find(&parentSteps).Error; err != nil {
		t.Fatalf("load parent group steps: %v", err)
	}
	if len(parentSteps) != 2 || parentSteps[0].StepKey != "parallel_review" || parentSteps[1].StepKey != "finalize" {
		t.Fatalf("unexpected parent group steps: %+v", parentSteps)
	}
	if parentSteps[0].Status != models.RunStepStatusSuccess || parentSteps[1].Status != models.RunStepStatusSuccess {
		t.Fatalf("parent group steps did not succeed: %+v", parentSteps)
	}

	var group models.DelegationGroup
	if err := databaseA.Where("parent_run_id = ?", parentRunID).First(&group).Error; err != nil {
		t.Fatalf("load persisted delegation group: %v", err)
	}
	if group.Status != models.DelegationGroupStatusSucceeded || group.SucceededMembers != 2 || group.TotalMembers != 2 {
		t.Fatalf("unexpected persisted delegation group: %+v", group)
	}
	var sourceDelegations []models.Delegation
	if err := databaseA.Where("delegation_group_id = ?", group.GroupID).Order("group_member_position ASC").Find(&sourceDelegations).Error; err != nil {
		t.Fatalf("load source group delegations: %v", err)
	}
	if len(sourceDelegations) != 2 || sourceDelegations[0].ChildRunID == sourceDelegations[1].ChildRunID {
		t.Fatalf("unexpected source group delegations: %+v", sourceDelegations)
	}
	for _, delegation := range sourceDelegations {
		if delegation.Status != models.DelegationStatusSucceeded || delegation.CallbackEventHash == "" {
			t.Fatalf("source group delegation was not completed by callback: %+v", delegation)
		}
	}

	for _, target := range []struct {
		database   *gorm.DB
		childRunID string
		agentCode  string
		pathsMu    *sync.Mutex
		paths      *[]string
	}{
		{database: databaseSecurity, childRunID: securityChildID, agentCode: "security-reviewer", pathsMu: &securityPathsMu, paths: &securityPaths},
		{database: databaseQuality, childRunID: qualityChildID, agentCode: "quality-reviewer", pathsMu: &qualityPathsMu, paths: &qualityPaths},
	} {
		var delegation models.Delegation
		if err := target.database.Where("child_run_id = ?", target.childRunID).First(&delegation).Error; err != nil {
			t.Fatalf("load %s target delegation: %v", target.agentCode, err)
		}
		if delegation.Status != models.DelegationStatusSucceeded || delegation.TraceID != "trace-agent-group-e2e" {
			t.Fatalf("unexpected %s target delegation: %+v", target.agentCode, delegation)
		}
		var messageCount int64
		if err := target.database.Model(&models.Message{}).Where("delegation_id = ?", delegation.DelegationID).Count(&messageCount).Error; err != nil {
			t.Fatalf("count %s delegation messages: %v", target.agentCode, err)
		}
		if messageCount != 2 {
			t.Fatalf("%s delegation message count=%d want=2", target.agentCode, messageCount)
		}
		target.pathsMu.Lock()
		paths := append([]string(nil), (*target.paths)...)
		target.pathsMu.Unlock()
		if !containsPath(paths, "/a2a/agents/"+target.agentCode+"/.well-known/agent-card.json") ||
			!containsPath(paths, "/a2a/agents/"+target.agentCode+"/message:send") {
			t.Fatalf("%s was not reached through A2A HTTP: %v", target.agentCode, paths)
		}
	}

	detail, err := parentService.GetRunDetailByRunID(context.Background(), 42, false, parentRunID)
	if err != nil {
		t.Fatalf("get parent group detail: %v", err)
	}
	if len(detail.DelegationGroups) != 1 || len(detail.DelegationGroups[0].Members) != 2 {
		t.Fatalf("run detail did not expose agent group: %+v", detail.DelegationGroups)
	}
	if detail.DelegationGroups[0].CoordinatorDelegationID != sourceDelegations[0].DelegationID {
		t.Fatalf("coordinator delegation=%s want=%s", detail.DelegationGroups[0].CoordinatorDelegationID, sourceDelegations[0].DelegationID)
	}
}
