package e2e_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"GoAI/a2aauth"
	"GoAI/a2aclient"
	"GoAI/a2agateway"
	"GoAI/e2e/externalagent"
	"GoAI/models"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"gorm.io/gorm"
)

func TestGoAIInvokesIndependentStandardExternalAgentThroughA2A(t *testing.T) {
	plannerSecret := []byte("test-only-planner-secret-at-least-32-bytes")
	externalSecret := []byte("test-only-external-secret-at-least-32-bytes")
	external, err := externalagent.New("external-writer", "write", externalSecret, map[string][]byte{
		"planner": plannerSecret,
	})
	if err != nil {
		t.Fatalf("create external agent: %v", err)
	}
	externalServer := httptest.NewServer(external)
	t.Cleanup(externalServer.Close)

	database := openE2EDB(t, "a2a_external_outbound")
	migrateE2ADB(t, database)
	planner, _ := seedAgent(t, database, "planner", "Planner", 11, `{"entry_node":"delegate","nodes":[{"key":"delegate","type":"agent","config":{"target_agent":"external-writer","capability":"write","timeout_ms":5000}}],"edges":[]}`)
	seedExternalRegistryAgent(t, database, "external-writer", "write", externalServer.URL+"/a2a/agents/external-writer", "external-key", 22)

	var sourceHandler atomic.Value
	sourceHandler.Store(httpHandlerHolder{handler: http.NotFoundHandler()})
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sourceHandler.Load().(httpHandlerHolder).handler.ServeHTTP(w, r)
	}))
	t.Cleanup(sourceServer.Close)
	seedEndpoint(t, database, planner, sourceServer.URL+"/a2a/agents/planner", "planner-key")
	resolver, err := a2aauth.NewStaticCredentialResolver(map[string]string{
		"planner-key":  string(plannerSecret),
		"external-key": string(externalSecret),
	})
	if err != nil {
		t.Fatalf("create credential resolver: %v", err)
	}
	verifier, err := a2aauth.NewVerifier(resolver, a2aauth.NewMemoryNonceStore(), time.Minute)
	if err != nil {
		t.Fatalf("create verifier: %v", err)
	}
	outbound, err := a2aclient.New(sourceServer.Client(), 5*time.Second,
		a2aclient.WithCallbackBaseURL(sourceServer.URL),
		a2aclient.WithAuthentication(resolver, true),
	)
	if err != nil {
		t.Fatalf("create outbound A2A client: %v", err)
	}

	var runService *services.RunService
	runService, err = services.NewRunService(database, syncRunPublisher{execute: func(ctx context.Context, runID string) error {
		return runService.HandleRunExecute(ctx, runID)
	}}, services.WithAgentInvoker(outbound))
	if err != nil {
		t.Fatalf("create source run service: %v", err)
	}
	resumePublisher := runResumePublisherFunc(func(ctx context.Context, runID, delegationID string) error {
		return runService.HandleRunResume(ctx, runID, delegationID)
	})
	runtime, err := services.NewRuntimeService(database, runService, services.WithRunResumePublisher(resumePublisher))
	if err != nil {
		t.Fatalf("create source runtime: %v", err)
	}
	gateway, err := a2agateway.New(runtime, a2agateway.WithAuthentication(verifier, true))
	if err != nil {
		t.Fatalf("create source gateway: %v", err)
	}
	sourceHandler.Store(httpHandlerHolder{handler: gateway})

	result, err := runService.CreateRun(context.Background(), 42, services.CreateRunRequest{
		AgentCode: "planner", ThreadID: "thread-external-outbound", TriggerType: "api", Input: []byte(`{"prompt":"draft"}`),
	})
	if err != nil {
		t.Fatalf("create parent run: %v", err)
	}
	waitForRunStatus(t, database, result.Run.RunID, models.RunStatusWaitingExternal)

	task := waitForExternalTask(t, external, "")
	if message := external.LastMessage(); message == nil || len(message.Extensions) != 0 || len(message.Metadata) != 0 {
		t.Fatalf("GoAI sent GoAI-only metadata to a standard Agent: %+v", message)
	}
	if err := external.Complete(context.Background(), string(task.ID), a2a.TaskStateCompleted, map[string]any{"answer": "external-ok"}, ""); err != nil {
		t.Fatalf("complete external task: %v", err)
	}

	completed := waitForRunStatus(t, database, result.Run.RunID, models.RunStatusSuccess)
	if completed.Status != models.RunStatusSuccess {
		t.Fatalf("parent run did not complete: %+v", completed)
	}
	var delegation models.Delegation
	if err := database.Where("parent_run_id = ?", result.Run.RunID).First(&delegation).Error; err != nil {
		t.Fatalf("load outbound delegation: %v", err)
	}
	if delegation.Status != models.DelegationStatusSucceeded || delegation.TargetAgentID == 0 {
		t.Fatalf("outbound delegation did not converge: %+v", delegation)
	}
	var step models.RunStep
	if err := database.Where("run_id = ? AND step_key = ?", result.Run.RunID, "delegate").First(&step).Error; err != nil {
		t.Fatalf("load outbound run step: %v", err)
	}
	if step.Status != models.RunStepStatusSuccess || !strings.Contains(step.OutputJSON, "external-ok") {
		t.Fatalf("outbound step did not resume from external callback: %+v", step)
	}
}

func TestIndependentExternalAgentSendsStandardMessageQueriesAndCancelsGoAITask(t *testing.T) {
	externalSecret := []byte("test-only-external-secret-at-least-32-bytes")
	writerSecret := []byte("test-only-writer-secret-at-least-32-bytes")
	external, err := externalagent.New("external-writer", "write", externalSecret, map[string][]byte{
		"writer": writerSecret,
	})
	if err != nil {
		t.Fatalf("create external agent: %v", err)
	}
	externalServer := httptest.NewServer(external)
	t.Cleanup(externalServer.Close)

	database := openE2EDB(t, "a2a_external_inbound")
	migrateE2ADB(t, database)
	externalModel := seedExternalRegistryAgent(t, database, "external-writer", "write", externalServer.URL+"/a2a/agents/external-writer", "external-key", 22)
	writer, workflow := seedAgent(t, database, "writer", "Writer", 11, `{"entry_node":"write","nodes":[{"key":"write","type":"noop"}],"edges":[]}`)
	seedCapability(t, database, writer, "write", workflow)

	var targetHandler atomic.Value
	targetHandler.Store(httpHandlerHolder{handler: http.NotFoundHandler()})
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHandler.Load().(httpHandlerHolder).handler.ServeHTTP(w, r)
	}))
	t.Cleanup(targetServer.Close)
	seedEndpoint(t, database, writer, targetServer.URL+"/a2a/agents/writer", "writer-key")
	_ = externalModel

	resolver, err := a2aauth.NewStaticCredentialResolver(map[string]string{
		"external-key": string(externalSecret),
		"writer-key":   string(writerSecret),
	})
	if err != nil {
		t.Fatalf("create credential resolver: %v", err)
	}
	verifier, err := a2aauth.NewVerifier(resolver, a2aauth.NewMemoryNonceStore(), time.Minute)
	if err != nil {
		t.Fatalf("create verifier: %v", err)
	}
	queued := make(chan string, 1)
	runService, err := services.NewRunService(database, queuedRunPublisher{runIDs: queued})
	if err != nil {
		t.Fatalf("create target run service: %v", err)
	}
	callbackSender, err := a2aclient.NewCallbackSender(targetServer.Client(), resolver, true)
	if err != nil {
		t.Fatalf("create callback sender: %v", err)
	}
	runtime, err := services.NewRuntimeService(database, runService,
		services.WithDelegationCallbackSender(callbackSender),
	)
	if err != nil {
		t.Fatalf("create target runtime: %v", err)
	}
	gateway, err := a2agateway.New(runtime, a2agateway.WithAuthentication(verifier, true))
	if err != nil {
		t.Fatalf("create target gateway: %v", err)
	}
	targetHandler.Store(httpHandlerHolder{handler: gateway})

	cardResponse, err := targetServer.Client().Get(targetServer.URL + "/a2a/agents/writer/.well-known/agent-card.json")
	if err != nil {
		t.Fatalf("discover GoAI Agent Card: %v", err)
	}
	defer cardResponse.Body.Close()
	if cardResponse.StatusCode != http.StatusOK {
		t.Fatalf("GoAI Agent Card status=%d", cardResponse.StatusCode)
	}

	messageID := "external-message-1"
	taskID := "external-task-1"
	callbackURL := externalServer.URL + "/a2a/agents/external-writer/callbacks/tasks/" + taskID
	task, err := external.SendMessage(context.Background(), targetServer.Client(), targetServer.URL+"/a2a/agents/writer", taskID, "external-thread", messageID, callbackURL, "external-callback-token", map[string]any{"prompt": "cancel me"})
	if err != nil {
		t.Fatalf("external standard message: %v", err)
	}
	if task.ID != a2a.TaskID(taskID) || task.Status.State != a2a.TaskStateSubmitted {
		t.Fatalf("unexpected accepted standard task: %+v", task)
	}

	unsignedBody, _ := json.Marshal(&a2a.SendMessageRequest{Message: &a2a.Message{ID: "unsigned", Role: a2a.MessageRoleUser, Parts: a2a.ContentParts{a2a.NewTextPart("unauthorized")}}})
	unsigned, err := targetServer.Client().Post(targetServer.URL+"/a2a/agents/writer/message:send", "application/json", strings.NewReader(string(unsignedBody)))
	if err != nil {
		t.Fatalf("unsigned message request: %v", err)
	}
	unsigned.Body.Close()
	if unsigned.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unsigned message status=%d want unauthorized", unsigned.StatusCode)
	}

	queried, err := external.GetRemoteTask(context.Background(), targetServer.Client(), targetServer.URL+"/a2a/agents/writer", taskID)
	if err != nil {
		t.Fatalf("query GoAI task: %v", err)
	}
	if queried.ID != a2a.TaskID(taskID) || queried.Status.State != a2a.TaskStateSubmitted {
		t.Fatalf("unexpected queried task: %+v", queried)
	}
	cancelled, err := external.CancelRemoteTask(context.Background(), targetServer.Client(), targetServer.URL+"/a2a/agents/writer", taskID)
	if err != nil {
		t.Fatalf("cancel GoAI task: %v", err)
	}
	if cancelled.Status.State != a2a.TaskStateCanceled {
		t.Fatalf("cancelled task state=%s want canceled", cancelled.Status.State)
	}

	callbacks := waitForExternalCallbacks(t, external, 1)
	if callbacks[0].TaskID != taskID || callbacks[0].State != a2a.TaskStateCanceled || callbacks[0].Token != "external-callback-token" {
		t.Fatalf("unexpected cancellation callback: %+v", callbacks)
	}
	if _, err := external.CancelRemoteTask(context.Background(), targetServer.Client(), targetServer.URL+"/a2a/agents/writer", taskID); err != nil {
		t.Fatalf("repeated cancel should be idempotent: %v", err)
	}
	if len(external.Callbacks()) != 1 {
		t.Fatalf("repeated cancel produced duplicate callback: %+v", external.Callbacks())
	}

	var delegation models.Delegation
	if err := database.Where("child_run_id = ?", taskID).First(&delegation).Error; err != nil {
		t.Fatalf("load inbound delegation: %v", err)
	}
	if delegation.Status != models.DelegationStatusCancelled || delegation.CapabilityCode != "write" || delegation.SourceAgentID != externalModel.ID {
		t.Fatalf("standard inbound delegation did not preserve domain state: %+v", delegation)
	}
	var run models.Run
	if err := database.Where("run_id = ?", taskID).First(&run).Error; err != nil {
		t.Fatalf("load inbound child run: %v", err)
	}
	if run.Status != models.RunStatusCancelled || !strings.HasPrefix(delegation.ParentRunID, "external-parent_") {
		t.Fatalf("standard inbound run correlation is invalid: run=%+v delegation=%+v", run, delegation)
	}
}

func seedExternalRegistryAgent(t *testing.T, database *gorm.DB, code, capabilityCode, address, credentialRef string, owner uint64) models.Agent {
	t.Helper()
	agent := models.Agent{AgentCode: code, Name: code, OwnerUserID: owner, Status: models.AgentStatusActive}
	if err := database.Create(&agent).Error; err != nil {
		t.Fatalf("create external registry agent %s: %v", code, err)
	}
	capability := models.AgentCapability{
		AgentID: agent.ID, CapabilityCode: capabilityCode, Name: capabilityCode,
		CapabilityType: models.AgentCapabilityTypeRemote, Version: "1", Status: models.AgentCapabilityStatusActive,
	}
	if err := database.Create(&capability).Error; err != nil {
		t.Fatalf("create external capability %s: %v", code, err)
	}
	endpoint := models.AgentEndpoint{
		AgentID: agent.ID, EndpointCode: "primary", Protocol: models.AgentEndpointProtocolA2A,
		Transport: models.AgentEndpointTransportHTTP, Address: address,
		AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: credentialRef,
		Status: models.AgentEndpointStatusActive,
	}
	if err := database.Create(&endpoint).Error; err != nil {
		t.Fatalf("create external endpoint %s: %v", code, err)
	}
	return agent
}

func waitForExternalTask(t *testing.T, agent *externalagent.Agent, taskID string) *a2a.Task {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if task, ok := agent.Task(taskID); ok {
			return task
		}
		if taskID != "" && time.Now().After(deadline) {
			t.Fatalf("external task %s was not created", taskID)
		}
		if taskID == "" && agent.LastMessage() != nil {
			if task, ok := agent.Task(string(agent.LastMessage().TaskID)); ok {
				return task
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("external task was not created")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitForExternalCallbacks(t *testing.T, agent *externalagent.Agent, count int) []externalagent.Callback {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		callbacks := agent.Callbacks()
		if len(callbacks) >= count {
			return callbacks
		}
		if time.Now().After(deadline) {
			t.Fatalf("external callbacks=%d want at least %d", len(callbacks), count)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
