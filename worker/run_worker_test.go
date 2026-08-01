package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"GoAI/kafka"
	"GoAI/models"
	"GoAI/services"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type noopDelegationReconciler struct{}

func (noopDelegationReconciler) ReconcileDelegation(context.Context, string) error { return nil }

type recordingDelegationReconciler struct {
	runIDs []string
	err    error
}

func (r *recordingDelegationReconciler) ReconcileDelegation(_ context.Context, runID string) error {
	r.runIDs = append(r.runIDs, runID)
	return r.err
}

type noopRunPublisher struct{}

func (noopRunPublisher) PublishRunExecute(context.Context, string) error { return nil }

var workerDBSequence atomic.Uint64

func openWorkerTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	name := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	database, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s_%d?mode=memory&cache=shared", name, workerDBSequence.Add(1))), &gorm.Config{})
	if err != nil {
		t.Fatalf("open worker database failed: %v", err)
	}
	sqlDB, err := database.DB()
	if err != nil {
		t.Fatalf("get worker SQL database failed: %v", err)
	}
	t.Cleanup(func() {
		if err := sqlDB.Close(); err != nil {
			t.Errorf("close worker database failed: %v", err)
		}
	})
	if err := database.AutoMigrate(
		&models.Agent{}, &models.AgentCapability{}, &models.Workflow{}, &models.Thread{}, &models.Message{},
		&models.Delegation{}, &models.Run{}, &models.RunStep{}, &models.RunIdempotency{},
	); err != nil {
		t.Fatalf("auto migrate worker database failed: %v", err)
	}
	return database
}

func createWorkerRun(t *testing.T, database *gorm.DB) (*services.RunService, *models.Run) {
	t.Helper()
	agent := models.Agent{AgentCode: "worker_agent", Name: "Worker Agent", OwnerUserID: 1, Status: models.AgentStatusActive}
	if err := database.Create(&agent).Error; err != nil {
		t.Fatalf("create worker agent failed: %v", err)
	}
	workflow := models.Workflow{
		AgentID: agent.ID, Version: 1,
		DefinitionJSON: `{"entry_node":"work","nodes":[{"key":"work","type":"noop"}],"edges":[]}`,
		Checksum:       "worker-v1", IsActive: true, CreatedBy: 1,
	}
	if err := database.Create(&workflow).Error; err != nil {
		t.Fatalf("create worker workflow failed: %v", err)
	}
	service, err := services.NewRunService(database, noopRunPublisher{})
	if err != nil {
		t.Fatalf("create run service failed: %v", err)
	}
	result, err := service.CreateRun(context.Background(), 1, services.CreateRunRequest{
		AgentCode: agent.AgentCode, ThreadID: "thread_worker", TriggerType: "api", Input: []byte(`{"prompt":"work"}`),
	})
	if err != nil {
		t.Fatalf("create worker run failed: %v", err)
	}
	return service, result.Run
}

func createWorkerDelegation(t *testing.T, database *gorm.DB) (*services.RunService, *services.RuntimeService, *services.DelegationResult) {
	t.Helper()
	source := models.Agent{AgentCode: "planner", Name: "Planner", OwnerUserID: 1, Status: models.AgentStatusActive}
	target := models.Agent{AgentCode: "writer", Name: "Writer", OwnerUserID: 2, Status: models.AgentStatusActive}
	if err := database.Create(&source).Error; err != nil {
		t.Fatalf("create source agent failed: %v", err)
	}
	if err := database.Create(&target).Error; err != nil {
		t.Fatalf("create target agent failed: %v", err)
	}
	definition := `{"entry_node":"work","nodes":[{"key":"work","type":"noop"}],"edges":[]}`
	sourceWorkflow := models.Workflow{AgentID: source.ID, Version: 1, DefinitionJSON: definition, Checksum: "source", IsActive: true, CreatedBy: 1}
	targetWorkflow := models.Workflow{AgentID: target.ID, Version: 1, DefinitionJSON: definition, Checksum: "target", IsActive: true, CreatedBy: 2}
	if err := database.Create(&sourceWorkflow).Error; err != nil {
		t.Fatalf("create source workflow failed: %v", err)
	}
	if err := database.Create(&targetWorkflow).Error; err != nil {
		t.Fatalf("create target workflow failed: %v", err)
	}
	capability := models.AgentCapability{
		AgentID: target.ID, CapabilityCode: "write", Name: "Write", CapabilityType: models.AgentCapabilityTypeWorkflow,
		WorkflowID: &targetWorkflow.ID, Version: "1", Status: models.AgentCapabilityStatusActive,
	}
	if err := database.Create(&capability).Error; err != nil {
		t.Fatalf("create target capability failed: %v", err)
	}
	parent := models.Run{
		RunID: "run_parent", ThreadID: "thread_delegate", AgentID: source.ID, WorkflowID: sourceWorkflow.ID,
		UserID: 1, TriggerType: "agui", InputJSON: `{}`, Status: models.RunStatusRunning,
	}
	if err := database.Create(&parent).Error; err != nil {
		t.Fatalf("create parent run failed: %v", err)
	}
	service, err := services.NewRunService(database, noopRunPublisher{})
	if err != nil {
		t.Fatalf("create run service failed: %v", err)
	}
	runtimeService, err := services.NewRuntimeService(database, service)
	if err != nil {
		t.Fatalf("create runtime service failed: %v", err)
	}
	result, err := runtimeService.AcceptDelegation(context.Background(), services.AcceptDelegationCommand{
		SourceAgentCode: "planner", TargetAgentCode: "writer", CapabilityCode: "write", ParentRunID: parent.RunID,
		ThreadID: parent.ThreadID, RequestedChildRunID: "run_child", RequestMessageID: "msg_delegate",
		Input: []byte(`{"prompt":"draft"}`), MetadataJSON: `{}`,
	})
	if err != nil {
		t.Fatalf("accept worker delegation failed: %v", err)
	}
	return service, runtimeService, result
}

func TestNewRunWorkerRejectsMissingDependencies(t *testing.T) {
	if _, err := NewRunWorker(nil, noopDelegationReconciler{}); err == nil {
		t.Fatal("expected nil service error")
	}
	if _, err := NewRunWorker(&services.RunService{}, nil); err == nil {
		t.Fatal("expected nil reconciler error")
	}
}

func TestRunWorkerExecutesRunAndAlwaysReconciles(t *testing.T) {
	database := openWorkerTestDB(t)
	service, run := createWorkerRun(t, database)
	reconciler := &recordingDelegationReconciler{}
	worker, err := NewRunWorker(service, reconciler)
	if err != nil {
		t.Fatalf("create worker failed: %v", err)
	}
	if err := worker.HandleRunExecuteMessage(context.Background(), kafka.RunExecuteMessage{RunID: run.RunID}); err != nil {
		t.Fatalf("handle run failed: %v", err)
	}
	var stored models.Run
	if err := database.Where("run_id = ?", run.RunID).First(&stored).Error; err != nil {
		t.Fatalf("load executed run failed: %v", err)
	}
	if stored.Status != models.RunStatusSuccess {
		t.Fatalf("run status got=%s want=%s", stored.Status, models.RunStatusSuccess)
	}
	if len(reconciler.runIDs) != 1 || reconciler.runIDs[0] != run.RunID {
		t.Fatalf("reconciler calls mismatch: %v", reconciler.runIDs)
	}
}

func TestRunWorkerReconcilesChildRunAndDeduplicatesKafkaDelivery(t *testing.T) {
	database := openWorkerTestDB(t)
	service, runtimeService, result := createWorkerDelegation(t, database)
	worker, err := NewRunWorker(service, runtimeService)
	if err != nil {
		t.Fatalf("create worker failed: %v", err)
	}
	message := kafka.RunExecuteMessage{RunID: result.Run.RunID}
	for range 2 {
		if err := worker.HandleRunExecuteMessage(context.Background(), message); err != nil {
			t.Fatalf("handle delegated run failed: %v", err)
		}
	}
	var delegation models.Delegation
	if err := database.Where("child_run_id = ?", result.Run.RunID).First(&delegation).Error; err != nil {
		t.Fatalf("load delegation failed: %v", err)
	}
	if delegation.Status != models.DelegationStatusSucceeded || delegation.ResultMessageID == "" {
		t.Fatalf("delegation was not reconciled: %+v", delegation)
	}
	var stepCount int64
	if err := database.Model(&models.RunStep{}).Where("run_id = ?", result.Run.RunID).Count(&stepCount).Error; err != nil {
		t.Fatalf("count steps failed: %v", err)
	}
	if stepCount != 1 {
		t.Fatalf("duplicate Kafka delivery executed workflow more than once: steps=%d", stepCount)
	}
	var resultCount int64
	if err := database.Model(&models.Message{}).
		Where("delegation_id = ? AND message_type = ?", delegation.DelegationID, models.MessageTypeResult).
		Count(&resultCount).Error; err != nil {
		t.Fatalf("count result messages failed: %v", err)
	}
	if resultCount != 1 {
		t.Fatalf("duplicate Kafka delivery created duplicate result messages: count=%d", resultCount)
	}
}

func TestRunWorkerReconcilesAlreadyFailedChildRun(t *testing.T) {
	database := openWorkerTestDB(t)
	service, runtimeService, result := createWorkerDelegation(t, database)
	if err := database.Model(&models.Run{}).Where("run_id = ?", result.Run.RunID).
		Updates(map[string]any{"status": models.RunStatusFailed, "error_message": "execution failed"}).Error; err != nil {
		t.Fatalf("mark child run failed: %v", err)
	}
	worker, err := NewRunWorker(service, runtimeService)
	if err != nil {
		t.Fatalf("create worker failed: %v", err)
	}
	if err := worker.HandleRunExecuteMessage(context.Background(), kafka.RunExecuteMessage{RunID: result.Run.RunID}); err != nil {
		t.Fatalf("reconcile failed child run: %v", err)
	}
	var delegation models.Delegation
	if err := database.Where("child_run_id = ?", result.Run.RunID).First(&delegation).Error; err != nil {
		t.Fatalf("load failed delegation: %v", err)
	}
	if delegation.Status != models.DelegationStatusFailed {
		t.Fatalf("delegation status got=%s want=%s", delegation.Status, models.DelegationStatusFailed)
	}
}

func TestRunWorkerPreservesExecutionAndReconcileErrors(t *testing.T) {
	database := openWorkerTestDB(t)
	service, run := createWorkerRun(t, database)
	reconcileErr := errors.New("reconcile failed")
	reconciler := &recordingDelegationReconciler{err: reconcileErr}
	worker, err := NewRunWorker(service, reconciler)
	if err != nil {
		t.Fatalf("create worker failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = worker.HandleRunExecuteMessage(ctx, kafka.RunExecuteMessage{RunID: run.RunID})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, reconcileErr) {
		t.Fatalf("joined error lost cause: %v", err)
	}
}
