package services

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"GoAI/models"
	"GoAI/observability"
	"GoAI/requestctx"

	"gorm.io/gorm"
)

func newTestObservabilityBundle(t *testing.T, logs *bytes.Buffer) *observability.Bundle {
	t.Helper()
	metrics, err := observability.NewMetrics()
	if err != nil {
		t.Fatalf("create metrics failed: %v", err)
	}
	return &observability.Bundle{
		Logger:  slog.New(slog.NewJSONHandler(logs, nil)),
		Metrics: metrics,
		Tracer:  observability.NewNoopTracer(),
	}
}

func scrapeTestMetrics(t *testing.T, metrics *observability.Metrics) string {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics endpoint returned %d", recorder.Code)
	}
	return recorder.Body.String()
}

func TestDelegationObservabilityRecordsTraceAndBusinessKeys(t *testing.T) {
	fixture := setupDelegationFixture(t)
	var logs bytes.Buffer
	bundle := newTestObservabilityBundle(t, &logs)
	fixture.runtime.observability = bundle

	ctx := requestctx.WithTraceID(context.Background(), "trace-delegation-test")
	result, err := fixture.runtime.AcceptDelegation(ctx, delegationCommand())
	if err != nil {
		t.Fatalf("accept delegation failed: %v", err)
	}
	if result == nil || result.Delegation == nil {
		t.Fatal("expected delegation result")
	}
	if _, err := fixture.runtime.DelegationSnapshot(ctx, "writer", "run_child"); err != nil {
		t.Fatalf("load delegation snapshot failed: %v", err)
	}
	if err := fixture.runtime.ReconcileDelegation(ctx, "run_child"); err != nil {
		t.Fatalf("reconcile delegation failed: %v", err)
	}

	metricsText := scrapeTestMetrics(t, bundle.Metrics)
	if !strings.Contains(metricsText, `goai_delegation_events_total{status="success"}`) {
		t.Fatalf("delegation metric missing from exposition:\n%s", metricsText)
	}
	for _, field := range []string{
		`"trace_id":"trace-delegation-test"`,
		`"delegation_id":"`,
		`"parent_run_id":"run_parent"`,
		`"child_run_id":"run_child"`,
		`"thread_id":"thread_shared"`,
	} {
		if !strings.Contains(logs.String(), field) {
			t.Fatalf("log field %s missing from:\n%s", field, logs.String())
		}
	}
}

func TestMetricsDoNotExposeHighCardinalityBusinessKeys(t *testing.T) {
	metrics, err := observability.NewMetrics()
	if err != nil {
		t.Fatalf("create metrics failed: %v", err)
	}
	metrics.ObserveHTTPRequest(http.MethodPost, "/api/runs", http.StatusAccepted, 10*time.Millisecond)
	metrics.ObserveKafka("publish", "run_execute", "success")
	metrics.ObserveRuntime("start_run", "success", 10*time.Millisecond)
	metrics.ObserveRun("a2a", "success", 10*time.Millisecond)
	metrics.ObserveDelegation("success")
	metrics.ObserveA2A("send_task", "success")
	metrics.ObserveLoop("run", "success", 10*time.Millisecond)

	metricsText := scrapeTestMetrics(t, metrics)
	for _, label := range []string{
		"trace_id=",
		"run_id=",
		"thread_id=",
		"delegation_id=",
		"provider=",
		"model=",
		"agent_code=",
	} {
		if strings.Contains(metricsText, label) {
			t.Fatalf("high-cardinality label %s found in metrics:\n%s", label, metricsText)
		}
	}
}

func TestRunServiceLoopObservabilityUsesRunAndStepTypes(t *testing.T) {
	database := openSQLiteTestDB(t)
	if err := database.AutoMigrate(&models.Run{}, &models.RunStep{}, &models.LoopRecord{}); err != nil {
		t.Fatalf("auto migrate run and loop models failed: %v", err)
	}

	var logs bytes.Buffer
	bundle := newTestObservabilityBundle(t, &logs)
	loopService, err := NewLoopService(database, WithLoopObservability(bundle))
	if err != nil {
		t.Fatalf("create loop service failed: %v", err)
	}
	runService, err := NewRunService(database, RunEventPublisherFunc(func(context.Context, string) error { return nil }), WithLoopService(loopService), WithRunObservability(bundle))
	if err != nil {
		t.Fatalf("create run service failed: %v", err)
	}

	rootLoopID := "loop-root-observe"
	run := &models.Run{
		RunID:       "run-observe",
		ThreadID:    "thread-observe",
		TraceID:     "trace-observe",
		LoopID:      &rootLoopID,
		AgentID:     1,
		WorkflowID:  1,
		UserID:      1,
		TriggerType: "api",
		InputJSON:   `{}`,
		Status:      models.RunStatusRunning,
	}
	if err := database.Create(run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	ctx := requestctx.WithTraceID(context.Background(), run.TraceID)
	if err := database.Transaction(func(tx *gorm.DB) error {
		return runService.startRunLoopTx(ctx, tx, run)
	}); err != nil {
		t.Fatalf("start run loop failed: %v", err)
	}
	if err := database.Transaction(func(tx *gorm.DB) error {
		return runService.finishRunLoopTx(ctx, tx, run, models.RunStatusSuccess, "")
	}); err != nil {
		t.Fatalf("finish run loop failed: %v", err)
	}

	step, err := runService.startRunStep(ctx, run, WorkflowNode{Key: "draft", Type: "llm"}, 1)
	if err != nil {
		t.Fatalf("start step failed: %v", err)
	}
	finishedAt := time.Now()
	if err := runService.finishRunStep(ctx, run, step, models.RunStepStatusSuccess, `{"text":"ok"}`, "", 3, &finishedAt); err != nil {
		t.Fatalf("finish step failed: %v", err)
	}

	metricsText := scrapeTestMetrics(t, bundle.Metrics)
	for _, label := range []string{
		`goai_loop_events_total{loop_type="run",status="success"}`,
		`goai_loop_events_total{loop_type="step",status="success"}`,
	} {
		if !strings.Contains(metricsText, label) {
			t.Fatalf("loop metric %s missing from exposition:\n%s", label, metricsText)
		}
	}
	for _, field := range []string{
		`"trace_id":"trace-observe"`,
		`"run_id":"run-observe"`,
		`"thread_id":"thread-observe"`,
	} {
		if !strings.Contains(logs.String(), field) {
			t.Fatalf("log field %s missing from:\n%s", field, logs.String())
		}
	}
}
