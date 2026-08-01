package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"GoAI/models"

	"gorm.io/gorm"
)

type recordingLoopEvaluator struct {
	called chan models.LoopRecord
	err    error
}

func (e *recordingLoopEvaluator) Evaluate(_ context.Context, loop models.LoopRecord) (LoopEvaluationResult, error) {
	if e.called != nil {
		e.called <- loop
	}
	if e.err != nil {
		return LoopEvaluationResult{}, e.err
	}
	score := 0.9
	return LoopEvaluationResult{Score: &score, ResultJSON: `{"label":"good"}`}, nil
}

type recordingLoopDispatcher struct {
	requests []LoopEvaluationRequest
}

func (d *recordingLoopDispatcher) Enqueue(_ context.Context, request LoopEvaluationRequest) error {
	d.requests = append(d.requests, request)
	return nil
}

func setupLoopTestService(t *testing.T, options ...LoopServiceOption) (*gorm.DB, *LoopService) {
	t.Helper()
	database := openSQLiteTestDB(t)
	if err := database.AutoMigrate(&models.LoopRecord{}, &models.LoopEvaluation{}); err != nil {
		t.Fatalf("auto migrate loop models failed: %v", err)
	}
	service, err := NewLoopService(database, options...)
	if err != nil {
		t.Fatalf("create loop service failed: %v", err)
	}
	return database, service
}

func TestLoopServiceStartAndFinishPersistsExecutionMetadata(t *testing.T) {
	database, service := setupLoopTestService(t)

	started, err := service.Start(context.Background(), LoopStartRequest{
		LoopID:            "loop-root-1",
		TraceID:           "trace-1",
		ThreadID:          "thread-1",
		RunID:             "run-1",
		ParentLoopID:      "parent-1",
		DelegationID:      "delegation-1",
		AgentID:           7,
		WorkflowID:        11,
		RunStepID:         13,
		LoopType:          models.LoopTypeStep,
		InputSnapshotJSON: `{"z":2,"a":1}`,
		PromptVersion:     "prompt-v1",
		Provider:          "deepseek",
		Model:             "deepseek-chat",
	})
	if err != nil {
		t.Fatalf("start loop failed: %v", err)
	}
	if started.Status != models.LoopStatusRunning {
		t.Fatalf("expected running loop, got %s", started.Status)
	}
	if started.InputSnapshotJSON != `{"a":1,"z":2}` {
		t.Fatalf("input snapshot was not normalized: %s", started.InputSnapshotJSON)
	}

	finishedAt := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	inputTokens := int64(12)
	outputTokens := int64(8)
	totalTokens := int64(20)
	if err := service.Finish(context.Background(), LoopFinishRequest{
		LoopID:             started.LoopID,
		Status:             models.LoopStatusSuccess,
		OutputSnapshotJSON: `{"message":"ok"}`,
		LatencyMS:          42,
		InputTokens:        &inputTokens,
		OutputTokens:       &outputTokens,
		TotalTokens:        &totalTokens,
		FinishedAt:         &finishedAt,
	}); err != nil {
		t.Fatalf("finish loop failed: %v", err)
	}

	var saved models.LoopRecord
	if err := database.Where("loop_id = ?", started.LoopID).First(&saved).Error; err != nil {
		t.Fatalf("load loop failed: %v", err)
	}
	if saved.Status != models.LoopStatusSuccess || saved.FinishedAt == nil {
		t.Fatalf("unexpected finished loop: %+v", saved)
	}
	if saved.OutputSnapshotJSON != `{"message":"ok"}` || saved.LatencyMS != 42 {
		t.Fatalf("execution result not persisted: %+v", saved)
	}
	if saved.InputTokens == nil || *saved.InputTokens != 12 || saved.OutputTokens == nil || *saved.OutputTokens != 8 || saved.TotalTokens == nil || *saved.TotalTokens != 20 {
		t.Fatalf("token usage not persisted: %+v", saved)
	}
}

func TestLoopServiceFinishValidatesTerminalTransitions(t *testing.T) {
	_, service := setupLoopTestService(t)
	started, err := service.Start(context.Background(), LoopStartRequest{RunID: "run-1", AgentID: 1, LoopType: models.LoopTypeRun})
	if err != nil {
		t.Fatalf("start loop failed: %v", err)
	}

	if err := service.Finish(context.Background(), LoopFinishRequest{LoopID: started.LoopID, Status: models.LoopStatusSuccess}); err != nil {
		t.Fatalf("first finish failed: %v", err)
	}
	if err := service.Finish(context.Background(), LoopFinishRequest{LoopID: started.LoopID, Status: models.LoopStatusSuccess}); err != nil {
		t.Fatalf("same terminal finish should be idempotent: %v", err)
	}
	if err := service.Finish(context.Background(), LoopFinishRequest{LoopID: started.LoopID, Status: models.LoopStatusFailed}); err == nil {
		t.Fatal("different terminal finish should fail")
	}
	if err := service.Finish(context.Background(), LoopFinishRequest{LoopID: "missing-loop", Status: models.LoopStatusFailed}); !errors.Is(err, ErrLoopNotFound()) {
		t.Fatalf("expected loop not found, got %v", err)
	}
	if err := service.Finish(context.Background(), LoopFinishRequest{LoopID: started.LoopID, Status: models.LoopStatusRunning}); err == nil {
		t.Fatal("running is not a terminal status")
	}
}

func TestLoopServiceQueueEvaluationIsIdempotent(t *testing.T) {
	dispatcher := &recordingLoopDispatcher{}
	database, service := setupLoopTestService(t, WithLoopEvaluationDispatcher(dispatcher))
	started, err := service.Start(context.Background(), LoopStartRequest{RunID: "run-1", AgentID: 1, LoopType: models.LoopTypeRun})
	if err != nil {
		t.Fatalf("start loop failed: %v", err)
	}

	request := LoopEvaluationRequest{LoopID: started.LoopID, EvaluatorCode: "quality-v1"}
	if err := service.QueueEvaluation(context.Background(), request); err != nil {
		t.Fatalf("queue evaluation failed: %v", err)
	}
	if err := service.QueueEvaluation(context.Background(), request); err != nil {
		t.Fatalf("duplicate queue should be idempotent: %v", err)
	}
	var count int64
	if err := database.Model(&models.LoopEvaluation{}).Where("loop_id = ? AND evaluator_code = ?", request.LoopID, request.EvaluatorCode).Count(&count).Error; err != nil {
		t.Fatalf("count evaluations failed: %v", err)
	}
	if count != 1 || len(dispatcher.requests) != 1 {
		t.Fatalf("expected one evaluation record and dispatch, count=%d dispatches=%d", count, len(dispatcher.requests))
	}
}

func TestAsyncLoopEvaluationDispatcherPersistsResultAndFailure(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		database := openSQLiteTestDB(t)
		if err := database.AutoMigrate(&models.LoopRecord{}, &models.LoopEvaluation{}); err != nil {
			t.Fatalf("auto migrate loop models failed: %v", err)
		}
		evaluator := &recordingLoopEvaluator{called: make(chan models.LoopRecord, 1)}
		dispatcher, err := NewAsyncLoopEvaluationDispatcher(database, evaluator, 1)
		if err != nil {
			t.Fatalf("create dispatcher failed: %v", err)
		}
		defer dispatcher.Close()
		service, err := NewLoopService(database, WithLoopEvaluationDispatcher(dispatcher))
		if err != nil {
			t.Fatalf("create loop service failed: %v", err)
		}
		started, err := service.Start(context.Background(), LoopStartRequest{RunID: "run-1", AgentID: 1, LoopType: models.LoopTypeRun})
		if err != nil {
			t.Fatalf("start loop failed: %v", err)
		}
		if err := service.Finish(context.Background(), LoopFinishRequest{LoopID: started.LoopID, Status: models.LoopStatusSuccess}); err != nil {
			t.Fatalf("finish loop failed: %v", err)
		}
		if err := service.QueueEvaluation(context.Background(), LoopEvaluationRequest{LoopID: started.LoopID, EvaluatorCode: "quality-v1"}); err != nil {
			t.Fatalf("queue evaluation failed: %v", err)
		}
		select {
		case <-evaluator.called:
		case <-time.After(time.Second):
			t.Fatal("evaluator was not called")
		}
		waitForEvaluationStatus(t, database, started.LoopID, models.EvaluationStatusSuccess)
	})

	t.Run("failure does not change loop status", func(t *testing.T) {
		database := openSQLiteTestDB(t)
		if err := database.AutoMigrate(&models.LoopRecord{}, &models.LoopEvaluation{}); err != nil {
			t.Fatalf("auto migrate loop models failed: %v", err)
		}
		evaluator := &recordingLoopEvaluator{called: make(chan models.LoopRecord, 1), err: errors.New("evaluator down")}
		dispatcher, err := NewAsyncLoopEvaluationDispatcher(database, evaluator, 1)
		if err != nil {
			t.Fatalf("create dispatcher failed: %v", err)
		}
		defer dispatcher.Close()
		service, err := NewLoopService(database, WithLoopEvaluationDispatcher(dispatcher))
		if err != nil {
			t.Fatalf("create loop service failed: %v", err)
		}
		started, err := service.Start(context.Background(), LoopStartRequest{RunID: "run-1", AgentID: 1, LoopType: models.LoopTypeRun})
		if err != nil {
			t.Fatalf("start loop failed: %v", err)
		}
		if err := service.Finish(context.Background(), LoopFinishRequest{LoopID: started.LoopID, Status: models.LoopStatusSuccess}); err != nil {
			t.Fatalf("finish loop failed: %v", err)
		}
		if err := service.QueueEvaluation(context.Background(), LoopEvaluationRequest{LoopID: started.LoopID, EvaluatorCode: "quality-v1"}); err != nil {
			t.Fatalf("queue evaluation failed: %v", err)
		}
		select {
		case <-evaluator.called:
		case <-time.After(time.Second):
			t.Fatal("evaluator was not called")
		}
		waitForEvaluationStatus(t, database, started.LoopID, models.EvaluationStatusFailed)
		var loop models.LoopRecord
		if err := database.Where("loop_id = ?", started.LoopID).First(&loop).Error; err != nil {
			t.Fatalf("load loop failed: %v", err)
		}
		if loop.Status != models.LoopStatusSuccess {
			t.Fatalf("evaluation failure changed loop status: %s", loop.Status)
		}
	})
}

func waitForEvaluationStatus(t *testing.T, database *gorm.DB, loopID, status string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var evaluation models.LoopEvaluation
		if err := database.Where("loop_id = ?", loopID).First(&evaluation).Error; err == nil && evaluation.Status == status {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	var evaluation models.LoopEvaluation
	if err := database.Where("loop_id = ?", loopID).First(&evaluation).Error; err != nil {
		t.Fatalf("load evaluation failed: %v", err)
	}
	t.Fatalf("expected evaluation status %s, got %s", status, evaluation.Status)
}

func TestAsyncLoopEvaluationDispatcherCloseRejectsNewJobs(t *testing.T) {
	database := openSQLiteTestDB(t)
	dispatcher, err := NewAsyncLoopEvaluationDispatcher(database, &recordingLoopEvaluator{}, 1)
	if err != nil {
		t.Fatalf("create dispatcher failed: %v", err)
	}
	if err := dispatcher.Close(); err != nil {
		t.Fatalf("close dispatcher failed: %v", err)
	}
	if err := dispatcher.Enqueue(context.Background(), LoopEvaluationRequest{LoopID: "loop-1", EvaluatorCode: "quality-v1"}); !errors.Is(err, ErrEvaluationUnavailable()) {
		t.Fatalf("expected enqueue after close to be rejected, got %v", err)
	}
	if err := dispatcher.Close(); err != nil {
		t.Fatalf("second close should be safe: %v", err)
	}
}
