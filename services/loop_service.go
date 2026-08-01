package services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"GoAI/models"
	"GoAI/observability"

	"gorm.io/gorm"
)

var (
	errLoopNotFound          = errors.New("loop not found")
	errEvaluationQueueFull   = errors.New("loop evaluation queue is full")
	errEvaluationUnavailable = errors.New("loop evaluator is unavailable")
)

// ErrLoopNotFound 返回指定 Loop 不存在的 sentinel error。
func ErrLoopNotFound() error { return errLoopNotFound }

// ErrEvaluationQueueFull 返回异步评估队列已满的 sentinel error。
func ErrEvaluationQueueFull() error { return errEvaluationQueueFull }

// ErrEvaluationUnavailable 返回没有配置评估器的 sentinel error。
func ErrEvaluationUnavailable() error { return errEvaluationUnavailable }

// LoopStartRequest 描述一个可观测执行片段的开始信息。
type LoopStartRequest struct {
	LoopID            string
	TraceID           string
	ThreadID          string
	RunID             string
	ParentLoopID      string
	DelegationID      string
	AgentID           uint64
	WorkflowID        uint64
	RunStepID         uint64
	LoopType          string
	InputSnapshotJSON string
	PromptVersion     string
	Provider          string
	Model             string
}

// LoopFinishRequest 描述一个 Loop 的终态信息。
type LoopFinishRequest struct {
	LoopID             string
	Status             string
	OutputSnapshotJSON string
	ErrorCode          string
	ErrorMessage       string
	LatencyMS          int64
	InputTokens        *int64
	OutputTokens       *int64
	TotalTokens        *int64
	FinishedAt         *time.Time
	TraceID            string
	ThreadID           string
	RunID              string
	DelegationID       string
	LoopType           string
}

// LoopEvaluationRequest 描述一次异步 Loop 评估任务。
type LoopEvaluationRequest struct {
	LoopID        string
	EvaluatorCode string
}

// LoopEvaluationResult 是 Evaluator 返回的可持久化结果。
type LoopEvaluationResult struct {
	Score      *float64
	ResultJSON string
}

// LoopEvaluator 执行一次 Loop 评估。评估失败不会改变 Run 或 Loop 的终态。
type LoopEvaluator interface {
	Evaluate(context.Context, models.LoopRecord) (LoopEvaluationResult, error)
}

// LoopEvaluationDispatcher 定义异步评估任务的投递边界。
type LoopEvaluationDispatcher interface {
	Enqueue(context.Context, LoopEvaluationRequest) error
}

// LoopService 统一管理 Loop 生命周期、执行快照和可选异步评估入口。
type LoopService struct {
	database      *gorm.DB
	dispatcher    LoopEvaluationDispatcher
	observability *observability.Bundle
}

// LoopServiceOption 配置 LoopService 的可选依赖。
type LoopServiceOption func(*LoopService) error

// WithLoopEvaluationDispatcher 注入异步 Evaluator 调度器。
func WithLoopEvaluationDispatcher(dispatcher LoopEvaluationDispatcher) LoopServiceOption {
	return func(service *LoopService) error {
		if dispatcher == nil {
			return errors.New("configuring loop service: evaluation dispatcher is nil")
		}
		service.dispatcher = dispatcher
		return nil
	}
}

// WithLoopObservability 注入 Loop 生命周期的日志、指标和 Trace 能力。
func WithLoopObservability(bundle *observability.Bundle) LoopServiceOption {
	return func(service *LoopService) error {
		if bundle == nil {
			return errors.New("configuring loop service: observability bundle is nil")
		}
		service.observability = bundle
		return nil
	}
}

// WithLoopService 将 Loop 生命周期服务注入 RunService，统一维护 Run、Step 和 Delegation 的观测记录。
func WithLoopService(loopService *LoopService) RunServiceOption {
	return func(service *RunService) error {
		if loopService == nil {
			return errors.New("configuring run service: loop service is nil")
		}
		service.loopService = loopService
		return nil
	}
}

// NewLoopService 使用显式数据库构造 LoopService。
func NewLoopService(database *gorm.DB, options ...LoopServiceOption) (*LoopService, error) {
	if database == nil {
		return nil, errors.New("creating loop service: database is nil")
	}
	service := &LoopService{database: database}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(service); err != nil {
			return nil, err
		}
	}
	return service, nil
}

// Start 在独立事务中创建一个 running Loop，供测试或非 RunService 场景使用。
func (s *LoopService) Start(ctx context.Context, request LoopStartRequest) (*models.LoopRecord, error) {
	if s == nil || s.database == nil {
		return nil, errors.New("starting loop: service is not configured")
	}
	return s.startObservedTx(ctx, s.database.WithContext(ctx), request)
}

func (s *LoopService) startObservedTx(ctx context.Context, tx *gorm.DB, request LoopStartRequest) (record *models.LoopRecord, err error) {
	observedCtx, span, startedAt := startServiceObservation(ctx, s.observability, "loop.start", request.RunID, request.ThreadID, request.DelegationID)
	status := "success"
	defer func() {
		if err != nil {
			status = "error"
		}
		loopType := request.LoopType
		loopID := strings.TrimSpace(request.LoopID)
		if record != nil {
			loopType = record.LoopType
			loopID = record.LoopID
		}
		finishServiceObservation(s.observability, observedCtx, span, "start_loop", status, startedAt, request.RunID, request.ThreadID, request.DelegationID, func(metrics *observability.Metrics, _, status string, elapsed time.Duration) {
			metrics.ObserveLoop(loopType, status, elapsed)
		}, err, slog.String("loop_id", loopID))
	}()
	return s.startTx(observedCtx, tx, request)
}

// startTx 在调用方事务中创建一个 running Loop。
func (s *LoopService) startTx(ctx context.Context, tx *gorm.DB, request LoopStartRequest) (*models.LoopRecord, error) {
	if tx == nil {
		return nil, errors.New("starting loop: transaction is nil")
	}
	if strings.TrimSpace(request.RunID) == "" || request.AgentID == 0 || strings.TrimSpace(request.LoopType) == "" {
		return nil, errors.New("starting loop: run_id, agent_id and loop_type are required")
	}
	inputJSON, err := normalizeLoopJSON(request.InputSnapshotJSON)
	if err != nil {
		return nil, fmt.Errorf("starting loop: invalid input snapshot: %w", err)
	}
	loopID := strings.TrimSpace(request.LoopID)
	if loopID == "" {
		loopID = newPrefixedID("loop")
	}
	now := time.Now()
	record := &models.LoopRecord{
		LoopID:             loopID,
		TraceID:            strings.TrimSpace(request.TraceID),
		ThreadID:           strings.TrimSpace(request.ThreadID),
		RunID:              strings.TrimSpace(request.RunID),
		ParentLoopID:       strings.TrimSpace(request.ParentLoopID),
		DelegationID:       strings.TrimSpace(request.DelegationID),
		AgentID:            request.AgentID,
		LoopType:           strings.TrimSpace(request.LoopType),
		Status:             models.LoopStatusRunning,
		InputSnapshotJSON:  inputJSON,
		OutputSnapshotJSON: "{}",
		PromptVersion:      strings.TrimSpace(request.PromptVersion),
		Provider:           strings.TrimSpace(request.Provider),
		Model:              strings.TrimSpace(request.Model),
		StartedAt:          &now,
	}
	if request.WorkflowID != 0 {
		workflowID := request.WorkflowID
		record.WorkflowID = &workflowID
	}
	if request.RunStepID != 0 {
		runStepID := request.RunStepID
		record.RunStepID = &runStepID
	}
	if err := tx.WithContext(ctx).Create(record).Error; err != nil {
		return nil, fmt.Errorf("creating loop record: %w", err)
	}
	return record, nil
}

// Finish 将 Loop 推进到指定终态，并在完成后按需投递异步评估任务。
func (s *LoopService) Finish(ctx context.Context, request LoopFinishRequest) error {
	if s == nil || s.database == nil {
		return errors.New("finishing loop: service is not configured")
	}
	return s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return s.finishObservedTx(ctx, tx, request)
	})
}

func (s *LoopService) finishObservedTx(ctx context.Context, tx *gorm.DB, request LoopFinishRequest) (err error) {
	observedCtx, span, startedAt := startServiceObservation(ctx, s.observability, "loop.finish", request.RunID, request.ThreadID, request.DelegationID)
	status := strings.TrimSpace(request.Status)
	if status == "" {
		status = "success"
	}
	defer func() {
		observationStatus := status
		if err != nil {
			observationStatus = "error"
		}
		finishServiceObservation(s.observability, observedCtx, span, "finish_loop", observationStatus, startedAt, request.RunID, request.ThreadID, request.DelegationID, func(metrics *observability.Metrics, _, metricStatus string, elapsed time.Duration) {
			metrics.ObserveLoop(request.LoopType, metricStatus, elapsed)
		}, err, slog.String("loop_id", strings.TrimSpace(request.LoopID)))
	}()
	return s.finishTx(observedCtx, tx, request)
}

// finishTx 在调用方事务中推进 Loop 终态。重复收敛到同一终态视为幂等成功。
func (s *LoopService) finishTx(ctx context.Context, tx *gorm.DB, request LoopFinishRequest) error {
	loopID := strings.TrimSpace(request.LoopID)
	if loopID == "" {
		return nil
	}
	var current models.LoopRecord
	if err := tx.WithContext(ctx).Where("loop_id = ?", loopID).First(&current).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return errLoopNotFound
		}
		return err
	}
	if current.Status != models.LoopStatusRunning {
		if current.Status == request.Status {
			return nil
		}
		return fmt.Errorf("invalid loop status transition: %s -> %s", current.Status, request.Status)
	}
	if !validLoopTerminalStatus(request.Status) {
		return fmt.Errorf("invalid loop terminal status: %s", request.Status)
	}
	outputJSON, err := normalizeLoopJSON(request.OutputSnapshotJSON)
	if err != nil {
		return fmt.Errorf("finishing loop: invalid output snapshot: %w", err)
	}
	finishedAt := time.Now()
	if request.FinishedAt != nil {
		finishedAt = *request.FinishedAt
	}
	latencyMS := request.LatencyMS
	if latencyMS <= 0 && current.StartedAt != nil {
		latencyMS = finishedAt.Sub(*current.StartedAt).Milliseconds()
		if latencyMS < 0 {
			latencyMS = 0
		}
	}
	inputTokens, outputTokens, totalTokens := tokenUsage(current.InputSnapshotJSON, outputJSON, request.InputTokens, request.OutputTokens, request.TotalTokens)
	updates := map[string]any{
		"status":               request.Status,
		"output_snapshot_json": outputJSON,
		"error_code":           strings.TrimSpace(request.ErrorCode),
		"error_message":        strings.TrimSpace(request.ErrorMessage),
		"latency_ms":           latencyMS,
		"input_tokens":         inputTokens,
		"output_tokens":        outputTokens,
		"total_tokens":         totalTokens,
		"finished_at":          finishedAt,
	}
	result := tx.WithContext(ctx).Model(&models.LoopRecord{}).
		Where("loop_id = ? AND status = ?", loopID, models.LoopStatusRunning).
		Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("loop %s was concurrently completed", loopID)
	}
	return nil
}

// QueueEvaluation 创建 pending 评估记录并将任务异步交给调度器。
func (s *LoopService) QueueEvaluation(ctx context.Context, request LoopEvaluationRequest) (err error) {
	if s == nil || s.database == nil {
		return errors.New("queueing loop evaluation: service is not configured")
	}
	observedCtx, span, startedAt := startServiceObservation(ctx, s.observability, "loop.queue_evaluation", "", "", "")
	status := "success"
	defer func() {
		if err != nil {
			status = "error"
		}
		finishServiceObservation(s.observability, observedCtx, span, "queue_loop_evaluation", status, startedAt, "", "", "", func(metrics *observability.Metrics, _, metricStatus string, elapsed time.Duration) {
			metrics.ObserveLoop("evaluation", metricStatus, elapsed)
		}, err, slog.String("loop_id", strings.TrimSpace(request.LoopID)), slog.String("evaluator_code", strings.TrimSpace(request.EvaluatorCode)))
	}()
	request.LoopID = strings.TrimSpace(request.LoopID)
	request.EvaluatorCode = strings.TrimSpace(request.EvaluatorCode)
	if request.LoopID == "" || request.EvaluatorCode == "" {
		return errors.New("queueing loop evaluation: loop_id and evaluator_code are required")
	}
	evaluation := &models.LoopEvaluation{
		LoopID:        request.LoopID,
		EvaluatorCode: request.EvaluatorCode,
		Status:        models.EvaluationStatusPending,
	}
	if err := s.database.WithContext(observedCtx).Create(evaluation).Error; err != nil {
		if isUniqueConstraintError(err) {
			return nil
		}
		return err
	}
	if s.dispatcher == nil {
		return nil
	}
	if err := s.dispatcher.Enqueue(observedCtx, request); err != nil {
		_ = s.database.WithContext(context.Background()).Model(&models.LoopEvaluation{}).
			Where("id = ? AND status = ?", evaluation.ID, models.EvaluationStatusPending).
			Updates(map[string]any{"status": models.EvaluationStatusFailed, "error_message": err.Error()}).Error
		return err
	}
	return nil
}

// AsyncLoopEvaluationDispatcher 使用有限队列异步执行 Evaluator。
type AsyncLoopEvaluationDispatcher struct {
	database  *gorm.DB
	evaluator LoopEvaluator
	jobs      chan LoopEvaluationRequest
	stopOnce  sync.Once
	stateMu   sync.Mutex
	closed    bool
	wg        sync.WaitGroup
}

// NewAsyncLoopEvaluationDispatcher 创建异步评估调度器并启动后台 worker。
func NewAsyncLoopEvaluationDispatcher(database *gorm.DB, evaluator LoopEvaluator, buffer int) (*AsyncLoopEvaluationDispatcher, error) {
	if database == nil {
		return nil, errors.New("creating loop evaluation dispatcher: database is nil")
	}
	if evaluator == nil {
		return nil, errors.New("creating loop evaluation dispatcher: evaluator is nil")
	}
	if buffer <= 0 {
		buffer = 32
	}
	dispatcher := &AsyncLoopEvaluationDispatcher{
		database:  database,
		evaluator: evaluator,
		jobs:      make(chan LoopEvaluationRequest, buffer),
	}
	dispatcher.wg.Add(1)
	go dispatcher.run()
	return dispatcher, nil
}

// Enqueue 非阻塞投递评估任务；队列满时返回明确错误，不影响原始 Run。
func (d *AsyncLoopEvaluationDispatcher) Enqueue(ctx context.Context, request LoopEvaluationRequest) error {
	if d == nil {
		return errEvaluationUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}

	d.stateMu.Lock()
	defer d.stateMu.Unlock()
	if d.closed {
		return errEvaluationUnavailable
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case d.jobs <- request:
		return nil
	default:
		return errEvaluationQueueFull
	}
}

func (d *AsyncLoopEvaluationDispatcher) run() {
	defer d.wg.Done()
	for request := range d.jobs {
		d.evaluate(request)
	}
}

func (d *AsyncLoopEvaluationDispatcher) evaluate(request LoopEvaluationRequest) {
	ctx := context.Background()
	var loop models.LoopRecord
	if err := d.database.WithContext(ctx).Where("loop_id = ?", request.LoopID).First(&loop).Error; err != nil {
		return
	}
	var evaluation models.LoopEvaluation
	if err := d.database.WithContext(ctx).
		Where("loop_id = ? AND evaluator_code = ?", request.LoopID, request.EvaluatorCode).
		First(&evaluation).Error; err != nil {
		return
	}
	claim := d.database.WithContext(ctx).Model(&models.LoopEvaluation{}).
		Where("id = ? AND status = ?", evaluation.ID, models.EvaluationStatusPending).
		Update("status", models.EvaluationStatusRunning)
	if claim.Error != nil || claim.RowsAffected != 1 {
		return
	}
	result, err := d.evaluator.Evaluate(ctx, loop)
	updates := map[string]any{"status": models.EvaluationStatusSuccess, "result_json": normalizeOptionalJSON(result.ResultJSON)}
	if result.Score != nil {
		updates["score"] = *result.Score
	}
	if err != nil {
		updates["status"] = models.EvaluationStatusFailed
		updates["error_message"] = err.Error()
	}
	_ = d.database.WithContext(ctx).Model(&models.LoopEvaluation{}).Where("id = ?", evaluation.ID).Updates(updates).Error
}

// Close 停止调度器并等待已入队评估任务处理完成。
func (d *AsyncLoopEvaluationDispatcher) Close() error {
	if d == nil {
		return nil
	}
	d.stopOnce.Do(func() {
		d.stateMu.Lock()
		d.closed = true
		close(d.jobs)
		d.stateMu.Unlock()
	})
	d.wg.Wait()
	return nil
}

func validLoopTerminalStatus(status string) bool {
	switch status {
	case models.LoopStatusSuccess, models.LoopStatusFailed, models.LoopStatusCancelled:
		return true
	default:
		return false
	}
}

func normalizeLoopJSON(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "{}", nil
	}
	if _, err := canonicalizeJSON([]byte(value)); err != nil {
		return "", err
	}
	return canonicalizeJSON([]byte(value))
}

func normalizeOptionalJSON(raw string) string {
	value, err := normalizeLoopJSON(raw)
	if err != nil {
		return "{}"
	}
	return value
}

func tokenUsage(inputJSON, outputJSON string, input, output, total *int64) (*int64, *int64, *int64) {
	if input == nil {
		value := estimateTokens(inputJSON)
		input = &value
	}
	if output == nil {
		value := estimateTokens(outputJSON)
		output = &value
	}
	if total == nil {
		value := *input + *output
		total = &value
	}
	return input, output, total
}

// estimateTokens 是 Provider 未返回 usage 时的保守估算，后续可被真实 usage 覆盖。
func estimateTokens(value string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	count := utf8.RuneCountInString(value)
	return int64((count + 3) / 4)
}
