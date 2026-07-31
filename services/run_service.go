package services

import (
	"GoAI/ai"
	"GoAI/domain/runstate"
	"GoAI/models"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

var (
	errRunNotFound           = errors.New("run not found")
	errRunForbidden          = errors.New("run does not belong to current user")
	errRunDispatchFailed     = errors.New("run execute event publish failed")
	errIdempotencyKeyReused  = errors.New("idempotency key reused with different request")
	errInvalidRunTransition  = errors.New("invalid run status transition")
	errInvalidStepTransition = errors.New("invalid step status transition")
)

var defaultStepRetryBackoffs = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}

// RunEventPublisher 定义 Run 创建和回放后的异步投递边界。
type RunEventPublisher interface {
	PublishRunExecute(context.Context, string) error
}

// RunEventPublisherFunc 让普通函数可以作为 Run 事件发布器注入。
type RunEventPublisherFunc func(context.Context, string) error

// PublishRunExecute 调用底层函数发布 Run 执行事件。
func (f RunEventPublisherFunc) PublishRunExecute(ctx context.Context, runID string) error {
	return f(ctx, runID)
}

// RunService 协调 Run 持久化、入队、查询、回放和异步执行。
type RunService struct {
	database          *gorm.DB
	publisher         RunEventPublisher
	stepRetryBackoffs []time.Duration
}

// NewRunService 使用显式数据库和事件发布器构造 RunService。
func NewRunService(database *gorm.DB, publisher RunEventPublisher) (*RunService, error) {
	if database == nil {
		return nil, errors.New("creating run service: database is nil")
	}
	if publisher == nil {
		return nil, errors.New("creating run service: publisher is nil")
	}
	return &RunService{
		database:          database,
		publisher:         publisher,
		stepRetryBackoffs: append([]time.Duration(nil), defaultStepRetryBackoffs...),
	}, nil
}

var allowedRunTriggerTypes = map[string]struct{}{
	"api":      {},
	"manual":   {},
	"replay":   {},
	"schedule": {},
	"webhook":  {},
}

func ErrRunNotFound() error {
	return errRunNotFound
}

func ErrRunForbidden() error {
	return errRunForbidden
}

// ErrRunDispatchFailed 返回 Run 入队失败的统一 sentinel error。
func ErrRunDispatchFailed() error {
	return errRunDispatchFailed
}

// ErrIdempotencyKeyReused 返回幂等键复用冲突的统一 sentinel error。
func ErrIdempotencyKeyReused() error {
	return errIdempotencyKeyReused
}

type CreateRunRequest struct {
	AgentCode       string          `json:"agent_code"`
	WorkflowVersion int             `json:"workflow_version"`
	ThreadID        string          `json:"thread_id"`
	TriggerType     string          `json:"trigger_type"`
	Input           json.RawMessage `json:"input"`
	Provider        string          `json:"provider"`
	Model           string          `json:"model"`
	IdempotencyKey  string          `json:"-"`
}

type CreateRunResponse struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

// RunMutationResult 描述创建或回放 Run 的返回结果以及是否命中幂等。
type RunMutationResult struct {
	Run           *models.Run
	IdempotentHit bool
}

// ValidateCreateRunRequest 校验 Run 创建请求的关键字段，避免非法输入进入执行主链路。
func ValidateCreateRunRequest(req CreateRunRequest) error {
	agentCode := strings.TrimSpace(req.AgentCode)
	if agentCode == "" {
		return errors.New("agent_code is required")
	}
	if len(agentCode) > 64 {
		return errors.New("agent_code must be at most 64 characters")
	}
	if req.WorkflowVersion < 0 {
		return errors.New("workflow_version must be greater than or equal to 0")
	}
	if len(strings.TrimSpace(req.ThreadID)) > 128 {
		return errors.New("thread_id must be at most 128 characters")
	}
	triggerType := strings.TrimSpace(req.TriggerType)
	if triggerType != "" {
		if _, ok := allowedRunTriggerTypes[triggerType]; !ok {
			return errors.New("trigger_type must be one of api,manual,replay,schedule,webhook")
		}
	}
	if len(strings.TrimSpace(req.Provider)) > 64 {
		return errors.New("provider must be at most 64 characters")
	}
	if len(strings.TrimSpace(req.Model)) > 128 {
		return errors.New("model must be at most 128 characters")
	}
	inputJSON := strings.TrimSpace(string(req.Input))
	if inputJSON != "" && !json.Valid(req.Input) {
		return errors.New("input must be valid JSON")
	}
	return nil
}

// normalizedCreateRunRequest 保存经过规范化的 Run 创建参数，便于做稳定哈希。
type normalizedCreateRunRequest struct {
	AgentCode       string
	WorkflowVersion int
	ThreadID        string
	TriggerType     string
	InputJSON       string
	Provider        string
	Model           string
	IdempotencyKey  string
}

// normalizeCreateRunRequest 对创建请求进行 trim 和默认值规范化。
func normalizeCreateRunRequest(req CreateRunRequest) (normalizedCreateRunRequest, error) {
	inputJSON, err := canonicalizeJSON(req.Input)
	if err != nil {
		return normalizedCreateRunRequest{}, err
	}
	if inputJSON == "" {
		inputJSON = "{}"
	}
	triggerType := strings.TrimSpace(req.TriggerType)
	if triggerType == "" {
		triggerType = "api"
	}
	return normalizedCreateRunRequest{
		AgentCode:       strings.TrimSpace(req.AgentCode),
		WorkflowVersion: req.WorkflowVersion,
		ThreadID:        strings.TrimSpace(req.ThreadID),
		TriggerType:     triggerType,
		InputJSON:       inputJSON,
		Provider:        strings.TrimSpace(req.Provider),
		Model:           strings.TrimSpace(req.Model),
		IdempotencyKey:  strings.TrimSpace(req.IdempotencyKey),
	}, nil
}

// canonicalizeJSON 将任意合法 JSON 归一化为稳定文本，便于请求哈希计算。
func canonicalizeJSON(raw json.RawMessage) (string, error) {
	inputJSON := strings.TrimSpace(string(raw))
	if inputJSON == "" {
		return "", nil
	}
	var payload any
	if err := json.Unmarshal([]byte(inputJSON), &payload); err != nil {
		return "", err
	}
	normalized, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(normalized), nil
}

// buildCreateRunRequestHash 使用规范化后的请求构造稳定哈希。
func buildCreateRunRequestHash(userID uint64, req normalizedCreateRunRequest) (string, error) {
	return hashPayload(map[string]any{
		"operation":        models.RunIdempotencyOperationCreate,
		"owner_user_id":    userID,
		"agent_code":       req.AgentCode,
		"workflow_version": req.WorkflowVersion,
		"thread_id":        req.ThreadID,
		"trigger_type":     req.TriggerType,
		"input":            req.InputJSON,
		"provider":         req.Provider,
		"model":            req.Model,
	})
}

// buildReplayRunRequestHash 使用 replay 源 run 构造稳定哈希。
func buildReplayRunRequestHash(userID uint64, sourceRunID string) (string, error) {
	return hashPayload(map[string]any{
		"operation":     models.RunIdempotencyOperationReplay,
		"owner_user_id": userID,
		"source_run_id": sourceRunID,
	})
}

// hashPayload 将任意结构体或 map 计算为 sha256 十六进制摘要。
func hashPayload(payload any) (string, error) {
	bs, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(bs)
	return hex.EncodeToString(sum[:]), nil
}

// loadRunByRunID 在指定 DB 上按 run_id 读取 Run。
func loadRunByRunID(tx *gorm.DB, runID string) (*models.Run, error) {
	var run models.Run
	if err := tx.Where("run_id = ?", strings.TrimSpace(runID)).First(&run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errRunNotFound
		}
		return nil, err
	}
	return &run, nil
}

// loadRunIdempotency 在指定 DB 上按幂等键读取映射记录。
func loadRunIdempotency(tx *gorm.DB, ownerUserID uint64, operation, idempotencyKey string) (*models.RunIdempotency, error) {
	var record models.RunIdempotency
	if err := tx.Where(
		"owner_user_id = ? AND operation = ? AND idempotency_key = ?",
		ownerUserID,
		operation,
		idempotencyKey,
	).First(&record).Error; err != nil {
		return nil, err
	}
	return &record, nil
}

// isUniqueConstraintError 判断是否命中了数据库唯一约束冲突。
func isUniqueConstraintError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate") || strings.Contains(msg, "unique constraint failed") || strings.Contains(msg, "unique violation")
}

// createQueuedRunWithIdempotency 在事务中创建 Run、回填幂等记录并推进到 queued。
func (s *RunService) createQueuedRunWithIdempotency(ctx context.Context, run *models.Run, idempotency *models.RunIdempotency) (*RunMutationResult, error) {
	result := &RunMutationResult{}
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if idempotency != nil {
			if err := tx.Create(idempotency).Error; err != nil {
				if !isUniqueConstraintError(err) {
					return err
				}
				existing, loadErr := loadRunIdempotency(tx, idempotency.OwnerUserID, idempotency.Operation, idempotency.IdempotencyKey)
				if loadErr != nil {
					return loadErr
				}
				if existing.RequestHash != idempotency.RequestHash {
					return errIdempotencyKeyReused
				}
				if strings.TrimSpace(existing.RunID) == "" {
					return fmt.Errorf("idempotency record has empty run_id")
				}
				existingRun, loadErr := loadRunByRunID(tx, existing.RunID)
				if loadErr != nil {
					return loadErr
				}
				result.Run = existingRun
				result.IdempotentHit = true
				return nil
			}
		}

		if err := tx.Create(run).Error; err != nil {
			return err
		}
		if err := transitionRunStatus(ctx, tx, run, models.RunStatusQueued, ""); err != nil {
			return err
		}
		if idempotency != nil {
			idempotency.RunID = run.RunID
			if err := tx.Model(&models.RunIdempotency{}).Where("id = ?", idempotency.ID).Update("run_id", run.RunID).Error; err != nil {
				return err
			}
		}
		result.Run = run
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// claimRunForExecution 将 queued 状态的 Run 原子推进到 running。
func (s *RunService) claimRunForExecution(ctx context.Context, runID string) (*models.Run, bool, error) {
	now := time.Now()
	result := s.database.WithContext(ctx).Model(&models.Run{}).
		Where("run_id = ? AND status = ?", strings.TrimSpace(runID), models.RunStatusQueued).
		Updates(map[string]any{
			"status":        models.RunStatusRunning,
			"started_at":    &now,
			"error_message": "",
		})
	if result.Error != nil {
		return nil, false, result.Error
	}
	if result.RowsAffected == 0 {
		run, err := s.fetchRunByRunID(ctx, runID)
		if err != nil {
			return nil, false, err
		}
		return run, false, nil
	}
	run, err := s.fetchRunByRunID(ctx, runID)
	if err != nil {
		return nil, false, err
	}
	run.Status = models.RunStatusRunning
	run.StartedAt = &now
	run.ErrorMessage = ""
	return run, true, nil
}

func (s *RunService) CreateRun(ctx context.Context, userID uint64, req CreateRunRequest) (*RunMutationResult, error) {
	if err := ValidateCreateRunRequest(req); err != nil {
		return nil, err
	}

	normalized, err := normalizeCreateRunRequest(req)
	if err != nil {
		return nil, err
	}

	var agent models.Agent
	if err := s.database.WithContext(ctx).
		Where("agent_code = ? AND status = ?", normalized.AgentCode, models.AgentStatusActive).
		First(&agent).Error; err != nil {
		return nil, fmt.Errorf("find active agent by code: %w", err)
	}

	workflow, err := s.resolveWorkflow(ctx, agent.ID, normalized.WorkflowVersion)
	if err != nil {
		return nil, err
	}
	if _, err := ParseAndValidateWorkflowDefinition(workflow.DefinitionJSON); err != nil {
		return nil, err
	}

	run := &models.Run{
		RunID:       newRunID(),
		ThreadID:    normalized.ThreadID,
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      userID,
		TriggerType: normalized.TriggerType,
		InputJSON:   normalized.InputJSON,
		Status:      models.RunStatusPending,
		Provider:    normalized.Provider,
		Model:       normalized.Model,
	}

	var idempotency *models.RunIdempotency
	if normalized.IdempotencyKey != "" {
		requestHash, hashErr := buildCreateRunRequestHash(userID, normalized)
		if hashErr != nil {
			return nil, hashErr
		}
		idempotency = &models.RunIdempotency{
			OwnerUserID:    userID,
			Operation:      models.RunIdempotencyOperationCreate,
			IdempotencyKey: normalized.IdempotencyKey,
			RequestHash:    requestHash,
		}
	}

	result, err := s.createQueuedRunWithIdempotency(ctx, run, idempotency)
	if err != nil {
		return nil, err
	}
	if result.IdempotentHit {
		return result, nil
	}

	if err := s.publisher.PublishRunExecute(ctx, result.Run.RunID); err != nil {
		_ = s.failRunWithMessage(ctx, result.Run.RunID, fmt.Sprintf("dispatch run message failed: %v", err))
		return result, fmt.Errorf("%w: %v", errRunDispatchFailed, err)
	}

	return result, nil
}

func (s *RunService) GetRunByRunID(ctx context.Context, userID uint64, isAdmin bool, runID string) (*models.Run, error) {
	run, err := s.fetchRunByRunID(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run.UserID != userID && !isAdmin {
		return nil, errRunForbidden
	}
	return run, nil
}

func (s *RunService) GetRunStepsByRunID(ctx context.Context, userID uint64, isAdmin bool, runID string) ([]models.RunStep, error) {
	run, err := s.fetchRunByRunID(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run.UserID != userID && !isAdmin {
		return nil, errRunForbidden
	}

	var steps []models.RunStep
	if err := s.database.WithContext(ctx).
		Where("run_id = ?", runID).
		Order("created_at ASC").
		Order("attempt ASC").
		Find(&steps).Error; err != nil {
		return nil, err
	}
	return steps, nil
}

func (s *RunService) ReplayRun(ctx context.Context, userID uint64, isAdmin bool, runID string, idempotencyKey string) (*RunMutationResult, error) {
	origin, err := s.GetRunByRunID(ctx, userID, isAdmin, runID)
	if err != nil {
		return nil, err
	}

	clone := &models.Run{
		RunID:       newRunID(),
		ThreadID:    origin.ThreadID,
		AgentID:     origin.AgentID,
		WorkflowID:  origin.WorkflowID,
		UserID:      userID,
		TriggerType: "replay",
		InputJSON:   strings.TrimSpace(origin.InputJSON),
		Status:      models.RunStatusPending,
		Provider:    origin.Provider,
		Model:       origin.Model,
	}
	if clone.InputJSON == "" {
		clone.InputJSON = "{}"
	}

	var idempotency *models.RunIdempotency
	trimmedKey := strings.TrimSpace(idempotencyKey)
	if trimmedKey != "" {
		requestHash, hashErr := buildReplayRunRequestHash(userID, origin.RunID)
		if hashErr != nil {
			return nil, hashErr
		}
		idempotency = &models.RunIdempotency{
			OwnerUserID:    userID,
			Operation:      models.RunIdempotencyOperationReplay,
			IdempotencyKey: trimmedKey,
			RequestHash:    requestHash,
			SourceRunID:    origin.RunID,
		}
	}

	result, err := s.createQueuedRunWithIdempotency(ctx, clone, idempotency)
	if err != nil {
		return nil, err
	}
	if result.IdempotentHit {
		return result, nil
	}

	if err := s.publisher.PublishRunExecute(ctx, result.Run.RunID); err != nil {
		_ = s.failRunWithMessage(ctx, result.Run.RunID, fmt.Sprintf("dispatch replay message failed: %v", err))
		return result, fmt.Errorf("%w: %v", errRunDispatchFailed, err)
	}
	return result, nil
}

func (s *RunService) HandleRunExecute(ctx context.Context, runID string) error {
	run, claimed, err := s.claimRunForExecution(ctx, runID)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}

	var workflow models.Workflow
	if err := s.database.WithContext(ctx).First(&workflow, "id = ?", run.WorkflowID).Error; err != nil {
		_ = s.failRunWithMessage(ctx, run.RunID, err.Error())
		return err
	}

	def, err := ParseAndValidateWorkflowDefinition(workflow.DefinitionJSON)
	if err != nil {
		_ = s.failRunWithMessage(ctx, run.RunID, err.Error())
		return err
	}

	order, err := ResolveExecutionOrder(def)
	if err != nil {
		_ = s.failRunWithMessage(ctx, run.RunID, err.Error())
		return err
	}

	if err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, node := range order {
			if err := tx.Model(&models.Run{}).
				Where("run_id = ?", run.RunID).
				Update("current_step", node.Key).Error; err != nil {
				return err
			}
			if execErr := s.executeNodeWithRetry(ctx, tx, run, node); execErr != nil {
				return transitionRunStatus(ctx, tx, run, models.RunStatusFailed, execErr.Error())
			}
		}
		return transitionRunStatus(ctx, tx, run, models.RunStatusSuccess, "")
	}); err != nil {
		_ = s.failRunWithMessage(ctx, run.RunID, err.Error())
		return err
	}
	return nil
}

func (s *RunService) executeNodeWithRetry(ctx context.Context, tx *gorm.DB, run *models.Run, node WorkflowNode) error {
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		startedAt := time.Now()
		step := &models.RunStep{
			RunID:     run.RunID,
			StepKey:   strings.TrimSpace(node.Key),
			StepType:  strings.TrimSpace(node.Type),
			Attempt:   attempt,
			Status:    models.RunStepStatusRunning,
			InputJSON: run.InputJSON,
			Provider:  run.Provider,
			Model:     run.Model,
			StartedAt: &startedAt,
		}
		if err := tx.Create(step).Error; err != nil {
			return err
		}

		outputJSON, err := executeWorkflowNode(ctx, run, node, attempt)
		finishedAt := time.Now()
		latencyMS := finishedAt.Sub(startedAt).Milliseconds()
		if err != nil {
			lastErr = err
			if updateErr := transitionStepStatus(ctx, tx, step, models.RunStepStatusFailed, "", err.Error(), latencyMS, &finishedAt); updateErr != nil {
				return updateErr
			}
			if err := tx.Model(&models.Run{}).
				Where("run_id = ?", run.RunID).
				Update("retry_count", gorm.Expr("retry_count + ?", 1)).Error; err != nil {
				return err
			}
			if attempt < len(s.stepRetryBackoffs)+1 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(s.stepRetryBackoffs[attempt-1]):
				}
			}
			continue
		}
		if updateErr := transitionStepStatus(ctx, tx, step, models.RunStepStatusSuccess, outputJSON, "", latencyMS, &finishedAt); updateErr != nil {
			return updateErr
		}
		return nil
	}
	if lastErr == nil {
		return errors.New("node execution failed after retries")
	}
	return lastErr
}

func executeWorkflowNode(ctx context.Context, run *models.Run, node WorkflowNode, attempt int) (string, error) {
	var conf map[string]any
	if len(node.Config) > 0 {
		if err := json.Unmarshal(node.Config, &conf); err != nil {
			return "", fmt.Errorf("invalid node config for %s: %w", node.Key, err)
		}
	}
	if forceFail, ok := conf["force_fail"].(bool); ok && forceFail {
		return "", fmt.Errorf("node %s forced failure", node.Key)
	}
	if failAttempts, ok := conf["fail_attempts"].(float64); ok && attempt <= int(failAttempts) {
		return "", fmt.Errorf("node %s failed on attempt %d", node.Key, attempt)
	}

	nodeType := strings.ToLower(strings.TrimSpace(node.Type))
	switch nodeType {
	case "llm":
		return executeLLMNode(ctx, run)
	case "tool", "noop", "planner":
		resp := map[string]any{
			"step_key": node.Key,
			"type":     nodeType,
			"result":   "ok",
		}
		bs, _ := json.Marshal(resp)
		return string(bs), nil
	default:
		resp := map[string]any{
			"step_key": node.Key,
			"type":     nodeType,
			"result":   "skipped_unknown_type",
		}
		bs, _ := json.Marshal(resp)
		return string(bs), nil
	}
}

func executeLLMNode(ctx context.Context, run *models.Run) (string, error) {
	var input struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
		Prompt string `json:"prompt"`
	}
	if err := json.Unmarshal([]byte(run.InputJSON), &input); err != nil {
		return "", fmt.Errorf("invalid run input json: %w", err)
	}
	messages := make([]ai.Message, 0, len(input.Messages))
	for _, m := range input.Messages {
		if strings.TrimSpace(m.Role) == "" || strings.TrimSpace(m.Content) == "" {
			continue
		}
		messages = append(messages, ai.Message{Role: m.Role, Content: m.Content})
	}
	if len(messages) == 0 && strings.TrimSpace(input.Prompt) != "" {
		messages = append(messages, ai.Message{Role: "user", Content: input.Prompt})
	}
	if len(messages) == 0 {
		return `{"message":"llm node executed without input messages"}`, nil
	}

	stream, err := Chat(ctx, messages, run.Provider, run.Model)
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	chunks := stream.Chunks
	errs := stream.Errs
	for chunks != nil || errs != nil {
		select {
		case content, ok := <-chunks:
			if !ok {
				chunks = nil
				continue
			}
			builder.WriteString(content)
		case streamErr, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if streamErr != nil {
				return "", streamErr
			}
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	result := map[string]any{
		"message": builder.String(),
	}
	bs, _ := json.Marshal(result)
	return string(bs), nil
}

func transitionRunStatus(ctx context.Context, tx *gorm.DB, run *models.Run, nextStatus string, errMsg string) error {
	if !isValidRunTransition(run.Status, nextStatus) {
		return fmt.Errorf("%w: %s -> %s", errInvalidRunTransition, run.Status, nextStatus)
	}
	updates := map[string]any{
		"status":        nextStatus,
		"error_message": errMsg,
	}
	now := time.Now()
	if nextStatus == models.RunStatusRunning && run.StartedAt == nil {
		updates["started_at"] = now
		run.StartedAt = &now
	}
	if nextStatus == models.RunStatusSuccess || nextStatus == models.RunStatusFailed || nextStatus == models.RunStatusCancelled {
		updates["finished_at"] = now
		run.FinishedAt = &now
	}
	if err := tx.WithContext(ctx).
		Model(&models.Run{}).
		Where("run_id = ?", run.RunID).
		Updates(updates).Error; err != nil {
		return err
	}
	run.Status = nextStatus
	run.ErrorMessage = errMsg
	return nil
}

func transitionStepStatus(ctx context.Context, tx *gorm.DB, step *models.RunStep, nextStatus string, outputJSON, errMsg string, latencyMS int64, finishedAt *time.Time) error {
	if !isValidRunStepTransition(step.Status, nextStatus) {
		return fmt.Errorf("%w: %s -> %s", errInvalidStepTransition, step.Status, nextStatus)
	}
	updates := map[string]any{
		"status":        nextStatus,
		"output_json":   outputJSON,
		"error_message": errMsg,
		"latency_ms":    latencyMS,
		"finished_at":   finishedAt,
	}
	if err := tx.WithContext(ctx).
		Model(&models.RunStep{}).
		Where("id = ?", step.ID).
		Updates(updates).Error; err != nil {
		return err
	}
	step.Status = nextStatus
	return nil
}

func isValidRunTransition(from, to string) bool {
	return runstate.IsValidRunTransition(from, to)
}

func isValidRunStepTransition(from, to string) bool {
	return runstate.IsValidRunStepTransition(from, to)
}

func (s *RunService) resolveWorkflow(ctx context.Context, agentID uint64, version int) (*models.Workflow, error) {
	var workflow models.Workflow
	query := s.database.WithContext(ctx).Where("agent_id = ?", agentID)
	if version > 0 {
		err := query.Where("version = ?", version).First(&workflow).Error
		if err != nil {
			return nil, fmt.Errorf("workflow version not found: %w", err)
		}
		return &workflow, nil
	}
	err := query.Where("is_active = ?", true).Order("version DESC").First(&workflow).Error
	if err != nil {
		return nil, fmt.Errorf("active workflow not found: %w", err)
	}
	return &workflow, nil
}

func (s *RunService) fetchRunByRunID(ctx context.Context, runID string) (*models.Run, error) {
	return loadRunByRunID(s.database.WithContext(ctx), runID)
}

func (s *RunService) failRunWithMessage(ctx context.Context, runID, msg string) error {
	return s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var run models.Run
		if err := tx.Where("run_id = ?", runID).First(&run).Error; err != nil {
			return err
		}
		if run.Status == models.RunStatusSuccess || run.Status == models.RunStatusFailed || run.Status == models.RunStatusCancelled {
			return nil
		}
		if run.Status == models.RunStatusQueued || run.Status == models.RunStatusRunning {
			return transitionRunStatus(ctx, tx, &run, models.RunStatusFailed, msg)
		}
		return fmt.Errorf("%w: %s -> %s", errInvalidRunTransition, run.Status, models.RunStatusFailed)
	})
}

func newRunID() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("run_%d", time.Now().UnixNano())
	}
	return "run_" + hex.EncodeToString(buf)
}
