package services

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"GoAI/ai"
	"GoAI/domain/runstate"
	"GoAI/models"
	"GoAI/observability"
	"GoAI/requestctx"

	"gorm.io/gorm"
)

var (
	errRunNotFound           = errors.New("run not found")
	errRunForbidden          = errors.New("run does not belong to current user")
	errRunDispatchFailed     = errors.New("run execute event publish failed")
	errIdempotencyKeyReused  = errors.New("idempotency key reused with different request")
	errRunAlreadyExists      = errors.New("run id already exists with different request")
	errThreadUnavailable     = errors.New("thread is not active")
	errMessageConflict       = errors.New("message id already exists with different content")
	errAgentNotFound         = errors.New("agent not found")
	errWorkflowNotFound      = errors.New("workflow not found")
	errInvalidRunTransition  = errors.New("invalid run status transition")
	errInvalidStepTransition = errors.New("invalid step status transition")
)

var defaultStepRetryBackoffs = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}

const (
	runDispatchTimeout           = 5 * time.Second
	runFailurePersistenceTimeout = 5 * time.Second
	maxNodeRetries               = 3
)

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

type workflowNodeExecutor func(context.Context, *models.Run, WorkflowNode, int) (string, error)

// RunService 协调 Run 持久化、入队、查询、回放和异步执行。
type RunService struct {
	database          *gorm.DB
	publisher         RunEventPublisher
	agentInvoker      AgentInvoker
	chatService       *ChatService
	loopService       *LoopService
	observability     *observability.Bundle
	executeNode       workflowNodeExecutor
	stepRetryBackoffs []time.Duration
}

// NewRunService 使用显式数据库和事件发布器构造 RunService。
func NewRunService(database *gorm.DB, publisher RunEventPublisher, options ...RunServiceOption) (*RunService, error) {
	if database == nil {
		return nil, errors.New("creating run service: database is nil")
	}
	if publisher == nil {
		return nil, errors.New("creating run service: publisher is nil")
	}
	service := &RunService{
		database:          database,
		publisher:         publisher,
		stepRetryBackoffs: append([]time.Duration(nil), defaultStepRetryBackoffs...),
	}
	service.executeNode = service.executeDefaultNode
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

var allowedRunTriggerTypes = map[string]struct{}{
	"api":      {},
	"agui":     {},
	"a2a":      {},
	"manual":   {},
	"replay":   {},
	"schedule": {},
	"webhook":  {},
}

// ErrRunNotFound 返回 Run 不存在的统一 sentinel error。
func ErrRunNotFound() error {
	return errRunNotFound
}

// ErrRunForbidden 返回当前用户无权访问 Run 的统一 sentinel error。
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

// ErrRunAlreadyExists 返回协议指定 RunID 与已有 Run 冲突的 sentinel error。
func ErrRunAlreadyExists() error {
	return errRunAlreadyExists
}

// ErrThreadUnavailable 返回 Thread 已关闭或归档的 sentinel error。
func ErrThreadUnavailable() error {
	return errThreadUnavailable
}

// ErrMessageConflict 返回同一 MessageID 对应不同消息内容的 sentinel error。
func ErrMessageConflict() error {
	return errMessageConflict
}

// ErrAgentNotFound 返回请求目标 Agent 不存在或未启用的 sentinel error。
func ErrAgentNotFound() error {
	return errAgentNotFound
}

// ErrWorkflowNotFound 返回目标 Agent 没有匹配 Workflow 的 sentinel error。
func ErrWorkflowNotFound() error {
	return errWorkflowNotFound
}

// CreateRunRequest 描述创建一次 Run 所需的协议无关输入。
type CreateRunRequest struct {
	AgentCode                 string          `json:"agent_code"`
	WorkflowVersion           int             `json:"workflow_version"`
	ThreadID                  string          `json:"thread_id"`
	TriggerType               string          `json:"trigger_type"`
	Input                     json.RawMessage `json:"input"`
	Provider                  string          `json:"provider"`
	Model                     string          `json:"model"`
	IdempotencyKey            string          `json:"-"`
	RequestedRunID            string          `json:"-"`
	RequestedWorkflowID       uint64          `json:"-"`
	allowGeneratedThreadReuse bool
}

// CreateRunResponse 描述 HTTP 创建或幂等命中 Run 时的最小响应。
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
	if len(strings.TrimSpace(req.ThreadID)) > 64 {
		return errors.New("thread_id must be at most 64 characters")
	}
	if len(strings.TrimSpace(req.RequestedRunID)) > 64 {
		return errors.New("run_id must be at most 64 characters")
	}
	triggerType := strings.TrimSpace(req.TriggerType)
	if triggerType != "" {
		if _, ok := allowedRunTriggerTypes[triggerType]; !ok {
			return errors.New("trigger_type must be one of api,agui,a2a,manual,replay,schedule,webhook")
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
	RequestedRunID  string
	WorkflowID      uint64
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
		RequestedRunID:  strings.TrimSpace(req.RequestedRunID),
		WorkflowID:      req.RequestedWorkflowID,
	}, nil
}

// canonicalizeJSON 将任意合法 JSON 归一化为稳定文本，便于请求哈希计算。
func canonicalizeJSON(raw json.RawMessage) (string, error) {
	inputJSON := strings.TrimSpace(string(raw))
	if inputJSON == "" {
		return "", nil
	}
	var payload any
	decoder := json.NewDecoder(strings.NewReader(inputJSON))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return "", err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return "", errors.New("multiple JSON values are not allowed")
		}
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

// runCreateHook 允许 Runtime 在 Run 创建事务内写入 Thread 与 Message。
type runCreateHook func(*gorm.DB, *models.Run, *models.Agent) error

// sameRunRequest 判断协议指定的 RunID 是否仍表示同一份执行请求。
func sameRunRequest(left, right *models.Run, allowGeneratedThreadReuse bool) bool {
	if left == nil || right == nil {
		return false
	}
	return left.RunID == right.RunID &&
		(allowGeneratedThreadReuse || left.ThreadID == right.ThreadID) &&
		left.AgentID == right.AgentID &&
		left.WorkflowID == right.WorkflowID &&
		left.UserID == right.UserID &&
		left.TriggerType == right.TriggerType &&
		left.InputJSON == right.InputJSON &&
		left.Provider == right.Provider &&
		left.Model == right.Model
}

// matchRequestedRun 校验协议指定的 RunID 是否已绑定到同一稳定请求。
func (s *RunService) matchRequestedRun(
	ctx context.Context,
	userID uint64,
	req normalizedCreateRunRequest,
	allowGeneratedThreadReuse bool,
) (*models.Run, *models.Agent, error) {
	if req.RequestedRunID == "" {
		return nil, nil, nil
	}

	existing, err := loadRunByRunID(s.database.WithContext(ctx), req.RequestedRunID)
	if errors.Is(err, errRunNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	var agent models.Agent
	if err := s.database.WithContext(ctx).First(&agent, "id = ?", existing.AgentID).Error; err != nil {
		return nil, nil, fmt.Errorf("loading requested run agent: %w", err)
	}
	if agent.AgentCode != req.AgentCode {
		return nil, nil, errRunAlreadyExists
	}

	if req.WorkflowVersion > 0 {
		var workflow models.Workflow
		if err := s.database.WithContext(ctx).Select("version").First(&workflow, "id = ?", existing.WorkflowID).Error; err != nil {
			return nil, nil, fmt.Errorf("loading requested run workflow: %w", err)
		}
		if workflow.Version != req.WorkflowVersion {
			return nil, nil, errRunAlreadyExists
		}
	}
	if req.WorkflowID != 0 && existing.WorkflowID != req.WorkflowID {
		return nil, nil, errRunAlreadyExists
	}

	candidate := &models.Run{
		RunID:       req.RequestedRunID,
		ThreadID:    req.ThreadID,
		AgentID:     existing.AgentID,
		WorkflowID:  existing.WorkflowID,
		UserID:      userID,
		TriggerType: req.TriggerType,
		InputJSON:   req.InputJSON,
		Provider:    req.Provider,
		Model:       req.Model,
	}
	if !sameRunRequest(existing, candidate, allowGeneratedThreadReuse) {
		return nil, nil, errRunAlreadyExists
	}
	return existing, &agent, nil
}

// createQueuedRunWithIdempotency 在事务中创建 Run、执行扩展写入、回填幂等记录并推进到 queued。
func (s *RunService) createQueuedRunWithIdempotency(
	ctx context.Context,
	run *models.Run,
	agent *models.Agent,
	idempotency *models.RunIdempotency,
	hook runCreateHook,
	allowGeneratedThreadReuse bool,
) (*RunMutationResult, error) {
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
			if !isUniqueConstraintError(err) || strings.TrimSpace(run.RunID) == "" {
				return err
			}
			existingRun, loadErr := loadRunByRunID(tx, run.RunID)
			if loadErr != nil {
				return loadErr
			}
			if !sameRunRequest(existingRun, run, allowGeneratedThreadReuse) {
				return errRunAlreadyExists
			}
			if idempotency != nil {
				if updateErr := tx.Model(&models.RunIdempotency{}).
					Where("id = ?", idempotency.ID).
					Update("run_id", existingRun.RunID).Error; updateErr != nil {
					return updateErr
				}
			}
			result.Run = existingRun
			result.IdempotentHit = true
			return nil
		}
		if err := s.startRunLoopTx(ctx, tx, run); err != nil {
			return err
		}
		if hook != nil {
			if err := hook(tx, run, agent); err != nil {
				return err
			}
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

// createRun 统一实现普通 API 与协议 Runtime 的 Run 创建流程。
func (s *RunService) createRun(ctx context.Context, userID uint64, req CreateRunRequest, hook runCreateHook) (*RunMutationResult, error) {
	if err := ValidateCreateRunRequest(req); err != nil {
		return nil, err
	}

	normalized, err := normalizeCreateRunRequest(req)
	if err != nil {
		return nil, err
	}

	existingRun, existingAgent, err := s.matchRequestedRun(ctx, userID, normalized, req.allowGeneratedThreadReuse)
	if err != nil {
		return nil, err
	}
	if existingRun != nil && normalized.IdempotencyKey == "" {
		return &RunMutationResult{Run: existingRun, IdempotentHit: true}, nil
	}

	var agent models.Agent
	workflowID := uint64(0)
	if existingRun != nil {
		agent = *existingAgent
		workflowID = existingRun.WorkflowID
	} else {
		if err := s.database.WithContext(ctx).
			Where("agent_code = ? AND status = ?", normalized.AgentCode, models.AgentStatusActive).
			First(&agent).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, errAgentNotFound
			}
			return nil, fmt.Errorf("find active agent by code: %w", err)
		}

		var workflow models.Workflow
		if normalized.WorkflowID != 0 {
			if err := s.database.WithContext(ctx).
				Where("id = ? AND agent_id = ? AND is_active = ?", normalized.WorkflowID, agent.ID, true).
				First(&workflow).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, errWorkflowNotFound
				}
				return nil, fmt.Errorf("find requested workflow: %w", err)
			}
		} else {
			resolved, resolveErr := s.resolveWorkflow(ctx, agent.ID, normalized.WorkflowVersion)
			if resolveErr != nil {
				return nil, resolveErr
			}
			workflow = *resolved
		}
		if _, validateErr := ParseAndValidateWorkflowDefinition(workflow.DefinitionJSON); validateErr != nil {
			return nil, validateErr
		}
		workflowID = workflow.ID
	}

	runID := normalized.RequestedRunID
	if runID == "" {
		runID = newRunID()
	}
	run := &models.Run{
		RunID:       runID,
		ThreadID:    normalized.ThreadID,
		AgentID:     agent.ID,
		WorkflowID:  workflowID,
		UserID:      userID,
		TriggerType: normalized.TriggerType,
		InputJSON:   normalized.InputJSON,
		Status:      models.RunStatusPending,
		Provider:    normalized.Provider,
		Model:       normalized.Model,
	}
	loopID := newPrefixedID("loop")
	run.LoopID = &loopID
	run.TraceID = requestctx.TraceIDFromContext(ctx)

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

	result, err := s.createQueuedRunWithIdempotency(
		ctx,
		run,
		&agent,
		idempotency,
		hook,
		req.allowGeneratedThreadReuse,
	)
	if err != nil {
		return nil, err
	}
	if result.IdempotentHit {
		return result, nil
	}

	if err := s.publishRunExecute(ctx, result.Run.RunID); err != nil {
		return result, s.handleRunDispatchFailure(ctx, result.Run.RunID, "run", err)
	}

	return result, nil
}

// CreateRun 创建并投递一个普通 API Run。
// CreateRun 创建并投递一个普通 API Run。
func (s *RunService) CreateRun(ctx context.Context, userID uint64, req CreateRunRequest) (result *RunMutationResult, err error) {
	observedCtx, span, startedAt := startServiceObservation(ctx, s.observability, "run.create", "", req.ThreadID, "")
	status := "success"
	defer func() {
		if err != nil {
			status = "error"
		}
		runID, threadID, triggerType := "", req.ThreadID, req.TriggerType
		if result != nil && result.Run != nil {
			runID = result.Run.RunID
			threadID = result.Run.ThreadID
			triggerType = result.Run.TriggerType
		}
		finishServiceObservation(s.observability, observedCtx, span, "create_run", status, startedAt, runID, threadID, "", func(metrics *observability.Metrics, _, status string, elapsed time.Duration) {
			metrics.ObserveRun(triggerType, status, elapsed)
		}, err)
	}()
	return s.createRun(observedCtx, userID, req, nil)
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
		Order("id ASC").
		Find(&steps).Error; err != nil {
		return nil, err
	}
	return steps, nil
}

func (s *RunService) ReplayRun(ctx context.Context, userID uint64, isAdmin bool, runID string, idempotencyKey string) (result *RunMutationResult, err error) {
	observedCtx, span, startedAt := startServiceObservation(ctx, s.observability, "run.replay", runID, "", "")
	status := "success"
	defer func() {
		if err != nil {
			status = "error"
		}
		observedRunID := runID
		threadID := ""
		if result != nil && result.Run != nil {
			observedRunID = result.Run.RunID
			threadID = result.Run.ThreadID
		}
		finishServiceObservation(s.observability, observedCtx, span, "replay_run", status, startedAt, observedRunID, threadID, "", func(metrics *observability.Metrics, _, status string, elapsed time.Duration) {
			metrics.ObserveRun("replay", status, elapsed)
		}, err)
	}()

	origin, err := s.GetRunByRunID(observedCtx, userID, isAdmin, runID)
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
	loopID := newPrefixedID("loop")
	clone.LoopID = &loopID
	clone.TraceID = requestctx.TraceIDFromContext(observedCtx)
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

	result, err = s.createQueuedRunWithIdempotency(observedCtx, clone, nil, idempotency, nil, false)
	if err != nil {
		return nil, err
	}
	if result.IdempotentHit {
		return result, nil
	}

	if err := s.publishRunExecute(observedCtx, result.Run.RunID); err != nil {
		return result, s.handleRunDispatchFailure(observedCtx, result.Run.RunID, "replay", err)
	}
	return result, nil
}
func (s *RunService) publishRunExecute(ctx context.Context, runID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runDispatchTimeout)
	defer cancel()
	return s.publisher.PublishRunExecute(publishCtx, runID)
}
func (s *RunService) handleRunDispatchFailure(ctx context.Context, runID, operation string, cause error) error {
	dispatchErr := fmt.Errorf("%w: %w", errRunDispatchFailed, cause)
	persistCtx, cancel := runFailurePersistenceContext(ctx)
	defer cancel()
	message := fmt.Sprintf("dispatch %s message failed: %v", operation, cause)
	if err := s.failRunWithMessage(persistCtx, runID, message); err != nil {
		return errors.Join(dispatchErr, fmt.Errorf("persisting dispatch failure: %w", err))
	}
	return dispatchErr
}

// HandleRunExecute 原子 claim Run，并以短事务逐步执行 Workflow。
func (s *RunService) HandleRunExecute(ctx context.Context, runID string) (err error) {
	observedCtx, span, startedAt := startServiceObservation(ctx, s.observability, "run.execute", runID, "", "")
	status := "success"
	var observedRun *models.Run
	defer func() {
		if err != nil {
			status = "error"
		}
		observedRunID, threadID, triggerType := runID, "", ""
		if observedRun != nil {
			observedRunID = observedRun.RunID
			threadID = observedRun.ThreadID
			triggerType = observedRun.TriggerType
		}
		finishServiceObservation(s.observability, observedCtx, span, "execute_run", status, startedAt, observedRunID, threadID, "", func(metrics *observability.Metrics, _, status string, elapsed time.Duration) {
			metrics.ObserveRun(triggerType, status, elapsed)
		}, err)
	}()

	run, claimed, err := s.claimRunForExecution(observedCtx, runID)
	if err != nil {
		return err
	}
	observedRun = run
	if !claimed {
		return nil
	}

	var workflow models.Workflow
	if err := s.database.WithContext(observedCtx).First(&workflow, "id = ?", run.WorkflowID).Error; err != nil {
		return s.failClaimedRun(observedCtx, run.RunID, err)
	}

	def, err := ParseAndValidateWorkflowDefinition(workflow.DefinitionJSON)
	if err != nil {
		return s.failClaimedRun(observedCtx, run.RunID, err)
	}

	order, err := ResolveExecutionOrder(def)
	if err != nil {
		return s.failClaimedRun(observedCtx, run.RunID, err)
	}

	for _, node := range order {
		if err := s.updateCurrentStep(observedCtx, run.RunID, node.Key); err != nil {
			return s.failClaimedRun(observedCtx, run.RunID, err)
		}
		if err := s.executeNodeWithRetry(observedCtx, run, node); err != nil {
			return s.failClaimedRun(observedCtx, run.RunID, err)
		}
	}

	if err := s.transitionRun(observedCtx, run, models.RunStatusSuccess, ""); err != nil {
		return s.failClaimedRun(observedCtx, run.RunID, err)
	}
	return nil
}

// updateCurrentStep 单独提交当前节点，供协议 Gateway 实时观察执行进度。
func (s *RunService) updateCurrentStep(ctx context.Context, runID, stepKey string) error {
	result := s.database.WithContext(ctx).
		Model(&models.Run{}).
		Where("run_id = ? AND status = ?", runID, models.RunStatusRunning).
		Update("current_step", strings.TrimSpace(stepKey))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("updating current step for run %s: run is not running", runID)
	}
	return nil
}

// transitionRun 使用短事务推进 Run 状态。
func (s *RunService) transitionRun(ctx context.Context, run *models.Run, nextStatus, errMsg string) error {
	return s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := transitionRunStatus(ctx, tx, run, nextStatus, errMsg); err != nil {
			return err
		}
		return s.finishRunLoopTx(ctx, tx, run, nextStatus, errMsg)
	})
}

// executeNodeWithRetry 将每次 attempt 的开始和结束分别提交，节点执行本身不持有数据库事务。
func (s *RunService) executeNodeWithRetry(ctx context.Context, run *models.Run, node WorkflowNode) error {
	var lastErr error
	nodeInput, err := s.resolveNodeInput(ctx, run, node)
	if err != nil {
		return err
	}
	for attempt := 1; attempt <= maxNodeRetries+1; attempt++ {
		nodeRun := *run
		nodeRun.InputJSON = nodeInput
		step, err := s.startRunStep(ctx, &nodeRun, node, attempt)
		if err != nil {
			return err
		}

		outputJSON, execErr := s.executeWorkflowNode(ctx, &nodeRun, node, attempt)
		if execErr == nil {
			outputJSON, execErr = normalizeNodeOutput(outputJSON)
		}
		if execErr == nil && ctx.Err() != nil {
			execErr = ctx.Err()
		}
		finishedAt := time.Now()
		latencyMS := finishedAt.Sub(*step.StartedAt).Milliseconds()
		if execErr != nil {
			lastErr = execErr
			persistCtx, cancel := runFailurePersistenceContext(ctx)
			finishErr := s.finishRunStep(persistCtx, run, step, models.RunStepStatusFailed, "{}", execErr.Error(), latencyMS, &finishedAt)
			cancel()
			if finishErr != nil {
				return errors.Join(execErr, fmt.Errorf("persisting failed run step: %w", finishErr))
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) {
				return execErr
			}
			if invocationErr, ok := execErr.(retryableInvocationError); ok && !invocationErr.Retryable() {
				return execErr
			}
			if attempt > maxNodeRetries || !isRetryableInvocationError(execErr) {
				continue
			}
			if err := s.incrementRunRetry(ctx, run.RunID); err != nil {
				return err
			}
			backoff := time.Duration(0)
			if attempt-1 < len(s.stepRetryBackoffs) {
				backoff = s.stepRetryBackoffs[attempt-1]
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			continue
		}

		persistCtx, cancel := runFailurePersistenceContext(ctx)
		finishErr := s.completeRunStep(persistCtx, run, step, outputJSON, latencyMS, &finishedAt)
		cancel()
		if finishErr != nil {
			failureCtx, failureCancel := runFailurePersistenceContext(ctx)
			markErr := s.finishRunStep(failureCtx, run, step, models.RunStepStatusFailed, "{}", finishErr.Error(), latencyMS, &finishedAt)
			failureCancel()
			if markErr != nil {
				return errors.Join(finishErr, fmt.Errorf("persisting failed run step after completion error: %w", markErr))
			}
			return fmt.Errorf("persisting successful run step: %w", finishErr)
		}
		return nil
	}
	if lastErr == nil {
		return errors.New("node execution failed after retries")
	}
	return lastErr
}

func (s *RunService) startRunLoopTx(ctx context.Context, tx *gorm.DB, run *models.Run) error {
	if s.loopService == nil || run == nil || run.LoopID == nil || strings.TrimSpace(*run.LoopID) == "" {
		return nil
	}
	return func() error {
		_, err := s.loopService.startObservedTx(ctx, tx, LoopStartRequest{
			LoopID:            *run.LoopID,
			TraceID:           run.TraceID,
			ThreadID:          run.ThreadID,
			RunID:             run.RunID,
			AgentID:           run.AgentID,
			WorkflowID:        run.WorkflowID,
			LoopType:          models.LoopTypeRun,
			InputSnapshotJSON: run.InputJSON,
			Provider:          run.Provider,
			Model:             run.Model,
		})
		return err
	}()
}

func (s *RunService) finishRunLoopTx(ctx context.Context, tx *gorm.DB, run *models.Run, status, errMsg string) error {
	if s.loopService == nil || run == nil || run.LoopID == nil || strings.TrimSpace(*run.LoopID) == "" {
		return nil
	}
	loopStatus, ok := loopStatusForRun(status)
	if !ok {
		return nil
	}
	return s.loopService.finishObservedTx(ctx, tx, LoopFinishRequest{
		LoopID:             *run.LoopID,
		Status:             loopStatus,
		OutputSnapshotJSON: fmt.Sprintf(`{"status":%q}`, status),
		ErrorMessage:       errMsg,
	})
}

func loopStatusForRun(status string) (string, bool) {
	switch status {
	case models.RunStatusSuccess:
		return models.LoopStatusSuccess, true
	case models.RunStatusFailed:
		return models.LoopStatusFailed, true
	case models.RunStatusCancelled:
		return models.LoopStatusCancelled, true
	default:
		return "", false
	}
}

// startRunStep 创建 RunStep，并在同一事务中创建对应的 Step Loop。
func (s *RunService) startRunStep(ctx context.Context, run *models.Run, node WorkflowNode, attempt int) (*models.RunStep, error) {
	startedAt := time.Now()
	loopID := newPrefixedID("loop")
	step := &models.RunStep{
		RunID:      run.RunID,
		TraceID:    run.TraceID,
		LoopID:     loopID,
		StepKey:    strings.TrimSpace(node.Key),
		StepType:   strings.TrimSpace(node.Type),
		Attempt:    attempt,
		Status:     models.RunStepStatusRunning,
		InputJSON:  run.InputJSON,
		OutputJSON: "{}",
		Provider:   run.Provider,
		Model:      run.Model,
		StartedAt:  &startedAt,
	}
	if err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(step).Error; err != nil {
			return err
		}
		if s.loopService == nil || run.LoopID == nil || strings.TrimSpace(*run.LoopID) == "" {
			return nil
		}
		_, err := s.loopService.startObservedTx(ctx, tx, LoopStartRequest{
			LoopID:            loopID,
			TraceID:           run.TraceID,
			ThreadID:          run.ThreadID,
			RunID:             run.RunID,
			ParentLoopID:      *run.LoopID,
			AgentID:           run.AgentID,
			WorkflowID:        run.WorkflowID,
			RunStepID:         step.ID,
			LoopType:          models.LoopTypeStep,
			InputSnapshotJSON: run.InputJSON,
			Provider:          run.Provider,
			Model:             run.Model,
		})
		return err
	}); err != nil {
		return nil, err
	}
	return step, nil
}

func (s *RunService) finishRunStep(ctx context.Context, run *models.Run, step *models.RunStep, status, outputJSON, errMsg string, latencyMS int64, finishedAt *time.Time) error {
	normalizedOutput, err := normalizeNodeOutput(outputJSON)
	if err != nil {
		return err
	}
	return s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := transitionStepStatus(ctx, tx, step, status, normalizedOutput, errMsg, latencyMS, finishedAt); err != nil {
			return err
		}
		return s.finishRunStepLoopTx(ctx, tx, run, step, status, normalizedOutput, errMsg, latencyMS, finishedAt)
	})
}

// completeRunStep 在同一短事务内提交成功步骤和其 Result Message，避免协议流看到不完整结果。
func (s *RunService) completeRunStep(ctx context.Context, run *models.Run, step *models.RunStep, outputJSON string, latencyMS int64, finishedAt *time.Time) error {
	normalizedOutput, err := normalizeNodeOutput(outputJSON)
	if err != nil {
		return err
	}
	previousStep := *step
	err = s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := transitionStepStatus(ctx, tx, step, models.RunStepStatusSuccess, normalizedOutput, "", latencyMS, finishedAt); err != nil {
			return err
		}
		if err := s.finishRunStepLoopTx(ctx, tx, run, step, models.RunStepStatusSuccess, normalizedOutput, "", latencyMS, finishedAt); err != nil {
			return err
		}
		return s.persistResultMessage(ctx, tx, run, step, normalizedOutput)
	})
	if err != nil {
		*step = previousStep
	}
	return err
}

func (s *RunService) finishRunStepLoopTx(ctx context.Context, tx *gorm.DB, run *models.Run, step *models.RunStep, status, outputJSON, errMsg string, latencyMS int64, finishedAt *time.Time) error {
	if s.loopService == nil || step == nil || strings.TrimSpace(step.LoopID) == "" {
		return nil
	}
	loopStatus, ok := loopStatusForStep(status)
	if !ok {
		return nil
	}
	return s.loopService.finishObservedTx(ctx, tx, LoopFinishRequest{
		LoopID:             step.LoopID,
		Status:             loopStatus,
		OutputSnapshotJSON: outputJSON,
		ErrorMessage:       errMsg,
		LatencyMS:          latencyMS,
		FinishedAt:         finishedAt,
	})
}

func loopStatusForStep(status string) (string, bool) {
	switch status {
	case models.RunStepStatusSuccess:
		return models.LoopStatusSuccess, true
	case models.RunStepStatusFailed, models.RunStepStatusSkipped:
		return models.LoopStatusFailed, true
	default:
		return "", false
	}
}

func normalizeNodeOutput(outputJSON string) (string, error) {
	normalized, err := canonicalizeJSON(json.RawMessage(outputJSON))
	if err != nil {
		return "", fmt.Errorf("node output must be valid JSON: %w", err)
	}
	if normalized == "" {
		return "{}", nil
	}
	return normalized, nil
}

func (s *RunService) incrementRunRetry(ctx context.Context, runID string) error {
	result := s.database.WithContext(ctx).
		Model(&models.Run{}).
		Where("run_id = ? AND status = ?", runID, models.RunStatusRunning).
		Update("retry_count", gorm.Expr("retry_count + ?", 1))
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: run %s is no longer running", errInvalidRunTransition, runID)
	}
	return nil
}

// persistResultMessage 将节点输出中的文本结果写入当前事务，独立于 SSE 连接生命周期。
func (s *RunService) persistResultMessage(ctx context.Context, tx *gorm.DB, run *models.Run, step *models.RunStep, outputJSON string) error {
	if strings.TrimSpace(run.ThreadID) == "" || strings.TrimSpace(outputJSON) == "" {
		return nil
	}
	var output struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(outputJSON), &output); err != nil || output.Message == "" {
		return nil
	}
	content, err := json.Marshal(map[string]string{"text": output.Message})
	if err != nil {
		return err
	}
	var agent models.Agent
	if err := tx.WithContext(ctx).Select("agent_code").First(&agent, "id = ?", run.AgentID).Error; err != nil {
		return fmt.Errorf("loading result message sender agent: %w", err)
	}
	receiverType := models.MessageSenderUser
	receiverID := fmt.Sprintf("%d", run.UserID)
	delegationID := ""
	parentMessageID := ""
	var delegation models.Delegation
	if !tx.Migrator().HasTable(&models.Delegation{}) {
		delegation = models.Delegation{}
	} else if err := tx.WithContext(ctx).Where("child_run_id = ?", run.RunID).First(&delegation).Error; err == nil {
		var sourceAgent models.Agent
		if err := tx.WithContext(ctx).Select("agent_code").First(&sourceAgent, "id = ?", delegation.SourceAgentID).Error; err != nil {
			return fmt.Errorf("loading delegation source agent: %w", err)
		}
		receiverType = models.MessageSenderAgent
		receiverID = sourceAgent.AgentCode
		delegationID = delegation.DelegationID
		parentMessageID = delegation.RequestMessageID
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("loading result message delegation: %w", err)
	}
	message := &models.Message{
		MessageID:       resultMessageID(run.RunID, step.StepKey, step.Attempt),
		ThreadID:        run.ThreadID,
		RunID:           run.RunID,
		DelegationID:    delegationID,
		ParentMessageID: parentMessageID,
		SenderType:      models.MessageSenderAgent,
		SenderID:        agent.AgentCode,
		ReceiverType:    receiverType,
		ReceiverID:      receiverID,
		MessageType:     models.MessageTypeResult,
		ContentType:     "text",
		ContentJSON:     string(content),
		MetadataJSON:    "{}",
		Status:          models.MessageStatusDelivered,
	}
	return tx.WithContext(ctx).Create(message).Error
}

func (s *RunService) resolveNodeInput(ctx context.Context, run *models.Run, node WorkflowNode) (string, error) {
	if strings.ToLower(strings.TrimSpace(node.Type)) != "agent" {
		return run.InputJSON, nil
	}
	config, err := ParseAgentNodeConfig(node)
	if err != nil {
		return "", err
	}
	if len(config.InputFrom) == 0 {
		return run.InputJSON, nil
	}
	var runInput any
	if err := json.Unmarshal([]byte(run.InputJSON), &runInput); err != nil {
		return "", fmt.Errorf("decoding run input for node %s: %w", node.Key, err)
	}
	outputs := make(map[string]any, len(config.InputFrom))
	for _, reference := range config.InputFrom {
		var step models.RunStep
		if err := s.database.WithContext(ctx).Where("run_id = ? AND step_key = ? AND status = ?", run.RunID, reference, models.RunStepStatusSuccess).Order("attempt DESC").First(&step).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return "", fmt.Errorf("input_from step %s has no successful output", reference)
			}
			return "", fmt.Errorf("loading input_from step %s: %w", reference, err)
		}
		var output any
		if err := json.Unmarshal([]byte(step.OutputJSON), &output); err != nil {
			return "", fmt.Errorf("decoding output of step %s: %w", reference, err)
		}
		outputs[reference] = output
	}
	encoded, err := json.Marshal(map[string]any{"run_input": runInput, "step_outputs": outputs})
	if err != nil {
		return "", fmt.Errorf("encoding aggregated node input: %w", err)
	}
	return string(encoded), nil
}

func (s *RunService) executeWorkflowNode(ctx context.Context, run *models.Run, node WorkflowNode, attempt int) (string, error) {
	if strings.ToLower(strings.TrimSpace(node.Type)) == "agent" {
		return s.executeAgentNode(ctx, run, node)
	}
	return s.executeNode(ctx, run, node, attempt)
}

func (s *RunService) executeAgentNode(ctx context.Context, run *models.Run, node WorkflowNode) (string, error) {
	if s.agentInvoker == nil {
		return "", errors.New("agent workflow node requires an A2A agent invoker")
	}
	config, err := ParseAgentNodeConfig(node)
	if err != nil {
		return "", err
	}
	var source models.Agent
	if err := s.database.WithContext(ctx).Where("id = ? AND status = ?", run.AgentID, models.AgentStatusActive).First(&source).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", errAgentNotFound
		}
		return "", fmt.Errorf("loading source agent: %w", err)
	}
	if source.AgentCode == config.TargetAgent {
		return "", fmt.Errorf("agent node %s cannot target source agent %s", node.Key, config.TargetAgent)
	}
	var target models.Agent
	if err := s.database.WithContext(ctx).Where("agent_code = ? AND status = ?", config.TargetAgent, models.AgentStatusActive).First(&target).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", errAgentNotFound
		}
		return "", fmt.Errorf("loading target agent: %w", err)
	}
	var capability models.AgentCapability
	if err := s.database.WithContext(ctx).Where("agent_id = ? AND capability_code = ? AND status = ?", target.ID, config.Capability, models.AgentCapabilityStatusActive).First(&capability).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", fmt.Errorf("target capability %s not found", config.Capability)
		}
		return "", fmt.Errorf("loading target capability: %w", err)
	}
	var endpoints []models.AgentEndpoint
	if err := s.database.WithContext(ctx).Where("agent_id = ? AND protocol = ? AND status = ?", target.ID, models.AgentEndpointProtocolA2A, models.AgentEndpointStatusActive).Order("id ASC").Find(&endpoints).Error; err != nil {
		return "", fmt.Errorf("loading target A2A endpoints: %w", err)
	}
	invocationEndpoints := make([]AgentInvocationEndpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		invocationEndpoints = append(invocationEndpoints, AgentInvocationEndpoint{Address: endpoint.Address, Transport: endpoint.Transport})
	}
	timeout := 120 * time.Second
	if config.TimeoutMS > 0 {
		timeout = time.Duration(config.TimeoutMS) * time.Millisecond
	}
	invokeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := s.agentInvoker.Invoke(invokeCtx, AgentInvocationRequest{
		SourceAgentCode: source.AgentCode,
		TargetAgentCode: target.AgentCode,
		CapabilityCode:  capability.CapabilityCode,
		ParentRunID:     run.RunID,
		ThreadID:        run.ThreadID,
		TaskID:          stableA2AID("task", run.RunID, node.Key),
		MessageID:       stableA2AID("message", run.RunID, node.Key),
		InputJSON:       run.InputJSON,
		Endpoints:       invocationEndpoints,
	})
	if err != nil {
		return "", err
	}
	result = normalizeInvocationResult(result)
	if result == nil {
		return "", errors.New("A2A agent invoker returned an empty result")
	}
	if result.State != "" && result.State != "TASK_STATE_COMPLETED" {
		return "", fmt.Errorf("target agent completed with unsupported state %s", result.State)
	}
	var raw json.RawMessage = json.RawMessage(result.OutputJSON)
	if !json.Valid(raw) {
		return "", errors.New("A2A agent result is not valid JSON")
	}
	output := map[string]any{
		"type":         "agent",
		"target_agent": target.AgentCode,
		"capability":   capability.CapabilityCode,
		"task_id":      result.TaskID,
		"state":        result.State,
		"result":       raw,
	}
	if result.Message != "" {
		output["message"] = result.Message
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		return "", fmt.Errorf("encoding A2A agent result: %w", err)
	}
	return string(encoded), nil
}

func stableA2AID(kind, parentRunID, nodeKey string) string {
	sum := sha256.Sum256([]byte(kind + "\x00" + parentRunID + "\x00" + nodeKey))
	return "a2a_" + hex.EncodeToString(sum[:])[:59]
}

func executeWorkflowNode(ctx context.Context, run *models.Run, node WorkflowNode, attempt int) (string, error) {
	return executeWorkflowNodeWithChatService(ctx, run, node, attempt, nil)
}

func (s *RunService) executeDefaultNode(ctx context.Context, run *models.Run, node WorkflowNode, attempt int) (string, error) {
	return executeWorkflowNodeWithChatService(ctx, run, node, attempt, s.chatService)
}

func executeWorkflowNodeWithChatService(ctx context.Context, run *models.Run, node WorkflowNode, attempt int, chatService *ChatService) (string, error) {
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
		return executeLLMNode(ctx, run, chatService)
	case "tool", "noop", "planner":
		resp := map[string]any{
			"step_key": node.Key,
			"type":     nodeType,
			"result":   "ok",
		}
		bs, _ := json.Marshal(resp)
		return string(bs), nil
	default:
		return "", fmt.Errorf("unsupported workflow node type %q for node %s", nodeType, node.Key)
	}
}

func executeLLMNode(ctx context.Context, run *models.Run, chatService *ChatService) (string, error) {
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

	if chatService == nil {
		return "", errors.New("llm workflow node requires a chat service")
	}
	stream, err := chatService.Chat(ctx, messages, run.Provider, run.Model)
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
	setStartedAt := nextStatus == models.RunStatusRunning && run.StartedAt == nil
	setFinishedAt := nextStatus == models.RunStatusSuccess || nextStatus == models.RunStatusFailed || nextStatus == models.RunStatusCancelled
	if setStartedAt {
		updates["started_at"] = now
	}
	if setFinishedAt {
		updates["finished_at"] = now
	}
	previousStatus := run.Status
	result := tx.WithContext(ctx).
		Model(&models.Run{}).
		Where("run_id = ? AND status = ?", run.RunID, previousStatus).
		Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: run %s is no longer in status %s", errInvalidRunTransition, run.RunID, previousStatus)
	}
	run.Status = nextStatus
	run.ErrorMessage = errMsg
	if setStartedAt {
		run.StartedAt = &now
	}
	if setFinishedAt {
		run.FinishedAt = &now
	}
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
	previousStatus := step.Status
	result := tx.WithContext(ctx).
		Model(&models.RunStep{}).
		Where("id = ? AND status = ?", step.ID, previousStatus).
		Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: run step %d is no longer in status %s", errInvalidStepTransition, step.ID, previousStatus)
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
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, errWorkflowNotFound
			}
			return nil, fmt.Errorf("workflow version not found: %w", err)
		}
		return &workflow, nil
	}
	err := query.Where("is_active = ?", true).Order("version DESC").First(&workflow).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errWorkflowNotFound
		}
		return nil, fmt.Errorf("active workflow not found: %w", err)
	}
	return &workflow, nil
}

func (s *RunService) fetchRunByRunID(ctx context.Context, runID string) (*models.Run, error) {
	return loadRunByRunID(s.database.WithContext(ctx), runID)
}

func runFailurePersistenceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), runFailurePersistenceTimeout)
}

func (s *RunService) failClaimedRun(ctx context.Context, runID string, cause error) error {
	cleanupCtx, cancel := runFailurePersistenceContext(ctx)
	defer cancel()
	if err := s.failRunWithMessage(cleanupCtx, runID, cause.Error()); err != nil {
		return errors.Join(cause, fmt.Errorf("persisting failed run: %w", err))
	}
	return cause
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
			if err := transitionRunStatus(ctx, tx, &run, models.RunStatusFailed, msg); err != nil {
				return err
			}
			return s.finishRunLoopTx(ctx, tx, &run, models.RunStatusFailed, msg)
		}
		return fmt.Errorf("%w: %s -> %s", errInvalidRunTransition, run.Status, models.RunStatusFailed)
	})
}

func newRunID() string {
	return newPrefixedID("run")
}

func newMessageID() string {
	return newPrefixedID("msg")
}

func resultMessageID(runID, stepKey string, attempt int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", runID, stepKey, attempt)))
	return "msg_" + hex.EncodeToString(sum[:])[:60]
}

func newThreadID() string {
	return newPrefixedID("thread")
}

func newPrefixedID(prefix string) string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(buf)
}
