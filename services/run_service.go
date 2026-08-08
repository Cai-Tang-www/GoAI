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
	"log/slog"
	"strings"
	"time"

	"GoAI/ai"
	"GoAI/domain/runstate"
	"GoAI/einoexecutor"
	"GoAI/models"
	"GoAI/observability"
	"GoAI/requestctx"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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

type agentInvocationAcceptedError struct {
	TaskID            string
	DelegationID      string
	MessageID         string
	SourceAgentID     uint64
	TargetAgentID     uint64
	CapabilityCode    string
	OutputJSON        string
	CallbackTokenHash string
	GroupID           string
}

func (e *agentInvocationAcceptedError) Error() string {
	return fmt.Sprintf("A2A task %s accepted for asynchronous execution", e.TaskID)
}

type runSuspendedError struct {
	TaskID        string
	DelegationID  string
	StepKey       string
	ResumeNodeKey string
}

func (e *runSuspendedError) Error() string {
	return fmt.Sprintf("run suspended waiting for A2A task %s", e.TaskID)
}

// runExternallyTerminatedError 表示 A2A 聚合在父 Run 挂起前已失败或取消，执行器应停止当前 Graph。
type runExternallyTerminatedError struct {
	GroupID string
	Status  string
}

func (e *runExternallyTerminatedError) Error() string {
	return fmt.Sprintf("agent group %s terminated parent run with status %s", e.GroupID, e.Status)
}

func runExecutionStopped(err error) bool {
	var suspended *runSuspendedError
	if errors.As(err, &suspended) {
		return true
	}
	var terminated *runExternallyTerminatedError
	return errors.As(err, &terminated)
}

type agentInvocationSettlement struct {
	OutputJSON     string
	Suspended      bool
	TerminalStatus string
}

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
	database                 *gorm.DB
	publisher                RunEventPublisher
	agentInvoker             AgentInvoker
	toolInvoker              ToolInvoker
	graphExecutor            *einoexecutor.Executor
	chatService              *ChatService
	loopService              *LoopService
	observability            *observability.Bundle
	executeNode              workflowNodeExecutor
	stepRetryBackoffs        []time.Duration
	resumeLeaseDuration      time.Duration
	resumeHeartbeatInterval  time.Duration
	resumePersistenceTimeout time.Duration
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
		database:                 database,
		publisher:                publisher,
		graphExecutor:            einoexecutor.New(),
		stepRetryBackoffs:        append([]time.Duration(nil), defaultStepRetryBackoffs...),
		resumeLeaseDuration:      30 * time.Second,
		resumeHeartbeatInterval:  10 * time.Second,
		resumePersistenceTimeout: runFailurePersistenceTimeout,
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

// RunResumeState 描述管理端查询 Run 时可见的最近一次 Parent resume 状态。
type RunResumeState struct {
	DelegationID     string     `json:"delegation_id"`
	Status           string     `json:"status"`
	Error            string     `json:"error"`
	PublishAttempts  int        `json:"publish_attempts"`
	ExecutionAttempt int        `json:"execution_attempt"`
	LeaseOwner       string     `json:"lease_owner"`
	LeaseClaimedAt   *time.Time `json:"lease_claimed_at,omitempty"`
	LeaseHeartbeatAt *time.Time `json:"lease_heartbeat_at,omitempty"`
	LeaseExpiresAt   *time.Time `json:"lease_expires_at,omitempty"`
}

// RunDelegationMemberState 描述 agent_group 中一个独立 A2A 委派成员的管理视图。
type RunDelegationMemberState struct {
	MemberKey    string `json:"member_key"`
	Position     int    `json:"position"`
	DelegationID string `json:"delegation_id"`
	ChildRunID   string `json:"child_run_id"`
	A2ATaskID    string `json:"a2a_task_id,omitempty"`
	TargetAgent  string `json:"target_agent"`
	Capability   string `json:"capability"`
	Status       string `json:"status"`
	OutputJSON   string `json:"output_json"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// RunDelegationGroupState 描述一个 agent_group 的策略、聚合计数和成员结果。
type RunDelegationGroupState struct {
	GroupID                 string                     `json:"group_id"`
	ParentStepKey           string                     `json:"parent_step_key"`
	Strategy                string                     `json:"strategy"`
	RequiredSuccesses       int                        `json:"required_successes"`
	TotalMembers            int                        `json:"total_members"`
	SucceededMembers        int                        `json:"succeeded_members"`
	FailedMembers           int                        `json:"failed_members"`
	CancelledMembers        int                        `json:"cancelled_members"`
	Status                  string                     `json:"status"`
	ResultJSON              string                     `json:"result_json"`
	ErrorMessage            string                     `json:"error_message,omitempty"`
	CoordinatorDelegationID string                     `json:"coordinator_delegation_id"`
	Members                 []RunDelegationMemberState `json:"members"`
}

// RunDetail 在保持原 Run JSON 字段兼容的同时，附加恢复租约和 fan-out 协调信息。
type RunDetail struct {
	models.Run
	Resume           *RunResumeState           `json:"resume,omitempty"`
	DelegationGroups []RunDelegationGroupState `json:"delegation_groups,omitempty"`
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

// GetRunDetailByRunID 返回 Run、最近一次 Parent resume 租约和 fan-out 协调状态。
func (s *RunService) GetRunDetailByRunID(ctx context.Context, userID uint64, isAdmin bool, runID string) (*RunDetail, error) {
	run, err := s.GetRunByRunID(ctx, userID, isAdmin, runID)
	if err != nil {
		return nil, err
	}
	detail := &RunDetail{Run: *run}
	detail.DelegationGroups, err = s.loadRunDelegationGroups(ctx, run.RunID)
	if err != nil {
		return nil, err
	}

	var delegation models.Delegation
	err = s.database.WithContext(ctx).
		Where("parent_run_id = ? AND resume_status <> ?", run.RunID, models.DelegationResumeStatusNone).
		Order("id DESC").First(&delegation).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return detail, nil
	}
	if err != nil {
		return nil, fmt.Errorf("loading run resume state: %w", err)
	}
	detail.Resume = &RunResumeState{
		DelegationID:     delegation.DelegationID,
		Status:           delegation.ResumeStatus,
		Error:            delegation.ResumeError,
		PublishAttempts:  delegation.ResumeAttemptCount,
		ExecutionAttempt: delegation.ResumeExecutionAttempt,
		LeaseOwner:       delegation.ResumeLeaseOwner,
		LeaseClaimedAt:   delegation.ResumeLeaseClaimedAt,
		LeaseHeartbeatAt: delegation.ResumeLeaseHeartbeatAt,
		LeaseExpiresAt:   delegation.ResumeLeaseExpiresAt,
	}
	return detail, nil
}

func (s *RunService) loadRunDelegationGroups(ctx context.Context, runID string) ([]RunDelegationGroupState, error) {
	var groups []models.DelegationGroup
	if err := s.database.WithContext(ctx).Where("parent_run_id = ?", runID).Order("id ASC").Find(&groups).Error; err != nil {
		return nil, fmt.Errorf("loading run delegation groups: %w", err)
	}
	if len(groups) == 0 {
		return nil, nil
	}

	groupIDs := make([]string, 0, len(groups))
	for _, group := range groups {
		groupIDs = append(groupIDs, group.GroupID)
	}
	var delegations []models.Delegation
	if err := s.database.WithContext(ctx).Where("delegation_group_id IN ?", groupIDs).Order("delegation_group_id ASC").Order("group_member_position ASC").Find(&delegations).Error; err != nil {
		return nil, fmt.Errorf("loading run delegation group members: %w", err)
	}
	targetIDs := make([]uint64, 0, len(delegations))
	for _, delegation := range delegations {
		targetIDs = append(targetIDs, delegation.TargetAgentID)
	}
	var targets []models.Agent
	if len(targetIDs) > 0 {
		if err := s.database.WithContext(ctx).Select("id", "agent_code").Where("id IN ?", targetIDs).Find(&targets).Error; err != nil {
			return nil, fmt.Errorf("loading delegation group target agents: %w", err)
		}
	}
	targetCodes := make(map[uint64]string, len(targets))
	for _, target := range targets {
		targetCodes[target.ID] = target.AgentCode
	}
	membersByGroup := make(map[string][]RunDelegationMemberState, len(groups))
	for _, delegation := range delegations {
		if delegation.DelegationGroupID == nil {
			continue
		}
		memberKey, taskID := "", ""
		if delegation.GroupMemberKey != nil {
			memberKey = *delegation.GroupMemberKey
		}
		if delegation.A2ATaskID != nil {
			taskID = *delegation.A2ATaskID
		}
		groupID := *delegation.DelegationGroupID
		membersByGroup[groupID] = append(membersByGroup[groupID], RunDelegationMemberState{
			MemberKey: memberKey, Position: delegation.GroupMemberPosition, DelegationID: delegation.DelegationID,
			ChildRunID: delegation.ChildRunID, A2ATaskID: taskID, TargetAgent: targetCodes[delegation.TargetAgentID],
			Capability: delegation.CapabilityCode, Status: delegation.Status, OutputJSON: delegation.OutputJSON, ErrorMessage: delegation.ErrorMessage,
		})
	}

	states := make([]RunDelegationGroupState, 0, len(groups))
	for _, group := range groups {
		states = append(states, RunDelegationGroupState{
			GroupID: group.GroupID, ParentStepKey: group.ParentStepKey, Strategy: group.Strategy,
			RequiredSuccesses: group.RequiredSuccesses, TotalMembers: group.TotalMembers,
			SucceededMembers: group.SucceededMembers, FailedMembers: group.FailedMembers, CancelledMembers: group.CancelledMembers,
			Status: group.Status, ResultJSON: group.ResultJSON, ErrorMessage: group.ErrorMessage,
			CoordinatorDelegationID: group.CoordinatorDelegationID, Members: membersByGroup[group.GroupID],
		})
	}
	return states, nil
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

	if s.graphExecutor == nil {
		return s.failClaimedRun(observedCtx, run.RunID, errors.New("run service graph executor is nil"))
	}
	outputJSON, err := s.graphExecutor.Execute(observedCtx, def, run.InputJSON, func(nodeCtx context.Context, node WorkflowNode, input string) (string, error) {
		if err := s.updateCurrentStep(nodeCtx, run.RunID, node.Key); err != nil {
			return "", err
		}
		resumeNodeKey, successorErr := workflowSuccessor(def, node.Key)
		if successorErr != nil {
			return "", successorErr
		}
		return s.executeNodeWithRetryInputAt(nodeCtx, run, node, input, resumeNodeKey)
	})
	if err != nil {
		if runExecutionStopped(err) {
			return nil
		}
		return s.failClaimedRun(observedCtx, run.RunID, err)
	}
	if err := s.validateRunOutputContract(observedCtx, run, outputJSON); err != nil {
		return s.failClaimedRun(observedCtx, run.RunID, err)
	}

	if err := s.transitionRun(observedCtx, run, models.RunStatusSuccess, ""); err != nil {
		return s.failClaimedRun(observedCtx, run.RunID, err)
	}
	return nil
}

// HandleRunResume 通过带 fencing token 的租约恢复 Parent Run，并从持久化 checkpoint 继续执行。
func (s *RunService) HandleRunResume(ctx context.Context, runID, delegationID string) error {
	runID = strings.TrimSpace(runID)
	delegationID = strings.TrimSpace(delegationID)
	if runID == "" || delegationID == "" {
		return errors.New("resuming run: run_id and delegation_id are required")
	}

	run, delegation, lease, claimed, err := s.claimRunResume(ctx, runID, delegationID)
	if err != nil || !claimed {
		return err
	}

	executeCtx, stopHeartbeat := s.startResumeLeaseHeartbeat(ctx, lease)
	stopped := false
	stopLease := func() error {
		stopped = true
		return stopHeartbeat()
	}
	defer func() {
		if !stopped {
			_ = stopLease()
		}
	}()

	fail := func(operationErr error) error {
		executionCause := context.Cause(executeCtx)
		heartbeatErr := stopLease()
		if executionCause != nil || heartbeatErr != nil || errors.Is(operationErr, errResumeLeaseLost) {
			cause := errors.Join(operationErr, executionCause, heartbeatErr)
			s.logResumeLease(executeCtx, slog.LevelWarn, "run resume execution yielded to recovery", lease, cause)
			return nil
		}
		if operationErr == nil {
			operationErr = errors.New("resumed run failed")
		}
		persisted, failureErr := s.failResumedRun(executeCtx, &run, &delegation, operationErr)
		if !persisted && resumeExecutionShouldRecover(failureErr) {
			s.logResumeLease(executeCtx, slog.LevelWarn, "run resume failure persistence lost lease", lease, failureErr)
			return nil
		}
		return failureErr
	}
	completeDelegation := func() error {
		executionCause := context.Cause(executeCtx)
		heartbeatErr := stopLease()
		if executionCause != nil || heartbeatErr != nil {
			cause := errors.Join(executionCause, heartbeatErr)
			s.logResumeLease(executeCtx, slog.LevelWarn, "run resume completion yielded to recovery", lease, cause)
			return nil
		}
		if err := s.completeSuspendedResume(executeCtx, &run, &delegation); err != nil {
			if resumeExecutionShouldRecover(err) {
				s.logResumeLease(executeCtx, slog.LevelWarn, "delegation resume completion lost lease", lease, err)
				return nil
			}
			return err
		}
		return nil
	}
	completeRun := func(outputJSON string) error {
		if err := s.validateRunOutputContract(executeCtx, &run, outputJSON); err != nil {
			return fail(err)
		}
		executionCause := context.Cause(executeCtx)
		heartbeatErr := stopLease()
		if executionCause != nil || heartbeatErr != nil {
			cause := errors.Join(executionCause, heartbeatErr)
			s.logResumeLease(executeCtx, slog.LevelWarn, "run resume completion yielded to recovery", lease, cause)
			return nil
		}
		if err := s.completeResumedRun(executeCtx, &run, &delegation); err != nil {
			if resumeExecutionShouldRecover(err) {
				s.logResumeLease(executeCtx, slog.LevelWarn, "run resume terminal write lost lease", lease, err)
				return nil
			}
			return err
		}
		return nil
	}

	callbackOutput, err := s.loadResumeCallbackOutput(executeCtx, run.RunID, delegation.ParentStepKey)
	if err != nil {
		return fail(err)
	}
	if strings.TrimSpace(delegation.ResumeNodeKey) == "" {
		return completeRun(callbackOutput)
	}
	if s.graphExecutor == nil {
		return fail(errors.New("run service graph executor is nil"))
	}

	var workflow models.Workflow
	if err := s.database.WithContext(executeCtx).First(&workflow, "id = ?", run.WorkflowID).Error; err != nil {
		return fail(err)
	}
	def, err := ParseAndValidateWorkflowDefinition(workflow.DefinitionJSON)
	if err != nil {
		return fail(err)
	}
	outputJSON, err := s.graphExecutor.ExecuteFrom(executeCtx, def, delegation.ResumeNodeKey, callbackOutput, func(nodeCtx context.Context, node WorkflowNode, input string) (string, error) {
		checkpointOutput, completed, checkpointErr := s.resumeNodeCheckpoint(nodeCtx, &run, node)
		if checkpointErr != nil {
			return "", checkpointErr
		}
		if completed {
			return checkpointOutput, nil
		}
		if err := s.updateCurrentStep(nodeCtx, run.RunID, node.Key); err != nil {
			return "", err
		}
		next, err := workflowSuccessor(def, node.Key)
		if err != nil {
			return "", err
		}
		return s.executeNodeWithRetryInputAt(nodeCtx, &run, node, input, next)
	})
	if err != nil {
		if runExecutionStopped(err) {
			return completeDelegation()
		}
		return fail(err)
	}
	return completeRun(outputJSON)
}

func workflowSuccessor(def *WorkflowDefinition, nodeKey string) (string, error) {
	nodeKey = strings.TrimSpace(nodeKey)
	successor := ""
	for _, edge := range def.Edges {
		if strings.TrimSpace(edge.From) != nodeKey {
			continue
		}
		if successor != "" {
			return "", fmt.Errorf("workflow node %s has multiple successors", nodeKey)
		}
		successor = strings.TrimSpace(edge.To)
	}
	return successor, nil
}

func (s *RunService) updateCurrentStep(ctx context.Context, runID, stepKey string) error {
	return s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.guardResumeLeaseTx(ctx, tx); err != nil {
			return err
		}
		result := tx.Model(&models.Run{}).
			Where("run_id = ? AND status = ?", runID, models.RunStatusRunning).
			Update("current_step", strings.TrimSpace(stepKey))
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("updating current step for run %s: run is not running", runID)
		}
		return nil
	})
}

// transitionRun 使用短事务推进 Run 状态。
func (s *RunService) transitionRun(ctx context.Context, run *models.Run, nextStatus, errMsg string) error {
	return s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.guardResumeLeaseTx(ctx, tx); err != nil {
			return err
		}
		if err := transitionRunStatus(ctx, tx, run, nextStatus, errMsg); err != nil {
			return err
		}
		return s.finishRunLoopTx(ctx, tx, run, nextStatus, errMsg)
	})
}

// executeNodeWithRetry 将每次 attempt 的开始和结束分别提交，节点执行本身不持有数据库事务。
func (s *RunService) executeNodeWithRetry(ctx context.Context, run *models.Run, node WorkflowNode) error {
	nodeInput, err := s.resolveNodeInput(ctx, run, node)
	if err != nil {
		return err
	}
	_, err = s.executeNodeWithRetryInput(ctx, run, node, nodeInput)
	return err
}

// executeNodeWithRetryInput 执行一个已确定输入的节点，并返回最终输出供 Eino 下游节点使用。
func (s *RunService) executeNodeWithRetryInput(ctx context.Context, run *models.Run, node WorkflowNode, nodeInput string) (string, error) {
	return s.executeNodeWithRetryInputAt(ctx, run, node, nodeInput, "")
}

func (s *RunService) executeNodeWithRetryInputAt(ctx context.Context, run *models.Run, node WorkflowNode, nodeInput, resumeNodeKey string) (string, error) {
	var lastErr error
	nodeType := strings.ToLower(strings.TrimSpace(node.Type))
	var explicitInputs bool
	switch nodeType {
	case "agent":
		config, err := ParseAgentNodeConfig(node)
		if err != nil {
			return "", err
		}
		explicitInputs = len(config.InputFrom) > 0
	case "agent_group":
		config, err := ParseAgentGroupNodeConfig(node)
		if err != nil {
			return "", err
		}
		explicitInputs = len(config.InputFrom) > 0
	case "tool":
		config, err := ParseToolNodeConfig(node)
		if err != nil {
			return "", err
		}
		explicitInputs = len(config.InputFrom) > 0
	}
	if explicitInputs {
		var err error
		nodeInput, err = s.resolveNodeInput(ctx, run, node)
		if err != nil {
			return "", err
		}
	}
	startAttempt, err := s.nextRunStepAttempt(ctx, run.RunID, node.Key)
	if err != nil {
		return "", err
	}
	for offset := 0; offset <= maxNodeRetries; offset++ {
		attempt := startAttempt + offset
		nodeRun := *run
		nodeRun.InputJSON = nodeInput
		step, err := s.startRunStep(ctx, &nodeRun, node, attempt)
		if err != nil {
			return "", err
		}

		outputJSON, execErr := s.executeWorkflowNodeWithInput(ctx, &nodeRun, node, attempt, nodeInput)
		if execErr == nil {
			outputJSON, execErr = normalizeNodeOutput(outputJSON)
		}
		if execErr == nil && ctx.Err() != nil {
			execErr = ctx.Err()
		}
		finishedAt := time.Now()
		latencyMS := finishedAt.Sub(*step.StartedAt).Milliseconds()
		if execErr != nil {
			var accepted *agentInvocationAcceptedError
			if errors.As(execErr, &accepted) {
				settlement, err := s.suspendRunForAgentInvocation(ctx, run, step, node, resumeNodeKey, accepted)
				if err != nil {
					return "", errors.Join(execErr, fmt.Errorf("persisting suspended run: %w", err))
				}
				if settlement.TerminalStatus != "" {
					return "", &runExternallyTerminatedError{GroupID: accepted.GroupID, Status: settlement.TerminalStatus}
				}
				if !settlement.Suspended {
					return settlement.OutputJSON, nil
				}
				return "", &runSuspendedError{TaskID: accepted.TaskID, DelegationID: accepted.DelegationID, StepKey: node.Key, ResumeNodeKey: resumeNodeKey}
			}
			lastErr = execErr
			persistCtx, cancel := runFailurePersistenceContext(ctx)
			finishErr := s.finishRunStep(persistCtx, run, step, models.RunStepStatusFailed, "{}", execErr.Error(), latencyMS, &finishedAt)
			cancel()
			if finishErr != nil {
				return "", errors.Join(execErr, fmt.Errorf("persisting failed run step: %w", finishErr))
			}
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) {
				return "", execErr
			}
			if !isRetryableInvocationError(execErr) {
				return "", execErr
			}
			if offset >= maxNodeRetries {
				var dispatchErr *agentGroupDispatchError
				if errors.As(execErr, &dispatchErr) {
					finalizeCtx, finalizeCancel := runFailurePersistenceContext(ctx)
					finalizeErr := s.finalizeAgentGroupDispatchFailure(finalizeCtx, dispatchErr.groupID, execErr)
					finalizeCancel()
					if finalizeErr != nil {
						return "", errors.Join(execErr, fmt.Errorf("finalizing failed agent group: %w", finalizeErr))
					}
				}
				break
			}
			if err := s.incrementRunRetry(ctx, run.RunID); err != nil {
				return "", err
			}
			backoff := time.Duration(0)
			if offset < len(s.stepRetryBackoffs) {
				backoff = s.stepRetryBackoffs[offset]
			}
			select {
			case <-ctx.Done():
				return "", ctx.Err()
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
				return "", errors.Join(finishErr, fmt.Errorf("persisting failed run step after completion error: %w", markErr))
			}
			return "", fmt.Errorf("persisting successful run step: %w", finishErr)
		}
		return outputJSON, nil
	}
	if lastErr == nil {
		return "", errors.New("node execution failed after retries")
	}
	return "", lastErr
}

// suspendRunForAgentInvocation 在同一事务中决定父 Run 进入等待态，或收敛已提前完成的 agent_group。
func (s *RunService) suspendRunForAgentInvocation(ctx context.Context, run *models.Run, step *models.RunStep, node WorkflowNode, resumeNodeKey string, accepted *agentInvocationAcceptedError) (agentInvocationSettlement, error) {
	if run == nil || step == nil || accepted == nil {
		return agentInvocationSettlement{}, errors.New("suspending run: run, step and invocation are required")
	}
	outputJSON, err := normalizeNodeOutput(accepted.OutputJSON)
	if err != nil {
		return agentInvocationSettlement{}, err
	}
	settlement := agentInvocationSettlement{OutputJSON: outputJSON, Suspended: true}
	err = s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.guardResumeLeaseTx(ctx, tx); err != nil {
			return err
		}

		if accepted.GroupID != "" {
			var group models.DelegationGroup
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("group_id = ?", accepted.GroupID).First(&group).Error; err != nil {
				return err
			}
			if err := tx.Model(&models.DelegationGroup{}).Where("id = ?", group.ID).Updates(map[string]any{
				"loop_id": step.LoopID, "resume_node_key": resumeNodeKey,
			}).Error; err != nil {
				return err
			}
			group.LoopID = step.LoopID
			group.ResumeNodeKey = resumeNodeKey
			if group.Status != models.DelegationGroupStatusWaiting {
				var settleErr error
				settlement, settleErr = s.settleTerminalAgentGroupBeforeSuspendTx(ctx, tx, run, step, &group)
				return settleErr
			}
		}

		if err := transitionStepStatus(ctx, tx, step, models.RunStepStatusWaitingExternal, outputJSON, "", 0, nil); err != nil {
			return err
		}
		if err := transitionRunStatus(ctx, tx, run, models.RunStatusWaitingExternal, ""); err != nil {
			return err
		}

		var delegation models.Delegation
		err := tx.Where("delegation_id = ?", accepted.DelegationID).First(&delegation).Error
		if err == nil {
			if delegation.ParentRunID != run.RunID || delegation.SourceAgentID != accepted.SourceAgentID || delegation.TargetAgentID != accepted.TargetAgentID {
				return errors.New("existing delegation does not match suspended parent run")
			}
			return tx.Model(&models.Delegation{}).Where("id = ?", delegation.ID).Updates(map[string]any{
				"a2_a_task_id": accepted.TaskID, "parent_step_key": node.Key, "resume_node_key": resumeNodeKey,
				"trace_id": run.TraceID, "loop_id": step.LoopID, "callback_token_hash": accepted.CallbackTokenHash,
			}).Error
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		var source, target models.Agent
		if err := tx.First(&source, "id = ?", accepted.SourceAgentID).Error; err != nil {
			return err
		}
		if err := tx.First(&target, "id = ?", accepted.TargetAgentID).Error; err != nil {
			return err
		}
		message := &models.Message{
			MessageID: accepted.MessageID, ThreadID: run.ThreadID, RunID: run.RunID, DelegationID: accepted.DelegationID,
			SenderType: models.MessageSenderAgent, SenderID: source.AgentCode, ReceiverType: models.MessageSenderAgent,
			ReceiverID: target.AgentCode, MessageType: models.MessageTypeDelegation, ContentType: "application/json",
			ContentJSON: step.InputJSON, MetadataJSON: "{}", Status: models.MessageStatusDelivered,
		}
		if err := tx.Create(message).Error; err != nil {
			return err
		}
		taskID := accepted.TaskID
		delegation = models.Delegation{
			DelegationID: accepted.DelegationID, ThreadID: run.ThreadID, ParentRunID: run.RunID, ChildRunID: accepted.TaskID,
			A2ATaskID: &taskID, TraceID: run.TraceID, LoopID: step.LoopID, SourceAgentID: accepted.SourceAgentID,
			TargetAgentID: accepted.TargetAgentID, CapabilityCode: accepted.CapabilityCode, RequestMessageID: accepted.MessageID,
			ParentStepKey: node.Key, ResumeNodeKey: resumeNodeKey, InputJSON: step.InputJSON, OutputJSON: "{}",
			Status: models.DelegationStatusAccepted, ResumeStatus: models.DelegationResumeStatusNone, CallbackTokenHash: accepted.CallbackTokenHash,
		}
		return tx.Create(&delegation).Error
	})
	return settlement, err
}

func (s *RunService) settleTerminalAgentGroupBeforeSuspendTx(ctx context.Context, tx *gorm.DB, run *models.Run, step *models.RunStep, group *models.DelegationGroup) (agentInvocationSettlement, error) {
	resultJSON, err := normalizeNodeOutput(group.ResultJSON)
	if err != nil {
		return agentInvocationSettlement{}, err
	}
	var storedRun models.Run
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("run_id = ?", run.RunID).First(&storedRun).Error; err != nil {
		return agentInvocationSettlement{}, err
	}
	var storedStep models.RunStep
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", step.ID).First(&storedStep).Error; err != nil {
		return agentInvocationSettlement{}, err
	}
	if storedRun.Status != models.RunStatusRunning || storedStep.Status != models.RunStepStatusRunning {
		return agentInvocationSettlement{}, fmt.Errorf("settling terminal agent group %s requires running parent run and step, got run=%s step=%s", group.GroupID, storedRun.Status, storedStep.Status)
	}

	now := time.Now()
	latency := int64(0)
	if storedStep.StartedAt != nil {
		latency = now.Sub(*storedStep.StartedAt).Milliseconds()
	}
	settlement := agentInvocationSettlement{OutputJSON: resultJSON}
	switch group.Status {
	case models.DelegationGroupStatusSucceeded:
		if err := transitionStepStatus(ctx, tx, &storedStep, models.RunStepStatusSuccess, resultJSON, "", latency, &now); err != nil {
			return agentInvocationSettlement{}, err
		}
		if err := s.finishRunStepLoopTx(ctx, tx, &storedRun, &storedStep, models.RunStepStatusSuccess, resultJSON, "", latency, &now); err != nil {
			return agentInvocationSettlement{}, err
		}
		if err := s.persistResultMessage(ctx, tx, &storedRun, &storedStep, resultJSON); err != nil {
			return agentInvocationSettlement{}, err
		}
	case models.DelegationGroupStatusFailed, models.DelegationGroupStatusCancelled:
		errMsg := strings.TrimSpace(group.ErrorMessage)
		if errMsg == "" {
			errMsg = "agent group " + group.Status
		}
		stepStatus := models.RunStepStatusFailed
		runStatus := models.RunStatusFailed
		if group.Status == models.DelegationGroupStatusCancelled {
			stepStatus = models.RunStepStatusSkipped
			runStatus = models.RunStatusCancelled
		}
		if err := transitionStepStatus(ctx, tx, &storedStep, stepStatus, resultJSON, errMsg, latency, &now); err != nil {
			return agentInvocationSettlement{}, err
		}
		if err := s.finishRunStepLoopTx(ctx, tx, &storedRun, &storedStep, stepStatus, resultJSON, errMsg, latency, &now); err != nil {
			return agentInvocationSettlement{}, err
		}
		if err := transitionRunStatus(ctx, tx, &storedRun, runStatus, errMsg); err != nil {
			return agentInvocationSettlement{}, err
		}
		if err := s.finishRunLoopTx(ctx, tx, &storedRun, runStatus, errMsg); err != nil {
			return agentInvocationSettlement{}, err
		}
		settlement.TerminalStatus = runStatus
	default:
		return agentInvocationSettlement{}, fmt.Errorf("unsupported terminal agent group status %s", group.Status)
	}
	*run = storedRun
	*step = storedStep
	return settlement, nil
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
		if err := s.guardResumeLeaseTx(ctx, tx); err != nil {
			return err
		}
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
		if err := s.guardResumeLeaseTx(ctx, tx); err != nil {
			return err
		}
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
		if err := s.guardResumeLeaseTx(ctx, tx); err != nil {
			return err
		}
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
	return s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.guardResumeLeaseTx(ctx, tx); err != nil {
			return err
		}
		result := tx.Model(&models.Run{}).
			Where("run_id = ? AND status = ?", runID, models.RunStatusRunning).
			Update("retry_count", gorm.Expr("retry_count + ?", 1))
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("%w: run %s is no longer running", errInvalidRunTransition, runID)
		}
		return nil
	})
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
	messageID := resultMessageID(run.RunID, step.StepKey, step.Attempt)
	messageRunID := run.RunID
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
		messageID = delegationResultMessageID(delegation.DelegationID)
		messageRunID = delegation.ParentRunID
		parentMessageID = delegation.RequestMessageID
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("loading result message delegation: %w", err)
	}
	message := &models.Message{
		MessageID:       messageID,
		ThreadID:        run.ThreadID,
		RunID:           messageRunID,
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
	nodeType := strings.ToLower(strings.TrimSpace(node.Type))
	var inputFrom []string
	switch nodeType {
	case "agent":
		config, err := ParseAgentNodeConfig(node)
		if err != nil {
			return "", err
		}
		inputFrom = config.InputFrom
	case "agent_group":
		config, err := ParseAgentGroupNodeConfig(node)
		if err != nil {
			return "", err
		}
		inputFrom = config.InputFrom
	case "tool":
		config, err := ParseToolNodeConfig(node)
		if err != nil {
			return "", err
		}
		inputFrom = config.InputFrom
	default:
		return run.InputJSON, nil
	}
	if len(inputFrom) == 0 {
		return run.InputJSON, nil
	}
	var runInput any
	if err := json.Unmarshal([]byte(run.InputJSON), &runInput); err != nil {
		return "", fmt.Errorf("decoding run input for node %s: %w", node.Key, err)
	}
	outputs := make(map[string]any, len(inputFrom))
	for _, reference := range inputFrom {
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

// executeWorkflowNodeWithInput executes a node with the value propagated by Eino.
func (s *RunService) executeWorkflowNodeWithInput(ctx context.Context, run *models.Run, node WorkflowNode, attempt int, input string) (string, error) {
	nodeRun := *run
	nodeRun.InputJSON = input
	switch strings.ToLower(strings.TrimSpace(node.Type)) {
	case "agent":
		return s.executeAgentNode(ctx, &nodeRun, node)
	case "agent_group":
		return s.executeAgentGroupNode(ctx, &nodeRun, node)
	default:
		return s.executeNode(ctx, &nodeRun, node, attempt)
	}
}
func (s *RunService) executeWorkflowNode(ctx context.Context, run *models.Run, node WorkflowNode, attempt int) (string, error) {
	switch strings.ToLower(strings.TrimSpace(node.Type)) {
	case "agent":
		return s.executeAgentNode(ctx, run, node)
	case "agent_group":
		return s.executeAgentGroupNode(ctx, run, node)
	default:
		return s.executeNode(ctx, run, node, attempt)
	}
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
	var sourceEndpoint models.AgentEndpoint
	if err := s.database.WithContext(ctx).Where("agent_id = ? AND protocol = ? AND status = ?", source.ID, models.AgentEndpointProtocolA2A, models.AgentEndpointStatusActive).Order("id ASC").First(&sourceEndpoint).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			sourceEndpoint.AuthType = models.AgentEndpointAuthTypeNone
		} else {
			return "", fmt.Errorf("loading source A2A identity endpoint: %w", err)
		}
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
		SourceAgentCode:     source.AgentCode,
		SourceAuthType:      sourceEndpoint.AuthType,
		SourceCredentialRef: sourceEndpoint.CredentialRef,
		TargetAgentCode:     target.AgentCode,
		CapabilityCode:      capability.CapabilityCode,
		ParentRunID:         run.RunID,
		TraceID:             run.TraceID,
		DelegationID:        stableA2AID("delegation", run.RunID, node.Key),
		ThreadID:            run.ThreadID,
		TaskID:              stableA2AID("task", run.RunID, node.Key),
		MessageID:           stableA2AID("message", run.RunID, node.Key),
		InputJSON:           run.InputJSON,
		Endpoints:           invocationEndpoints,
	})
	if err != nil {
		return "", err
	}
	result = normalizeInvocationResult(result)
	if result == nil {
		return "", errors.New("A2A agent invoker returned an empty result")
	}
	if result.State != AgentInvocationStateAccepted && result.State != AgentInvocationStateCompleted {
		return "", fmt.Errorf("target agent returned unsupported invocation state %q", result.State)
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
	if result.State == AgentInvocationStateAccepted {
		return "", &agentInvocationAcceptedError{
			TaskID: result.TaskID, DelegationID: stableA2AID("delegation", run.RunID, node.Key),
			MessageID: stableA2AID("message", run.RunID, node.Key), SourceAgentID: source.ID,
			TargetAgentID: target.ID, CapabilityCode: capability.CapabilityCode, OutputJSON: string(encoded),
			CallbackTokenHash: callbackTokenHash(result.NotificationToken),
		}
	}
	return string(encoded), nil
}

func callbackTokenHash(token string) string {
	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func stableA2AID(kind, parentRunID, nodeKey string) string {
	sum := sha256.Sum256([]byte(kind + "\x00" + parentRunID + "\x00" + nodeKey))
	return "a2a_" + hex.EncodeToString(sum[:])[:59]
}

func executeWorkflowNode(ctx context.Context, run *models.Run, node WorkflowNode, attempt int) (string, error) {
	return executeWorkflowNodeWithChatService(ctx, run, node, attempt, nil)
}

func (s *RunService) executeDefaultNode(ctx context.Context, run *models.Run, node WorkflowNode, attempt int) (string, error) {
	if strings.EqualFold(strings.TrimSpace(node.Type), "tool") {
		return s.executeToolNode(ctx, run, node)
	}
	return executeWorkflowNodeWithChatService(ctx, run, node, attempt, s.chatService)
}

func (s *RunService) executeToolNode(ctx context.Context, run *models.Run, node WorkflowNode) (string, error) {
	if s.toolInvoker == nil {
		return "", errMCPToolInvokerUnavailable
	}
	config, err := ParseToolNodeConfig(node)
	if err != nil {
		return "", err
	}
	var agent models.Agent
	if err := s.database.WithContext(ctx).Select("owner_user_id").First(&agent, run.AgentID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", errAgentNotFound
		}
		return "", fmt.Errorf("loading tool node Agent owner: %w", err)
	}
	arguments := config.Input
	if arguments == nil {
		if err := json.Unmarshal([]byte(run.InputJSON), &arguments); err != nil {
			return "", fmt.Errorf("decoding tool node input for %s: %w", node.Key, err)
		}
		if arguments == nil {
			arguments = map[string]any{}
		}
	}
	timeout := 120 * time.Second
	if config.TimeoutMS > 0 {
		timeout = time.Duration(config.TimeoutMS) * time.Millisecond
	}
	invokeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := s.toolInvoker.Invoke(invokeCtx, ToolInvocationRequest{
		OwnerUserID: agent.OwnerUserID, ServerCode: config.ServerCode,
		ToolName: config.ToolName, Arguments: arguments,
	})
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(map[string]any{
		"type": "tool", "server_code": config.ServerCode, "tool_name": config.ToolName, "result": result,
	})
	if err != nil {
		return "", fmt.Errorf("encoding MCP tool result: %w", err)
	}
	return string(encoded), nil
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
	case "noop", "planner":
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
