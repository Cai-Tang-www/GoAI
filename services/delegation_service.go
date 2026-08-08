package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"GoAI/domain/runstate"
	"GoAI/models"
	"GoAI/observability"
	"GoAI/requestctx"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	errDelegationNotFound  = errors.New("delegation not found")
	errCapabilityNotFound  = errors.New("agent capability not found")
	errDelegationConflict  = errors.New("delegation already exists with different request")
	errInvalidDelegation   = errors.New("invalid delegation request")
	errDelegationForbidden = errors.New("delegation access forbidden")
	errPushConfigNotFound  = errors.New("A2A push config not found")
)

// ErrDelegationNotFound 返回委派记录不存在的统一 sentinel error。
func ErrDelegationNotFound() error { return errDelegationNotFound }

// ErrCapabilityNotFound 返回目标 Agent 未暴露指定能力的统一 sentinel error。
func ErrCapabilityNotFound() error { return errCapabilityNotFound }

// ErrDelegationConflict 返回同一 A2A Task 被复用于不同委派请求的统一 sentinel error。
func ErrDelegationConflict() error { return errDelegationConflict }

// ErrInvalidDelegation 返回委派命令不满足运行时约束的统一 sentinel error。
func ErrInvalidDelegation() error { return errInvalidDelegation }

// ErrDelegationForbidden 返回认证 Agent 无权访问委派的统一 sentinel error。
func ErrDelegationForbidden() error { return errDelegationForbidden }

// ErrPushConfigNotFound 返回 A2A Push Notification 配置不存在的统一 sentinel error。
func ErrPushConfigNotFound() error { return errPushConfigNotFound }

// DelegationPushConfig 是协议 Gateway 映射到 Runtime 的 Push Notification 配置。
type DelegationPushConfig struct {
	ConfigID    string
	TaskID      string
	CallbackURL string
	Token       string
}

// AcceptDelegationCommand 是 A2A Gateway 映射到 Runtime 的协议无关委派命令。
type AcceptDelegationCommand struct {
	SourceAgentCode       string
	TargetAgentCode       string
	CapabilityCode        string
	ParentRunID           string
	TraceID               string
	RequestedDelegationID string
	ThreadID              string
	RequestedChildRunID   string
	RequestMessageID      string
	Input                 json.RawMessage
	MetadataJSON          string
	PushConfig            *DelegationPushConfig
}

// DelegationResult 返回已接受委派及其目标 Child Run。
type DelegationResult struct {
	Delegation *models.Delegation
	Run        *models.Run
	Reused     bool
}

// DelegationSnapshot 是协议 Gateway 查询委派执行结果所需的稳定领域快照。
type DelegationSnapshot struct {
	Delegation  models.Delegation
	Run         models.Run
	Messages    []models.Message
	SourceAgent models.Agent
	TargetAgent models.Agent
}

// AgentCapabilityDescriptor 描述协议发现阶段可公开的 Agent 能力。
type AgentCapabilityDescriptor struct {
	Code             string
	Name             string
	Description      string
	Type             string
	Version          string
	InputSchemaJSON  string
	OutputSchemaJSON string
}

// AgentEndpointDescriptor 是 Runtime 与 Gateway 使用的内部网络入口描述；CredentialRef 不得写入 Agent Card 或协议响应。
type AgentEndpointDescriptor struct {
	Code          string
	Transport     string
	Address       string
	AuthType      string
	CredentialRef string
}

// AgentDescriptor 是协议 Gateway 构造 Agent Card 所需的稳定内部描述。
type AgentDescriptor struct {
	Code         string
	Name         string
	Description  string
	Capabilities []AgentCapabilityDescriptor
	Endpoints    []AgentEndpointDescriptor
}

// DelegationRuntime 定义 A2A Gateway 与多 Agent 协作运行时之间的边界。
type DelegationRuntime interface {
	DescribeAgent(context.Context, string) (*AgentDescriptor, error)
	AcceptDelegation(context.Context, AcceptDelegationCommand) (*DelegationResult, error)
	CancelDelegation(context.Context, string, string, string) (*DelegationSnapshot, error)
	DelegationSnapshot(context.Context, string, string, string) (*DelegationSnapshot, error)
	CreateDelegationPushConfig(context.Context, string, string, DelegationPushConfig) (*DelegationPushConfig, error)
	GetDelegationPushConfig(context.Context, string, string, string, string) (*DelegationPushConfig, error)
	ListDelegationPushConfigs(context.Context, string, string, string) ([]DelegationPushConfig, error)
	DeleteDelegationPushConfig(context.Context, string, string, string, string) error
	AcceptDelegationCallback(context.Context, DelegationCallbackCommand) error
	ReconcileDelegation(context.Context, string) error
}

// CancelDelegation 通过 A2A 取消来源 Agent 发起的 Child Run，并回送统一终态 callback。
func (s *RuntimeService) CancelDelegation(ctx context.Context, targetAgentCode, sourceAgentCode, taskID string) (snapshot *DelegationSnapshot, err error) {
	targetAgentCode = strings.TrimSpace(targetAgentCode)
	sourceAgentCode = strings.TrimSpace(sourceAgentCode)
	taskID = strings.TrimSpace(taskID)
	if targetAgentCode == "" || taskID == "" {
		return nil, fmt.Errorf("%w: target agent and task id are required", errInvalidDelegation)
	}
	const cancellationMessage = "A2A task cancelled by source agent"
	var childRunID string
	err = s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var delegation models.Delegation
		query := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("a2_a_task_id = ? OR child_run_id = ?", taskID, taskID).
			First(&delegation)
		if errors.Is(query.Error, gorm.ErrRecordNotFound) {
			return errDelegationNotFound
		}
		if query.Error != nil {
			return fmt.Errorf("loading delegation for cancellation: %w", query.Error)
		}
		var source, target models.Agent
		if err := tx.First(&source, "id = ?", delegation.SourceAgentID).Error; err != nil {
			return fmt.Errorf("loading delegation source agent: %w", err)
		}
		if err := tx.First(&target, "id = ?", delegation.TargetAgentID).Error; err != nil {
			return fmt.Errorf("loading delegation target agent: %w", err)
		}
		if target.AgentCode != targetAgentCode || (sourceAgentCode != "" && source.AgentCode != sourceAgentCode) {
			return errDelegationForbidden
		}

		var run models.Run
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("run_id = ?", delegation.ChildRunID).First(&run).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errRunNotFound
			}
			return fmt.Errorf("loading child run for cancellation: %w", err)
		}
		childRunID = run.RunID
		if run.Status == models.RunStatusSuccess || run.Status == models.RunStatusFailed || run.Status == models.RunStatusCancelled {
			return nil
		}
		if run.Status != models.RunStatusPending && run.Status != models.RunStatusQueued && run.Status != models.RunStatusRunning && run.Status != models.RunStatusWaitingExternal {
			return fmt.Errorf("%w: cannot cancel child run in status %s", errInvalidRunTransition, run.Status)
		}

		now := time.Now()
		var steps []models.RunStep
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("run_id = ? AND status IN ?", run.RunID, []string{models.RunStepStatusPending, models.RunStepStatusRunning, models.RunStepStatusWaitingExternal}).
			Order("id ASC").Find(&steps).Error; err != nil {
			return fmt.Errorf("loading active child run steps for cancellation: %w", err)
		}
		for index := range steps {
			step := &steps[index]
			latency := int64(0)
			if step.StartedAt != nil {
				latency = now.Sub(*step.StartedAt).Milliseconds()
			}
			if err := transitionStepStatus(ctx, tx, step, models.RunStepStatusSkipped, "{}", cancellationMessage, latency, &now); err != nil {
				return err
			}
			if err := s.runService.finishRunStepLoopTx(ctx, tx, &run, step, models.RunStepStatusSkipped, "{}", cancellationMessage, latency, &now); err != nil {
				return err
			}
		}
		if err := transitionRunStatus(ctx, tx, &run, models.RunStatusCancelled, cancellationMessage); err != nil {
			return err
		}
		return s.runService.finishRunLoopTx(ctx, tx, &run, models.RunStatusCancelled, cancellationMessage)
	})
	if err != nil {
		return nil, err
	}
	s.runService.cancelActiveRun(childRunID)
	if err := s.ReconcileDelegation(ctx, childRunID); err != nil {
		return nil, err
	}
	return s.DelegationSnapshot(ctx, targetAgentCode, sourceAgentCode, childRunID)
}

// DescribeAgent 返回活跃 Agent、能力和 A2A Endpoint 的协议无关发现描述。
func (s *RuntimeService) DescribeAgent(ctx context.Context, agentCode string) (*AgentDescriptor, error) {
	code := strings.TrimSpace(agentCode)
	if code == "" {
		return nil, errors.New("agent_code is required")
	}

	var agent models.Agent
	if err := s.database.WithContext(ctx).
		Where("agent_code = ? AND status = ?", code, models.AgentStatusActive).
		First(&agent).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errAgentNotFound
		}
		return nil, fmt.Errorf("loading active agent: %w", err)
	}

	var capabilities []models.AgentCapability
	if err := s.database.WithContext(ctx).
		Where("agent_id = ? AND status = ?", agent.ID, models.AgentCapabilityStatusActive).
		Order("capability_code ASC").
		Find(&capabilities).Error; err != nil {
		return nil, fmt.Errorf("loading agent capabilities: %w", err)
	}
	var endpoints []models.AgentEndpoint
	if err := s.database.WithContext(ctx).
		Where("agent_id = ? AND protocol = ? AND status = ?", agent.ID, models.AgentEndpointProtocolA2A, models.AgentEndpointStatusActive).
		Order("endpoint_code ASC").
		Find(&endpoints).Error; err != nil {
		return nil, fmt.Errorf("loading agent endpoints: %w", err)
	}

	descriptor := &AgentDescriptor{
		Code:         agent.AgentCode,
		Name:         agent.Name,
		Description:  agent.Description,
		Capabilities: make([]AgentCapabilityDescriptor, 0, len(capabilities)),
		Endpoints:    make([]AgentEndpointDescriptor, 0, len(endpoints)),
	}
	for _, capability := range capabilities {
		if capability.CapabilityType == models.AgentCapabilityTypeWorkflow {
			if capability.WorkflowID == nil || *capability.WorkflowID == 0 {
				continue
			}
			var workflow models.Workflow
			if err := s.database.WithContext(ctx).Where("id = ? AND agent_id = ? AND is_active = ?", *capability.WorkflowID, agent.ID, true).First(&workflow).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					continue
				}
				return nil, fmt.Errorf("loading capability workflow: %w", err)
			}
			if capability.Version != "" && capability.Version != strconv.Itoa(workflow.Version) {
				continue
			}
		} else if capability.CapabilityType != models.AgentCapabilityTypeRemote {
			continue
		}
		descriptor.Capabilities = append(descriptor.Capabilities, AgentCapabilityDescriptor{
			Code:             capability.CapabilityCode,
			Name:             capability.Name,
			Description:      capability.Description,
			Type:             capability.CapabilityType,
			Version:          capability.Version,
			InputSchemaJSON:  capability.InputSchemaJSON,
			OutputSchemaJSON: capability.OutputSchemaJSON,
		})
	}
	for _, endpoint := range endpoints {
		descriptor.Endpoints = append(descriptor.Endpoints, AgentEndpointDescriptor{
			Code:          endpoint.EndpointCode,
			Transport:     endpoint.Transport,
			Address:       endpoint.Address,
			AuthType:      endpoint.AuthType,
			CredentialRef: endpoint.CredentialRef,
		})
	}
	return descriptor, nil
}

// AcceptDelegation 原子创建请求 Message、Delegation 与目标 Child Run，并在提交后投递执行消息。
func (s *RuntimeService) AcceptDelegation(ctx context.Context, command AcceptDelegationCommand) (delegationResult *DelegationResult, err error) {
	sourceCode := strings.TrimSpace(command.SourceAgentCode)
	targetCode := strings.TrimSpace(command.TargetAgentCode)
	capabilityCode := strings.TrimSpace(command.CapabilityCode)
	parentRunID := strings.TrimSpace(command.ParentRunID)
	requestedTraceID := strings.TrimSpace(command.TraceID)
	traceID := requestedTraceID
	requestedDelegationID := strings.TrimSpace(command.RequestedDelegationID)
	childRunID := strings.TrimSpace(command.RequestedChildRunID)
	messageID := strings.TrimSpace(command.RequestMessageID)
	threadID := strings.TrimSpace(command.ThreadID)
	observedCtx, span, startedAt := startServiceObservation(ctx, s.observability, "delegation.accept", parentRunID, threadID, "")
	ctx = observedCtx
	if traceID == "" {
		traceID = requestctx.TraceIDFromContext(ctx)
	}
	if traceID == "" {
		traceID = requestctx.NewTraceID()
	}
	ctx = requestctx.WithTraceID(ctx, traceID)
	status := "success"
	defer func() {
		if err != nil {
			status = "error"
		}
		observedRunID := parentRunID
		delegationID := ""
		if delegationResult != nil {
			if delegationResult.Run != nil && strings.TrimSpace(delegationResult.Run.RunID) != "" {
				observedRunID = delegationResult.Run.RunID
			}
			if delegationResult.Delegation != nil {
				delegationID = delegationResult.Delegation.DelegationID
			}
		}
		finishServiceObservation(s.observability, observedCtx, span, "accept_delegation", status, startedAt, observedRunID, threadID, delegationID, func(metrics *observability.Metrics, _, status string, elapsed time.Duration) {
			metrics.ObserveDelegation(status)
		}, err,
			slog.String("parent_run_id", parentRunID),
			slog.String("child_run_id", childRunID),
			slog.String("source_agent_code", sourceCode),
			slog.String("target_agent_code", targetCode),
		)
	}()
	if sourceCode == "" || targetCode == "" || parentRunID == "" || childRunID == "" || messageID == "" || threadID == "" {
		return nil, fmt.Errorf("%w: source_agent_code, target_agent_code, parent_run_id, child_run_id, message_id and thread_id are required", errInvalidDelegation)
	}
	if len(childRunID) > 64 || len(threadID) > 64 || len(messageID) > 64 || len(parentRunID) > 64 || len(requestedDelegationID) > 64 {
		return nil, fmt.Errorf("%w: delegation identifiers must be at most 64 characters", errInvalidDelegation)
	}
	if len(requestedTraceID) > 128 {
		return nil, fmt.Errorf("%w: trace_id must be at most 128 characters", errInvalidDelegation)
	}
	inputJSON, err := canonicalizeJSON(command.Input)
	if err != nil {
		return nil, fmt.Errorf("%w: normalizing delegation input: %v", errInvalidDelegation, err)
	}
	if inputJSON == "" {
		inputJSON = "{}"
	}
	metadataJSON, err := canonicalizeJSON(json.RawMessage(command.MetadataJSON))
	if err != nil {
		return nil, fmt.Errorf("%w: delegation metadata must be valid JSON", errInvalidDelegation)
	}
	if metadataJSON == "" {
		metadataJSON = "{}"
	}
	var pushConfig *DelegationPushConfig
	if command.PushConfig != nil {
		normalized, normalizeErr := normalizeDelegationPushConfig(*command.PushConfig)
		if normalizeErr != nil {
			return nil, normalizeErr
		}
		if normalized.TaskID != childRunID {
			return nil, fmt.Errorf("%w: push config task_id does not match child run", errInvalidDelegation)
		}
		pushConfig = &normalized
	}

	var sourceAgent, targetAgent models.Agent
	if err := s.database.WithContext(ctx).Where("agent_code = ? AND status = ?", sourceCode, models.AgentStatusActive).First(&sourceAgent).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errAgentNotFound
		}
		return nil, fmt.Errorf("loading source agent: %w", err)
	}
	if err := s.database.WithContext(ctx).Where("agent_code = ? AND status = ?", targetCode, models.AgentStatusActive).First(&targetAgent).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errAgentNotFound
		}
		return nil, fmt.Errorf("loading target agent: %w", err)
	}
	if sourceAgent.ID == targetAgent.ID {
		return nil, fmt.Errorf("%w: source and target agents must be different", errInvalidDelegation)
	}
	if capabilityCode == "" {
		var capabilities []models.AgentCapability
		if err := s.database.WithContext(ctx).
			Where("agent_id = ? AND capability_type = ? AND status = ?", targetAgent.ID, models.AgentCapabilityTypeWorkflow, models.AgentCapabilityStatusActive).
			Order("capability_code ASC").Find(&capabilities).Error; err != nil {
			return nil, fmt.Errorf("loading target workflow capabilities: %w", err)
		}
		switch len(capabilities) {
		case 0:
			return nil, errCapabilityNotFound
		case 1:
			capabilityCode = capabilities[0].CapabilityCode
		default:
			return nil, fmt.Errorf("%w: standard A2A message does not identify a capability and target exposes multiple workflow capabilities", errInvalidDelegation)
		}
	}

	var capability models.AgentCapability
	if err := s.database.WithContext(ctx).
		Where("agent_id = ? AND capability_code = ? AND status = ?", targetAgent.ID, capabilityCode, models.AgentCapabilityStatusActive).
		First(&capability).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errCapabilityNotFound
		}
		return nil, fmt.Errorf("loading target capability: %w", err)
	}
	if capability.CapabilityType != models.AgentCapabilityTypeWorkflow || capability.WorkflowID == nil || *capability.WorkflowID == 0 {
		return nil, fmt.Errorf("%w: target capability is not backed by an active workflow", errInvalidDelegation)
	}
	var workflow models.Workflow
	if err := s.database.WithContext(ctx).Where("id = ?", *capability.WorkflowID).First(&workflow).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("%w: target capability workflow does not exist", errInvalidDelegation)
		}
		return nil, fmt.Errorf("loading target capability workflow: %w", err)
	}
	if workflow.AgentID != targetAgent.ID || !workflow.IsActive {
		return nil, fmt.Errorf("%w: target capability workflow is inactive or belongs to another agent", errInvalidDelegation)
	}
	if capability.Version != "" && capability.Version != strconv.Itoa(workflow.Version) {
		return nil, fmt.Errorf("%w: target capability workflow version does not match capability version", errInvalidDelegation)
	}
	if err := validateCapabilityInput(capability.InputSchemaJSON, inputJSON); err != nil {
		return nil, fmt.Errorf("%w: capability input contract: %v", errInvalidDelegation, err)
	}

	ownerUserID := targetAgent.OwnerUserID
	var parentRun models.Run
	if err := s.database.WithContext(ctx).Where("run_id = ?", parentRunID).First(&parentRun).Error; err == nil {
		if parentRun.AgentID != sourceAgent.ID {
			return nil, fmt.Errorf("%w: parent run does not belong to source agent", errInvalidDelegation)
		}
		if parentRun.ThreadID != "" && parentRun.ThreadID != threadID {
			return nil, fmt.Errorf("%w: delegation thread does not match parent run", errInvalidDelegation)
		}
		ownerUserID = parentRun.UserID
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("loading parent run: %w", err)
	}

	delegationID := requestedDelegationID
	if delegationID == "" {
		delegationID = newPrefixedID("dlg")
	}
	if pushConfig != nil && pushConfig.ConfigID == "" {
		pushConfig.ConfigID = delegationID
	}
	var createdDelegation *models.Delegation
	hook := func(tx *gorm.DB, run *models.Run, _ *models.Agent) error {
		if _, err := ensureThread(tx, threadID, ownerUserID); err != nil {
			return err
		}
		message := &models.Message{
			MessageID:    messageID,
			ThreadID:     threadID,
			RunID:        run.RunID,
			DelegationID: delegationID,
			SenderType:   models.MessageSenderAgent,
			SenderID:     sourceCode,
			ReceiverType: models.MessageSenderAgent,
			ReceiverID:   targetCode,
			MessageType:  models.MessageTypeDelegation,
			ContentType:  "application/json",
			ContentJSON:  inputJSON,
			MetadataJSON: metadataJSON,
			Status:       models.MessageStatusDelivered,
		}
		if err := tx.Create(message).Error; err != nil {
			return fmt.Errorf("creating delegation request message: %w", err)
		}
		delegation := &models.Delegation{
			DelegationID:     delegationID,
			ThreadID:         threadID,
			ParentRunID:      parentRunID,
			ChildRunID:       run.RunID,
			TraceID:          traceID,
			SourceAgentID:    sourceAgent.ID,
			TargetAgentID:    targetAgent.ID,
			CapabilityCode:   capabilityCode,
			RequestMessageID: messageID,
			InputJSON:        inputJSON,
			OutputJSON:       "{}",
			Status:           models.DelegationStatusAccepted,
		}
		if err := tx.Create(delegation).Error; err != nil {
			return fmt.Errorf("creating delegation: %w", err)
		}
		if pushConfig != nil {
			if err := createDelegationPushConfig(tx, delegation, *pushConfig); err != nil {
				return err
			}
		}
		createdDelegation = delegation
		return nil
	}

	result, err := s.runService.createRun(ctx, ownerUserID, CreateRunRequest{
		AgentCode:           targetCode,
		ThreadID:            threadID,
		TriggerType:         "a2a",
		Input:               json.RawMessage(inputJSON),
		RequestedRunID:      childRunID,
		RequestedWorkflowID: *capability.WorkflowID,
	}, hook)
	if err != nil {
		return nil, err
	}
	if result.IdempotentHit {
		var existing models.Delegation
		if err := s.database.WithContext(ctx).Where("child_run_id = ?", result.Run.RunID).First(&existing).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, errDelegationConflict
			}
			return nil, fmt.Errorf("loading existing delegation: %w", err)
		}
		var existingMessage models.Message
		if err := s.database.WithContext(ctx).Where("message_id = ?", existing.RequestMessageID).First(&existingMessage).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil, errDelegationConflict
			}
			return nil, fmt.Errorf("loading existing delegation request message: %w", err)
		}
		if existing.SourceAgentID != sourceAgent.ID || existing.TargetAgentID != targetAgent.ID || existing.CapabilityCode != capabilityCode || existing.ParentRunID != parentRunID || existing.RequestMessageID != messageID || existing.InputJSON != inputJSON || existingMessage.ThreadID != threadID || existingMessage.SenderID != sourceCode || existingMessage.ReceiverID != targetCode || existingMessage.MessageType != models.MessageTypeDelegation || existingMessage.ContentType != "application/json" || existingMessage.ContentJSON != inputJSON || existingMessage.MetadataJSON != metadataJSON || (requestedDelegationID != "" && existing.DelegationID != delegationID) || (requestedTraceID != "" && existing.TraceID != traceID) {
			return nil, errDelegationConflict
		}
		if pushConfig != nil {
			var existingPush models.A2APushConfig
			if err := s.database.WithContext(ctx).Where("task_id = ? AND config_id = ?", pushConfig.TaskID, pushConfig.ConfigID).First(&existingPush).Error; err != nil {
				return nil, errDelegationConflict
			}
			if existingPush.DelegationID != existing.DelegationID || existingPush.SourceAgentID != sourceAgent.ID || existingPush.TargetAgentID != targetAgent.ID || existingPush.CallbackURL != pushConfig.CallbackURL || existingPush.Token != pushConfig.Token {
				return nil, errDelegationConflict
			}
		}
		createdDelegation = &existing
	}
	delegationResult = &DelegationResult{Delegation: createdDelegation, Run: result.Run, Reused: result.IdempotentHit}
	return delegationResult, nil
}

// DelegationSnapshot 返回指定目标 Agent 可见的 A2A Child Run 与协作消息快照。
func (s *RuntimeService) DelegationSnapshot(ctx context.Context, targetAgentCode, sourceAgentCode, childRunID string) (snapshotResult *DelegationSnapshot, err error) {
	targetCode := strings.TrimSpace(targetAgentCode)
	sourceCode := strings.TrimSpace(sourceAgentCode)
	childID := strings.TrimSpace(childRunID)
	observedCtx, span, startedAt := startServiceObservation(ctx, s.observability, "delegation.snapshot", childID, "", "")
	ctx = observedCtx
	status := "success"
	defer func() {
		if err != nil {
			status = "error"
		}
		delegationID := ""
		threadID := ""
		parentRunID := ""
		if snapshotResult != nil {
			delegationID = snapshotResult.Delegation.DelegationID
			threadID = snapshotResult.Delegation.ThreadID
			parentRunID = snapshotResult.Delegation.ParentRunID
		}
		finishServiceObservation(s.observability, observedCtx, span, "snapshot_delegation", status, startedAt, childID, threadID, delegationID, func(metrics *observability.Metrics, _, status string, elapsed time.Duration) {
			metrics.ObserveDelegation(status)
		}, err,
			slog.String("parent_run_id", parentRunID),
			slog.String("child_run_id", childID),
			slog.String("target_agent_code", targetCode),
		)
	}()
	if err := s.ReconcileDelegation(ctx, childID); err != nil {
		return nil, err
	}
	var snapshot DelegationSnapshot
	query := s.database.WithContext(ctx).
		Table("delegations").
		Select("delegations.*").
		Joins("JOIN agents target_agents ON target_agents.id = delegations.target_agent_id").
		Where("delegations.child_run_id = ? AND target_agents.agent_code = ?", childID, targetCode).
		First(&snapshot.Delegation)
	if errors.Is(query.Error, gorm.ErrRecordNotFound) {
		return nil, errDelegationNotFound
	}
	if query.Error != nil {
		return nil, fmt.Errorf("loading delegation snapshot: %w", query.Error)
	}
	if err := s.database.WithContext(ctx).Where("run_id = ?", snapshot.Delegation.ChildRunID).First(&snapshot.Run).Error; err != nil {
		return nil, fmt.Errorf("loading delegation child run: %w", err)
	}
	if err := s.database.WithContext(ctx).First(&snapshot.SourceAgent, "id = ?", snapshot.Delegation.SourceAgentID).Error; err != nil {
		return nil, fmt.Errorf("loading delegation source agent: %w", err)
	}
	if sourceCode != "" && snapshot.SourceAgent.AgentCode != sourceCode {
		return nil, errDelegationForbidden
	}
	if err := s.database.WithContext(ctx).First(&snapshot.TargetAgent, "id = ?", snapshot.Delegation.TargetAgentID).Error; err != nil {
		return nil, fmt.Errorf("loading delegation target agent: %w", err)
	}
	if err := s.database.WithContext(ctx).
		Where("delegation_id = ?", snapshot.Delegation.DelegationID).
		Order("created_at ASC, id ASC").
		Find(&snapshot.Messages).Error; err != nil {
		return nil, fmt.Errorf("loading delegation messages: %w", err)
	}
	snapshotResult = &snapshot
	return snapshotResult, nil
}

// ReconcileDelegation 将 Child Run 终态收敛为 Delegation 终态，并在提交后发送 A2A callback。
func (s *RuntimeService) ReconcileDelegation(ctx context.Context, childRunID string) (err error) {
	childID := strings.TrimSpace(childRunID)
	observedCtx, span, startedAt := startServiceObservation(ctx, s.observability, "delegation.reconcile", childID, "", "")
	ctx = observedCtx
	status := "success"
	var reconciled models.Delegation
	terminal := false
	defer func() {
		if err != nil {
			status = "error"
		}
		finishServiceObservation(s.observability, observedCtx, span, "reconcile_delegation", status, startedAt, childID, reconciled.ThreadID, reconciled.DelegationID, func(metrics *observability.Metrics, _, status string, elapsed time.Duration) {
			metrics.ObserveDelegation(status)
		}, err, slog.String("parent_run_id", reconciled.ParentRunID), slog.String("child_run_id", childID))
	}()
	err = s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("child_run_id = ?", childID).First(&reconciled).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			return err
		}
		var run models.Run
		if err := tx.Where("run_id = ?", reconciled.ChildRunID).First(&run).Error; err != nil {
			return err
		}
		nextStatus := delegationStatusForRun(run.Status)
		if nextStatus == "" {
			return nil
		}
		if nextStatus != reconciled.Status {
			if !runstate.IsValidDelegationTransition(reconciled.Status, nextStatus) {
				return fmt.Errorf("invalid delegation status transition: %s -> %s", reconciled.Status, nextStatus)
			}
			updates := map[string]any{"status": nextStatus, "error_message": run.ErrorMessage}
			if run.StartedAt != nil {
				updates["started_at"] = run.StartedAt
			}
			if run.FinishedAt != nil {
				updates["finished_at"] = run.FinishedAt
			}
			claim := tx.Model(&models.Delegation{}).Where("id = ? AND status = ?", reconciled.ID, reconciled.Status).Updates(updates)
			if claim.Error != nil {
				return claim.Error
			}
			if claim.RowsAffected == 0 {
				return nil
			}
			reconciled.Status = nextStatus
			reconciled.ErrorMessage = run.ErrorMessage
		}
		terminal = nextStatus == models.DelegationStatusSucceeded || nextStatus == models.DelegationStatusFailed || nextStatus == models.DelegationStatusCancelled
		if !terminal {
			return nil
		}
		message, outputJSON, err := ensureDelegationResultMessage(tx, &reconciled, &run)
		if err != nil {
			return err
		}
		reconciled.ResultMessageID = message.MessageID
		reconciled.OutputJSON = outputJSON
		return tx.Model(&models.Delegation{}).Where("id = ?", reconciled.ID).Updates(map[string]any{"result_message_id": message.MessageID, "output_json": outputJSON}).Error
	})
	if err != nil || !terminal || s.callbackSender == nil {
		return err
	}
	return s.sendDelegationCallbacks(ctx, &reconciled)
}

func (s *RuntimeService) sendDelegationCallbacks(ctx context.Context, delegation *models.Delegation) error {
	var configs []models.A2APushConfig
	if err := s.database.WithContext(ctx).Where("delegation_id = ? AND status IN ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?)", delegation.DelegationID, []string{models.A2APushStatusPending, models.A2APushStatusFailed}, time.Now()).Find(&configs).Error; err != nil {
		return err
	}
	if len(configs) == 0 {
		return nil
	}
	var target models.Agent
	if err := s.database.WithContext(ctx).First(&target, "id = ?", delegation.TargetAgentID).Error; err != nil {
		return err
	}
	credentialRef := ""
	if err := s.database.WithContext(ctx).Model(&models.AgentEndpoint{}).Where("agent_id = ? AND protocol = ? AND status = ?", target.ID, models.AgentEndpointProtocolA2A, models.AgentEndpointStatusActive).Order("id ASC").Pluck("credential_ref", &credentialRef).Error; err != nil {
		return err
	}
	state := DelegationCallbackStateSucceeded
	if delegation.Status == models.DelegationStatusFailed {
		state = DelegationCallbackStateFailed
	}
	if delegation.Status == models.DelegationStatusCancelled {
		state = DelegationCallbackStateCancelled
	}
	var joined error
	for i := range configs {
		config := &configs[i]
		delivery := DelegationCallbackDelivery{CallbackURL: config.CallbackURL, NotificationToken: config.Token, SenderAgentCode: target.AgentCode, SenderCredentialRef: credentialRef, TaskID: config.TaskID, ThreadID: delegation.ThreadID, State: state, OutputJSON: delegation.OutputJSON, ErrorMessage: delegation.ErrorMessage, TraceID: delegation.TraceID}
		sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runDispatchTimeout)
		err := s.callbackSender.SendDelegationCallback(sendCtx, delivery)
		cancel()
		updates := map[string]any{"attempt_count": gorm.Expr("attempt_count + ?", 1)}
		if err != nil {
			next := time.Now().Add(time.Minute)
			updates["status"] = models.A2APushStatusFailed
			updates["last_error"] = err.Error()
			updates["next_attempt_at"] = next
			joined = errors.Join(joined, err)
		} else {
			now := time.Now()
			updates["status"] = models.A2APushStatusSent
			updates["last_error"] = ""
			updates["sent_at"] = now
			updates["next_attempt_at"] = nil
		}
		if updateErr := s.database.WithContext(context.WithoutCancel(ctx)).Model(&models.A2APushConfig{}).Where("id = ?", config.ID).Updates(updates).Error; updateErr != nil {
			joined = errors.Join(joined, updateErr)
		}
	}
	return joined
}

func delegationStatusForRun(status string) string {
	switch status {
	case models.RunStatusRunning:
		return models.DelegationStatusRunning
	case models.RunStatusSuccess:
		return models.DelegationStatusSucceeded
	case models.RunStatusFailed:
		return models.DelegationStatusFailed
	case models.RunStatusCancelled:
		return models.DelegationStatusCancelled
	default:
		return ""
	}
}

func ensureDelegationResultMessage(tx *gorm.DB, delegation *models.Delegation, run *models.Run) (*models.Message, string, error) {
	var existing models.Message
	if err := tx.Where("delegation_id = ? AND message_type = ? AND sender_type = ?", delegation.DelegationID, models.MessageTypeResult, models.MessageSenderAgent).
		Order("created_at DESC, id DESC").First(&existing).Error; err == nil {
		return &existing, existing.ContentJSON, nil
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, "", err
	}

	outputJSON := "{}"
	if run.Status == models.RunStatusSuccess {
		var step models.RunStep
		if err := tx.Where("run_id = ? AND status = ?", run.RunID, models.RunStepStatusSuccess).
			Order("id DESC").First(&step).Error; err == nil && strings.TrimSpace(step.OutputJSON) != "" {
			outputJSON = step.OutputJSON
		} else if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, "", err
		}
	} else {
		payload, err := json.Marshal(map[string]string{"error": run.ErrorMessage})
		if err != nil {
			return nil, "", err
		}
		outputJSON = string(payload)
	}
	var sourceAgent, targetAgent models.Agent
	if err := tx.First(&sourceAgent, "id = ?", delegation.SourceAgentID).Error; err != nil {
		return nil, "", err
	}
	if err := tx.First(&targetAgent, "id = ?", delegation.TargetAgentID).Error; err != nil {
		return nil, "", err
	}
	message := &models.Message{
		MessageID:       delegationResultMessageID(delegation.DelegationID),
		ThreadID:        delegation.ThreadID,
		RunID:           delegation.ParentRunID,
		DelegationID:    delegation.DelegationID,
		ParentMessageID: delegation.RequestMessageID,
		SenderType:      models.MessageSenderAgent,
		SenderID:        targetAgent.AgentCode,
		ReceiverType:    models.MessageSenderAgent,
		ReceiverID:      sourceAgent.AgentCode,
		MessageType:     models.MessageTypeResult,
		ContentType:     "application/json",
		ContentJSON:     outputJSON,
		MetadataJSON:    "{}",
		Status:          models.MessageStatusDelivered,
	}
	if err := tx.Where("message_id = ?", message.MessageID).FirstOrCreate(message).Error; err != nil {
		return nil, "", err
	}
	return message, message.ContentJSON, nil
}

var _ DelegationRuntime = (*RuntimeService)(nil)
