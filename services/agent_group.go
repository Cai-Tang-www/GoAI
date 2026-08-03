package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"GoAI/domain/delegationgroup"
	"GoAI/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type preparedAgentGroupMember struct {
	Config       AgentGroupMember
	Position     int
	Target       models.Agent
	Capability   models.AgentCapability
	Endpoints    []AgentInvocationEndpoint
	DelegationID string
	TaskID       string
	MessageID    string
}

type agentGroupInvocationOutcome struct {
	Member preparedAgentGroupMember
	Result *AgentInvocationResult
	Err    error
}

type agentGroupDispatchError struct {
	groupID string
	errors  []error
}

func (e *agentGroupDispatchError) Error() string {
	return fmt.Sprintf("agent group dispatch failed: %v", errors.Join(e.errors...))
}

func (e *agentGroupDispatchError) Unwrap() error {
	return errors.Join(e.errors...)
}

func (e *agentGroupDispatchError) Retryable() bool {
	return true
}

type agentGroupTerminalError struct {
	status string
}

func (e *agentGroupTerminalError) Error() string {
	return fmt.Sprintf("agent group reached terminal status %s", e.status)
}

func (e *agentGroupTerminalError) Retryable() bool {
	return false
}

// executeAgentGroupNode 通过 A2A 并发委派成员，并按显式策略收敛为一个 Workflow 节点结果。
func (s *RunService) executeAgentGroupNode(ctx context.Context, run *models.Run, node WorkflowNode) (string, error) {
	if s.agentInvoker == nil {
		return "", errors.New("agent_group workflow node requires an A2A agent invoker")
	}
	config, err := ParseAgentGroupNodeConfig(node)
	if err != nil {
		return "", err
	}
	source, sourceEndpoint, members, err := s.prepareAgentGroup(ctx, run, node, config)
	if err != nil {
		return "", err
	}
	groupID := stableA2AID("group", run.RunID, node.Key)
	coordinatorID := members[0].DelegationID
	if err := s.ensureAgentGroupRecords(ctx, run, node, config, source, groupID, coordinatorID, members); err != nil {
		return "", err
	}

	var stored []models.Delegation
	if err := s.database.WithContext(ctx).Where("delegation_group_id = ?", groupID).Order("group_member_position ASC").Find(&stored).Error; err != nil {
		return "", fmt.Errorf("loading agent group members: %w", err)
	}
	storedByID := make(map[string]models.Delegation, len(stored))
	for _, delegation := range stored {
		storedByID[delegation.DelegationID] = delegation
	}

	outcomes := make([]agentGroupInvocationOutcome, len(members))
	var wg sync.WaitGroup
	for index, member := range members {
		outcomes[index].Member = member
		existing := storedByID[member.DelegationID]
		if existing.Status != models.DelegationStatusPending {
			continue
		}
		wg.Add(1)
		go func(index int, member preparedAgentGroupMember) {
			defer wg.Done()
			timeout := 120 * time.Second
			if member.Config.TimeoutMS > 0 {
				timeout = time.Duration(member.Config.TimeoutMS) * time.Millisecond
			}
			invokeCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			result, invokeErr := s.agentInvoker.Invoke(invokeCtx, AgentInvocationRequest{
				SourceAgentCode: source.AgentCode, SourceAuthType: sourceEndpoint.AuthType,
				SourceCredentialRef: sourceEndpoint.CredentialRef, TargetAgentCode: member.Target.AgentCode,
				CapabilityCode: member.Capability.CapabilityCode, ParentRunID: run.RunID, TraceID: run.TraceID,
				DelegationID: member.DelegationID, DelegationGroupID: groupID, GroupMemberKey: member.Config.Key,
				ThreadID: run.ThreadID, TaskID: member.TaskID, MessageID: member.MessageID,
				InputJSON: run.InputJSON, Endpoints: member.Endpoints,
			})
			outcomes[index].Result = normalizeInvocationResult(result)
			outcomes[index].Err = invokeErr
		}(index, member)
	}
	wg.Wait()

	dispatchErrs := make([]error, 0)
	for _, outcome := range outcomes {
		if outcome.Result == nil && outcome.Err == nil {
			continue
		}
		if err := s.persistAgentGroupOutcome(ctx, run, source, outcome); err != nil {
			dispatchErrs = append(dispatchErrs, err)
			continue
		}
		if outcome.Err != nil {
			dispatchErrs = append(dispatchErrs, fmt.Errorf("dispatching member %s: %w", outcome.Member.Config.Key, outcome.Err))
		}
	}

	delegations, decision, aggregate, err := s.evaluateAgentGroup(ctx, groupID, config)
	if err != nil {
		return "", err
	}
	if err := s.persistAgentGroupDecision(ctx, groupID, decision, aggregate); err != nil {
		return "", err
	}
	if decision.Ready {
		if decision.Status == models.DelegationGroupStatusSucceeded {
			return aggregate, nil
		}
		return "", &agentGroupTerminalError{status: decision.Status}
	}
	if len(dispatchErrs) > 0 {
		return "", &agentGroupDispatchError{groupID: groupID, errors: dispatchErrs}
	}
	coordinator := delegations[0]
	for index := range delegations {
		if delegations[index].DelegationID == coordinatorID {
			coordinator = delegations[index]
			break
		}
	}
	taskID := coordinator.ChildRunID
	if coordinator.A2ATaskID != nil && strings.TrimSpace(*coordinator.A2ATaskID) != "" {
		taskID = *coordinator.A2ATaskID
	}
	return "", &agentInvocationAcceptedError{
		TaskID: taskID, DelegationID: coordinator.DelegationID, MessageID: coordinator.RequestMessageID,
		SourceAgentID: coordinator.SourceAgentID, TargetAgentID: coordinator.TargetAgentID,
		CapabilityCode: coordinator.CapabilityCode, OutputJSON: aggregate,
		CallbackTokenHash: coordinator.CallbackTokenHash, GroupID: groupID,
	}
}

func (s *RunService) prepareAgentGroup(ctx context.Context, run *models.Run, node WorkflowNode, config *AgentGroupNodeConfig) (models.Agent, models.AgentEndpoint, []preparedAgentGroupMember, error) {
	var source models.Agent
	if err := s.database.WithContext(ctx).Where("id = ? AND status = ?", run.AgentID, models.AgentStatusActive).First(&source).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.Agent{}, models.AgentEndpoint{}, nil, errAgentNotFound
		}
		return models.Agent{}, models.AgentEndpoint{}, nil, fmt.Errorf("loading source agent: %w", err)
	}
	var sourceEndpoint models.AgentEndpoint
	if err := s.database.WithContext(ctx).Where("agent_id = ? AND protocol = ? AND status = ?", source.ID, models.AgentEndpointProtocolA2A, models.AgentEndpointStatusActive).Order("id ASC").First(&sourceEndpoint).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return models.Agent{}, models.AgentEndpoint{}, nil, fmt.Errorf("loading source A2A identity endpoint: %w", err)
		}
		sourceEndpoint.AuthType = models.AgentEndpointAuthTypeNone
	}
	members := make([]preparedAgentGroupMember, 0, len(config.Members))
	for position, memberConfig := range config.Members {
		if source.AgentCode == memberConfig.TargetAgent {
			return models.Agent{}, models.AgentEndpoint{}, nil, fmt.Errorf("agent_group node %s member %s cannot target source agent %s", node.Key, memberConfig.Key, source.AgentCode)
		}
		var target models.Agent
		if err := s.database.WithContext(ctx).Where("agent_code = ? AND status = ?", memberConfig.TargetAgent, models.AgentStatusActive).First(&target).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return models.Agent{}, models.AgentEndpoint{}, nil, errAgentNotFound
			}
			return models.Agent{}, models.AgentEndpoint{}, nil, fmt.Errorf("loading target agent %s: %w", memberConfig.TargetAgent, err)
		}
		var capability models.AgentCapability
		if err := s.database.WithContext(ctx).Where("agent_id = ? AND capability_code = ? AND status = ?", target.ID, memberConfig.Capability, models.AgentCapabilityStatusActive).First(&capability).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return models.Agent{}, models.AgentEndpoint{}, nil, fmt.Errorf("target capability %s not found", memberConfig.Capability)
			}
			return models.Agent{}, models.AgentEndpoint{}, nil, fmt.Errorf("loading target capability: %w", err)
		}
		var endpointModels []models.AgentEndpoint
		if err := s.database.WithContext(ctx).Where("agent_id = ? AND protocol = ? AND status = ?", target.ID, models.AgentEndpointProtocolA2A, models.AgentEndpointStatusActive).Order("id ASC").Find(&endpointModels).Error; err != nil {
			return models.Agent{}, models.AgentEndpoint{}, nil, fmt.Errorf("loading target A2A endpoints: %w", err)
		}
		endpoints := make([]AgentInvocationEndpoint, 0, len(endpointModels))
		for _, endpoint := range endpointModels {
			endpoints = append(endpoints, AgentInvocationEndpoint{Address: endpoint.Address, Transport: endpoint.Transport})
		}
		memberKey := node.Key + "#" + memberConfig.Key
		members = append(members, preparedAgentGroupMember{
			Config: memberConfig, Position: position, Target: target, Capability: capability, Endpoints: endpoints,
			DelegationID: stableA2AID("delegation", run.RunID, memberKey), TaskID: stableA2AID("task", run.RunID, memberKey),
			MessageID: stableA2AID("message", run.RunID, memberKey),
		})
	}
	return source, sourceEndpoint, members, nil
}

func (s *RunService) ensureAgentGroupRecords(ctx context.Context, run *models.Run, node WorkflowNode, config *AgentGroupNodeConfig, source models.Agent, groupID, coordinatorID string, members []preparedAgentGroupMember) error {
	now := time.Now()
	return s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var group models.DelegationGroup
		err := tx.Where("group_id = ?", groupID).First(&group).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			group = models.DelegationGroup{
				GroupID: groupID, ThreadID: run.ThreadID, ParentRunID: run.RunID, ParentStepKey: node.Key,
				CoordinatorDelegationID: coordinatorID, TraceID: run.TraceID, Strategy: config.Strategy,
				RequiredSuccesses: config.RequiredSuccesses, TotalMembers: len(members), Status: models.DelegationGroupStatusWaiting,
				ResultJSON: "{}", StartedAt: &now,
			}
			if err := tx.Create(&group).Error; err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else if group.ParentRunID != run.RunID || group.ParentStepKey != node.Key || group.Strategy != config.Strategy || group.RequiredSuccesses != config.RequiredSuccesses || group.TotalMembers != len(members) {
			return errors.New("existing delegation group does not match workflow node")
		}
		for _, member := range members {
			groupIDCopy, memberKey, taskID := groupID, member.Config.Key, member.TaskID
			delegation := models.Delegation{
				DelegationID: member.DelegationID, ThreadID: run.ThreadID, ParentRunID: run.RunID, ChildRunID: member.TaskID,
				A2ATaskID: &taskID, TraceID: run.TraceID, SourceAgentID: source.ID, TargetAgentID: member.Target.ID,
				CapabilityCode: member.Capability.CapabilityCode, RequestMessageID: member.MessageID, ParentStepKey: node.Key,
				DelegationGroupID: &groupIDCopy, GroupMemberKey: &memberKey, GroupMemberPosition: member.Position,
				InputJSON: run.InputJSON, OutputJSON: "{}", Status: models.DelegationStatusPending,
			}
			if err := tx.Where("delegation_id = ?", member.DelegationID).FirstOrCreate(&delegation).Error; err != nil {
				return err
			}
			message := models.Message{
				MessageID: member.MessageID, ThreadID: run.ThreadID, RunID: run.RunID, DelegationID: member.DelegationID,
				SenderType: models.MessageSenderAgent, SenderID: source.AgentCode, ReceiverType: models.MessageSenderAgent,
				ReceiverID: member.Target.AgentCode, MessageType: models.MessageTypeDelegation, ContentType: "application/json",
				ContentJSON: run.InputJSON, MetadataJSON: "{}", Status: models.MessageStatusPending,
			}
			if err := tx.Where("message_id = ?", member.MessageID).FirstOrCreate(&message).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *RunService) persistAgentGroupOutcome(ctx context.Context, run *models.Run, source models.Agent, outcome agentGroupInvocationOutcome) error {
	if outcome.Err != nil {
		return s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(&models.Delegation{}).Where("delegation_id = ? AND status = ?", outcome.Member.DelegationID, models.DelegationStatusPending).Update("error_message", outcome.Err.Error()).Error; err != nil {
				return err
			}
			return tx.Model(&models.Message{}).Where("message_id = ?", outcome.Member.MessageID).Update("status", models.MessageStatusFailed).Error
		})
	}
	result := outcome.Result
	if result == nil {
		return errors.New("A2A agent invoker returned an empty result")
	}
	if result.TaskID == "" {
		return errors.New("A2A agent invoker returned an empty task id")
	}
	if result.State != AgentInvocationStateAccepted && result.State != AgentInvocationStateCompleted {
		return fmt.Errorf("target agent returned unsupported invocation state %q", result.State)
	}
	outputJSON, err := canonicalizeJSON(json.RawMessage(result.OutputJSON))
	if err != nil {
		return fmt.Errorf("A2A agent result is not valid JSON: %w", err)
	}
	now := time.Now()
	return s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		status := models.DelegationStatusAccepted
		updates := map[string]any{
			"a2_a_task_id": result.TaskID, "status": status, "output_json": outputJSON,
			"error_message": "", "callback_token_hash": callbackTokenHash(result.NotificationToken),
		}
		if result.State == AgentInvocationStateCompleted {
			status = models.DelegationStatusSucceeded
			resultMessageID := delegationResultMessageID(outcome.Member.DelegationID)
			updates["status"] = status
			updates["result_message_id"] = resultMessageID
			updates["finished_at"] = now
			message := models.Message{
				MessageID: resultMessageID, ThreadID: run.ThreadID, RunID: run.RunID, DelegationID: outcome.Member.DelegationID,
				ParentMessageID: outcome.Member.MessageID, SenderType: models.MessageSenderAgent, SenderID: outcome.Member.Target.AgentCode,
				ReceiverType: models.MessageSenderAgent, ReceiverID: source.AgentCode, MessageType: models.MessageTypeResult,
				ContentType: "application/json", ContentJSON: outputJSON, MetadataJSON: "{}", Status: models.MessageStatusDelivered,
			}
			if err := tx.Where("message_id = ?", resultMessageID).FirstOrCreate(&message).Error; err != nil {
				return err
			}
		}
		update := tx.Model(&models.Delegation{}).Where("delegation_id = ? AND status = ?", outcome.Member.DelegationID, models.DelegationStatusPending).Updates(updates)
		if update.Error != nil {
			return update.Error
		}
		// callback 可能先于出站响应完成；无论 Delegation 是否仍为 pending，请求消息都应收敛为已投递。
		return tx.Model(&models.Message{}).Where("message_id = ?", outcome.Member.MessageID).Updates(map[string]any{"status": models.MessageStatusDelivered}).Error
	})
}

func (s *RunService) evaluateAgentGroup(ctx context.Context, groupID string, config *AgentGroupNodeConfig) ([]models.Delegation, delegationgroup.Decision, string, error) {
	var delegations []models.Delegation
	if err := s.database.WithContext(ctx).Where("delegation_group_id = ?", groupID).Order("group_member_position ASC").Find(&delegations).Error; err != nil {
		return nil, delegationgroup.Decision{}, "", err
	}
	statuses := make([]string, 0, len(delegations))
	for _, delegation := range delegations {
		statuses = append(statuses, delegation.Status)
	}
	decision, err := delegationgroup.Evaluate(config.Strategy, config.RequiredSuccesses, statuses)
	if err != nil {
		return nil, delegationgroup.Decision{}, "", err
	}
	aggregate, err := marshalAgentGroupResult(config, decision.Status, delegations)
	return delegations, decision, aggregate, err
}

func marshalAgentGroupResult(config *AgentGroupNodeConfig, status string, delegations []models.Delegation) (string, error) {
	members := make([]map[string]any, 0, len(delegations))
	for index, delegation := range delegations {
		member := map[string]any{
			"key": "", "target_agent_id": delegation.TargetAgentID, "capability": delegation.CapabilityCode,
			"task_id": delegation.ChildRunID, "status": delegation.Status,
		}
		if delegation.GroupMemberKey != nil {
			member["key"] = *delegation.GroupMemberKey
		}
		if delegation.A2ATaskID != nil && strings.TrimSpace(*delegation.A2ATaskID) != "" {
			member["task_id"] = *delegation.A2ATaskID
		}
		if strings.TrimSpace(delegation.OutputJSON) != "" && json.Valid([]byte(delegation.OutputJSON)) {
			member["result"] = json.RawMessage(delegation.OutputJSON)
		}
		if delegation.ErrorMessage != "" {
			member["error"] = delegation.ErrorMessage
		}
		if index < len(config.Members) {
			member["target_agent"] = config.Members[index].TargetAgent
		}
		members = append(members, member)
	}
	payload := map[string]any{
		"type": "agent_group", "strategy": config.Strategy, "required_successes": config.RequiredSuccesses,
		"status": status, "members": members,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encoding agent group result: %w", err)
	}
	return string(encoded), nil
}

func (s *RunService) persistAgentGroupDecision(ctx context.Context, groupID string, decision delegationgroup.Decision, aggregate string) error {
	updates := map[string]any{
		"succeeded_members": decision.Counts.Succeeded, "failed_members": decision.Counts.Failed,
		"cancelled_members": decision.Counts.Cancelled, "result_json": aggregate,
	}
	if decision.Ready {
		now := time.Now()
		updates["status"] = decision.Status
		updates["finished_at"] = now
	}
	return s.database.WithContext(ctx).Model(&models.DelegationGroup{}).Where("group_id = ?", groupID).Updates(updates).Error
}

// finalizeAgentGroupDispatchFailure 在节点重试耗尽后终结未完成的委派，避免 Group 永久停留在 waiting。
func (s *RunService) finalizeAgentGroupDispatchFailure(ctx context.Context, groupID string, cause error) error {
	groupID = strings.TrimSpace(groupID)
	if groupID == "" {
		return errors.New("finalizing agent group dispatch failure: group id is required")
	}
	errorMessage := "agent group dispatch retries exhausted"
	if cause != nil && strings.TrimSpace(cause.Error()) != "" {
		errorMessage += ": " + cause.Error()
	}
	now := time.Now()
	return s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var group models.DelegationGroup
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("group_id = ?", groupID).First(&group).Error; err != nil {
			return fmt.Errorf("loading delegation group for dispatch failure: %w", err)
		}
		if group.Status != models.DelegationGroupStatusWaiting {
			return nil
		}

		activeStatuses := []string{
			models.DelegationStatusPending,
			models.DelegationStatusAccepted,
			models.DelegationStatusRunning,
		}
		if err := tx.Model(&models.Delegation{}).
			Where("delegation_group_id = ? AND status IN ?", groupID, activeStatuses).
			Updates(map[string]any{
				"status":        models.DelegationStatusFailed,
				"error_message": errorMessage,
				"finished_at":   now,
			}).Error; err != nil {
			return fmt.Errorf("finalizing delegation group members: %w", err)
		}

		var members []models.Delegation
		if err := tx.Where("delegation_group_id = ?", groupID).Order("group_member_position ASC").Find(&members).Error; err != nil {
			return fmt.Errorf("loading finalized delegation group members: %w", err)
		}
		statuses := make([]string, 0, len(members))
		for _, member := range members {
			statuses = append(statuses, member.Status)
		}
		decision, err := delegationgroup.Evaluate(group.Strategy, group.RequiredSuccesses, statuses)
		if err != nil {
			return fmt.Errorf("evaluating finalized delegation group: %w", err)
		}
		if !decision.Ready {
			return errors.New("finalized delegation group did not reach a terminal decision")
		}
		config, err := loadPersistedAgentGroupConfigTx(tx, &group, members)
		if err != nil {
			return fmt.Errorf("loading finalized delegation group config: %w", err)
		}
		aggregate, err := marshalAgentGroupResult(config, decision.Status, members)
		if err != nil {
			return err
		}
		updates := map[string]any{
			"status":            decision.Status,
			"succeeded_members": decision.Counts.Succeeded,
			"failed_members":    decision.Counts.Failed,
			"cancelled_members": decision.Counts.Cancelled,
			"result_json":       aggregate,
			"error_message":     errorMessage,
			"finished_at":       now,
		}
		update := tx.Model(&models.DelegationGroup{}).
			Where("id = ? AND status = ?", group.ID, models.DelegationGroupStatusWaiting).
			Updates(updates)
		if update.Error != nil {
			return fmt.Errorf("finalizing delegation group: %w", update.Error)
		}
		if update.RowsAffected != 1 {
			return errors.New("finalizing delegation group affected no rows")
		}
		return nil
	})
}
