package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"GoAI/domain/delegationgroup"
	"GoAI/domain/runstate"
	"GoAI/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func (s *RuntimeService) acceptDelegationGroupCallbackTx(ctx context.Context, tx *gorm.DB, delegation *models.Delegation, command DelegationCallbackCommand, outputJSON, eventHash string) (*models.Delegation, bool, error) {
	if delegation == nil || delegation.DelegationGroupID == nil || strings.TrimSpace(*delegation.DelegationGroupID) == "" {
		return nil, false, errors.New("accepting delegation group callback: group id is required")
	}
	groupID := *delegation.DelegationGroupID
	var group models.DelegationGroup
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("group_id = ?", groupID).First(&group).Error; err != nil {
		return nil, false, err
	}
	if delegation.CallbackEventHash != "" {
		if delegation.CallbackEventHash != eventHash {
			return nil, false, fmt.Errorf("%w: conflicting terminal callback", errInvalidDelegation)
		}
		group, coordinator, err := loadDelegationGroupCoordinatorTx(tx, groupID)
		if err != nil {
			return nil, false, err
		}
		return coordinator, group.Status == models.DelegationGroupStatusSucceeded && resumeNeedsPublish(coordinator.ResumeStatus), nil
	}

	now := time.Now()
	claim := tx.Model(&models.Delegation{}).Where("id = ? AND callback_event_hash = ''", delegation.ID).
		Updates(map[string]any{"callback_event_hash": eventHash, "callback_received_at": now})
	if claim.Error != nil {
		return nil, false, claim.Error
	}
	if claim.RowsAffected == 0 {
		var current models.Delegation
		if err := tx.Where("id = ?", delegation.ID).First(&current).Error; err != nil {
			return nil, false, err
		}
		if current.CallbackEventHash != eventHash {
			return nil, false, fmt.Errorf("%w: conflicting terminal callback", errInvalidDelegation)
		}
		group, coordinator, err := loadDelegationGroupCoordinatorTx(tx, groupID)
		if err != nil {
			return nil, false, err
		}
		return coordinator, group.Status == models.DelegationGroupStatusSucceeded && resumeNeedsPublish(coordinator.ResumeStatus), nil
	}

	delegationStatus := models.DelegationStatusSucceeded
	errorMessage := ""
	switch command.State {
	case DelegationCallbackStateFailed:
		delegationStatus = models.DelegationStatusFailed
		errorMessage = strings.TrimSpace(command.ErrorMessage)
	case DelegationCallbackStateCancelled:
		delegationStatus = models.DelegationStatusCancelled
		errorMessage = strings.TrimSpace(command.ErrorMessage)
	}
	if delegation.Status != delegationStatus {
		if delegationTerminal(delegation.Status) {
			return nil, false, fmt.Errorf("%w: conflicting terminal delegation state", errInvalidDelegation)
		}
		if !runstate.IsValidDelegationTransition(delegation.Status, delegationStatus) {
			return nil, false, fmt.Errorf("invalid delegation status transition: %s -> %s", delegation.Status, delegationStatus)
		}
	}

	var source, target models.Agent
	if err := tx.First(&source, "id = ?", delegation.SourceAgentID).Error; err != nil {
		return nil, false, err
	}
	if err := tx.First(&target, "id = ?", delegation.TargetAgentID).Error; err != nil {
		return nil, false, err
	}
	resultMessageID := delegationResultMessageID(delegation.DelegationID)
	message := models.Message{
		MessageID: resultMessageID, ThreadID: delegation.ThreadID, RunID: delegation.ParentRunID,
		DelegationID: delegation.DelegationID, ParentMessageID: delegation.RequestMessageID,
		SenderType: models.MessageSenderAgent, SenderID: target.AgentCode,
		ReceiverType: models.MessageSenderAgent, ReceiverID: source.AgentCode,
		MessageType: models.MessageTypeResult, ContentType: "application/json", ContentJSON: outputJSON,
		MetadataJSON: "{}", Status: models.MessageStatusDelivered,
	}
	if err := tx.Where("message_id = ?", resultMessageID).FirstOrCreate(&message).Error; err != nil {
		return nil, false, err
	}
	updates := map[string]any{
		"status": delegationStatus, "output_json": outputJSON, "error_message": errorMessage,
		"result_message_id": resultMessageID, "finished_at": now,
	}
	update := tx.Model(&models.Delegation{}).Where("id = ? AND callback_event_hash = ?", delegation.ID, eventHash).Updates(updates)
	if update.Error != nil {
		return nil, false, update.Error
	}
	if update.RowsAffected != 1 {
		return nil, false, errors.New("updating claimed delegation group callback affected no rows")
	}

	var members []models.Delegation
	if err := tx.Where("delegation_group_id = ?", groupID).Order("group_member_position ASC").Find(&members).Error; err != nil {
		return nil, false, err
	}
	statuses := make([]string, 0, len(members))
	for _, member := range members {
		statuses = append(statuses, member.Status)
	}
	decision, err := delegationgroup.Evaluate(group.Strategy, group.RequiredSuccesses, statuses)
	if err != nil {
		return nil, false, err
	}
	config, err := loadPersistedAgentGroupConfigTx(tx, &group, members)
	if err != nil {
		return nil, false, err
	}
	aggregate, err := marshalAgentGroupResult(config, decision.Status, members)
	if err != nil {
		return nil, false, err
	}
	groupUpdates := map[string]any{
		"succeeded_members": decision.Counts.Succeeded, "failed_members": decision.Counts.Failed,
		"cancelled_members": decision.Counts.Cancelled, "result_json": aggregate,
	}
	coordinator := models.Delegation{}
	if err := tx.Where("delegation_id = ?", group.CoordinatorDelegationID).First(&coordinator).Error; err != nil {
		return nil, false, err
	}
	if group.Status != models.DelegationGroupStatusWaiting || !decision.Ready {
		if err := tx.Model(&models.DelegationGroup{}).Where("id = ?", group.ID).Updates(groupUpdates).Error; err != nil {
			return nil, false, err
		}
		return &coordinator, false, nil
	}

	groupUpdates["status"] = decision.Status
	groupUpdates["finished_at"] = now
	groupUpdates["error_message"] = ""
	if decision.Status != models.DelegationGroupStatusSucceeded {
		groupUpdates["error_message"] = delegationGroupError(members)
	}
	publishResume := false
	var parentRun models.Run
	if err := tx.Where("run_id = ?", group.ParentRunID).First(&parentRun).Error; err != nil {
		return nil, false, err
	}
	var step models.RunStep
	if err := tx.Where("run_id = ? AND step_key = ?", group.ParentRunID, group.ParentStepKey).Order("attempt DESC").First(&step).Error; err != nil {
		return nil, false, err
	}
	if parentRun.Status == models.RunStatusWaitingExternal && step.Status == models.RunStepStatusWaitingExternal {
		latency := int64(0)
		if step.StartedAt != nil {
			latency = now.Sub(*step.StartedAt).Milliseconds()
		}
		switch decision.Status {
		case models.DelegationGroupStatusSucceeded:
			if err := transitionStepStatus(ctx, tx, &step, models.RunStepStatusSuccess, aggregate, "", latency, &now); err != nil {
				return nil, false, err
			}
			if err := s.runService.finishRunStepLoopTx(ctx, tx, &parentRun, &step, models.RunStepStatusSuccess, aggregate, "", latency, &now); err != nil {
				return nil, false, err
			}
			if err := tx.Model(&models.Delegation{}).Where("id = ?", coordinator.ID).Updates(map[string]any{
				"resume_status": models.DelegationResumeStatusPending, "resume_error": "",
			}).Error; err != nil {
				return nil, false, err
			}
			coordinator.ResumeStatus = models.DelegationResumeStatusPending
			publishResume = true
		case models.DelegationGroupStatusFailed:
			errorMessage := delegationGroupError(members)
			if err := transitionStepStatus(ctx, tx, &step, models.RunStepStatusFailed, aggregate, errorMessage, latency, &now); err != nil {
				return nil, false, err
			}
			if err := s.runService.finishRunStepLoopTx(ctx, tx, &parentRun, &step, models.RunStepStatusFailed, aggregate, errorMessage, latency, &now); err != nil {
				return nil, false, err
			}
			if err := transitionRunStatus(ctx, tx, &parentRun, models.RunStatusFailed, errorMessage); err != nil {
				return nil, false, err
			}
			if err := s.runService.finishRunLoopTx(ctx, tx, &parentRun, models.RunStatusFailed, errorMessage); err != nil {
				return nil, false, err
			}
		case models.DelegationGroupStatusCancelled:
			errorMessage := delegationGroupError(members)
			if err := transitionStepStatus(ctx, tx, &step, models.RunStepStatusSkipped, aggregate, errorMessage, latency, &now); err != nil {
				return nil, false, err
			}
			if err := s.runService.finishRunStepLoopTx(ctx, tx, &parentRun, &step, models.RunStepStatusSkipped, aggregate, errorMessage, latency, &now); err != nil {
				return nil, false, err
			}
			if err := transitionRunStatus(ctx, tx, &parentRun, models.RunStatusCancelled, errorMessage); err != nil {
				return nil, false, err
			}
			if err := s.runService.finishRunLoopTx(ctx, tx, &parentRun, models.RunStatusCancelled, errorMessage); err != nil {
				return nil, false, err
			}
		}
	}
	if err := tx.Model(&models.DelegationGroup{}).Where("id = ? AND status = ?", group.ID, models.DelegationGroupStatusWaiting).Updates(groupUpdates).Error; err != nil {
		return nil, false, err
	}
	return &coordinator, publishResume, nil
}

func loadDelegationGroupCoordinatorTx(tx *gorm.DB, groupID string) (*models.DelegationGroup, *models.Delegation, error) {
	var group models.DelegationGroup
	if err := tx.Where("group_id = ?", groupID).First(&group).Error; err != nil {
		return nil, nil, err
	}
	var coordinator models.Delegation
	if err := tx.Where("delegation_id = ?", group.CoordinatorDelegationID).First(&coordinator).Error; err != nil {
		return nil, nil, err
	}
	return &group, &coordinator, nil
}

func loadPersistedAgentGroupConfigTx(tx *gorm.DB, group *models.DelegationGroup, members []models.Delegation) (*AgentGroupNodeConfig, error) {
	config := &AgentGroupNodeConfig{Strategy: group.Strategy, RequiredSuccesses: group.RequiredSuccesses, Members: make([]AgentGroupMember, 0, len(members))}
	for _, member := range members {
		var target models.Agent
		if err := tx.Select("agent_code").First(&target, "id = ?", member.TargetAgentID).Error; err != nil {
			return nil, err
		}
		memberKey := ""
		if member.GroupMemberKey != nil {
			memberKey = *member.GroupMemberKey
		}
		config.Members = append(config.Members, AgentGroupMember{Key: memberKey, TargetAgent: target.AgentCode, Capability: member.CapabilityCode})
	}
	return config, nil
}

func delegationGroupError(members []models.Delegation) string {
	parts := make([]string, 0)
	for _, member := range members {
		if member.Status != models.DelegationStatusFailed && member.Status != models.DelegationStatusCancelled {
			continue
		}
		key := member.DelegationID
		if member.GroupMemberKey != nil && *member.GroupMemberKey != "" {
			key = *member.GroupMemberKey
		}
		message := strings.TrimSpace(member.ErrorMessage)
		if message == "" {
			message = member.Status
		}
		parts = append(parts, key+": "+message)
	}
	if len(parts) == 0 {
		return "agent group policy was not satisfied"
	}
	return strings.Join(parts, "; ")
}
