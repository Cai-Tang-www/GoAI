package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"GoAI/models"
	"GoAI/requestctx"

	"gorm.io/gorm"
)

const resumeRepublishDelay = 30 * time.Second

// RecoverPendingCallbacks 重试已到期但尚未成功投递的 A2A 终态 callback。
func (s *RuntimeService) RecoverPendingCallbacks(ctx context.Context, limit int) error {
	if s == nil || s.database == nil {
		return errors.New("recovering A2A callbacks: runtime service is nil")
	}
	if s.callbackSender == nil {
		return nil
	}
	if limit <= 0 {
		limit = 100
	}
	var delegationIDs []string
	if err := s.database.WithContext(ctx).Model(&models.A2APushConfig{}).
		Where("status IN ? AND (next_attempt_at IS NULL OR next_attempt_at <= ?)", []string{models.A2APushStatusPending, models.A2APushStatusFailed}, time.Now()).
		Distinct().Limit(limit).Pluck("delegation_id", &delegationIDs).Error; err != nil {
		return fmt.Errorf("loading recoverable A2A callbacks: %w", err)
	}
	var joined error
	for _, delegationID := range delegationIDs {
		var delegation models.Delegation
		if err := s.database.WithContext(ctx).Where("delegation_id = ? AND status IN ?", delegationID, []string{
			models.DelegationStatusSucceeded,
			models.DelegationStatusFailed,
			models.DelegationStatusCancelled,
		}).First(&delegation).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			joined = errors.Join(joined, err)
			continue
		}
		callbackCtx := requestctx.WithTraceID(ctx, delegation.TraceID)
		joined = errors.Join(joined, s.sendDelegationCallbacks(callbackCtx, &delegation))
	}
	return joined
}

// RecoverPendingResumes 收敛终态租约，并重新发布未入队、发布超时或执行租约过期的 Parent Run 恢复事件。
func (s *RuntimeService) RecoverPendingResumes(ctx context.Context, limit int) error {
	if s == nil || s.database == nil {
		return errors.New("recovering run resumes: runtime service is nil")
	}
	if s.resumePublisher == nil {
		return nil
	}
	if limit <= 0 {
		limit = 100
	}

	now := time.Now()
	staleAt := now.Add(-resumeRepublishDelay)
	joined := s.completeTerminalResumeClaims(ctx, limit)

	var delegations []models.Delegation
	if err := s.database.WithContext(ctx).
		Joins("JOIN runs ON runs.run_id = delegations.parent_run_id").
		Joins("LEFT JOIN delegation_groups ON delegation_groups.coordinator_delegation_id = delegations.delegation_id").
		Where(`(delegations.callback_event_hash <> '' OR delegation_groups.status = ?) AND (
			(runs.status = ? AND (
				delegations.resume_status IN ? OR
				(delegations.resume_status = ? AND delegations.updated_at <= ?) OR
				(delegations.resume_status = ? AND delegations.resume_published_at <= ?) OR
				(delegations.resume_status = ? AND (delegations.resume_lease_expires_at IS NULL OR delegations.resume_lease_expires_at <= ?))
			)) OR
			(runs.status = ? AND delegations.resume_execution_attempt > 0 AND (
				(delegations.resume_status = ? AND delegations.resume_lease_owner = '' AND delegations.resume_lease_expires_at IS NULL) OR
				(delegations.resume_status = ? AND delegations.resume_lease_owner = '' AND delegations.updated_at <= ?) OR
				(delegations.resume_status = ? AND delegations.resume_lease_owner = '' AND delegations.resume_published_at <= ?) OR
				(delegations.resume_status = ? AND (delegations.resume_lease_expires_at IS NULL OR delegations.resume_lease_expires_at <= ?))
			))
		)`,
			models.DelegationGroupStatusSucceeded,
			models.RunStatusWaitingExternal,
			[]string{models.DelegationResumeStatusPending, models.DelegationResumeStatusPublishFailed},
			models.DelegationResumeStatusPublishing,
			staleAt,
			models.DelegationResumeStatusPublished,
			staleAt,
			models.DelegationResumeStatusClaimed,
			now,
			models.RunStatusRunning,
			models.DelegationResumeStatusPublishFailed,
			models.DelegationResumeStatusPublishing,
			staleAt,
			models.DelegationResumeStatusPublished,
			staleAt,
			models.DelegationResumeStatusClaimed,
			now,
		).
		Order("delegations.updated_at ASC").Limit(limit).Find(&delegations).Error; err != nil {
		return errors.Join(joined, fmt.Errorf("loading recoverable run resumes: %w", err))
	}

	for i := range delegations {
		delegation := &delegations[i]
		ready, err := s.prepareDelegationResumeRecovery(ctx, delegation, now, staleAt)
		if err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		if !ready {
			continue
		}
		resumeCtx := requestctx.WithTraceID(ctx, delegation.TraceID)
		joined = errors.Join(joined, s.publishDelegationResume(resumeCtx, delegation))
	}
	return joined
}

func (s *RuntimeService) completeTerminalResumeClaims(ctx context.Context, limit int) error {
	var delegations []models.Delegation
	if err := s.database.WithContext(ctx).
		Joins("JOIN runs ON runs.run_id = delegations.parent_run_id").
		Where("runs.status IN ? AND delegations.resume_status = ?", []string{
			models.RunStatusSuccess,
			models.RunStatusFailed,
			models.RunStatusCancelled,
		}, models.DelegationResumeStatusClaimed).
		Order("delegations.updated_at ASC").Limit(limit).Find(&delegations).Error; err != nil {
		return fmt.Errorf("loading terminal run resume claims: %w", err)
	}

	var joined error
	for i := range delegations {
		delegation := &delegations[i]
		result := s.database.WithContext(ctx).Model(&models.Delegation{}).
			Where(
				"id = ? AND resume_status = ? AND resume_execution_attempt = ? AND resume_lease_owner = ?",
				delegation.ID,
				models.DelegationResumeStatusClaimed,
				delegation.ResumeExecutionAttempt,
				delegation.ResumeLeaseOwner,
			).
			Updates(map[string]any{
				"resume_status":           models.DelegationResumeStatusCompleted,
				"resume_error":            "parent run reached a terminal state before resume completion",
				"resume_lease_owner":      "",
				"resume_lease_expires_at": nil,
			})
		if result.Error != nil {
			joined = errors.Join(joined, fmt.Errorf("completing terminal resume claim %s: %w", delegation.DelegationID, result.Error))
		}
	}
	return joined
}

func (s *RuntimeService) prepareDelegationResumeRecovery(ctx context.Context, delegation *models.Delegation, now, staleAt time.Time) (bool, error) {
	if delegation == nil {
		return false, nil
	}

	query := s.database.WithContext(ctx).Model(&models.Delegation{}).Where("id = ?", delegation.ID)
	updates := map[string]any{
		"resume_status": models.DelegationResumeStatusPublishFailed,
	}
	switch delegation.ResumeStatus {
	case models.DelegationResumeStatusPending, models.DelegationResumeStatusPublishFailed:
		return true, nil
	case models.DelegationResumeStatusPublishing:
		query = query.Where("resume_status = ? AND updated_at <= ?", delegation.ResumeStatus, staleAt)
		updates["resume_error"] = "resume event publishing did not complete before recovery timeout"
	case models.DelegationResumeStatusPublished:
		query = query.Where("resume_status = ? AND resume_published_at <= ?", delegation.ResumeStatus, staleAt)
		updates["resume_error"] = "resume event was not claimed before recovery timeout"
	case models.DelegationResumeStatusClaimed:
		query = query.Where(
			"resume_status = ? AND resume_execution_attempt = ? AND resume_lease_owner = ? AND (resume_lease_expires_at IS NULL OR resume_lease_expires_at <= ?)",
			models.DelegationResumeStatusClaimed,
			delegation.ResumeExecutionAttempt,
			delegation.ResumeLeaseOwner,
			now,
		)
		updates["resume_error"] = "resume execution lease expired before completion; recovery event scheduled"
		updates["resume_lease_owner"] = ""
		updates["resume_lease_expires_at"] = nil
	default:
		return false, nil
	}

	result := query.Updates(updates)
	if result.Error != nil {
		return false, fmt.Errorf("preparing delegation resume recovery %s: %w", delegation.DelegationID, result.Error)
	}
	if result.RowsAffected != 1 {
		return false, nil
	}
	wasClaimed := delegation.ResumeStatus == models.DelegationResumeStatusClaimed
	delegation.ResumeStatus = models.DelegationResumeStatusPublishFailed
	if message, ok := updates["resume_error"].(string); ok {
		delegation.ResumeError = message
	}
	if wasClaimed {
		delegation.ResumeLeaseOwner = ""
		delegation.ResumeLeaseExpiresAt = nil
	}
	return true, nil
}
