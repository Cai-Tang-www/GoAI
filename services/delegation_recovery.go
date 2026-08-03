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

// RecoverPendingResumes 重新发布未成功入队或发布后长期未被 claim 的 Parent Run 恢复事件。
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
	staleAt := time.Now().Add(-resumeRepublishDelay)
	var delegations []models.Delegation
	if err := s.database.WithContext(ctx).
		Joins("JOIN runs ON runs.run_id = delegations.parent_run_id").
		Where("runs.status = ? AND delegations.callback_event_hash <> '' AND (delegations.resume_status IN ? OR (delegations.resume_status = ? AND delegations.updated_at <= ?) OR (delegations.resume_status = ? AND delegations.resume_published_at <= ?))",
			models.RunStatusWaitingExternal,
			[]string{models.DelegationResumeStatusPending, models.DelegationResumeStatusPublishFailed},
			models.DelegationResumeStatusPublishing,
			staleAt,
			models.DelegationResumeStatusPublished,
			staleAt,
		).
		Order("delegations.updated_at ASC").Limit(limit).Find(&delegations).Error; err != nil {
		return fmt.Errorf("loading recoverable run resumes: %w", err)
	}
	var joined error
	for i := range delegations {
		if delegations[i].ResumeStatus == models.DelegationResumeStatusPublishing || delegations[i].ResumeStatus == models.DelegationResumeStatusPublished {
			reset := s.database.WithContext(ctx).Model(&models.Delegation{}).
				Where("id = ? AND resume_status = ?", delegations[i].ID, delegations[i].ResumeStatus).
				Updates(map[string]any{"resume_status": models.DelegationResumeStatusPublishFailed, "resume_error": "resume event was not claimed before recovery timeout"})
			if reset.Error != nil {
				joined = errors.Join(joined, reset.Error)
				continue
			}
			if reset.RowsAffected == 0 {
				continue
			}
		}
		resumeCtx := requestctx.WithTraceID(ctx, delegations[i].TraceID)
		joined = errors.Join(joined, s.publishDelegationResume(resumeCtx, &delegations[i]))
	}
	return joined
}
