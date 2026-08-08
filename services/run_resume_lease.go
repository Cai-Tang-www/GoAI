package services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"GoAI/models"
	"GoAI/requestctx"

	"gorm.io/gorm"
)

var errResumeLeaseLost = errors.New("resume execution lease lost")

const abandonedResumeStepError = "resume execution lease expired before step completion"

type resumeLease struct {
	RunID        string
	DelegationID uint64
	Owner        string
	Attempt      int
}

type resumeLeaseContextKey struct{}

func withResumeLease(ctx context.Context, lease resumeLease) context.Context {
	return context.WithValue(ctx, resumeLeaseContextKey{}, lease)
}

func resumeLeaseFromContext(ctx context.Context) (resumeLease, bool) {
	if ctx == nil {
		return resumeLease{}, false
	}
	lease, ok := ctx.Value(resumeLeaseContextKey{}).(resumeLease)
	return lease, ok && strings.TrimSpace(lease.RunID) != "" && lease.DelegationID != 0 && strings.TrimSpace(lease.Owner) != "" && lease.Attempt > 0
}

func (s *RunService) claimRunResume(ctx context.Context, runID, delegationID string) (models.Run, models.Delegation, resumeLease, bool, error) {
	var run models.Run
	var delegation models.Delegation
	var lease resumeLease
	claimed := false
	now := time.Now()

	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("delegation_id = ? AND parent_run_id = ?", delegationID, runID).First(&delegation).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errRunNotFound
			}
			return err
		}
		if err := tx.Where("run_id = ?", runID).First(&run).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errRunNotFound
			}
			return err
		}
		if delegation.ResumeStatus == models.DelegationResumeStatusCompleted {
			return nil
		}

		staleClaim := delegation.ResumeStatus == models.DelegationResumeStatusClaimed &&
			(delegation.ResumeLeaseExpiresAt == nil || !delegation.ResumeLeaseExpiresAt.After(now))
		firstClaim := delegation.ResumeStatus == models.DelegationResumeStatusPending ||
			delegation.ResumeStatus == models.DelegationResumeStatusPublishFailed ||
			delegation.ResumeStatus == models.DelegationResumeStatusPublishing ||
			delegation.ResumeStatus == models.DelegationResumeStatusPublished
		recoveredPublish := run.Status == models.RunStatusRunning && firstClaim &&
			delegation.ResumeExecutionAttempt > 0 && strings.TrimSpace(delegation.ResumeLeaseOwner) == "" && delegation.ResumeLeaseExpiresAt == nil

		switch run.Status {
		case models.RunStatusWaitingExternal:
			if !firstClaim && !staleClaim {
				return nil
			}
		case models.RunStatusRunning:
			if !staleClaim && !recoveredPublish {
				return nil
			}
		default:
			return nil
		}

		owner := newPrefixedID("lease")
		expiresAt := now.Add(s.resumeLeaseDuration)
		attempt := delegation.ResumeExecutionAttempt + 1
		query := tx.Model(&models.Delegation{}).Where("id = ?", delegation.ID)
		if staleClaim {
			query = query.Where(
				"resume_status = ? AND resume_execution_attempt = ? AND (resume_lease_expires_at IS NULL OR resume_lease_expires_at <= ?)",
				models.DelegationResumeStatusClaimed,
				delegation.ResumeExecutionAttempt,
				now,
			)
		} else {
			query = query.Where("resume_status = ? AND resume_execution_attempt = ?", delegation.ResumeStatus, delegation.ResumeExecutionAttempt)
		}
		claim := query.Updates(map[string]any{
			"resume_status":             models.DelegationResumeStatusClaimed,
			"resume_execution_attempt":  attempt,
			"resume_lease_owner":        owner,
			"resume_lease_claimed_at":   now,
			"resume_lease_heartbeat_at": now,
			"resume_lease_expires_at":   expiresAt,
		})
		if claim.Error != nil {
			return claim.Error
		}
		if claim.RowsAffected != 1 {
			return nil
		}
		if run.Status == models.RunStatusWaitingExternal {
			if err := transitionRunStatus(ctx, tx, &run, models.RunStatusRunning, ""); err != nil {
				return err
			}
		}

		delegation.ResumeStatus = models.DelegationResumeStatusClaimed
		delegation.ResumeExecutionAttempt = attempt
		delegation.ResumeLeaseOwner = owner
		delegation.ResumeLeaseClaimedAt = &now
		delegation.ResumeLeaseHeartbeatAt = &now
		delegation.ResumeLeaseExpiresAt = &expiresAt
		lease = resumeLease{RunID: run.RunID, DelegationID: delegation.ID, Owner: owner, Attempt: attempt}
		claimed = true
		return nil
	})
	return run, delegation, lease, claimed, err
}

func (s *RunService) startResumeLeaseHeartbeat(parent context.Context, lease resumeLease) (context.Context, func() error) {
	ctx, cancel := context.WithCancelCause(withResumeLease(parent, lease))
	stop := make(chan struct{})
	done := make(chan error, 1)
	var stopOnce sync.Once
	var waitOnce sync.Once
	var waitErr error

	go func() {
		ticker := time.NewTicker(s.resumeHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				done <- nil
				return
			case <-ctx.Done():
				done <- context.Cause(ctx)
				return
			case <-ticker.C:
				if err := s.renewResumeLease(ctx, lease); err != nil {
					s.recordResumeLeaseError(ctx, lease, err)
					s.logResumeLease(ctx, slog.LevelError, "run resume lease heartbeat failed", lease, err)
					cancel(err)
					done <- err
					return
				}
			}
		}
	}()

	return ctx, func() error {
		stopOnce.Do(func() { close(stop) })
		waitOnce.Do(func() {
			waitErr = <-done
			cancel(context.Canceled)
		})
		if errors.Is(waitErr, context.Canceled) {
			return nil
		}
		return waitErr
	}
}

func (s *RunService) renewResumeLease(ctx context.Context, lease resumeLease) error {
	persistCtx, cancel := s.resumePersistenceContext(ctx)
	defer cancel()

	now := time.Now()
	expiresAt := now.Add(s.resumeLeaseDuration)
	result := s.database.WithContext(persistCtx).Model(&models.Delegation{}).
		Where(
			"id = ? AND resume_status = ? AND resume_lease_owner = ? AND resume_execution_attempt = ? AND resume_lease_expires_at > ?",
			lease.DelegationID,
			models.DelegationResumeStatusClaimed,
			lease.Owner,
			lease.Attempt,
			now,
		).
		Updates(map[string]any{"resume_lease_heartbeat_at": now, "resume_lease_expires_at": expiresAt})
	if result.Error != nil {
		return fmt.Errorf("renewing resume lease: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return errResumeLeaseLost
	}
	return nil
}

func (s *RunService) guardResumeLeaseTx(ctx context.Context, tx *gorm.DB) error {
	lease, ok := resumeLeaseFromContext(ctx)
	if !ok {
		return nil
	}
	now := time.Now()
	expiresAt := now.Add(s.resumeLeaseDuration)
	result := tx.WithContext(ctx).Model(&models.Delegation{}).
		Where(
			"id = ? AND resume_status = ? AND resume_lease_owner = ? AND resume_execution_attempt = ? AND resume_lease_expires_at > ?",
			lease.DelegationID,
			models.DelegationResumeStatusClaimed,
			lease.Owner,
			lease.Attempt,
			now,
		).
		Updates(map[string]any{"resume_lease_heartbeat_at": now, "resume_lease_expires_at": expiresAt})
	if result.Error != nil {
		return fmt.Errorf("guarding resume lease: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return errResumeLeaseLost
	}
	return nil
}

func (s *RunService) assertResumeLease(ctx context.Context) error {
	if _, ok := resumeLeaseFromContext(ctx); !ok {
		return nil
	}
	return s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return s.guardResumeLeaseTx(ctx, tx)
	})
}

func (s *RunService) recordResumeLeaseError(ctx context.Context, lease resumeLease, cause error) {
	if cause == nil {
		return
	}
	persistCtx, cancel := s.resumePersistenceContext(ctx)
	defer cancel()
	_ = s.database.WithContext(persistCtx).Model(&models.Delegation{}).
		Where(
			"id = ? AND resume_status = ? AND resume_lease_owner = ? AND resume_execution_attempt = ?",
			lease.DelegationID,
			models.DelegationResumeStatusClaimed,
			lease.Owner,
			lease.Attempt,
		).
		Update("resume_error", cause.Error()).Error
}

func (s *RunService) loadResumeCallbackOutput(ctx context.Context, runID, stepKey string) (string, error) {
	if err := s.assertResumeLease(ctx); err != nil {
		return "", err
	}
	var step models.RunStep
	if err := s.database.WithContext(ctx).
		Where("run_id = ? AND step_key = ?", runID, strings.TrimSpace(stepKey)).
		Order("attempt DESC").First(&step).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", fmt.Errorf("resume checkpoint callback step %s is missing", stepKey)
		}
		return "", fmt.Errorf("loading resume callback step: %w", err)
	}
	if step.Status != models.RunStepStatusSuccess {
		return "", fmt.Errorf("resume checkpoint callback step %s has status %q", step.StepKey, step.Status)
	}
	return step.OutputJSON, nil
}

func (s *RunService) resumeNodeCheckpoint(ctx context.Context, run *models.Run, node WorkflowNode) (string, bool, error) {
	if err := s.assertResumeLease(ctx); err != nil {
		return "", false, err
	}
	var step models.RunStep
	err := s.database.WithContext(ctx).
		Where("run_id = ? AND step_key = ?", run.RunID, node.Key).
		Order("attempt DESC").First(&step).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}

	switch step.Status {
	case models.RunStepStatusSuccess:
		return step.OutputJSON, true, nil
	case models.RunStepStatusWaitingExternal:
		nodeType := strings.ToLower(strings.TrimSpace(node.Type))
		if nodeType != "agent" && nodeType != "agent_tool" && nodeType != "agent_group" {
			return "", false, fmt.Errorf("resume checkpoint step %s waits externally but is not an agent node", step.StepKey)
		}
		delegation, err := s.loadSuspendedNodeCoordinator(ctx, run.RunID, node.Key, nodeType)
		if err != nil {
			return "", false, err
		}
		taskID := delegation.ChildRunID
		if delegation.A2ATaskID != nil && strings.TrimSpace(*delegation.A2ATaskID) != "" {
			taskID = *delegation.A2ATaskID
		}
		return "", false, &runSuspendedError{
			TaskID:        taskID,
			DelegationID:  delegation.DelegationID,
			StepKey:       node.Key,
			ResumeNodeKey: delegation.ResumeNodeKey,
		}
	case models.RunStepStatusRunning:
		finishedAt := time.Now()
		latency := int64(0)
		if step.StartedAt != nil {
			latency = finishedAt.Sub(*step.StartedAt).Milliseconds()
		}
		persistCtx, cancel := runFailurePersistenceContext(ctx)
		err := s.finishRunStep(persistCtx, run, &step, models.RunStepStatusFailed, "{}", abandonedResumeStepError, latency, &finishedAt)
		cancel()
		if err != nil {
			return "", false, fmt.Errorf("closing abandoned resume step: %w", err)
		}
		return "", false, nil
	case models.RunStepStatusFailed:
		if strings.Contains(step.ErrorMessage, abandonedResumeStepError) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("resume checkpoint step %s previously failed: %s", step.StepKey, step.ErrorMessage)
	default:
		return "", false, fmt.Errorf("resume checkpoint step %s has incompatible status %q", step.StepKey, step.Status)
	}
}

func (s *RunService) loadSuspendedNodeCoordinator(ctx context.Context, runID, stepKey, nodeType string) (models.Delegation, error) {
	if nodeType == "agent_group" {
		var group models.DelegationGroup
		if err := s.database.WithContext(ctx).Where("parent_run_id = ? AND parent_step_key = ?", runID, stepKey).First(&group).Error; err != nil {
			return models.Delegation{}, fmt.Errorf("loading suspended delegation group checkpoint: %w", err)
		}
		var coordinator models.Delegation
		if err := s.database.WithContext(ctx).Where("delegation_id = ?", group.CoordinatorDelegationID).First(&coordinator).Error; err != nil {
			return models.Delegation{}, fmt.Errorf("loading suspended delegation group coordinator: %w", err)
		}
		return coordinator, nil
	}

	var delegation models.Delegation
	if err := s.database.WithContext(ctx).
		Where("parent_run_id = ? AND parent_step_key = ? AND status IN ?", runID, stepKey, []string{
			models.DelegationStatusPending,
			models.DelegationStatusAccepted,
			models.DelegationStatusRunning,
		}).
		Order("id DESC").First(&delegation).Error; err != nil {
		return models.Delegation{}, fmt.Errorf("loading suspended delegation checkpoint: %w", err)
	}
	return delegation, nil
}

func (s *RunService) resumePersistenceContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := s.resumePersistenceTimeout
	if timeout <= 0 {
		timeout = runFailurePersistenceTimeout
	}
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}

func (s *RunService) nextRunStepAttempt(ctx context.Context, runID, stepKey string) (int, error) {
	var maxAttempt int
	if err := s.database.WithContext(ctx).Model(&models.RunStep{}).
		Where("run_id = ? AND step_key = ?", runID, strings.TrimSpace(stepKey)).
		Select("COALESCE(MAX(attempt), 0)").Scan(&maxAttempt).Error; err != nil {
		return 0, err
	}
	return maxAttempt + 1, nil
}

// completeSuspendedResume 原子恢复已持久化的外部等待状态，并终结当前恢复租约。
func (s *RunService) completeSuspendedResume(ctx context.Context, run *models.Run, delegation *models.Delegation) error {
	if run == nil || delegation == nil {
		return errors.New("completing suspended resume: run and delegation are required")
	}
	persistCtx, cancel := runFailurePersistenceContext(ctx)
	defer cancel()
	return s.database.WithContext(persistCtx).Transaction(func(tx *gorm.DB) error {
		if err := s.guardResumeLeaseTx(persistCtx, tx); err != nil {
			return err
		}

		var current models.Run
		if err := tx.WithContext(persistCtx).Where("run_id = ?", run.RunID).First(&current).Error; err != nil {
			return err
		}
		switch current.Status {
		case models.RunStatusRunning:
			if err := transitionRunStatus(persistCtx, tx, &current, models.RunStatusWaitingExternal, ""); err != nil {
				return err
			}
		case models.RunStatusWaitingExternal:
		default:
			return fmt.Errorf("completing suspended resume: run is in status %q", current.Status)
		}
		if err := completeDelegationResumeTx(persistCtx, tx, delegation, ""); err != nil {
			return err
		}
		*run = current
		return nil
	})
}

func (s *RunService) completeDelegationResume(ctx context.Context, delegation *models.Delegation, resumeError string) error {
	persistCtx, cancel := runFailurePersistenceContext(ctx)
	defer cancel()
	return s.database.WithContext(persistCtx).Transaction(func(tx *gorm.DB) error {
		if err := s.guardResumeLeaseTx(persistCtx, tx); err != nil {
			return err
		}
		return completeDelegationResumeTx(persistCtx, tx, delegation, resumeError)
	})
}

func completeDelegationResumeTx(ctx context.Context, tx *gorm.DB, delegation *models.Delegation, resumeError string) error {
	lease, ok := resumeLeaseFromContext(ctx)
	if !ok || delegation == nil || lease.DelegationID != delegation.ID {
		return errors.New("completing delegation resume: active lease is required")
	}
	result := tx.WithContext(ctx).Model(&models.Delegation{}).
		Where(
			"id = ? AND resume_status = ? AND resume_lease_owner = ? AND resume_execution_attempt = ?",
			delegation.ID,
			models.DelegationResumeStatusClaimed,
			lease.Owner,
			lease.Attempt,
		).
		Updates(map[string]any{
			"resume_status":           models.DelegationResumeStatusCompleted,
			"resume_error":            resumeError,
			"resume_lease_owner":      "",
			"resume_lease_expires_at": nil,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errResumeLeaseLost
	}
	delegation.ResumeStatus = models.DelegationResumeStatusCompleted
	delegation.ResumeError = resumeError
	delegation.ResumeLeaseOwner = ""
	delegation.ResumeLeaseExpiresAt = nil
	return nil
}

func (s *RunService) completeResumedRun(ctx context.Context, run *models.Run, delegation *models.Delegation) error {
	persistCtx, cancel := runFailurePersistenceContext(ctx)
	defer cancel()
	return s.database.WithContext(persistCtx).Transaction(func(tx *gorm.DB) error {
		if err := s.guardResumeLeaseTx(persistCtx, tx); err != nil {
			return err
		}
		if err := transitionRunStatus(persistCtx, tx, run, models.RunStatusSuccess, ""); err != nil {
			return err
		}
		if err := s.finishRunLoopTx(persistCtx, tx, run, models.RunStatusSuccess, ""); err != nil {
			return err
		}
		return completeDelegationResumeTx(persistCtx, tx, delegation, "")
	})
}

func (s *RunService) failResumedRun(ctx context.Context, run *models.Run, delegation *models.Delegation, cause error) (bool, error) {
	if cause == nil {
		cause = errors.New("resumed run failed")
	}
	persistCtx, cancel := runFailurePersistenceContext(ctx)
	defer cancel()
	persistErr := s.database.WithContext(persistCtx).Transaction(func(tx *gorm.DB) error {
		if err := s.guardResumeLeaseTx(persistCtx, tx); err != nil {
			return err
		}
		var current models.Run
		if err := tx.WithContext(persistCtx).Where("run_id = ?", run.RunID).First(&current).Error; err != nil {
			return err
		}
		if current.Status != models.RunStatusRunning {
			return fmt.Errorf("failing resumed run: run is in status %q", current.Status)
		}
		if err := transitionRunStatus(persistCtx, tx, &current, models.RunStatusFailed, cause.Error()); err != nil {
			return err
		}
		if err := s.finishRunLoopTx(persistCtx, tx, &current, models.RunStatusFailed, cause.Error()); err != nil {
			return err
		}
		if err := completeDelegationResumeTx(persistCtx, tx, delegation, cause.Error()); err != nil {
			return err
		}
		*run = current
		return nil
	})
	if persistErr != nil {
		return false, errors.Join(cause, fmt.Errorf("persisting resumed run failure: %w", persistErr))
	}
	return true, cause
}

func resumeExecutionShouldRecover(err error) bool {
	return errors.Is(err, errResumeLeaseLost) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (s *RunService) logResumeLease(ctx context.Context, level slog.Level, message string, lease resumeLease, err error) {
	if s == nil || s.observability == nil || s.observability.Logger == nil {
		return
	}
	attrs := []any{
		"trace_id", requestctx.TraceIDFromContext(ctx),
		"run_id", lease.RunID,
		"delegation_id", lease.DelegationID,
		"lease_owner", lease.Owner,
		"resume_attempt", lease.Attempt,
	}
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}
	s.observability.Logger.Log(ctx, level, message, attrs...)
}
