package services

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"GoAI/domain/runstate"
	"GoAI/models"
	"GoAI/requestctx"

	"gorm.io/gorm"
)

const (
	DelegationCallbackStateSucceeded = "succeeded"
	DelegationCallbackStateFailed    = "failed"
	DelegationCallbackStateCancelled = "cancelled"
)

// RunResumePublisher 定义 callback 提交后发布 Parent Run 恢复事件的边界。
type RunResumePublisher interface {
	PublishRunResume(context.Context, string, string) error
}

// DelegationCallbackDelivery 描述目标 Agent 向来源 Agent 回推的终态任务。
type DelegationCallbackDelivery struct {
	CallbackURL         string
	NotificationToken   string
	SenderAgentCode     string
	SenderCredentialRef string
	TaskID              string
	ThreadID            string
	State               string
	OutputJSON          string
	ErrorMessage        string
	TraceID             string
}

// DelegationCallbackSender 通过 A2A HTTP(S) 发送终态任务通知。
type DelegationCallbackSender interface {
	SendDelegationCallback(context.Context, DelegationCallbackDelivery) error
}

// DelegationCallbackCommand 是 A2A Gateway 映射后的协议无关终态通知。
type DelegationCallbackCommand struct {
	SourceAgentCode   string
	TargetAgentCode   string
	TaskID            string
	State             string
	OutputJSON        string
	ErrorMessage      string
	NotificationToken string
	EventJSON         string
}

// AcceptDelegationCallback 幂等收敛 A2A 终态 callback，并在成功结果后发布 Parent Run 恢复事件。
func (s *RuntimeService) AcceptDelegationCallback(ctx context.Context, command DelegationCallbackCommand) error {
	command.SourceAgentCode = strings.TrimSpace(command.SourceAgentCode)
	command.TargetAgentCode = strings.TrimSpace(command.TargetAgentCode)
	command.TaskID = strings.TrimSpace(command.TaskID)
	command.State = strings.TrimSpace(command.State)
	command.OutputJSON = strings.TrimSpace(command.OutputJSON)
	command.NotificationToken = strings.TrimSpace(command.NotificationToken)
	if command.SourceAgentCode == "" || command.TargetAgentCode == "" || command.TaskID == "" {
		return fmt.Errorf("%w: callback source, target and task are required", errInvalidDelegation)
	}
	if command.State != DelegationCallbackStateSucceeded && command.State != DelegationCallbackStateFailed && command.State != DelegationCallbackStateCancelled {
		return fmt.Errorf("%w: callback state is not terminal", errInvalidDelegation)
	}
	if command.OutputJSON == "" {
		command.OutputJSON = "{}"
	}
	outputJSON, err := canonicalizeJSON(json.RawMessage(command.OutputJSON))
	if err != nil {
		return fmt.Errorf("%w: callback output is invalid: %v", errInvalidDelegation, err)
	}
	eventJSON, err := canonicalizeJSON(json.RawMessage(command.EventJSON))
	if err != nil {
		return fmt.Errorf("%w: callback event is invalid: %v", errInvalidDelegation, err)
	}
	eventHash := sha256.Sum256([]byte(eventJSON))
	hash := hex.EncodeToString(eventHash[:])

	var delegation models.Delegation
	publishResume := false
	err = s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("a2_a_task_id = ?", command.TaskID).First(&delegation).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errDelegationNotFound
			}
			return err
		}
		var source, target models.Agent
		if err := tx.First(&source, "id = ?", delegation.SourceAgentID).Error; err != nil {
			return err
		}
		if err := tx.First(&target, "id = ?", delegation.TargetAgentID).Error; err != nil {
			return err
		}
		if source.AgentCode != command.SourceAgentCode || target.AgentCode != command.TargetAgentCode {
			return errDelegationForbidden
		}
		if !callbackTokenMatches(delegation.CallbackTokenHash, command.NotificationToken) {
			return errDelegationForbidden
		}
		now := time.Now()
		if delegation.CallbackEventHash != "" {
			if delegation.CallbackEventHash != hash {
				return fmt.Errorf("%w: conflicting terminal callback", errInvalidDelegation)
			}
			publishResume = resumeNeedsPublish(delegation.ResumeStatus)
			return nil
		}
		claim := tx.Model(&models.Delegation{}).
			Where("id = ? AND callback_event_hash = ''", delegation.ID).
			Updates(map[string]any{"callback_event_hash": hash, "callback_received_at": now})
		if claim.Error != nil {
			return claim.Error
		}
		if claim.RowsAffected == 0 {
			var current models.Delegation
			if err := tx.Where("id = ?", delegation.ID).First(&current).Error; err != nil {
				return err
			}
			if current.CallbackEventHash != hash {
				return fmt.Errorf("%w: conflicting terminal callback", errInvalidDelegation)
			}
			delegation = current
			publishResume = resumeNeedsPublish(current.ResumeStatus)
			return nil
		}

		var parentRun models.Run
		if err := tx.Where("run_id = ?", delegation.ParentRunID).First(&parentRun).Error; err != nil {
			return err
		}
		var step models.RunStep
		if err := tx.Where("run_id = ? AND step_key = ?", delegation.ParentRunID, delegation.ParentStepKey).Order("attempt DESC").First(&step).Error; err != nil {
			return err
		}
		if parentRun.Status != models.RunStatusWaitingExternal || step.Status != models.RunStepStatusWaitingExternal {
			return fmt.Errorf("%w: parent run is not waiting for callback", errInvalidDelegation)
		}
		stepStatus := models.RunStepStatusSuccess
		runStatus := ""
		delegationStatus := models.DelegationStatusSucceeded
		resumeStatus := models.DelegationResumeStatusPending
		errorMessage := ""
		if command.State == DelegationCallbackStateFailed {
			stepStatus = models.RunStepStatusFailed
			runStatus = models.RunStatusFailed
			delegationStatus = models.DelegationStatusFailed
			resumeStatus = models.DelegationResumeStatusNone
			errorMessage = strings.TrimSpace(command.ErrorMessage)
		}
		if command.State == DelegationCallbackStateCancelled {
			stepStatus = models.RunStepStatusSkipped
			runStatus = models.RunStatusCancelled
			delegationStatus = models.DelegationStatusCancelled
			resumeStatus = models.DelegationResumeStatusNone
			errorMessage = strings.TrimSpace(command.ErrorMessage)
		}
		latency := int64(0)
		if step.StartedAt != nil {
			latency = now.Sub(*step.StartedAt).Milliseconds()
		}
		if err := transitionStepStatus(ctx, tx, &step, stepStatus, outputJSON, errorMessage, latency, &now); err != nil {
			return err
		}
		if err := s.runService.finishRunStepLoopTx(ctx, tx, &parentRun, &step, stepStatus, outputJSON, errorMessage, latency, &now); err != nil {
			return err
		}
		if runStatus != "" {
			if err := transitionRunStatus(ctx, tx, &parentRun, runStatus, errorMessage); err != nil {
				return err
			}
			if err := s.runService.finishRunLoopTx(ctx, tx, &parentRun, runStatus, errorMessage); err != nil {
				return err
			}
		}
		if delegation.Status != delegationStatus {
			if delegationTerminal(delegation.Status) {
				return fmt.Errorf("%w: conflicting terminal delegation state", errInvalidDelegation)
			}
			if !runstate.IsValidDelegationTransition(delegation.Status, delegationStatus) {
				return fmt.Errorf("invalid delegation status transition: %s -> %s", delegation.Status, delegationStatus)
			}
			delegation.Status = delegationStatus
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
			return err
		}
		updates := map[string]any{
			"status": delegationStatus, "output_json": outputJSON, "error_message": errorMessage,
			"result_message_id": resultMessageID,
			"resume_status":     resumeStatus, "resume_error": "",
		}
		if runStatus != "" {
			updates["finished_at"] = now
		}
		update := tx.Model(&models.Delegation{}).Where("id = ? AND callback_event_hash = ?", delegation.ID, hash).Updates(updates)
		if update.Error != nil {
			return update.Error
		}
		if update.RowsAffected != 1 {
			return errors.New("updating claimed delegation callback affected no rows")
		}
		delegation.CallbackEventHash = hash
		delegation.ResumeStatus = resumeStatus
		publishResume = runStatus == ""
		return nil
	})
	if err != nil || !publishResume {
		return err
	}
	return s.publishDelegationResume(ctx, &delegation)
}

func resumeNeedsPublish(status string) bool {
	return status == models.DelegationResumeStatusPending || status == models.DelegationResumeStatusPublishFailed
}

func delegationTerminal(status string) bool {
	return status == models.DelegationStatusSucceeded || status == models.DelegationStatusFailed || status == models.DelegationStatusCancelled
}

func (s *RuntimeService) publishDelegationResume(ctx context.Context, delegation *models.Delegation) error {
	if s.resumePublisher == nil {
		return errors.New("publishing run resume: publisher is nil")
	}
	claimCtx, cancelClaim := runFailurePersistenceContext(ctx)
	claim := s.database.WithContext(claimCtx).Model(&models.Delegation{}).
		Where("id = ? AND resume_status IN ?", delegation.ID, []string{models.DelegationResumeStatusPending, models.DelegationResumeStatusPublishFailed}).
		Updates(map[string]any{
			"resume_status":        models.DelegationResumeStatusPublishing,
			"resume_attempt_count": gorm.Expr("resume_attempt_count + ?", 1),
		})
	cancelClaim()
	if claim.Error != nil {
		return claim.Error
	}
	if claim.RowsAffected == 0 {
		return nil
	}
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runDispatchTimeout)
	defer cancel()
	publishCtx = requestctx.WithTraceID(publishCtx, delegation.TraceID)
	err := s.resumePublisher.PublishRunResume(publishCtx, delegation.ParentRunID, delegation.DelegationID)
	updates := map[string]any{}
	if err != nil {
		updates["resume_status"] = models.DelegationResumeStatusPublishFailed
		updates["resume_error"] = err.Error()
	} else {
		now := time.Now()
		updates["resume_status"] = models.DelegationResumeStatusPublished
		updates["resume_published_at"] = now
	}
	persistCtx, cancelPersist := runFailurePersistenceContext(ctx)
	persist := s.database.WithContext(persistCtx).Model(&models.Delegation{}).
		Where("id = ? AND resume_status = ?", delegation.ID, models.DelegationResumeStatusPublishing).
		Updates(updates)
	cancelPersist()
	if persist.Error != nil {
		return errors.Join(err, persist.Error)
	}
	if persist.RowsAffected == 0 {
		var current models.Delegation
		loadCtx, cancelLoad := runFailurePersistenceContext(ctx)
		loadErr := s.database.WithContext(loadCtx).Select("resume_status").First(&current, "id = ?", delegation.ID).Error
		cancelLoad()
		if loadErr != nil {
			return errors.Join(err, loadErr)
		}
		if current.ResumeStatus == models.DelegationResumeStatusClaimed || current.ResumeStatus == models.DelegationResumeStatusCompleted {
			return nil
		}
		return errors.Join(err, fmt.Errorf("persisting run resume publish result: delegation is in status %s", current.ResumeStatus))
	}
	return err
}

func callbackTokenMatches(expectedHash, token string) bool {
	if strings.TrimSpace(expectedHash) == "" || strings.TrimSpace(token) == "" {
		return false
	}
	actual := sha256.Sum256([]byte(strings.TrimSpace(token)))
	actualHex := hex.EncodeToString(actual[:])
	return subtle.ConstantTimeCompare([]byte(expectedHash), []byte(actualHex)) == 1
}

func delegationResultMessageID(delegationID string) string {
	sum := sha256.Sum256([]byte("delegation-result\x00" + delegationID))
	return "msg_" + hex.EncodeToString(sum[:])[:60]
}
