package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"GoAI/models"
	"GoAI/requestctx"

	"gorm.io/gorm"
)

var (
	errThreadNotFound   = errors.New("thread not found")
	errRunNotReplayable = errors.New("run is not replayable")
)

// ThreadReplayCommand 描述基于 Thread 历史创建新 Run 的协议无关命令。
type ThreadReplayCommand struct {
	OwnerUserID    uint64
	IsAdmin        bool
	ThreadID       string
	SourceRunID    string
	IdempotencyKey string
}

// ThreadReplayResult 返回 Thread、回放来源和新建或复用的 Run。
type ThreadReplayResult struct {
	Thread        *models.Thread
	Run           *models.Run
	SourceRunID   string
	IdempotentHit bool
}

// ErrThreadNotFound 返回 Thread 不存在的统一 sentinel error。
func ErrThreadNotFound() error {
	return errThreadNotFound
}

// ErrRunNotReplayable 返回来源 Run 尚未进入可回放终态的 sentinel error。
func ErrRunNotReplayable() error {
	return errRunNotReplayable
}

var replayableRunStatuses = []string{
	models.RunStatusSuccess,
	models.RunStatusFailed,
	models.RunStatusCancelled,
}

type threadReplayInput struct {
	ThreadID    string                `json:"thread_id"`
	SourceRunID string                `json:"source_run_id"`
	Messages    []threadReplayMessage `json:"messages"`
}

type threadReplayMessage struct {
	MessageID       string          `json:"message_id"`
	RunID           string          `json:"run_id,omitempty"`
	DelegationID    string          `json:"delegation_id,omitempty"`
	ParentMessageID string          `json:"parent_message_id,omitempty"`
	SenderType      string          `json:"sender_type"`
	SenderID        string          `json:"sender_id,omitempty"`
	ReceiverType    string          `json:"receiver_type,omitempty"`
	ReceiverID      string          `json:"receiver_id,omitempty"`
	MessageType     string          `json:"message_type"`
	ContentType     string          `json:"content_type"`
	Role            string          `json:"role"`
	Content         string          `json:"content"`
	ContentJSON     json.RawMessage `json:"content_json"`
	Metadata        json.RawMessage `json:"metadata"`
	Status          string          `json:"status"`
	CreatedAt       time.Time       `json:"created_at"`
}

// ReplayThread 基于 Thread 的持久化消息历史创建新的 replay Run，并复用现有 Kafka 调度链路。
func (s *RuntimeService) ReplayThread(ctx context.Context, command ThreadReplayCommand) (result *ThreadReplayResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if command.OwnerUserID == 0 {
		return nil, errors.New("owner_user_id is required")
	}
	threadID := strings.TrimSpace(command.ThreadID)
	if threadID == "" {
		return nil, errors.New("thread_id is required")
	}
	if len(threadID) > 64 {
		return nil, errors.New("thread_id must be at most 64 characters")
	}

	var thread models.Thread
	if err := s.database.WithContext(ctx).Where("thread_id = ?", threadID).First(&thread).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errThreadNotFound
		}
		return nil, fmt.Errorf("loading thread: %w", err)
	}
	if !command.IsAdmin && thread.OwnerUserID != command.OwnerUserID {
		return nil, errRunForbidden
	}
	idempotencyKey := strings.TrimSpace(command.IdempotencyKey)
	if idempotencyKey != "" {
		if existing, lookupErr := loadRunIdempotency(s.database.WithContext(ctx), command.OwnerUserID, models.RunIdempotencyOperationReplay, idempotencyKey); lookupErr == nil {
			sourceRunID := strings.TrimSpace(command.SourceRunID)
			if sourceRunID == "" {
				sourceRunID = strings.TrimSpace(existing.SourceRunID)
			}
			expectedHash, hashErr := buildThreadReplayRequestHash(command.OwnerUserID, threadID, sourceRunID)
			if hashErr != nil {
				return nil, hashErr
			}
			if existing.RequestHash != expectedHash {
				return nil, errIdempotencyKeyReused
			}
			if strings.TrimSpace(existing.RunID) == "" {
				return nil, errors.New("thread replay idempotency record has empty run_id")
			}
			existingRun, runErr := loadRunByRunID(s.database.WithContext(ctx), existing.RunID)
			if runErr != nil {
				return nil, runErr
			}
			if existingRun.ThreadID != threadID {
				return nil, errIdempotencyKeyReused
			}
			return &ThreadReplayResult{
				Thread:        &thread,
				Run:           existingRun,
				SourceRunID:   strings.TrimSpace(existing.SourceRunID),
				IdempotentHit: true,
			}, nil
		} else if !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("loading thread replay idempotency: %w", lookupErr)
		}
	}
	if thread.Status != models.ThreadStatusActive {
		return nil, errThreadUnavailable
	}

	sourceRunID := strings.TrimSpace(command.SourceRunID)
	sourceRun, err := s.loadReplaySourceRun(ctx, threadID, sourceRunID)
	if err != nil {
		return nil, err
	}
	messages, err := s.loadThreadReplayMessages(ctx, threadID)
	if err != nil {
		return nil, err
	}
	inputJSON, err := marshalThreadReplayInput(threadID, sourceRun.RunID, messages)
	if err != nil {
		return nil, err
	}

	var idempotency *models.RunIdempotency
	if idempotencyKey != "" {
		requestHash, hashErr := buildThreadReplayRequestHash(command.OwnerUserID, threadID, sourceRun.RunID)
		if hashErr != nil {
			return nil, hashErr
		}
		idempotency = &models.RunIdempotency{
			OwnerUserID:    command.OwnerUserID,
			Operation:      models.RunIdempotencyOperationReplay,
			IdempotencyKey: idempotencyKey,
			RequestHash:    requestHash,
			SourceRunID:    sourceRun.RunID,
		}
	}

	traceID := requestctx.TraceIDFromContext(ctx)
	if traceID == "" || traceID == sourceRun.TraceID {
		traceID = requestctx.NewTraceID()
	}
	ctx = requestctx.WithTraceID(ctx, traceID)
	loopID := newPrefixedID("loop")
	run := &models.Run{
		RunID:       newRunID(),
		ThreadID:    threadID,
		TraceID:     traceID,
		LoopID:      &loopID,
		AgentID:     sourceRun.AgentID,
		WorkflowID:  sourceRun.WorkflowID,
		UserID:      thread.OwnerUserID,
		TriggerType: "replay",
		InputJSON:   inputJSON,
		Status:      models.RunStatusPending,
		Provider:    sourceRun.Provider,
		Model:       sourceRun.Model,
	}

	mutation, err := s.runService.createQueuedRunWithIdempotency(ctx, run, nil, idempotency, nil, false)
	if err != nil {
		return nil, err
	}
	if !mutation.IdempotentHit {
		if err := s.runService.publishRunExecute(ctx, mutation.Run.RunID); err != nil {
			return nil, s.runService.handleRunDispatchFailure(ctx, mutation.Run.RunID, "thread_replay", err)
		}
	}
	return &ThreadReplayResult{
		Thread:        &thread,
		Run:           mutation.Run,
		SourceRunID:   sourceRun.RunID,
		IdempotentHit: mutation.IdempotentHit,
	}, nil
}

func (s *RuntimeService) loadReplaySourceRun(ctx context.Context, threadID, sourceRunID string) (*models.Run, error) {
	query := s.database.WithContext(ctx).Where("thread_id = ?", threadID)
	if sourceRunID != "" {
		query = query.Where("run_id = ?", sourceRunID)
	} else {
		query = query.Where("status IN ?", replayableRunStatuses).
			Order("created_at DESC").Order("id DESC")
	}

	var run models.Run
	if err := query.First(&run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if sourceRunID != "" {
				return nil, errRunNotFound
			}
			return nil, errRunNotReplayable
		}
		return nil, fmt.Errorf("loading replay source run: %w", err)
	}
	if !isReplayableRunStatus(run.Status) {
		return nil, errRunNotReplayable
	}
	return &run, nil
}

func (s *RuntimeService) loadThreadReplayMessages(ctx context.Context, threadID string) ([]models.Message, error) {
	messages := make([]models.Message, 0)
	if err := s.database.WithContext(ctx).
		Where("thread_id = ?", threadID).
		Order("created_at ASC").Order("id ASC").Find(&messages).Error; err != nil {
		return nil, fmt.Errorf("loading thread messages: %w", err)
	}
	return messages, nil
}

func marshalThreadReplayInput(threadID, sourceRunID string, messages []models.Message) (string, error) {
	snapshot := threadReplayInput{
		ThreadID:    threadID,
		SourceRunID: sourceRunID,
		Messages:    make([]threadReplayMessage, 0, len(messages)),
	}
	for _, message := range messages {
		contentJSON, err := canonicalizeJSON(json.RawMessage(message.ContentJSON))
		if err != nil {
			return "", fmt.Errorf("normalizing message %s content: %w", message.MessageID, err)
		}
		if contentJSON == "" {
			contentJSON = "null"
		}
		metadataJSON, err := canonicalizeJSON(json.RawMessage(message.MetadataJSON))
		if err != nil {
			return "", fmt.Errorf("normalizing message %s metadata: %w", message.MessageID, err)
		}
		if metadataJSON == "" {
			metadataJSON = "{}"
		}
		content := replayMessageContent(contentJSON)
		snapshot.Messages = append(snapshot.Messages, threadReplayMessage{
			MessageID:       message.MessageID,
			RunID:           message.RunID,
			DelegationID:    message.DelegationID,
			ParentMessageID: message.ParentMessageID,
			SenderType:      message.SenderType,
			SenderID:        message.SenderID,
			ReceiverType:    message.ReceiverType,
			ReceiverID:      message.ReceiverID,
			MessageType:     message.MessageType,
			ContentType:     message.ContentType,
			Role:            replayMessageRole(message),
			Content:         content,
			ContentJSON:     json.RawMessage(contentJSON),
			Metadata:        json.RawMessage(metadataJSON),
			Status:          message.Status,
			CreatedAt:       message.CreatedAt,
		})
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return "", fmt.Errorf("marshalling thread replay input: %w", err)
	}
	return string(encoded), nil
}

func replayMessageRole(message models.Message) string {
	switch message.SenderType {
	case models.MessageSenderUser:
		return "user"
	case models.MessageSenderTool:
		return "tool"
	case models.MessageSenderSystem, models.MessageSenderRuntime:
		return "system"
	case models.MessageSenderAgent:
		return "assistant"
	default:
		return "user"
	}
}

func replayMessageContent(contentJSON string) string {
	var text string
	if err := json.Unmarshal([]byte(contentJSON), &text); err == nil {
		return text
	}
	var envelope struct {
		Text    string `json:"text"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal([]byte(contentJSON), &envelope); err == nil {
		if strings.TrimSpace(envelope.Text) != "" {
			return envelope.Text
		}
		if strings.TrimSpace(envelope.Message) != "" {
			return envelope.Message
		}
	}
	return contentJSON
}

func buildThreadReplayRequestHash(ownerUserID uint64, threadID, sourceRunID string) (string, error) {
	return hashPayload(map[string]any{
		"operation":     models.RunIdempotencyOperationReplay,
		"owner_user_id": ownerUserID,
		"thread_id":     threadID,
		"source_run_id": sourceRunID,
	})
}

func isReplayableRunStatus(status string) bool {
	for _, replayable := range replayableRunStatuses {
		if status == replayable {
			return true
		}
	}
	return false
}
