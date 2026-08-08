package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"GoAI/models"
	"GoAI/observability"

	"gorm.io/gorm"
)

// IncomingMessage 是协议 Gateway 映射到 Runtime 的稳定内部消息结构。
type IncomingMessage struct {
	MessageID       string
	SenderType      string
	SenderID        string
	ReceiverType    string
	ReceiverID      string
	MessageType     string
	ContentType     string
	ContentJSON     string
	MetadataJSON    string
	ParentMessageID string
}

// StartRunCommand 描述协议无关的 Thread、Message 与 Run 创建命令。
type StartRunCommand struct {
	OwnerUserID    uint64
	AgentCode      string
	ThreadID       string
	RequestedRunID string
	ParentRunID    string
	TriggerType    string
	Input          json.RawMessage
	Provider       string
	Model          string
	Messages       []IncomingMessage
}

// ResumeInterruptCommand 描述一次协议无关的 Interrupt 处理请求。
type ResumeInterruptCommand struct {
	InterruptID string
	Status      string
	PayloadJSON string
}

// ResumeRunCommand 描述恢复一个等待用户输入的 Run 所需的命令。
type ResumeRunCommand struct {
	OwnerUserID uint64
	AgentCode   string
	RunID       string
	Interrupts  []ResumeInterruptCommand
}

// StartRunResult 返回 Runtime 创建或复用的 Thread 与 Run。
type StartRunResult struct {
	Thread *models.Thread
	Run    *models.Run
	Reused bool
}

// RunSnapshot 是协议 Gateway 可观察的 Run、Step 与 Message 快照。
type RunSnapshot struct {
	Run        models.Run
	Steps      []models.RunStep
	Messages   []models.Message
	Interrupts []models.RunInterrupt
}

// Runtime 定义协议 Gateway 与多 Agent 运行时之间的稳定边界。
type Runtime interface {
	StartRun(context.Context, StartRunCommand) (*StartRunResult, error)
	ResumeRun(context.Context, ResumeRunCommand) (*StartRunResult, error)
	Snapshot(context.Context, uint64, string) (*RunSnapshot, error)
	ReplayThread(context.Context, ThreadReplayCommand) (*ThreadReplayResult, error)
}

// RuntimeService 协调 Thread、Message 与 Run 的原子创建和查询。
type RuntimeService struct {
	database        *gorm.DB
	runService      *RunService
	observability   *observability.Bundle
	resumePublisher RunResumePublisher
	callbackSender  DelegationCallbackSender
	taskCanceller   AgentTaskCanceller
}

// RuntimeServiceOption 配置 RuntimeService 的可选运行时依赖。
type RuntimeServiceOption func(*RuntimeService) error

// WithRuntimeObservability 注入 Runtime 协调的日志、指标和 Trace 能力。
func WithRuntimeObservability(bundle *observability.Bundle) RuntimeServiceOption {
	return func(service *RuntimeService) error {
		if bundle == nil {
			return errors.New("configuring runtime service: observability bundle is nil")
		}
		service.observability = bundle
		return nil
	}
}

// WithRunResumePublisher 注入 Parent Run 恢复事件发布边界。
func WithRunResumePublisher(publisher RunResumePublisher) RuntimeServiceOption {
	return func(service *RuntimeService) error {
		if publisher == nil {
			return errors.New("configuring runtime service: resume publisher is nil")
		}
		service.resumePublisher = publisher
		return nil
	}
}

// WithDelegationCallbackSender 注入 A2A 终态 callback 投递边界。
func WithDelegationCallbackSender(sender DelegationCallbackSender) RuntimeServiceOption {
	return func(service *RuntimeService) error {
		if sender == nil {
			return errors.New("configuring runtime service: callback sender is nil")
		}
		service.callbackSender = sender
		return nil
	}
}

// WithAgentTaskCanceller 注入通过 A2A 取消远程 Child Task 的协议客户端。
func WithAgentTaskCanceller(canceller AgentTaskCanceller) RuntimeServiceOption {
	return func(service *RuntimeService) error {
		if canceller == nil {
			return errors.New("configuring runtime service: agent task canceller is nil")
		}
		service.taskCanceller = canceller
		return nil
	}
}

// NewRuntimeService 使用显式依赖构造 RuntimeService。
func NewRuntimeService(database *gorm.DB, runService *RunService, options ...RuntimeServiceOption) (*RuntimeService, error) {
	if database == nil {
		return nil, errors.New("creating runtime service: database is nil")
	}
	if runService == nil {
		return nil, errors.New("creating runtime service: run service is nil")
	}
	service := &RuntimeService{database: database, runService: runService}
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

// CancelRemoteTask 提供 Runtime 使用的远程 A2A Task 取消边界。
func (s *RuntimeService) CancelRemoteTask(ctx context.Context, request AgentTaskCancellationRequest) error {
	if s == nil || s.taskCanceller == nil {
		return errors.New("cancelling remote task: A2A task canceller is unavailable")
	}
	return s.taskCanceller.CancelTask(ctx, request)
}

// StartRun 在同一事务中创建或复用 Thread、持久化输入 Message 并创建 Run。
func (s *RuntimeService) StartRun(ctx context.Context, command StartRunCommand) (result *StartRunResult, err error) {
	observedCtx, span, startedAt := startServiceObservation(ctx, s.observability, "runtime.start_run", "", command.ThreadID, "")
	status := "success"
	defer func() {
		if err != nil {
			status = "error"
		}
		runID, threadID := "", ""
		if result != nil {
			if result.Run != nil {
				runID = result.Run.RunID
			}
			if result.Thread != nil {
				threadID = result.Thread.ThreadID
			}
		}
		finishServiceObservation(s.observability, observedCtx, span, "start_run", status, startedAt, runID, threadID, "", func(metrics *observability.Metrics, operation, status string, elapsed time.Duration) {
			metrics.ObserveRuntime(operation, status, elapsed)
		}, err)
	}()
	return s.startRun(observedCtx, command)
}

func (s *RuntimeService) startRun(ctx context.Context, command StartRunCommand) (*StartRunResult, error) {
	if command.OwnerUserID == 0 {
		return nil, errors.New("owner_user_id is required")
	}
	if len(command.Messages) == 0 {
		return nil, errors.New("messages are required")
	}
	for _, message := range command.Messages {
		if err := validateIncomingMessage(message); err != nil {
			return nil, err
		}
	}
	threadID := strings.TrimSpace(command.ThreadID)
	parentRunID := strings.TrimSpace(command.ParentRunID)
	if parentRunID != "" {
		if strings.TrimSpace(command.RequestedRunID) == parentRunID {
			return nil, errors.New("parent_run_id cannot equal run_id")
		}
		parent, err := s.runService.GetRunByRunID(ctx, command.OwnerUserID, false, parentRunID)
		if err != nil {
			return nil, err
		}
		if threadID == "" {
			threadID = strings.TrimSpace(parent.ThreadID)
		} else if threadID != strings.TrimSpace(parent.ThreadID) {
			return nil, errParentRunThreadMismatch
		}
		if threadID == "" {
			return nil, errors.New("parent run thread_id is required")
		}
	}
	generatedThreadID := threadID == ""
	if generatedThreadID {
		threadID = newThreadID()
	}
	if len(threadID) > 64 {
		return nil, errors.New("thread_id must be at most 64 characters")
	}

	var runtimeThread *models.Thread
	hook := func(tx *gorm.DB, run *models.Run, agent *models.Agent) error {
		thread, err := ensureThread(tx, threadID, command.OwnerUserID)
		if err != nil {
			return err
		}
		runtimeThread = thread
		for _, incoming := range command.Messages {
			if err := persistIncomingMessage(tx, run, agent, command.OwnerUserID, incoming); err != nil {
				return err
			}
		}
		return nil
	}

	result, err := s.runService.createRun(ctx, command.OwnerUserID, CreateRunRequest{
		AgentCode:                 command.AgentCode,
		ThreadID:                  threadID,
		TriggerType:               command.TriggerType,
		Input:                     command.Input,
		Provider:                  command.Provider,
		Model:                     command.Model,
		RequestedRunID:            command.RequestedRunID,
		ParentRunID:               parentRunID,
		allowGeneratedThreadReuse: generatedThreadID && strings.TrimSpace(command.RequestedRunID) != "",
	}, hook)
	if err != nil {
		return nil, err
	}
	if result.IdempotentHit {
		var thread models.Thread
		if err := s.database.WithContext(ctx).Where("thread_id = ?", result.Run.ThreadID).First(&thread).Error; err != nil {
			return nil, err
		}
		agent := &models.Agent{AgentCode: strings.TrimSpace(command.AgentCode)}
		if err := s.validateExistingIncomingMessages(ctx, result.Run, agent, command.OwnerUserID, command.Messages); err != nil {
			return nil, err
		}
		runtimeThread = &thread
	}
	if runtimeThread == nil {
		return nil, errors.New("runtime thread was not resolved")
	}
	return &StartRunResult{Thread: runtimeThread, Run: result.Run, Reused: result.IdempotentHit}, nil
}

// ResumeRun 原子处理等待中的 Interrupt，并在全部暂停点解决后重新投递 Run。
func (s *RuntimeService) ResumeRun(ctx context.Context, command ResumeRunCommand) (*StartRunResult, error) {
	if command.OwnerUserID == 0 {
		return nil, errors.New("owner_user_id is required")
	}
	if strings.TrimSpace(command.AgentCode) == "" {
		return nil, errors.New("agent_code is required")
	}
	if strings.TrimSpace(command.RunID) == "" {
		return nil, errors.New("run_id is required")
	}
	if len(command.Interrupts) == 0 {
		return nil, errResumeEntriesRequired
	}
	mutation, shouldPublish, err := s.runService.resumeRun(ctx, command)
	if err != nil {
		return nil, err
	}
	if shouldPublish {
		if err := s.runService.publishRunExecute(ctx, mutation.Run.RunID); err != nil {
			return nil, s.runService.handleRunDispatchFailure(ctx, mutation.Run.RunID, "resume", err)
		}
	}
	var thread models.Thread
	if err := s.database.WithContext(ctx).Where("thread_id = ?", mutation.Run.ThreadID).First(&thread).Error; err != nil {
		return nil, err
	}
	return &StartRunResult{Thread: &thread, Run: mutation.Run, Reused: mutation.IdempotentHit}, nil
}

// Snapshot 返回当前用户可见的 Run 执行快照。
func (s *RuntimeService) Snapshot(ctx context.Context, ownerUserID uint64, runID string) (snapshot *RunSnapshot, err error) {
	observedCtx, span, startedAt := startServiceObservation(ctx, s.observability, "runtime.snapshot", runID, "", "")
	status := "success"
	defer func() {
		if err != nil {
			status = "error"
		}
		threadID := ""
		if snapshot != nil {
			threadID = snapshot.Run.ThreadID
		}
		finishServiceObservation(s.observability, observedCtx, span, "snapshot", status, startedAt, runID, threadID, "", func(metrics *observability.Metrics, operation, status string, elapsed time.Duration) {
			metrics.ObserveRuntime(operation, status, elapsed)
		}, err)
	}()
	return s.snapshot(observedCtx, ownerUserID, runID)
}

func (s *RuntimeService) snapshot(ctx context.Context, ownerUserID uint64, runID string) (*RunSnapshot, error) {
	run, err := s.runService.GetRunByRunID(ctx, ownerUserID, false, runID)
	if err != nil {
		return nil, err
	}
	steps, err := s.runService.GetRunStepsByRunID(ctx, ownerUserID, false, runID)
	if err != nil {
		return nil, err
	}
	var messages []models.Message
	if err := s.database.WithContext(ctx).
		Where("run_id = ?", runID).
		Order("created_at ASC").
		Order("id ASC").
		Find(&messages).Error; err != nil {
		return nil, err
	}
	var interrupts []models.RunInterrupt
	if err := s.database.WithContext(ctx).
		Where("run_id = ?", runID).
		Order("created_at ASC").
		Order("id ASC").
		Find(&interrupts).Error; err != nil {
		return nil, err
	}
	return &RunSnapshot{Run: *run, Steps: steps, Messages: messages, Interrupts: interrupts}, nil
}

func ensureThread(tx *gorm.DB, threadID string, ownerUserID uint64) (*models.Thread, error) {
	var thread models.Thread
	err := tx.Where("thread_id = ?", threadID).First(&thread).Error
	if err == nil {
		if thread.OwnerUserID != ownerUserID {
			return nil, errRunForbidden
		}
		if thread.Status != models.ThreadStatusActive {
			return nil, errThreadUnavailable
		}
		return &thread, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	thread = models.Thread{
		ThreadID:     threadID,
		OwnerUserID:  ownerUserID,
		Status:       models.ThreadStatusActive,
		MetadataJSON: "{}",
	}
	if err := tx.Create(&thread).Error; err != nil {
		if !isUniqueConstraintError(err) {
			return nil, err
		}
		return ensureThread(tx, threadID, ownerUserID)
	}
	return &thread, nil
}

func persistIncomingMessage(tx *gorm.DB, run *models.Run, agent *models.Agent, ownerUserID uint64, incoming IncomingMessage) error {
	message, err := normalizeIncomingMessage(run, agent, ownerUserID, incoming)
	if err != nil {
		return err
	}
	if err := tx.Create(message).Error; err != nil {
		if !isUniqueConstraintError(err) {
			return err
		}
		var existing models.Message
		if loadErr := tx.Where("message_id = ?", message.MessageID).First(&existing).Error; loadErr != nil {
			return loadErr
		}
		if !sameMessage(&existing, message) {
			return errMessageConflict
		}
	}
	return nil
}

func (s *RuntimeService) validateExistingIncomingMessages(
	ctx context.Context,
	run *models.Run,
	agent *models.Agent,
	ownerUserID uint64,
	incomingMessages []IncomingMessage,
) error {
	for _, incoming := range incomingMessages {
		expected, err := normalizeIncomingMessage(run, agent, ownerUserID, incoming)
		if err != nil {
			return err
		}
		var existing models.Message
		if err := s.database.WithContext(ctx).Where("message_id = ?", expected.MessageID).First(&existing).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return errMessageConflict
			}
			return err
		}
		if !sameMessage(&existing, expected) {
			return errMessageConflict
		}
	}
	return nil
}

func normalizeIncomingMessage(run *models.Run, agent *models.Agent, ownerUserID uint64, incoming IncomingMessage) (*models.Message, error) {
	if err := validateIncomingMessage(incoming); err != nil {
		return nil, err
	}
	contentJSON := strings.TrimSpace(incoming.ContentJSON)
	if contentJSON == "" || !json.Valid([]byte(contentJSON)) {
		return nil, errors.New("message content must be valid JSON")
	}
	metadataJSON := strings.TrimSpace(incoming.MetadataJSON)
	if metadataJSON == "" {
		metadataJSON = "{}"
	} else if !json.Valid([]byte(metadataJSON)) {
		return nil, errors.New("message metadata must be valid JSON")
	}
	message := &models.Message{
		MessageID:       strings.TrimSpace(incoming.MessageID),
		ThreadID:        run.ThreadID,
		RunID:           run.RunID,
		ParentMessageID: strings.TrimSpace(incoming.ParentMessageID),
		SenderType:      strings.TrimSpace(incoming.SenderType),
		SenderID:        strings.TrimSpace(incoming.SenderID),
		ReceiverType:    strings.TrimSpace(incoming.ReceiverType),
		ReceiverID:      strings.TrimSpace(incoming.ReceiverID),
		MessageType:     strings.TrimSpace(incoming.MessageType),
		ContentType:     strings.TrimSpace(incoming.ContentType),
		ContentJSON:     contentJSON,
		MetadataJSON:    metadataJSON,
		Status:          models.MessageStatusDelivered,
	}
	if message.SenderID == "" && message.SenderType == models.MessageSenderUser {
		message.SenderID = fmt.Sprintf("%d", ownerUserID)
	}
	if message.ReceiverID == "" && message.ReceiverType == models.MessageSenderAgent && agent != nil {
		message.ReceiverID = strings.TrimSpace(agent.AgentCode)
	}
	return message, nil
}

func validateIncomingMessage(incoming IncomingMessage) error {
	messageID := strings.TrimSpace(incoming.MessageID)
	if messageID == "" {
		return errors.New("message_id is required")
	}
	if len(messageID) > 64 {
		return errors.New("message_id must be at most 64 characters")
	}
	if len(strings.TrimSpace(incoming.ParentMessageID)) > 64 {
		return errors.New("parent_message_id must be at most 64 characters")
	}
	if !isValidMessagePartyType(strings.TrimSpace(incoming.SenderType)) {
		return errors.New("sender_type is invalid")
	}
	if len(strings.TrimSpace(incoming.SenderID)) > 64 {
		return errors.New("sender_id must be at most 64 characters")
	}
	if !isValidMessagePartyType(strings.TrimSpace(incoming.ReceiverType)) {
		return errors.New("receiver_type is invalid")
	}
	if len(strings.TrimSpace(incoming.ReceiverID)) > 64 {
		return errors.New("receiver_id must be at most 64 characters")
	}
	if !isValidMessageType(strings.TrimSpace(incoming.MessageType)) {
		return errors.New("message_type is invalid")
	}
	contentType := strings.TrimSpace(incoming.ContentType)
	if contentType == "" {
		return errors.New("content_type is required")
	}
	if len(contentType) > 32 {
		return errors.New("content_type must be at most 32 characters")
	}
	return nil
}

func isValidMessagePartyType(value string) bool {
	switch value {
	case models.MessageSenderUser, models.MessageSenderAgent, models.MessageSenderTool, models.MessageSenderRuntime, models.MessageSenderSystem:
		return true
	default:
		return false
	}
}

func isValidMessageType(value string) bool {
	switch value {
	case models.MessageTypeInput,
		models.MessageTypeDelegation,
		models.MessageTypeResult,
		models.MessageTypeToolResult,
		models.MessageTypeStatusUpdate,
		models.MessageTypeSystemEvent:
		return true
	default:
		return false
	}
}

func sameMessage(left, right *models.Message) bool {
	if left == nil || right == nil {
		return false
	}
	return left.MessageID == right.MessageID &&
		left.ThreadID == right.ThreadID &&
		left.ParentMessageID == right.ParentMessageID &&
		left.SenderType == right.SenderType &&
		left.SenderID == right.SenderID &&
		left.ReceiverType == right.ReceiverType &&
		left.ReceiverID == right.ReceiverID &&
		left.MessageType == right.MessageType &&
		left.ContentType == right.ContentType &&
		left.ContentJSON == right.ContentJSON
}
