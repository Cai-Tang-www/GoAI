package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"GoAI/middlewares"
	"GoAI/models"
	"GoAI/services"

	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/events"
	aguitypes "github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/core/types"
	"github.com/ag-ui-protocol/ag-ui/sdks/community/go/pkg/encoding/sse"
	"github.com/gin-gonic/gin"
)

const defaultAGUIPollInterval = 50 * time.Millisecond

// AGUIHandler 将官方 AG-UI 请求映射到内部 Runtime，并把执行快照编码为标准事件流。
type AGUIHandler struct {
	runtime      services.Runtime
	writer       *sse.SSEWriter
	pollInterval time.Duration
}

// NewAGUIHandler 使用协议无关 Runtime 构造 AG-UI Gateway。
func NewAGUIHandler(runtime services.Runtime) (*AGUIHandler, error) {
	if runtime == nil {
		return nil, errors.New("creating AG-UI handler: runtime is nil")
	}
	return &AGUIHandler{
		runtime:      runtime,
		writer:       sse.NewSSEWriter(),
		pollInterval: defaultAGUIPollInterval,
	}, nil
}

// RunAgent 处理一个 AG-UI RunAgentInput，并持续回传 Run、Step 与 Message 事件。
func (h *AGUIHandler) RunAgent(c *gin.Context) {
	userID, _, ok := authPrincipal(c)
	if !ok {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidToken())
		return
	}
	agentCode := strings.TrimSpace(c.Param("agent_code"))
	if agentCode == "" || len(agentCode) > 64 {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("agent_code is required and must be at most 64 characters", nil))
		return
	}

	var input aguitypes.RunAgentInput
	if err := c.ShouldBindJSON(&input); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid AG-UI request", nil))
		return
	}
	command, err := buildAGUIStartRunCommand(userID, agentCode, input)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed(err.Error(), nil))
		return
	}
	result, err := h.runtime.StartRun(c.Request.Context(), command)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Status(http.StatusOK)
	if err := h.writer.WriteEvent(c.Request.Context(), c.Writer, events.NewRunStartedEvent(result.Thread.ThreadID, result.Run.RunID)); err != nil {
		return
	}
	h.streamRun(c, userID, result.Run.RunID, result.Thread.ThreadID)
}

func (h *AGUIHandler) streamRun(c *gin.Context, userID uint64, runID, threadID string) {
	ticker := time.NewTicker(h.pollInterval)
	defer ticker.Stop()

	startedSteps := make(map[uint64]struct{})
	finishedSteps := make(map[uint64]struct{})
	emittedMessages := make(map[uint64]struct{})
	for {
		done, err := h.emitSnapshot(c, userID, runID, threadID, startedSteps, finishedSteps, emittedMessages)
		if err != nil || done {
			return
		}
		select {
		case <-c.Request.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func (h *AGUIHandler) emitSnapshot(
	c *gin.Context,
	userID uint64,
	runID string,
	threadID string,
	startedSteps map[uint64]struct{},
	finishedSteps map[uint64]struct{},
	emittedMessages map[uint64]struct{},
) (bool, error) {
	snapshot, err := h.runtime.Snapshot(c.Request.Context(), userID, runID)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			(errors.Is(err, context.DeadlineExceeded) && c.Request.Context().Err() != nil) {
			return true, nil
		}
		log.Printf("AG-UI snapshot failed trace_id=%s run_id=%s error=%v", middlewares.TraceID(c), runID, err)
		return true, h.writer.WriteEvent(c.Request.Context(), c.Writer, events.NewRunErrorEvent("run observation failed", events.WithRunID(runID), events.WithErrorCode(middlewares.CodeInternalError)))
	}
	for i := range snapshot.Steps {
		step := &snapshot.Steps[i]
		if _, seen := startedSteps[step.ID]; !seen {
			if err := h.writer.WriteEvent(c.Request.Context(), c.Writer, events.NewStepStartedEvent(step.StepKey)); err != nil {
				return true, err
			}
			startedSteps[step.ID] = struct{}{}
		}
		if isStepTerminal(step.Status) {
			if _, seen := finishedSteps[step.ID]; !seen {
				if err := h.writer.WriteEvent(c.Request.Context(), c.Writer, events.NewStepFinishedEvent(step.StepKey)); err != nil {
					return true, err
				}
				finishedSteps[step.ID] = struct{}{}
			}
		}
	}
	for i := range snapshot.Messages {
		message := &snapshot.Messages[i]
		if message.MessageType != models.MessageTypeResult || isAGUIInputMessage(message.MetadataJSON) {
			continue
		}
		if _, seen := emittedMessages[message.ID]; seen {
			continue
		}
		text := messageText(message.ContentJSON)
		if text == "" {
			continue
		}
		if err := h.writer.WriteEvent(c.Request.Context(), c.Writer, events.NewTextMessageStartEvent(message.MessageID, events.WithRole("assistant"))); err != nil {
			return true, err
		}
		if err := h.writer.WriteEvent(c.Request.Context(), c.Writer, events.NewTextMessageContentEvent(message.MessageID, text)); err != nil {
			return true, err
		}
		if err := h.writer.WriteEvent(c.Request.Context(), c.Writer, events.NewTextMessageEndEvent(message.MessageID)); err != nil {
			return true, err
		}
		emittedMessages[message.ID] = struct{}{}
	}

	switch snapshot.Run.Status {
	case models.RunStatusSuccess:
		return true, h.writer.WriteEvent(c.Request.Context(), c.Writer, events.NewRunFinishedEvent(threadID, runID))
	case models.RunStatusFailed, models.RunStatusCancelled:
		if message := strings.TrimSpace(snapshot.Run.ErrorMessage); message != "" {
			log.Printf("AG-UI run failed trace_id=%s run_id=%s error=%s", middlewares.TraceID(c), runID, message)
		}
		return true, h.writer.WriteEvent(c.Request.Context(), c.Writer, events.NewRunErrorEvent("run did not complete successfully", events.WithRunID(runID), events.WithErrorCode(middlewares.CodeInternalError)))
	default:
		return false, nil
	}
}

func buildAGUIStartRunCommand(userID uint64, agentCode string, input aguitypes.RunAgentInput) (services.StartRunCommand, error) {
	threadID := strings.TrimSpace(input.ThreadID)
	if len(threadID) > 64 {
		return services.StartRunCommand{}, errors.New("threadId must be at most 64 characters")
	}
	runID := strings.TrimSpace(input.RunID)
	if len(runID) > 64 {
		return services.StartRunCommand{}, errors.New("runId must be at most 64 characters")
	}
	if input.ParentRunID != nil && strings.TrimSpace(*input.ParentRunID) != "" {
		return services.StartRunCommand{}, errors.New("parentRunId branching is not supported in V1")
	}
	if len(input.Resume) > 0 {
		return services.StartRunCommand{}, errors.New("resume is not supported in V1")
	}
	if nonEmpty, err := hasNonEmptyAGUIValue(input.State); err != nil {
		return services.StartRunCommand{}, fmt.Errorf("validate state: %w", err)
	} else if nonEmpty {
		return services.StartRunCommand{}, errors.New("state is not supported in V1")
	}
	if len(input.Tools) > 0 {
		return services.StartRunCommand{}, errors.New("client-provided tools are not supported in V1")
	}
	if len(input.Context) > 0 {
		return services.StartRunCommand{}, errors.New("context entries are not supported in V1")
	}
	if nonEmpty, err := hasNonEmptyAGUIValue(input.ForwardedProps); err != nil {
		return services.StartRunCommand{}, fmt.Errorf("validate forwardedProps: %w", err)
	} else if nonEmpty {
		return services.StartRunCommand{}, errors.New("forwardedProps is not supported in V1")
	}
	if len(input.Messages) == 0 {
		return services.StartRunCommand{}, errors.New("messages are required")
	}
	messages := make([]services.IncomingMessage, 0, len(input.Messages))
	llmMessages := make([]map[string]string, 0, len(input.Messages))
	for _, message := range input.Messages {
		if err := validateAGUIMessage(message); err != nil {
			return services.StartRunCommand{}, err
		}
		text, err := aguiTextContent(message.Content)
		if err != nil {
			return services.StartRunCommand{}, err
		}
		if strings.TrimSpace(text) == "" {
			return services.StartRunCommand{}, errors.New("message content must not be empty")
		}
		if len(strings.TrimSpace(message.ID)) > 64 {
			return services.StartRunCommand{}, errors.New("message id must be at most 64 characters")
		}
		incoming, err := mapAGUIMessage(userID, agentCode, message, text)
		if err != nil {
			return services.StartRunCommand{}, err
		}
		messages = append(messages, incoming)
		llmMessages = append(llmMessages, map[string]string{"role": string(message.Role), "content": text})
	}
	payload := map[string]any{"messages": llmMessages}
	raw, err := json.Marshal(payload)
	if err != nil {
		return services.StartRunCommand{}, err
	}
	return services.StartRunCommand{
		OwnerUserID:    userID,
		AgentCode:      agentCode,
		ThreadID:       threadID,
		RequestedRunID: runID,
		TriggerType:    "agui",
		Input:          raw,
		Messages:       messages,
	}, nil
}

func hasNonEmptyAGUIValue(value any) (bool, error) {
	if value == nil {
		return false, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return false, err
	}
	switch strings.TrimSpace(string(raw)) {
	case "null", "{}", "[]":
		return false, nil
	default:
		return true, nil
	}
}
func validateAGUIMessage(message aguitypes.Message) error {
	messageID := strings.TrimSpace(message.ID)
	if messageID == "" {
		return errors.New("message id is required")
	}
	if len(messageID) > 64 {
		return errors.New("message id must be at most 64 characters")
	}
	switch message.Role {
	case aguitypes.RoleUser, aguitypes.RoleAssistant, aguitypes.RoleSystem, aguitypes.RoleDeveloper:
	case aguitypes.RoleTool, aguitypes.RoleActivity, aguitypes.RoleReasoning:
		return errors.New("tool, activity, and reasoning messages are not supported in V1")
	default:
		return errors.New("message role is not supported in V1")
	}
	if strings.TrimSpace(message.Name) != "" {
		return errors.New("named messages are not supported in V1")
	}
	if strings.TrimSpace(message.EncryptedContent) != "" || strings.TrimSpace(message.EncryptedValue) != "" {
		return errors.New("encrypted message fields are not supported in V1")
	}
	if len(message.ToolCalls) > 0 || strings.TrimSpace(message.ToolCallID) != "" || strings.TrimSpace(message.Error) != "" {
		return errors.New("tool call message fields are not supported in V1")
	}
	if strings.TrimSpace(message.ActivityType) != "" {
		return errors.New("activity message fields are not supported in V1")
	}
	return nil
}

func mapAGUIMessage(userID uint64, agentCode string, message aguitypes.Message, text string) (services.IncomingMessage, error) {
	content, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return services.IncomingMessage{}, err
	}
	mapped := services.IncomingMessage{
		MessageID:    strings.TrimSpace(message.ID),
		ContentType:  "text",
		ContentJSON:  string(content),
		MetadataJSON: `{"source":"agui_request"}`,
	}
	switch message.Role {
	case aguitypes.RoleUser:
		mapped.SenderType = models.MessageSenderUser
		mapped.SenderID = strconv.FormatUint(userID, 10)
		mapped.ReceiverType = models.MessageSenderAgent
		mapped.ReceiverID = agentCode
		mapped.MessageType = models.MessageTypeInput
	case aguitypes.RoleAssistant:
		mapped.SenderType = models.MessageSenderAgent
		mapped.SenderID = agentCode
		mapped.ReceiverType = models.MessageSenderUser
		mapped.ReceiverID = strconv.FormatUint(userID, 10)
		mapped.MessageType = models.MessageTypeResult
	case aguitypes.RoleSystem, aguitypes.RoleDeveloper:
		mapped.SenderType = models.MessageSenderSystem
		mapped.ReceiverType = models.MessageSenderAgent
		mapped.ReceiverID = agentCode
		mapped.MessageType = models.MessageTypeSystemEvent
	default:
		return services.IncomingMessage{}, errors.New("message role is not supported in V1")
	}
	return mapped, nil
}

func aguiTextContent(content any) (string, error) {
	switch value := content.(type) {
	case string:
		return value, nil
	case []aguitypes.InputContent:
		return joinTextFragments(value)
	case []any:
		raw, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		var fragments []aguitypes.InputContent
		if err := json.Unmarshal(raw, &fragments); err != nil {
			return "", errors.New("message content must be text")
		}
		return joinTextFragments(fragments)
	default:
		return "", errors.New("message content must be text in V1")
	}
}

func joinTextFragments(fragments []aguitypes.InputContent) (string, error) {
	var builder strings.Builder
	for _, fragment := range fragments {
		if fragment.Type != aguitypes.InputContentTypeText {
			return "", errors.New("multimodal message content is not supported in V1")
		}
		builder.WriteString(fragment.Text)
	}
	return builder.String(), nil
}

func isAGUIInputMessage(metadataJSON string) bool {
	var metadata struct {
		Source string `json:"source"`
	}
	if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
		return false
	}
	return metadata.Source == "agui_request"
}

func messageText(contentJSON string) string {
	var content struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(contentJSON), &content); err != nil {
		return ""
	}
	return content.Text
}

func isStepTerminal(status string) bool {
	return status == models.RunStepStatusSuccess || status == models.RunStepStatusFailed || status == models.RunStepStatusSkipped
}
