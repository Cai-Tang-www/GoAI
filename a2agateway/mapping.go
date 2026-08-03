package a2agateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"GoAI/models"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

type delegationExtension struct {
	SourceAgentCode string `json:"sourceAgentCode"`
	CapabilityCode  string `json:"capabilityCode"`
	ParentRunID     string `json:"parentRunId"`
	TraceID         string `json:"traceId"`
	DelegationID    string `json:"delegationId"`
}

func commandFromRequest(targetAgent string, request *a2a.SendMessageRequest) (services.AcceptDelegationCommand, error) {
	if request == nil || request.Message == nil {
		return services.AcceptDelegationCommand{}, a2a.NewError(a2a.ErrInvalidParams, "message is required")
	}
	message := request.Message
	if strings.TrimSpace(message.ID) == "" {
		return services.AcceptDelegationCommand{}, a2a.NewError(a2a.ErrInvalidParams, "message id is required")
	}
	if len(message.ID) > 64 {
		return services.AcceptDelegationCommand{}, a2a.NewError(a2a.ErrInvalidParams, "message id is too long")
	}
	if message.Role != a2a.MessageRoleUser {
		return services.AcceptDelegationCommand{}, a2a.NewError(a2a.ErrInvalidParams, "delegation message role must be user")
	}
	extension, err := parseDelegationExtension(message, request)
	if err != nil {
		return services.AcceptDelegationCommand{}, err
	}
	input, err := inputFromParts(message.Parts)
	if err != nil {
		return services.AcceptDelegationCommand{}, err
	}

	childRunID := strings.TrimSpace(string(message.TaskID))
	if childRunID == "" {
		childRunID = stableID("a2a", message.ID)
	}
	threadID := strings.TrimSpace(message.ContextID)
	if threadID == "" {
		threadID = stableID("thread", message.ID)
	}
	metadata, err := json.Marshal(message.Metadata)
	if err != nil {
		return services.AcceptDelegationCommand{}, a2a.NewError(a2a.ErrInvalidParams, "message metadata is invalid")
	}
	var pushConfig *services.DelegationPushConfig
	if request.Config != nil && request.Config.PushConfig != nil {
		protocolConfig := request.Config.PushConfig
		if protocolConfig.Auth != nil {
			return services.AcceptDelegationCommand{}, a2a.NewError(a2a.ErrInvalidParams, "push config authentication is managed by the GoAI A2A machine identity")
		}
		if strings.TrimSpace(string(protocolConfig.TaskID)) != childRunID {
			return services.AcceptDelegationCommand{}, a2a.NewError(a2a.ErrInvalidParams, "push config taskId must match message taskId")
		}
		pushConfig = &services.DelegationPushConfig{
			ConfigID:    strings.TrimSpace(protocolConfig.ID),
			TaskID:      childRunID,
			CallbackURL: strings.TrimSpace(protocolConfig.URL),
			Token:       strings.TrimSpace(protocolConfig.Token),
		}
	}
	return services.AcceptDelegationCommand{
		SourceAgentCode:       extension.SourceAgentCode,
		TargetAgentCode:       targetAgent,
		CapabilityCode:        extension.CapabilityCode,
		ParentRunID:           extension.ParentRunID,
		TraceID:               extension.TraceID,
		RequestedDelegationID: extension.DelegationID,
		ThreadID:              threadID,
		RequestedChildRunID:   childRunID,
		RequestMessageID:      message.ID,
		Input:                 input,
		MetadataJSON:          string(metadata),
		PushConfig:            pushConfig,
	}, nil
}

func parseDelegationExtension(message *a2a.Message, request *a2a.SendMessageRequest) (delegationExtension, error) {
	if !containsString(message.Extensions, DelegationExtensionURI) {
		return delegationExtension{}, a2a.NewError(a2a.ErrExtensionSupportRequired, "GoAI delegation extension is required")
	}
	value, ok := message.Metadata[DelegationExtensionURI]
	if !ok && request != nil {
		value, ok = request.Metadata[DelegationExtensionURI]
	}
	if !ok {
		return delegationExtension{}, a2a.NewError(a2a.ErrInvalidParams, "delegation metadata is required")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return delegationExtension{}, a2a.NewError(a2a.ErrInvalidParams, "delegation metadata is invalid")
	}
	var extension delegationExtension
	if err := json.Unmarshal(payload, &extension); err != nil {
		return delegationExtension{}, a2a.NewError(a2a.ErrInvalidParams, "delegation metadata is invalid")
	}
	extension.SourceAgentCode = strings.TrimSpace(extension.SourceAgentCode)
	extension.CapabilityCode = strings.TrimSpace(extension.CapabilityCode)
	extension.ParentRunID = strings.TrimSpace(extension.ParentRunID)
	extension.TraceID = strings.TrimSpace(extension.TraceID)
	extension.DelegationID = strings.TrimSpace(extension.DelegationID)
	if extension.SourceAgentCode == "" || extension.CapabilityCode == "" || extension.ParentRunID == "" {
		return delegationExtension{}, a2a.NewError(a2a.ErrInvalidParams, "sourceAgentCode, capabilityCode and parentRunId are required")
	}
	if len(extension.TraceID) > 128 {
		return delegationExtension{}, a2a.NewError(a2a.ErrInvalidParams, "traceId is too long")
	}
	if len(extension.DelegationID) > 64 {
		return delegationExtension{}, a2a.NewError(a2a.ErrInvalidParams, "delegationId is too long")
	}
	return extension, nil
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func inputFromParts(parts a2a.ContentParts) (json.RawMessage, error) {
	if len(parts) == 0 {
		return nil, a2a.NewError(a2a.ErrInvalidParams, "message parts are required")
	}
	texts := make([]string, 0, len(parts))
	var data any
	dataParts := 0
	for _, part := range parts {
		if part == nil {
			return nil, a2a.NewError(a2a.ErrInvalidParams, "message part is invalid")
		}
		switch content := part.Content.(type) {
		case a2a.Text:
			text := strings.TrimSpace(string(content))
			if text != "" {
				texts = append(texts, text)
			}
		case a2a.Data:
			dataParts++
			data = content.Value
		default:
			return nil, a2a.NewError(a2a.ErrUnsupportedContentType, "only text and structured JSON parts are supported")
		}
	}
	if len(texts) > 0 && dataParts > 0 || dataParts > 1 {
		return nil, a2a.NewError(a2a.ErrUnsupportedContentType, "mixed or multiple structured parts are not supported")
	}
	var payload any
	switch {
	case len(texts) > 0:
		payload = map[string]any{"messages": []map[string]string{{"role": "user", "content": strings.Join(texts, "\n")}}}
	case dataParts == 1:
		payload = data
	default:
		return nil, a2a.NewError(a2a.ErrInvalidParams, "message content is empty")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, a2a.NewError(a2a.ErrInvalidParams, "message content is invalid")
	}
	return encoded, nil
}

func taskFromSnapshot(snapshot *services.DelegationSnapshot, historyLength *int) (*a2a.Task, error) {
	if snapshot == nil {
		return nil, a2a.NewError(a2a.ErrInternalError, "task snapshot is empty")
	}
	state, err := taskState(snapshot.Run.Status)
	if err != nil {
		return nil, err
	}
	history := make([]*a2a.Message, 0, len(snapshot.Messages))
	for _, message := range snapshot.Messages {
		if message.MessageType == models.MessageTypeResult && snapshot.Run.Status != models.RunStatusSuccess {
			continue
		}
		mapped, mapErr := messageFromModel(message, snapshot.Run.RunID, snapshot.Run.ThreadID)
		if mapErr != nil {
			return nil, mapErr
		}
		history = append(history, mapped)
	}
	history = trimHistory(history, historyLength)
	updatedAt := snapshot.Run.UpdatedAt
	task := &a2a.Task{
		ID:        a2a.TaskID(snapshot.Run.RunID),
		ContextID: snapshot.Run.ThreadID,
		Status: a2a.TaskStatus{
			State:     state,
			Timestamp: &updatedAt,
		},
		History: history,
		Metadata: map[string]any{DelegationExtensionURI: map[string]any{
			"delegationId":    snapshot.Delegation.DelegationID,
			"parentRunId":     snapshot.Delegation.ParentRunID,
			"capabilityCode":  snapshot.Delegation.CapabilityCode,
			"sourceAgentCode": snapshot.SourceAgent.AgentCode,
			"targetAgentCode": snapshot.TargetAgent.AgentCode,
		}},
	}
	if state == a2a.TaskStateCompleted {
		artifact, artifactErr := resultArtifact(snapshot)
		if artifactErr != nil {
			return nil, artifactErr
		}
		if artifact != nil {
			task.Artifacts = []*a2a.Artifact{artifact}
		}
	}
	if state == a2a.TaskStateFailed || state == a2a.TaskStateCanceled {
		messageText := "target agent execution failed"
		if state == a2a.TaskStateCanceled {
			messageText = "target agent execution was canceled"
		}
		task.Status.Message = a2a.NewMessageForTask(a2a.MessageRoleAgent, task, a2a.NewTextPart(messageText))
		task.Status.Message.ID = stableID("status", snapshot.Run.RunID)
	}
	return task, nil
}

func taskState(status string) (a2a.TaskState, error) {
	switch status {
	case models.RunStatusPending, models.RunStatusQueued:
		return a2a.TaskStateSubmitted, nil
	case models.RunStatusRunning:
		return a2a.TaskStateWorking, nil
	case models.RunStatusSuccess:
		return a2a.TaskStateCompleted, nil
	case models.RunStatusFailed:
		return a2a.TaskStateFailed, nil
	case models.RunStatusCancelled:
		return a2a.TaskStateCanceled, nil
	default:
		return a2a.TaskStateUnspecified, a2a.NewError(a2a.ErrInternalError, "task has an invalid runtime state")
	}
}

func messageFromModel(message models.Message, runID, threadID string) (*a2a.Message, error) {
	role := a2a.MessageRoleUser
	if message.SenderType == models.MessageSenderAgent && message.MessageType == models.MessageTypeResult {
		role = a2a.MessageRoleAgent
	}
	parts, err := partsFromJSON(message.ContentJSON)
	if err != nil {
		return nil, err
	}
	metadata := map[string]any{}
	if strings.TrimSpace(message.MetadataJSON) != "" {
		if err := json.Unmarshal([]byte(message.MetadataJSON), &metadata); err != nil {
			return nil, a2a.NewError(a2a.ErrInternalError, "stored message metadata is invalid")
		}
	}
	return &a2a.Message{
		ID:        message.MessageID,
		ContextID: threadID,
		TaskID:    a2a.TaskID(runID),
		Role:      role,
		Parts:     parts,
		Metadata:  metadata,
	}, nil
}

func partsFromJSON(contentJSON string) (a2a.ContentParts, error) {
	var value any
	if err := json.Unmarshal([]byte(contentJSON), &value); err != nil {
		return nil, a2a.NewError(a2a.ErrInternalError, "stored message content is invalid")
	}
	if text := extractText(value); text != "" {
		return a2a.ContentParts{a2a.NewTextPart(text)}, nil
	}
	return a2a.ContentParts{a2a.NewDataPart(value)}, nil
}

func extractText(value any) string {
	object, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	if text, ok := object["text"].(string); ok {
		return text
	}
	if text, ok := object["message"].(string); ok {
		return text
	}
	messages, ok := object["messages"].([]any)
	if !ok || len(messages) == 0 {
		return ""
	}
	last, ok := messages[len(messages)-1].(map[string]any)
	if !ok {
		return ""
	}
	text, _ := last["content"].(string)
	return text
}

func resultArtifact(snapshot *services.DelegationSnapshot) (*a2a.Artifact, error) {
	if strings.TrimSpace(snapshot.Delegation.ResultMessageID) == "" {
		return nil, nil
	}
	for _, message := range snapshot.Messages {
		if message.MessageID != snapshot.Delegation.ResultMessageID {
			continue
		}
		parts, err := partsFromJSON(message.ContentJSON)
		if err != nil {
			return nil, err
		}
		return &a2a.Artifact{
			ID:          a2a.ArtifactID(stableID("artifact", snapshot.Run.RunID)),
			Name:        "delegation-result",
			Description: "Result produced by the target Agent",
			Parts:       parts,
		}, nil
	}
	return nil, nil
}

func trimHistory(history []*a2a.Message, historyLength *int) []*a2a.Message {
	if historyLength == nil || *historyLength < 0 || *historyLength >= len(history) {
		return history
	}
	if *historyLength == 0 {
		return nil
	}
	return history[len(history)-*historyLength:]
}

func stableID(prefix, seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(sum[:24]))
}
