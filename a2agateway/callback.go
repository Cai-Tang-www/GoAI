package a2agateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

const (
	callbackPrefix          = "/callbacks/tasks/"
	notificationTokenHeader = "A2A-Notification-Token"
)

func callbackTaskID(protocolPath string) (string, bool) {
	if !strings.HasPrefix(protocolPath, callbackPrefix) {
		return "", false
	}
	taskID := strings.TrimSpace(strings.TrimPrefix(protocolPath, callbackPrefix))
	return taskID, taskID != "" && !strings.Contains(taskID, "/")
}

func (g *Gateway) serveTaskCallback(ctx context.Context, w http.ResponseWriter, request *http.Request, sourceAgent, targetAgent, taskID string) {
	if request.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if strings.TrimSpace(targetAgent) == "" {
		writeAuthenticationError(w, http.StatusUnauthorized, "A2A callback authentication failed")
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, 1<<20))
	if err != nil {
		http.Error(w, "invalid callback body", http.StatusBadRequest)
		return
	}
	var response a2a.StreamResponse
	if err := json.Unmarshal(body, &response); err != nil {
		http.Error(w, "invalid A2A callback event", http.StatusBadRequest)
		return
	}
	command, err := callbackCommand(sourceAgent, targetAgent, taskID, request.Header.Get(notificationTokenHeader), body, response.Event)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := g.runtime.AcceptDelegationCallback(ctx, command); err != nil {
		switch {
		case errors.Is(err, services.ErrDelegationNotFound()):
			http.Error(w, "A2A task not found", http.StatusNotFound)
		case errors.Is(err, services.ErrDelegationForbidden()):
			http.Error(w, "A2A callback forbidden", http.StatusForbidden)
		default:
			http.Error(w, "A2A callback rejected", http.StatusConflict)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"accepted":true}`))
}

func callbackCommand(sourceAgent, targetAgent, taskID, token string, raw []byte, event a2a.Event) (services.DelegationCallbackCommand, error) {
	command := services.DelegationCallbackCommand{
		SourceAgentCode:   sourceAgent,
		TargetAgentCode:   targetAgent,
		TaskID:            taskID,
		NotificationToken: token,
		EventJSON:         string(raw),
		OutputJSON:        "{}",
	}
	var state a2a.TaskState
	switch value := event.(type) {
	case *a2a.Task:
		if strings.TrimSpace(string(value.ID)) != taskID {
			return command, errors.New("callback task id does not match path")
		}
		state = value.Status.State
		if state == a2a.TaskStateCompleted && len(value.Artifacts) > 0 {
			output, err := inputFromParts(value.Artifacts[0].Parts)
			if err != nil {
				return command, fmt.Errorf("decoding callback artifact: %w", err)
			}
			command.OutputJSON = string(output)
		}
		command.ErrorMessage = callbackStatusMessage(value.Status.Message)
	case *a2a.TaskStatusUpdateEvent:
		if strings.TrimSpace(string(value.TaskID)) != taskID {
			return command, errors.New("callback task id does not match path")
		}
		state = value.Status.State
		command.ErrorMessage = callbackStatusMessage(value.Status.Message)
	default:
		return command, fmt.Errorf("unsupported callback event %T", event)
	}
	if !state.Terminal() {
		return command, errors.New("callback event is not terminal")
	}
	switch state {
	case a2a.TaskStateCompleted:
		command.State = services.DelegationCallbackStateSucceeded
	case a2a.TaskStateCanceled:
		command.State = services.DelegationCallbackStateCancelled
	case a2a.TaskStateFailed, a2a.TaskStateRejected:
		command.State = services.DelegationCallbackStateFailed
	default:
		return command, fmt.Errorf("unsupported terminal task state %s", state)
	}
	return command, nil
}

func callbackStatusMessage(message *a2a.Message) string {
	if message == nil {
		return ""
	}
	parts := make([]string, 0, len(message.Parts))
	for _, part := range message.Parts {
		if part == nil {
			continue
		}
		if text, ok := part.Content.(a2a.Text); ok {
			if value := strings.TrimSpace(string(text)); value != "" {
				parts = append(parts, value)
			}
		}
	}
	return strings.Join(parts, "\n")
}
