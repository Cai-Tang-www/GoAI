package a2agateway

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"strconv"
	"strings"

	"GoAI/a2aauth"
	"GoAI/requestctx"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

type requestHandler struct {
	runtime      services.DelegationRuntime
	authRequired bool
	logger       *slog.Logger
}

func (h *requestHandler) GetTask(ctx context.Context, request *a2a.GetTaskRequest) (*a2a.Task, error) {
	target, err := targetAgentFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if request == nil || request.ID == "" {
		return nil, a2a.NewError(a2a.ErrInvalidParams, "task id is required")
	}
	source, err := h.authenticatedSource(ctx)
	if err != nil {
		return nil, err
	}
	snapshot, err := h.runtime.DelegationSnapshot(ctx, target, source, string(request.ID))
	if err != nil {
		if errors.Is(err, services.ErrDelegationForbidden()) {
			h.logAuthorizationRejection(ctx, target, source, "delegation_source_forbidden")
		}
		return nil, mapRuntimeError(err)
	}
	return taskFromSnapshot(snapshot, request.HistoryLength)
}

func (h *requestHandler) ListTasks(context.Context, *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	return nil, a2a.ErrUnsupportedOperation
}

func (h *requestHandler) CancelTask(context.Context, *a2a.CancelTaskRequest) (*a2a.Task, error) {
	return nil, a2a.ErrTaskNotCancelable
}

func (h *requestHandler) SendMessage(ctx context.Context, request *a2a.SendMessageRequest) (a2a.SendMessageResult, error) {
	target, err := targetAgentFromContext(ctx)
	if err != nil {
		return nil, err
	}
	command, err := commandFromRequest(target, request)
	if err != nil {
		return nil, err
	}
	source, err := h.authenticatedSource(ctx)
	if err != nil {
		return nil, err
	}
	if h.authRequired && source != command.SourceAgentCode {
		h.logAuthorizationRejection(ctx, target, source, "source_metadata_mismatch")
		return nil, a2a.ErrUnauthorized
	}
	result, err := h.runtime.AcceptDelegation(ctx, command)
	if err != nil {
		return nil, mapRuntimeError(err)
	}
	snapshot, err := h.runtime.DelegationSnapshot(ctx, target, source, result.Run.RunID)
	if err != nil {
		return nil, mapRuntimeError(err)
	}
	return taskFromSnapshot(snapshot, nil)
}

func (h *requestHandler) SubscribeToTask(context.Context, *a2a.SubscribeToTaskRequest) iter.Seq2[a2a.Event, error] {
	return unsupportedEventSequence()
}

func (h *requestHandler) SendStreamingMessage(context.Context, *a2a.SendMessageRequest) iter.Seq2[a2a.Event, error] {
	return unsupportedEventSequence()
}

func (h *requestHandler) GetTaskPushConfig(ctx context.Context, request *a2a.GetTaskPushConfigRequest) (*a2a.PushConfig, error) {
	target, source, err := h.pushConfigAgents(ctx)
	if err != nil {
		return nil, err
	}
	if request == nil || strings.TrimSpace(string(request.TaskID)) == "" || strings.TrimSpace(request.ID) == "" {
		return nil, a2a.NewError(a2a.ErrInvalidParams, "taskId and id are required")
	}
	config, err := h.runtime.GetDelegationPushConfig(ctx, target, source, string(request.TaskID), request.ID)
	if err != nil {
		return nil, mapRuntimeError(err)
	}
	return pushConfigToProtocol(config), nil
}

func (h *requestHandler) ListTaskPushConfigs(ctx context.Context, request *a2a.ListTaskPushConfigRequest) (*a2a.ListTaskPushConfigResponse, error) {
	target, source, err := h.pushConfigAgents(ctx)
	if err != nil {
		return nil, err
	}
	if request == nil || strings.TrimSpace(string(request.TaskID)) == "" || request.PageSize < 0 {
		return nil, a2a.NewError(a2a.ErrInvalidParams, "taskId is required and pageSize must not be negative")
	}
	configs, err := h.runtime.ListDelegationPushConfigs(ctx, target, source, string(request.TaskID))
	if err != nil {
		return nil, mapRuntimeError(err)
	}
	start := 0
	if token := strings.TrimSpace(request.PageToken); token != "" {
		start, err = strconv.Atoi(token)
		if err != nil || start < 0 || start > len(configs) {
			return nil, a2a.NewError(a2a.ErrInvalidParams, "pageToken is invalid")
		}
	}
	end := len(configs)
	if request.PageSize > 0 && start+request.PageSize < end {
		end = start + request.PageSize
	}
	result := &a2a.ListTaskPushConfigResponse{Configs: make([]*a2a.PushConfig, 0, end-start)}
	for i := start; i < end; i++ {
		result.Configs = append(result.Configs, pushConfigToProtocol(&configs[i]))
	}
	if end < len(configs) {
		result.NextPageToken = strconv.Itoa(end)
	}
	return result, nil
}

func (h *requestHandler) CreateTaskPushConfig(ctx context.Context, request *a2a.PushConfig) (*a2a.PushConfig, error) {
	target, source, err := h.pushConfigAgents(ctx)
	if err != nil {
		return nil, err
	}
	if request == nil || strings.TrimSpace(string(request.TaskID)) == "" || strings.TrimSpace(request.URL) == "" {
		return nil, a2a.NewError(a2a.ErrInvalidParams, "taskId and url are required")
	}
	if request.Auth != nil {
		return nil, a2a.NewError(a2a.ErrInvalidParams, "push config authentication is managed by the GoAI A2A machine identity")
	}
	config, err := h.runtime.CreateDelegationPushConfig(ctx, target, source, services.DelegationPushConfig{
		ConfigID:    request.ID,
		TaskID:      string(request.TaskID),
		CallbackURL: request.URL,
		Token:       request.Token,
	})
	if err != nil {
		return nil, mapRuntimeError(err)
	}
	return pushConfigToProtocol(config), nil
}

func (h *requestHandler) DeleteTaskPushConfig(ctx context.Context, request *a2a.DeleteTaskPushConfigRequest) error {
	target, source, err := h.pushConfigAgents(ctx)
	if err != nil {
		return err
	}
	if request == nil || strings.TrimSpace(string(request.TaskID)) == "" || strings.TrimSpace(request.ID) == "" {
		return a2a.NewError(a2a.ErrInvalidParams, "taskId and id are required")
	}
	return mapRuntimeError(h.runtime.DeleteDelegationPushConfig(ctx, target, source, string(request.TaskID), request.ID))
}

func (h *requestHandler) GetExtendedAgentCard(ctx context.Context, _ *a2a.GetExtendedAgentCardRequest) (*a2a.AgentCard, error) {
	target, err := targetAgentFromContext(ctx)
	if err != nil {
		return nil, err
	}
	descriptor, err := h.runtime.DescribeAgent(ctx, target)
	if err != nil {
		return nil, mapRuntimeError(err)
	}
	card, err := buildAgentCardWithAuthentication(descriptor, h.authRequired)
	if err != nil {
		return nil, a2a.NewError(a2a.ErrInternalError, "agent card is unavailable")
	}
	return card, nil
}

func (h *requestHandler) pushConfigAgents(ctx context.Context) (string, string, error) {
	target, err := targetAgentFromContext(ctx)
	if err != nil {
		return "", "", err
	}
	source, err := h.authenticatedSource(ctx)
	if err != nil {
		return "", "", err
	}
	return target, source, nil
}

func pushConfigToProtocol(config *services.DelegationPushConfig) *a2a.PushConfig {
	if config == nil {
		return nil
	}
	return &a2a.PushConfig{
		TaskID: a2a.TaskID(config.TaskID),
		ID:     config.ConfigID,
		URL:    config.CallbackURL,
		Token:  config.Token,
	}
}

func (h *requestHandler) logAuthorizationRejection(ctx context.Context, targetAgent, sourceAgent, reason string) {
	if h == nil || h.logger == nil {
		return
	}
	h.logger.WarnContext(ctx, "A2A security request rejected",
		slog.String("trace_id", requestctx.TraceIDFromContext(ctx)),
		slog.String("target_agent", safeAuditValue(targetAgent)),
		slog.String("source_agent", safeAuditValue(sourceAgent)),
		slog.String("reason", reason),
	)
}
func (h *requestHandler) authenticatedSource(ctx context.Context) (string, error) {
	if !h.authRequired {
		return "", nil
	}
	source, ok := a2aauth.AuthenticatedAgentFromContext(ctx)
	if !ok {
		return "", a2a.ErrUnauthorized
	}
	return source, nil
}

func unsupportedEventSequence() iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(nil, a2a.ErrUnsupportedOperation)
	}
}

func mapRuntimeError(err error) error {
	switch {
	case errors.Is(err, services.ErrDelegationForbidden()):
		return a2a.ErrUnauthorized
	case errors.Is(err, services.ErrDelegationNotFound()), errors.Is(err, services.ErrRunNotFound()), errors.Is(err, services.ErrPushConfigNotFound()):
		return a2a.NewError(a2a.ErrTaskNotFound, "task not found")
	case errors.Is(err, services.ErrAgentNotFound()):
		return a2a.NewError(a2a.ErrInvalidParams, "source or target agent not found")
	case errors.Is(err, services.ErrCapabilityNotFound()):
		return a2a.NewError(a2a.ErrInvalidParams, "target agent capability not found")
	case errors.Is(err, services.ErrInvalidDelegation()):
		return a2a.NewError(a2a.ErrInvalidParams, "delegation request is invalid")
	case errors.Is(err, services.ErrDelegationConflict()), errors.Is(err, services.ErrRunAlreadyExists()), errors.Is(err, services.ErrMessageConflict()):
		return a2a.NewError(a2a.ErrInvalidRequest, "task or message identifier conflicts with an existing delegation")
	default:
		return a2a.NewError(a2a.ErrInternalError, "A2A runtime request failed")
	}
}

var _ interface {
	GetTask(context.Context, *a2a.GetTaskRequest) (*a2a.Task, error)
} = (*requestHandler)(nil)
