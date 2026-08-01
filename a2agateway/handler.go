package a2agateway

import (
	"context"
	"errors"
	"iter"

	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

type requestHandler struct {
	runtime services.DelegationRuntime
}

func (h *requestHandler) GetTask(ctx context.Context, request *a2a.GetTaskRequest) (*a2a.Task, error) {
	target, err := targetAgentFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if request == nil || request.ID == "" {
		return nil, a2a.NewError(a2a.ErrInvalidParams, "task id is required")
	}
	snapshot, err := h.runtime.DelegationSnapshot(ctx, target, string(request.ID))
	if err != nil {
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
	result, err := h.runtime.AcceptDelegation(ctx, command)
	if err != nil {
		return nil, mapRuntimeError(err)
	}
	snapshot, err := h.runtime.DelegationSnapshot(ctx, target, result.Run.RunID)
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

func (h *requestHandler) GetTaskPushConfig(context.Context, *a2a.GetTaskPushConfigRequest) (*a2a.PushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (h *requestHandler) ListTaskPushConfigs(context.Context, *a2a.ListTaskPushConfigRequest) (*a2a.ListTaskPushConfigResponse, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (h *requestHandler) CreateTaskPushConfig(context.Context, *a2a.PushConfig) (*a2a.PushConfig, error) {
	return nil, a2a.ErrPushNotificationNotSupported
}

func (h *requestHandler) DeleteTaskPushConfig(context.Context, *a2a.DeleteTaskPushConfigRequest) error {
	return a2a.ErrPushNotificationNotSupported
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
	card, err := buildAgentCard(descriptor)
	if err != nil {
		return nil, a2a.NewError(a2a.ErrInternalError, "agent card is unavailable")
	}
	return card, nil
}

func unsupportedEventSequence() iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		yield(nil, a2a.ErrUnsupportedOperation)
	}
}

func mapRuntimeError(err error) error {
	switch {
	case errors.Is(err, services.ErrDelegationNotFound()), errors.Is(err, services.ErrRunNotFound()):
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
