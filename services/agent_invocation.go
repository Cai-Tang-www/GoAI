package services

import (
	"context"
	"errors"
	"strings"

	"GoAI/einoexecutor"
	"GoAI/observability"
)

// AgentInvocationEndpoint 描述目标 Agent 可供出站 A2A 客户端访问的协议入口。
type AgentInvocationEndpoint struct {
	Address   string
	Transport string
}

// AgentInvocationRequest 描述 Workflow agent 节点发起的一次跨 Agent 委派。
type AgentInvocationRequest struct {
	SourceAgentCode string
	TargetAgentCode string
	CapabilityCode  string
	ParentRunID     string
	TraceID         string
	DelegationID    string
	ThreadID        string
	TaskID          string
	MessageID       string
	InputJSON       string
	Endpoints       []AgentInvocationEndpoint
}

// AgentInvocationState 表示跨 Agent 调用映射到 Runtime 后的协议无关终态。
type AgentInvocationState string

const (
	// AgentInvocationStateCompleted 表示目标 Agent 已成功完成委派。
	AgentInvocationStateCompleted AgentInvocationState = "completed"
)

// AgentInvocationResult 是 A2A Task 终态映射到平台内部的协议无关结果。
type AgentInvocationResult struct {
	TaskID     string
	State      AgentInvocationState
	OutputJSON string
	Message    string
}

// AgentInvoker 是 RunService 消费的跨 Agent 调用边界，实现必须通过 A2A 协议访问目标 Agent。
type AgentInvoker interface {
	Invoke(context.Context, AgentInvocationRequest) (*AgentInvocationResult, error)
}

// RunServiceOption 为 RunService 装配可选运行时依赖。
type RunServiceOption func(*RunService) error

// WithChatService 注入 RunService 使用的 LLM ChatService，避免执行链路依赖包级 Provider 状态。
func WithChatService(chatService *ChatService) RunServiceOption {
	return func(service *RunService) error {
		if chatService == nil {
			return errors.New("configuring run service: chat service is nil")
		}
		service.chatService = chatService
		return nil
	}
}

// WithAgentInvoker 注入通过 A2A 协议执行 Workflow agent 节点的客户端。
func WithAgentInvoker(invoker AgentInvoker) RunServiceOption {
	return func(service *RunService) error {
		if invoker == nil {
			return errors.New("configuring run service: agent invoker is nil")
		}
		service.agentInvoker = invoker
		return nil
	}
}

// WithGraphExecutor 注入单个 Agent 的 Eino Graph 执行器。
func WithGraphExecutor(executor *einoexecutor.Executor) RunServiceOption {
	return func(service *RunService) error {
		if executor == nil {
			return errors.New("configuring run service: graph executor is nil")
		}
		service.graphExecutor = executor
		return nil
	}
}

// WithRunObservability 注入 Run 执行的日志、指标和 Trace 能力。
func WithRunObservability(bundle *observability.Bundle) RunServiceOption {
	return func(service *RunService) error {
		if bundle == nil {
			return errors.New("configuring run service: observability bundle is nil")
		}
		service.observability = bundle
		return nil
	}
}

type retryableInvocationError interface {
	Retryable() bool
}

func isRetryableInvocationError(err error) bool {
	var retryable retryableInvocationError
	return !errors.As(err, &retryable) || retryable.Retryable()
}

func normalizeInvocationResult(result *AgentInvocationResult) *AgentInvocationResult {
	if result == nil {
		return nil
	}
	result.TaskID = strings.TrimSpace(result.TaskID)
	result.State = AgentInvocationState(strings.TrimSpace(string(result.State)))
	result.OutputJSON = strings.TrimSpace(result.OutputJSON)
	result.Message = strings.TrimSpace(result.Message)
	if result.OutputJSON == "" {
		result.OutputJSON = "{}"
	}
	return result
}
