package services

import (
	"context"
	"errors"
	"strings"
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
	ThreadID        string
	TaskID          string
	MessageID       string
	InputJSON       string
	Endpoints       []AgentInvocationEndpoint
}

// AgentInvocationResult 是 A2A Task 终态映射到平台内部的协议无关结果。
type AgentInvocationResult struct {
	TaskID     string
	State      string
	OutputJSON string
	Message    string
}

// AgentInvoker 是 RunService 消费的跨 Agent 调用边界，实现必须通过 A2A 协议访问目标 Agent。
type AgentInvoker interface {
	Invoke(context.Context, AgentInvocationRequest) (*AgentInvocationResult, error)
}

// RunServiceOption 为 RunService 装配可选运行时依赖。
type RunServiceOption func(*RunService) error

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
	result.State = strings.TrimSpace(result.State)
	result.OutputJSON = strings.TrimSpace(result.OutputJSON)
	result.Message = strings.TrimSpace(result.Message)
	if result.OutputJSON == "" {
		result.OutputJSON = "{}"
	}
	return result
}
