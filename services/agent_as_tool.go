package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	einoTool "github.com/cloudwego/eino/components/tool"
	einoschema "github.com/cloudwego/eino/schema"
	"github.com/eino-contrib/jsonschema"
)

// AgentAsToolConfig 描述一个通过 A2A 委派目标 Agent 的 Eino Tool。
// Invocation 只保存协议请求模板，InvokableRun 会用模型传入的 JSON 参数覆盖 InputJSON。
type AgentAsToolConfig struct {
	ToolName         string
	Description      string
	InputSchemaJSON  string
	OutputSchemaJSON string
	Invoker          AgentInvoker
	Invocation       AgentInvocationRequest
	SourceAgentID    uint64
	TargetAgentID    uint64
	SelectionReason  string
	RoutingPolicy    string
	WorkflowVersion  int
	Timeout          time.Duration
	ResultType       string
}

// AgentAsTool 将 Registry 中的 Agent Capability 暴露为 Eino 可调用 Tool。
// 它不持有目标 Agent 的 Service、Executor 或 Provider，所有业务调用都经过 A2A Client。
type AgentAsTool struct {
	toolName         string
	description      string
	inputSchemaJSON  string
	outputSchemaJSON string
	invoker          AgentInvoker
	invocation       AgentInvocationRequest
	sourceAgentID    uint64
	targetAgentID    uint64
	selectionReason  string
	routingPolicy    string
	workflowVersion  int
	timeout          time.Duration
	resultType       string
}

var _ einoTool.InvokableTool = (*AgentAsTool)(nil)

// NewAgentAsTool 创建一个由 Runtime 管理生命周期的 AgentAsTool。
func NewAgentAsTool(config AgentAsToolConfig) (*AgentAsTool, error) {
	if config.Invoker == nil {
		return nil, errors.New("creating AgentAsTool: A2A agent invoker is nil")
	}
	config.ToolName = strings.TrimSpace(config.ToolName)
	if config.ToolName == "" {
		return nil, errors.New("creating AgentAsTool: tool name is required")
	}
	if len(config.ToolName) > 64 {
		return nil, errors.New("creating AgentAsTool: tool name must be at most 64 characters")
	}
	if config.Timeout < 0 || config.Timeout > 300*time.Second {
		return nil, errors.New("creating AgentAsTool: timeout must be between 0 and 300 seconds")
	}
	if _, err := parseAgentToolSchema(config.InputSchemaJSON); err != nil {
		return nil, fmt.Errorf("creating AgentAsTool: input schema is invalid: %w", err)
	}
	if _, err := parseAgentToolSchema(config.OutputSchemaJSON); err != nil {
		return nil, fmt.Errorf("creating AgentAsTool: output schema is invalid: %w", err)
	}
	config.Description = strings.TrimSpace(config.Description)
	if config.Description == "" {
		config.Description = fmt.Sprintf("Invoke Agent capability %s", config.Invocation.CapabilityCode)
	}
	config.ResultType = strings.TrimSpace(config.ResultType)
	if config.ResultType == "" {
		config.ResultType = "agent_tool"
	}
	return &AgentAsTool{
		toolName:         config.ToolName,
		description:      config.Description,
		inputSchemaJSON:  strings.TrimSpace(config.InputSchemaJSON),
		outputSchemaJSON: strings.TrimSpace(config.OutputSchemaJSON),
		invoker:          config.Invoker,
		invocation:       config.Invocation,
		sourceAgentID:    config.SourceAgentID,
		targetAgentID:    config.TargetAgentID,
		selectionReason:  strings.TrimSpace(config.SelectionReason),
		routingPolicy:    strings.TrimSpace(config.RoutingPolicy),
		workflowVersion:  config.WorkflowVersion,
		timeout:          config.Timeout,
		resultType:       config.ResultType,
	}, nil
}

// Info 返回目标 Capability 的 Tool 名称、描述和输入 JSON Schema。
func (t *AgentAsTool) Info(context.Context) (*einoschema.ToolInfo, error) {
	if t == nil {
		return nil, errors.New("reading AgentAsTool info: tool is nil")
	}
	info := &einoschema.ToolInfo{Name: t.toolName, Desc: t.description}
	if strings.TrimSpace(t.inputSchemaJSON) == "" {
		return info, nil
	}
	parsed, err := parseAgentToolSchema(t.inputSchemaJSON)
	if err != nil {
		return nil, fmt.Errorf("reading AgentAsTool info: %w", err)
	}
	info.ParamsOneOf = einoschema.NewParamsOneOfByJSONSchema(parsed)
	return info, nil
}

// InvokableRun 接收 JSON 参数并通过 A2A 调用目标 Agent。
func (t *AgentAsTool) InvokableRun(ctx context.Context, argumentsInJSON string, _ ...einoTool.Option) (string, error) {
	if t == nil {
		return "", errors.New("invoking AgentAsTool: tool is nil")
	}
	if t.invoker == nil {
		return "", errors.New("invoking AgentAsTool: A2A agent invoker is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	inputJSON := strings.TrimSpace(argumentsInJSON)
	if inputJSON == "" {
		inputJSON = "{}"
	}
	if !json.Valid([]byte(inputJSON)) {
		return "", errors.New("invoking AgentAsTool: arguments must be valid JSON")
	}
	normalized, err := canonicalizeJSON(json.RawMessage(inputJSON))
	if err != nil {
		return "", fmt.Errorf("invoking AgentAsTool: normalizing arguments: %w", err)
	}
	if normalized == "" {
		normalized = "{}"
	}
	if err := validateCapabilityInput(t.inputSchemaJSON, normalized); err != nil {
		return "", fmt.Errorf("invoking AgentAsTool: capability input contract: %w", err)
	}

	invokeCtx := ctx
	var cancel context.CancelFunc
	if t.timeout > 0 {
		invokeCtx, cancel = context.WithTimeout(ctx, t.timeout)
		defer cancel()
	}
	request := t.invocation
	request.InputJSON = normalized
	result, err := t.invoker.Invoke(invokeCtx, request)
	if err != nil {
		return "", err
	}
	result = normalizeInvocationResult(result)
	if result == nil {
		return "", errors.New("invoking AgentAsTool: A2A agent invoker returned an empty result")
	}
	if result.State != AgentInvocationStateAccepted && result.State != AgentInvocationStateCompleted {
		return "", fmt.Errorf("invoking AgentAsTool: target agent returned unsupported invocation state %q", result.State)
	}
	outputJSON := strings.TrimSpace(result.OutputJSON)
	if outputJSON == "" {
		outputJSON = "{}"
	}
	if !json.Valid([]byte(outputJSON)) {
		return "", errors.New("invoking AgentAsTool: A2A agent result is not valid JSON")
	}
	if result.State == AgentInvocationStateCompleted {
		if err := validateCapabilityOutput(t.outputSchemaJSON, outputJSON); err != nil {
			return "", fmt.Errorf("invoking AgentAsTool: capability output contract: %w", err)
		}
	}
	taskID := strings.TrimSpace(result.TaskID)
	if taskID == "" {
		taskID = strings.TrimSpace(request.TaskID)
	}
	output := map[string]any{
		"type":             t.resultType,
		"target_agent":     request.TargetAgentCode,
		"capability":       request.CapabilityCode,
		"task_id":          taskID,
		"state":            result.State,
		"result":           json.RawMessage(outputJSON),
		"routing_policy":   t.routingPolicy,
		"selection_reason": t.selectionReason,
		"workflow_version": t.workflowVersion,
	}
	if result.Message != "" {
		output["message"] = result.Message
	}
	encoded, err := json.Marshal(output)
	if err != nil {
		return "", fmt.Errorf("invoking AgentAsTool: encoding result: %w", err)
	}
	if result.State == AgentInvocationStateAccepted {
		return "", &agentInvocationAcceptedError{
			TaskID: taskID, DelegationID: request.DelegationID, MessageID: request.MessageID,
			SourceAgentID: t.sourceAgentID, TargetAgentID: t.targetAgentID,
			CapabilityCode: request.CapabilityCode, OutputJSON: string(encoded),
			CallbackTokenHash: callbackTokenHash(result.NotificationToken),
		}
	}
	return string(encoded), nil
}

func parseAgentToolSchema(raw string) (*jsonschema.Schema, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, err
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, errors.New("schema must be a JSON object")
	}
	var parsed jsonschema.Schema
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, err
	}
	return &parsed, nil
}
