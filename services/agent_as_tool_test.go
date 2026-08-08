package services

import (
	"context"
	"errors"
	"strings"
	"testing"

	einoTool "github.com/cloudwego/eino/components/tool"
)

type agentAsToolInvoker struct {
	request AgentInvocationRequest
	result  *AgentInvocationResult
}

func (i *agentAsToolInvoker) Invoke(_ context.Context, request AgentInvocationRequest) (*AgentInvocationResult, error) {
	i.request = request
	return i.result, nil
}

func TestAgentAsToolImplementsEinoToolAndValidatesContracts(t *testing.T) {
	invoker := &agentAsToolInvoker{result: &AgentInvocationResult{
		TaskID: "task-1", State: AgentInvocationStateCompleted, OutputJSON: `{"answer":"done"}`,
	}}
	tool, err := NewAgentAsTool(AgentAsToolConfig{
		ToolName: "writer_tool", Description: "Write a document",
		InputSchemaJSON:  `{"type":"object","required":["prompt"],"properties":{"prompt":{"type":"string"}},"additionalProperties":false}`,
		OutputSchemaJSON: `{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`,
		Invoker:          invoker, Invocation: AgentInvocationRequest{
			SourceAgentCode: "planner", TargetAgentCode: "writer", CapabilityCode: "write", ParentRunID: "run-1",
			DelegationID: "delegation-1", MessageID: "message-1", TaskID: "task-1",
		}, SourceAgentID: 1, TargetAgentID: 2,
	})
	if err != nil {
		t.Fatalf("create AgentAsTool: %v", err)
	}
	var _ einoTool.InvokableTool = tool

	info, err := tool.Info(context.Background())
	if err != nil {
		t.Fatalf("read tool info: %v", err)
	}
	if info.Name != "writer_tool" || info.ParamsOneOf == nil {
		t.Fatalf("unexpected tool info: %+v", info)
	}

	output, err := tool.InvokableRun(context.Background(), `{"prompt":"draft"}`)
	if err != nil {
		t.Fatalf("invoke AgentAsTool: %v", err)
	}
	if !strings.Contains(output, `"type":"agent_tool"`) || !strings.Contains(output, `"answer":"done"`) {
		t.Fatalf("unexpected tool output: %s", output)
	}
	if invoker.request.InputJSON != `{"prompt":"draft"}` {
		t.Fatalf("tool did not pass normalized input to A2A invoker: %s", invoker.request.InputJSON)
	}

	if _, err := tool.InvokableRun(context.Background(), `{}`); err == nil || !strings.Contains(err.Error(), "prompt") {
		t.Fatalf("expected input contract error, got %v", err)
	}
}

func TestAgentAsToolAcceptedResultSuspendsThroughExistingRuntimeError(t *testing.T) {
	invoker := &agentAsToolInvoker{result: &AgentInvocationResult{
		TaskID: "task-accepted", State: AgentInvocationStateAccepted, OutputJSON: `{}`, NotificationToken: "token",
	}}
	tool, err := NewAgentAsTool(AgentAsToolConfig{
		ToolName: "writer_tool", InputSchemaJSON: `{}`, Invoker: invoker,
		Invocation: AgentInvocationRequest{
			SourceAgentCode: "planner", TargetAgentCode: "writer", CapabilityCode: "write", ParentRunID: "run-1",
			DelegationID: "delegation-1", MessageID: "message-1", TaskID: "task-accepted",
		}, SourceAgentID: 1, TargetAgentID: 2,
	})
	if err != nil {
		t.Fatalf("create AgentAsTool: %v", err)
	}
	_, err = tool.InvokableRun(context.Background(), `{}`)
	var accepted *agentInvocationAcceptedError
	if !errors.As(err, &accepted) {
		t.Fatalf("expected accepted runtime error, got %v", err)
	}
	if accepted.TaskID != "task-accepted" || accepted.DelegationID != "delegation-1" || accepted.SourceAgentID != 1 || accepted.TargetAgentID != 2 {
		t.Fatalf("unexpected accepted error: %+v", accepted)
	}
}

func TestAgentAsToolRejectsInvalidOutputContract(t *testing.T) {
	tool, err := NewAgentAsTool(AgentAsToolConfig{
		ToolName: "writer_tool", InputSchemaJSON: `{}`, OutputSchemaJSON: `{"type":"object","required":["answer"]}`,
		Invoker:    &agentAsToolInvoker{result: &AgentInvocationResult{TaskID: "task", State: AgentInvocationStateCompleted, OutputJSON: `{}`}},
		Invocation: AgentInvocationRequest{TargetAgentCode: "writer", CapabilityCode: "write"},
	})
	if err != nil {
		t.Fatalf("create AgentAsTool: %v", err)
	}
	if _, err := tool.InvokableRun(context.Background(), `{}`); err == nil || !strings.Contains(err.Error(), "answer") {
		t.Fatalf("expected output contract error, got %v", err)
	}
}
