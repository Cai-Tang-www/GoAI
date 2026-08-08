package services

import "GoAI/domain/workflow"

type WorkflowDefinition = workflow.Definition
type WorkflowNode = workflow.Node
type WorkflowEdge = workflow.Edge
type AgentNodeConfig = workflow.AgentNodeConfig
type ToolNodeConfig = workflow.ToolNodeConfig
type InterruptNodeConfig = workflow.InterruptNodeConfig
type AgentGroupNodeConfig = workflow.AgentGroupNodeConfig
type AgentGroupMember = workflow.AgentGroupMember

// ParseAndValidateWorkflowDefinition 解析并校验 Workflow 定义。
func ParseAndValidateWorkflowDefinition(raw string) (*WorkflowDefinition, error) {
	return workflow.ParseAndValidate(raw)
}

// ParseAgentNodeConfig 解析 Workflow agent 节点配置。
func ParseAgentNodeConfig(node WorkflowNode) (*AgentNodeConfig, error) {
	return workflow.ParseAgentNodeConfig(node)
}

// ParseToolNodeConfig 解析 Workflow tool 节点配置。
func ParseToolNodeConfig(node WorkflowNode) (*ToolNodeConfig, error) {
	return workflow.ParseToolNodeConfig(node)
}

// ParseInterruptNodeConfig 解析 Workflow 的人工输入暂停节点。
func ParseInterruptNodeConfig(node WorkflowNode) (*InterruptNodeConfig, error) {
	return workflow.ParseInterruptNodeConfig(node)
}

// ParseAgentGroupNodeConfig 解析 Workflow agent_group 节点配置。
func ParseAgentGroupNodeConfig(node WorkflowNode) (*AgentGroupNodeConfig, error) {
	return workflow.ParseAgentGroupNodeConfig(node)
}

// ValidateWorkflowDefinition 校验 Workflow 定义。
func ValidateWorkflowDefinition(def *WorkflowDefinition) error {
	return workflow.Validate(def)
}

// ResolveExecutionOrder 返回 Workflow 的拓扑执行顺序。
func ResolveExecutionOrder(def *WorkflowDefinition) ([]WorkflowNode, error) {
	return workflow.ResolveExecutionOrder(def)
}
