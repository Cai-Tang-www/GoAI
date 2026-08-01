package services

import "GoAI/domain/workflow"

type WorkflowDefinition = workflow.Definition
type WorkflowNode = workflow.Node
type WorkflowEdge = workflow.Edge
type AgentNodeConfig = workflow.AgentNodeConfig

// ParseAndValidateWorkflowDefinition 解析并校验 Workflow 定义。
func ParseAndValidateWorkflowDefinition(raw string) (*WorkflowDefinition, error) {
	return workflow.ParseAndValidate(raw)
}

// ParseAgentNodeConfig 解析 Workflow agent 节点配置。
func ParseAgentNodeConfig(node WorkflowNode) (*AgentNodeConfig, error) {
	return workflow.ParseAgentNodeConfig(node)
}

// ValidateWorkflowDefinition 校验 Workflow 定义。
func ValidateWorkflowDefinition(def *WorkflowDefinition) error {
	return workflow.Validate(def)
}

// ResolveExecutionOrder 返回 Workflow 的拓扑执行顺序。
func ResolveExecutionOrder(def *WorkflowDefinition) ([]WorkflowNode, error) {
	return workflow.ResolveExecutionOrder(def)
}
