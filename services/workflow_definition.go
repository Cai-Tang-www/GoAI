package services

import "GoAI/domain/workflow"

type WorkflowDefinition = workflow.Definition
type WorkflowNode = workflow.Node
type WorkflowEdge = workflow.Edge

func ParseAndValidateWorkflowDefinition(raw string) (*WorkflowDefinition, error) {
	return workflow.ParseAndValidate(raw)
}

func ValidateWorkflowDefinition(def *WorkflowDefinition) error {
	return workflow.Validate(def)
}

func ResolveExecutionOrder(def *WorkflowDefinition) ([]WorkflowNode, error) {
	return workflow.ResolveExecutionOrder(def)
}
