package services

import "testing"

func TestResolveExecutionOrder(t *testing.T) {
	raw := `{
		"entry_node":"a",
		"nodes":[
			{"key":"a","type":"planner"},
			{"key":"b","type":"noop"},
			{"key":"c","type":"llm"}
		],
		"edges":[
			{"from":"a","to":"b"},
			{"from":"b","to":"c"}
		]
	}`
	def, err := ParseAndValidateWorkflowDefinition(raw)
	if err != nil {
		t.Fatalf("parse definition failed: %v", err)
	}
	order, err := ResolveExecutionOrder(def)
	if err != nil {
		t.Fatalf("resolve order failed: %v", err)
	}
	if len(order) != 3 {
		t.Fatalf("expected 3 nodes in order, got %d", len(order))
	}
	if order[0].Key != "a" || order[1].Key != "b" || order[2].Key != "c" {
		t.Fatalf("unexpected order: %+v", order)
	}
}

func TestValidateWorkflowDefinitionDetectsMissingEntry(t *testing.T) {
	raw := `{"entry_node":"x","nodes":[{"key":"a","type":"planner"}],"edges":[]}`
	if _, err := ParseAndValidateWorkflowDefinition(raw); err == nil {
		t.Fatal("expected validation error but got nil")
	}
}
