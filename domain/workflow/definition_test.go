package workflow

import "testing"

func TestValidateAgentNodeConfig(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{name: "missing config", raw: `{"entry_node":"delegate","nodes":[{"key":"delegate","type":"agent"}]}`, wantErr: "config is required"},
		{name: "missing target", raw: `{"entry_node":"delegate","nodes":[{"key":"delegate","type":"agent","config":{"capability":"write"}}]}`, wantErr: "target_agent is required"},
		{name: "missing capability", raw: `{"entry_node":"delegate","nodes":[{"key":"delegate","type":"agent","config":{"target_agent":"writer"}}]}`, wantErr: "capability is required"},
		{name: "unknown input step", raw: `{"entry_node":"delegate","nodes":[{"key":"delegate","type":"agent","config":{"target_agent":"writer","capability":"write","input_from":["missing"]}}]}`, wantErr: "input_from node not found"},
		{name: "self input step", raw: `{"entry_node":"delegate","nodes":[{"key":"delegate","type":"agent","config":{"target_agent":"writer","capability":"write","input_from":["delegate"]}}]}`, wantErr: "cannot reference itself"},
		{name: "timeout too large", raw: `{"entry_node":"delegate","nodes":[{"key":"delegate","type":"agent","config":{"target_agent":"writer","capability":"write","timeout_ms":300001}}]}`, wantErr: "timeout_ms must be between"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseAndValidate(tt.raw)
			if err == nil || !containsText(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestResolveExecutionOrderSupportsAgentNode(t *testing.T) {
	def, err := ParseAndValidate(`{
		"entry_node":"planner",
		"nodes":[
			{"key":"planner","type":"planner"},
			{"key":"delegate","type":"agent","config":{"target_agent":"writer","capability":"write","input_from":["planner"]}},
			{"key":"finish","type":"tool"}
		],
		"edges":[{"from":"planner","to":"delegate"},{"from":"delegate","to":"finish"}]
	}`)
	if err != nil {
		t.Fatalf("parse definition: %v", err)
	}
	order, err := ResolveExecutionOrder(def)
	if err != nil {
		t.Fatalf("resolve execution order: %v", err)
	}
	if len(order) != 3 || order[0].Key != "planner" || order[1].Key != "delegate" || order[2].Key != "finish" {
		t.Fatalf("unexpected order: %+v", order)
	}
}

func TestResolveExecutionOrderRejectsReachableCycle(t *testing.T) {
	def := &Definition{
		EntryNode: "planner",
		Nodes:     []Node{{Key: "planner", Type: "planner"}, {Key: "delegate", Type: "agent", Config: []byte(`{"target_agent":"writer","capability":"write"}`)}},
		Edges:     []Edge{{From: "planner", To: "delegate"}, {From: "delegate", To: "planner"}},
	}
	if _, err := ResolveExecutionOrder(def); err == nil || !containsText(err.Error(), "cycle") {
		t.Fatalf("expected reachable cycle error, got %v", err)
	}
}

func containsText(value, target string) bool {
	for index := 0; index+len(target) <= len(value); index++ {
		if value[index:index+len(target)] == target {
			return true
		}
	}
	return false
}
