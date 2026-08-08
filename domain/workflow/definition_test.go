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

func TestValidateAgentGroupNodeConfig(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{name: "missing config", raw: "{\"entry_node\":\"review\",\"nodes\":[{\"key\":\"review\",\"type\":\"agent_group\"}]}", wantErr: "config is required"},
		{name: "too few members", raw: "{\"entry_node\":\"review\",\"nodes\":[{\"key\":\"review\",\"type\":\"agent_group\",\"config\":{\"strategy\":\"all\",\"members\":[{\"key\":\"one\",\"target_agent\":\"reviewer\",\"capability\":\"review\"}]}}]}", wantErr: "between 2 and 16"},
		{name: "duplicate member", raw: "{\"entry_node\":\"review\",\"nodes\":[{\"key\":\"review\",\"type\":\"agent_group\",\"config\":{\"strategy\":\"all\",\"members\":[{\"key\":\"same\",\"target_agent\":\"security\",\"capability\":\"review\"},{\"key\":\"same\",\"target_agent\":\"quality\",\"capability\":\"review\"}]}}]}", wantErr: "duplicate member key"},
		{name: "duplicate target capability", raw: "{\"entry_node\":\"review\",\"nodes\":[{\"key\":\"review\",\"type\":\"agent_group\",\"config\":{\"strategy\":\"all\",\"members\":[{\"key\":\"one\",\"target_agent\":\"security\",\"capability\":\"review\"},{\"key\":\"two\",\"target_agent\":\"security\",\"capability\":\"review\"}]}}]}", wantErr: "repeats target"},
		{name: "invalid strategy", raw: "{\"entry_node\":\"review\",\"nodes\":[{\"key\":\"review\",\"type\":\"agent_group\",\"config\":{\"strategy\":\"race\",\"members\":[{\"key\":\"one\",\"target_agent\":\"security\",\"capability\":\"review\"},{\"key\":\"two\",\"target_agent\":\"quality\",\"capability\":\"review\"}]}}]}", wantErr: "strategy must be"},
		{name: "invalid quorum", raw: "{\"entry_node\":\"review\",\"nodes\":[{\"key\":\"review\",\"type\":\"agent_group\",\"config\":{\"strategy\":\"quorum\",\"required_successes\":3,\"members\":[{\"key\":\"one\",\"target_agent\":\"security\",\"capability\":\"review\"},{\"key\":\"two\",\"target_agent\":\"quality\",\"capability\":\"review\"}]}}]}", wantErr: "required_successes"},
		{name: "unknown input", raw: "{\"entry_node\":\"review\",\"nodes\":[{\"key\":\"review\",\"type\":\"agent_group\",\"config\":{\"strategy\":\"any\",\"input_from\":[\"missing\"],\"members\":[{\"key\":\"one\",\"target_agent\":\"security\",\"capability\":\"review\"},{\"key\":\"two\",\"target_agent\":\"quality\",\"capability\":\"review\"}]}}]}", wantErr: "input_from node not found"},
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

func TestParseAgentGroupNodeConfigNormalizesPolicy(t *testing.T) {
	def, err := ParseAndValidate("{\"entry_node\":\"review\",\"nodes\":[{\"key\":\"review\",\"type\":\"agent_group\",\"config\":{\"strategy\":\" ALL \",\"members\":[{\"key\":\" security \",\"target_agent\":\"security\",\"capability\":\"review\"},{\"key\":\"quality\",\"target_agent\":\"quality\",\"capability\":\"review\"}]}}]}")
	if err != nil {
		t.Fatalf("parse group definition: %v", err)
	}
	config, err := ParseAgentGroupNodeConfig(def.Nodes[0])
	if err != nil {
		t.Fatalf("parse group config: %v", err)
	}
	if config.Strategy != "all" || config.RequiredSuccesses != 2 || config.Members[0].Key != "security" {
		t.Fatalf("unexpected normalized config: %+v", config)
	}
}

func TestResolveExecutionOrderSupportsAgentNode(t *testing.T) {
	def, err := ParseAndValidate(`{
		"entry_node":"planner",
		"nodes":[
			{"key":"planner","type":"planner"},
			{"key":"delegate","type":"agent","config":{"target_agent":"writer","capability":"write","input_from":["planner"]}},
			{"key":"finish","type":"noop"}
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
