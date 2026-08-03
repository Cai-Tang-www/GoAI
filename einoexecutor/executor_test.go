package einoexecutor

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"GoAI/domain/workflow"
)

func TestExecuteBuildsAndInvokesEinoGraph(t *testing.T) {
	definition := &workflow.Definition{
		EntryNode: "first",
		Nodes: []workflow.Node{
			{Key: "first", Type: "noop"},
			{Key: "second", Type: "noop"},
		},
		Edges: []workflow.Edge{{From: "first", To: "second"}},
	}
	var seen []string
	executor := New()
	output, err := executor.Execute(context.Background(), definition, `{"input":"ok"}`, func(_ context.Context, node workflow.Node, input string) (string, error) {
		seen = append(seen, node.Key+":"+input)
		return input + "." + node.Key, nil
	})
	if err != nil {
		t.Fatalf("execute graph: %v", err)
	}
	if !reflect.DeepEqual(seen, []string{`first:{"input":"ok"}`, `second:{"input":"ok"}.first`}) {
		t.Fatalf("unexpected node inputs: %#v", seen)
	}
	if output != `{"input":"ok"}.first.second` {
		t.Fatalf("output=%q, want final node output", output)
	}
}

func TestExecuteRejectsParallelGraphUntilMergePolicyExists(t *testing.T) {
	definition := &workflow.Definition{
		EntryNode: "start",
		Nodes: []workflow.Node{
			{Key: "start", Type: "noop"},
			{Key: "left", Type: "noop"},
			{Key: "right", Type: "noop"},
		},
		Edges: []workflow.Edge{
			{From: "start", To: "left"},
			{From: "start", To: "right"},
		},
	}
	_, err := New().Execute(context.Background(), definition, `{}`, func(context.Context, workflow.Node, string) (string, error) {
		return `{}`, nil
	})
	if err == nil || !strings.Contains(err.Error(), "serial edges") {
		t.Fatalf("expected serial graph error, got %v", err)
	}
}

func TestExecutePropagatesHandlerError(t *testing.T) {
	definition := &workflow.Definition{
		EntryNode: "only",
		Nodes:     []workflow.Node{{Key: "only", Type: "noop"}},
	}
	want := errors.New("node failed")
	_, err := New().Execute(context.Background(), definition, `{}`, func(context.Context, workflow.Node, string) (string, error) {
		return "", want
	})
	if !errors.Is(err, want) {
		t.Fatalf("expected handler error, got %v", err)
	}
}

func TestExecuteFromRunsOnlyWorkflowSuffix(t *testing.T) {
	definition := &workflow.Definition{
		EntryNode: "prepare",
		Nodes: []workflow.Node{
			{Key: "prepare", Type: "tool"},
			{Key: "delegate", Type: "agent", Config: []byte(`{"target_agent":"writer","capability":"write"}`)},
			{Key: "summarize", Type: "llm"},
		},
		Edges: []workflow.Edge{{From: "prepare", To: "delegate"}, {From: "delegate", To: "summarize"}},
	}
	var executed []string
	output, err := New().ExecuteFrom(context.Background(), definition, "summarize", `{"child":"done"}`, func(_ context.Context, node workflow.Node, input string) (string, error) {
		executed = append(executed, node.Key)
		if input != `{"child":"done"}` {
			t.Fatalf("resume input = %s", input)
		}
		return `{"parent":"done"}`, nil
	})
	if err != nil {
		t.Fatalf("ExecuteFrom failed: %v", err)
	}
	if !reflect.DeepEqual(executed, []string{"summarize"}) {
		t.Fatalf("executed nodes = %v", executed)
	}
	if output != `{"parent":"done"}` {
		t.Fatalf("output = %s", output)
	}
}

func TestExecuteFromRejectsUnreachableResumeNode(t *testing.T) {
	definition := &workflow.Definition{
		EntryNode: "prepare",
		Nodes:     []workflow.Node{{Key: "prepare", Type: "tool"}},
	}
	_, err := New().ExecuteFrom(context.Background(), definition, "missing", `{}`, func(context.Context, workflow.Node, string) (string, error) {
		return `{}`, nil
	})
	if err == nil || !strings.Contains(err.Error(), "resume node missing is not reachable") {
		t.Fatalf("ExecuteFrom error = %v", err)
	}
}
