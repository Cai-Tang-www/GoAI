package einoexecutor

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"GoAI/domain/workflow"

	"github.com/cloudwego/eino/compose"
)

// NodeHandler 执行一个已编译 Workflow 节点，并返回可传给下游节点的 JSON。
//
// Handler 由 Runtime 提供，因此 Eino 只负责图拓扑和数据流，不绕过 RunStep、
// Delegation、Provider 或其他运行时能力。
type NodeHandler func(context.Context, workflow.Node, string) (string, error)

// Executor 使用 Eino Graph 执行一个 Agent 的 Workflow 定义。
type Executor struct {
	maxRunSteps int
}

// Option 配置 Eino Graph 执行器。
type Option func(*Executor)

// WithMaxRunSteps 限制一次 Graph 执行最多经过的节点数。
func WithMaxRunSteps(maxRunSteps int) Option {
	return func(executor *Executor) {
		if maxRunSteps > 0 {
			executor.maxRunSteps = maxRunSteps
		}
	}
}

// New 创建 Eino Graph 执行器。
func New(options ...Option) *Executor {
	executor := &Executor{maxRunSteps: 128}
	for _, option := range options {
		if option != nil {
			option(executor)
		}
	}
	return executor
}

// Execute 将 Workflow DSL 编译为 Eino Graph，并按图执行节点。
//
// V1 只允许串行可达图。这样可以保持当前 RunStep 的顺序语义，同时避免在
// 没有定义 fan-in 合并规则时错误地合并多个 Agent 输出。后续增加并行编排
// 时，应显式增加 Eino fan-in 合并策略，而不是隐式覆盖输出。
func (e *Executor) Execute(ctx context.Context, definition *workflow.Definition, input string, handler NodeHandler) (string, error) {
	if definition == nil {
		return "", errors.New("workflow definition is nil")
	}
	return e.ExecuteFrom(ctx, definition, definition.EntryNode, input, handler)
}

// ExecuteFrom 从指定节点开始执行串行 Workflow 后缀，用于 Parent Run 从持久化游标恢复。
func (e *Executor) ExecuteFrom(ctx context.Context, definition *workflow.Definition, startNode, input string, handler NodeHandler) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if e == nil {
		return "", errors.New("eino executor is nil")
	}
	if handler == nil {
		return "", errors.New("eino node handler is nil")
	}
	if err := workflow.Validate(definition); err != nil {
		return "", err
	}
	order, err := workflow.ResolveExecutionOrder(definition)
	if err != nil {
		return "", err
	}
	if len(order) == 0 {
		return "", errors.New("workflow has no reachable nodes")
	}
	if err := validateSerialGraph(definition, order); err != nil {
		return "", err
	}
	executionOrder, err := executionOrderFrom(order, startNode)
	if err != nil {
		return "", err
	}

	graph := compose.NewGraph[string, string]()
	for _, node := range executionOrder {
		current := node
		if err := graph.AddLambdaNode(current.Key, compose.InvokableLambda(func(ctx context.Context, value string) (string, error) {
			return handler(ctx, current, value)
		})); err != nil {
			return "", fmt.Errorf("add Eino node %s: %w", current.Key, err)
		}
	}
	if err := graph.AddEdge(compose.START, startNode); err != nil {
		return "", fmt.Errorf("connect workflow entry node: %w", err)
	}
	for _, edge := range definition.Edges {
		if !containsNode(executionOrder, edge.From) || !containsNode(executionOrder, edge.To) {
			continue
		}
		if err := graph.AddEdge(edge.From, edge.To); err != nil {
			return "", fmt.Errorf("connect workflow edge %s -> %s: %w", edge.From, edge.To, err)
		}
	}
	last := executionOrder[len(executionOrder)-1]
	if err := graph.AddEdge(last.Key, compose.END); err != nil {
		return "", fmt.Errorf("connect workflow end node: %w", err)
	}

	runnable, err := graph.Compile(
		ctx,
		compose.WithGraphName("goai-agent-workflow"),
		compose.WithMaxRunSteps(e.maxRunSteps),
	)
	if err != nil {
		return "", fmt.Errorf("compile Eino workflow: %w", err)
	}
	output, err := runnable.Invoke(ctx, input)
	if err != nil {
		return "", fmt.Errorf("invoke Eino workflow: %w", err)
	}
	return output, nil
}

func executionOrderFrom(order []workflow.Node, startNode string) ([]workflow.Node, error) {
	startNode = strings.TrimSpace(startNode)
	if startNode == "" {
		return nil, errors.New("workflow resume node is required")
	}
	for index, node := range order {
		if node.Key == startNode {
			return order[index:], nil
		}
	}
	return nil, fmt.Errorf("workflow resume node %s is not reachable", startNode)
}
func validateSerialGraph(definition *workflow.Definition, reachable []workflow.Node) error {
	reachableSet := make(map[string]struct{}, len(reachable))
	for _, node := range reachable {
		reachableSet[node.Key] = struct{}{}
	}
	inDegree := make(map[string]int, len(reachable))
	outDegree := make(map[string]int, len(reachable))
	for _, node := range reachable {
		inDegree[node.Key] = 0
		outDegree[node.Key] = 0
	}
	for _, edge := range definition.Edges {
		if _, ok := reachableSet[edge.From]; !ok {
			continue
		}
		if _, ok := reachableSet[edge.To]; !ok {
			continue
		}
		outDegree[edge.From]++
		inDegree[edge.To]++
	}
	for _, node := range reachable {
		if outDegree[node.Key] > 1 || inDegree[node.Key] > 1 {
			return fmt.Errorf("workflow graph requires explicit serial edges for node %s", node.Key)
		}
	}
	return nil
}

func containsNode(nodes []workflow.Node, key string) bool {
	key = strings.TrimSpace(key)
	for _, node := range nodes {
		if node.Key == key {
			return true
		}
	}
	return false
}
