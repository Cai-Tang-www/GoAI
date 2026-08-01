package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Definition 描述一个 Agent 的可执行 Workflow 图。
type Definition struct {
	EntryNode string `json:"entry_node"`
	Nodes     []Node `json:"nodes"`
	Edges     []Edge `json:"edges"`
}

// Node 描述 Workflow 中的一个执行节点。
type Node struct {
	Key    string          `json:"key"`
	Type   string          `json:"type"`
	Config json.RawMessage `json:"config,omitempty"`
}

// Edge 描述 Workflow 节点之间的有向边。
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// AgentNodeConfig 定义 Workflow 中 agent 节点的跨 Agent 委派参数。
type AgentNodeConfig struct {
	TargetAgent string   `json:"target_agent"`
	Capability  string   `json:"capability"`
	InputFrom   []string `json:"input_from,omitempty"`
	TimeoutMS   int      `json:"timeout_ms,omitempty"`
}

// ParseAgentNodeConfig 解析并校验单个 agent 节点的配置结构。
func ParseAgentNodeConfig(node Node) (*AgentNodeConfig, error) {
	if strings.TrimSpace(node.Type) != "agent" {
		return nil, fmt.Errorf("node %s is not an agent node", node.Key)
	}
	var config AgentNodeConfig
	if len(node.Config) == 0 {
		return nil, fmt.Errorf("agent node %s config is required", node.Key)
	}
	if err := json.Unmarshal(node.Config, &config); err != nil {
		return nil, fmt.Errorf("agent node %s config is invalid: %w", node.Key, err)
	}
	config.TargetAgent = strings.TrimSpace(config.TargetAgent)
	config.Capability = strings.TrimSpace(config.Capability)
	if config.TargetAgent == "" {
		return nil, fmt.Errorf("agent node %s target_agent is required", node.Key)
	}
	if config.Capability == "" {
		return nil, fmt.Errorf("agent node %s capability is required", node.Key)
	}
	if config.TimeoutMS < 0 || config.TimeoutMS > 300000 {
		return nil, fmt.Errorf("agent node %s timeout_ms must be between 0 and 300000", node.Key)
	}
	for index, reference := range config.InputFrom {
		config.InputFrom[index] = strings.TrimSpace(reference)
		if config.InputFrom[index] == "" {
			return nil, fmt.Errorf("agent node %s input_from contains an empty step", node.Key)
		}
		if config.InputFrom[index] == node.Key {
			return nil, fmt.Errorf("agent node %s input_from cannot reference itself", node.Key)
		}
	}
	return &config, nil
}

// ParseAndValidate 解析并校验 Workflow JSON。
func ParseAndValidate(raw string) (*Definition, error) {
	var def Definition
	if err := json.Unmarshal([]byte(raw), &def); err != nil {
		return nil, fmt.Errorf("workflow definition json invalid: %w", err)
	}
	if err := Validate(&def); err != nil {
		return nil, err
	}
	return &def, nil
}

// Validate 校验节点、边和 agent 节点配置的基础结构。
func Validate(def *Definition) error {
	if def == nil {
		return errors.New("workflow definition is nil")
	}
	if len(def.Nodes) == 0 {
		return errors.New("workflow nodes is empty")
	}
	entry := strings.TrimSpace(def.EntryNode)
	if entry == "" {
		return errors.New("entry_node is required")
	}

	nodeSet := make(map[string]Node, len(def.Nodes))
	for _, n := range def.Nodes {
		key := strings.TrimSpace(n.Key)
		if key == "" {
			return errors.New("workflow node key is required")
		}
		if _, exists := nodeSet[key]; exists {
			return fmt.Errorf("duplicate workflow node key: %s", key)
		}
		if strings.TrimSpace(n.Type) == "" {
			return fmt.Errorf("workflow node type is required for key: %s", key)
		}
		n.Key = key
		n.Type = strings.TrimSpace(n.Type)
		nodeSet[key] = n
	}
	if _, exists := nodeSet[entry]; !exists {
		return fmt.Errorf("entry_node %s does not exist in nodes", entry)
	}

	for _, node := range nodeSet {
		if node.Type != "agent" {
			continue
		}
		config, err := ParseAgentNodeConfig(node)
		if err != nil {
			return err
		}
		for _, reference := range config.InputFrom {
			if _, exists := nodeSet[reference]; !exists {
				return fmt.Errorf("agent node %s input_from node not found: %s", node.Key, reference)
			}
		}
	}

	for _, e := range def.Edges {
		from := strings.TrimSpace(e.From)
		to := strings.TrimSpace(e.To)
		if from == "" || to == "" {
			return errors.New("workflow edge from/to is required")
		}
		if _, exists := nodeSet[from]; !exists {
			return fmt.Errorf("edge from node not found: %s", from)
		}
		if _, exists := nodeSet[to]; !exists {
			return fmt.Errorf("edge to node not found: %s", to)
		}
	}
	return nil
}

// ResolveExecutionOrder 返回 entry 可达节点的拓扑执行顺序。
func ResolveExecutionOrder(def *Definition) ([]Node, error) {
	if err := Validate(def); err != nil {
		return nil, err
	}

	nodeSet := make(map[string]Node, len(def.Nodes))
	for _, n := range def.Nodes {
		nodeSet[n.Key] = n
	}

	adj := make(map[string][]string, len(def.Nodes))
	for key := range nodeSet {
		adj[key] = []string{}
	}
	for _, e := range def.Edges {
		adj[e.From] = append(adj[e.From], e.To)
	}

	reachable := map[string]struct{}{}
	stack := []string{def.EntryNode}
	for len(stack) > 0 {
		last := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := reachable[last]; ok {
			continue
		}
		reachable[last] = struct{}{}
		for _, next := range adj[last] {
			stack = append(stack, next)
		}
	}

	inDegree := make(map[string]int, len(reachable))
	for key := range reachable {
		inDegree[key] = 0
	}
	for from, outs := range adj {
		if _, ok := reachable[from]; !ok {
			continue
		}
		for _, to := range outs {
			if _, ok := reachable[to]; ok {
				inDegree[to]++
			}
		}
	}

	queue := []string{}
	for key, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, key)
		}
	}

	result := make([]Node, 0, len(reachable))
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		result = append(result, nodeSet[cur])
		for _, next := range adj[cur] {
			if _, ok := inDegree[next]; !ok {
				continue
			}
			inDegree[next]--
			if inDegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}

	if len(result) != len(reachable) {
		return nil, errors.New("workflow graph has cycle in reachable nodes")
	}
	return result, nil
}
