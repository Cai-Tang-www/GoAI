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
	TargetAgent   string   `json:"target_agent,omitempty"`
	Capability    string   `json:"capability"`
	RoutingPolicy string   `json:"routing_policy,omitempty"`
	InputFrom     []string `json:"input_from,omitempty"`
	TimeoutMS     int      `json:"timeout_ms,omitempty"`
}

// ToolNodeConfig 定义 Workflow 中 tool 节点的 MCP 稳定引用与输入来源。
type ToolNodeConfig struct {
	ServerCode string         `json:"server_code"`
	ToolName   string         `json:"tool_name"`
	Input      map[string]any `json:"input,omitempty"`
	InputFrom  []string       `json:"input_from,omitempty"`
	TimeoutMS  int            `json:"timeout_ms,omitempty"`
}

// InterruptNodeConfig 定义一个需要用户输入后继续的 Workflow 暂停节点。
type InterruptNodeConfig struct {
	InterruptID    string         `json:"interrupt_id"`
	Reason         string         `json:"reason"`
	Message        string         `json:"message,omitempty"`
	ResponseSchema map[string]any `json:"response_schema,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

// ParseInterruptNodeConfig 解析并严格校验 interrupt 节点配置。
func ParseInterruptNodeConfig(node Node) (*InterruptNodeConfig, error) {
	if strings.TrimSpace(node.Type) != "interrupt" {
		return nil, fmt.Errorf("node %s is not an interrupt node", node.Key)
	}
	if len(node.Config) == 0 {
		return nil, fmt.Errorf("interrupt node %s config is required", node.Key)
	}
	var config InterruptNodeConfig
	decoder := json.NewDecoder(strings.NewReader(string(node.Config)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("interrupt node %s config is invalid: %w", node.Key, err)
	}
	config.InterruptID = strings.TrimSpace(config.InterruptID)
	config.Reason = strings.TrimSpace(config.Reason)
	config.Message = strings.TrimSpace(config.Message)
	if config.InterruptID == "" || len(config.InterruptID) > 128 {
		return nil, fmt.Errorf("interrupt node %s interrupt_id is required and must be at most 128 characters", node.Key)
	}
	if config.Reason == "" || len(config.Reason) > 128 {
		return nil, fmt.Errorf("interrupt node %s reason is required and must be at most 128 characters", node.Key)
	}
	return &config, nil
}

// ParseToolNodeConfig 解析并严格校验 MCP tool 节点配置。
func ParseToolNodeConfig(node Node) (*ToolNodeConfig, error) {
	if strings.TrimSpace(node.Type) != "tool" {
		return nil, fmt.Errorf("node %s is not a tool node", node.Key)
	}
	if len(node.Config) == 0 {
		return nil, fmt.Errorf("tool node %s config is required", node.Key)
	}
	var config ToolNodeConfig
	decoder := json.NewDecoder(strings.NewReader(string(node.Config)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("tool node %s config is invalid: %w", node.Key, err)
	}
	config.ServerCode = strings.TrimSpace(config.ServerCode)
	config.ToolName = strings.TrimSpace(config.ToolName)
	if config.ServerCode == "" {
		return nil, fmt.Errorf("tool node %s server_code is required", node.Key)
	}
	if config.ToolName == "" {
		return nil, fmt.Errorf("tool node %s tool_name is required", node.Key)
	}
	if config.TimeoutMS < 0 || config.TimeoutMS > 300000 {
		return nil, fmt.Errorf("tool node %s timeout_ms must be between 0 and 300000", node.Key)
	}
	if config.Input == nil && len(config.InputFrom) == 0 {
		return nil, fmt.Errorf("tool node %s requires input or input_from", node.Key)
	}
	if config.Input != nil && len(config.InputFrom) > 0 {
		return nil, fmt.Errorf("tool node %s input and input_from cannot be used together", node.Key)
	}
	seen := make(map[string]struct{}, len(config.InputFrom))
	for index, reference := range config.InputFrom {
		reference = strings.TrimSpace(reference)
		config.InputFrom[index] = reference
		if reference == "" {
			return nil, fmt.Errorf("tool node %s input_from contains an empty step", node.Key)
		}
		if reference == node.Key {
			return nil, fmt.Errorf("tool node %s input_from cannot reference itself", node.Key)
		}
		if _, exists := seen[reference]; exists {
			return nil, fmt.Errorf("tool node %s input_from contains duplicate step: %s", node.Key, reference)
		}
		seen[reference] = struct{}{}
	}
	return &config, nil
}

// AgentGroupMember 定义 agent_group 节点中的一个稳定委派成员。
type AgentGroupMember struct {
	Key         string `json:"key"`
	TargetAgent string `json:"target_agent"`
	Capability  string `json:"capability"`
	TimeoutMS   int    `json:"timeout_ms,omitempty"`
}

// AgentGroupNodeConfig 定义并行 A2A 委派及其 fan-in 聚合策略。
type AgentGroupNodeConfig struct {
	Members           []AgentGroupMember `json:"members"`
	Strategy          string             `json:"strategy"`
	RequiredSuccesses int                `json:"required_successes,omitempty"`
	InputFrom         []string           `json:"input_from,omitempty"`
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
	decoder := json.NewDecoder(strings.NewReader(string(node.Config)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("agent node %s config is invalid: %w", node.Key, err)
	}
	config.TargetAgent = strings.TrimSpace(config.TargetAgent)
	config.Capability = strings.TrimSpace(config.Capability)
	config.RoutingPolicy = strings.ToLower(strings.TrimSpace(config.RoutingPolicy))
	if config.TargetAgent == "" && config.RoutingPolicy != "registry" {
		return nil, fmt.Errorf("agent node %s requires target_agent or routing_policy=registry", node.Key)
	}
	if config.RoutingPolicy != "" && config.RoutingPolicy != "registry" {
		return nil, fmt.Errorf("agent node %s routing_policy must be registry", node.Key)
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

// ParseAgentGroupNodeConfig 解析并校验 agent_group 节点配置。
func ParseAgentGroupNodeConfig(node Node) (*AgentGroupNodeConfig, error) {
	if strings.TrimSpace(node.Type) != "agent_group" {
		return nil, fmt.Errorf("node %s is not an agent_group node", node.Key)
	}
	var config AgentGroupNodeConfig
	if len(node.Config) == 0 {
		return nil, fmt.Errorf("agent_group node %s config is required", node.Key)
	}
	if err := json.Unmarshal(node.Config, &config); err != nil {
		return nil, fmt.Errorf("agent_group node %s config is invalid: %w", node.Key, err)
	}
	if len(config.Members) < 2 || len(config.Members) > 16 {
		return nil, fmt.Errorf("agent_group node %s members must contain between 2 and 16 entries", node.Key)
	}
	memberKeys := make(map[string]struct{}, len(config.Members))
	targets := make(map[string]struct{}, len(config.Members))
	for index := range config.Members {
		member := &config.Members[index]
		member.Key = strings.TrimSpace(member.Key)
		member.TargetAgent = strings.TrimSpace(member.TargetAgent)
		member.Capability = strings.TrimSpace(member.Capability)
		if member.Key == "" || len(member.Key) > 64 {
			return nil, fmt.Errorf("agent_group node %s member key is required and must be at most 64 characters", node.Key)
		}
		if _, exists := memberKeys[member.Key]; exists {
			return nil, fmt.Errorf("agent_group node %s has duplicate member key: %s", node.Key, member.Key)
		}
		memberKeys[member.Key] = struct{}{}
		if member.TargetAgent == "" || member.Capability == "" {
			return nil, fmt.Errorf("agent_group node %s member %s target_agent and capability are required", node.Key, member.Key)
		}
		if member.TimeoutMS < 0 || member.TimeoutMS > 300000 {
			return nil, fmt.Errorf("agent_group node %s member %s timeout_ms must be between 0 and 300000", node.Key, member.Key)
		}
		targetKey := member.TargetAgent + "\x00" + member.Capability
		if _, exists := targets[targetKey]; exists {
			return nil, fmt.Errorf("agent_group node %s repeats target %s capability %s", node.Key, member.TargetAgent, member.Capability)
		}
		targets[targetKey] = struct{}{}
	}
	config.Strategy = strings.ToLower(strings.TrimSpace(config.Strategy))
	switch config.Strategy {
	case "all":
		config.RequiredSuccesses = len(config.Members)
	case "any":
		config.RequiredSuccesses = 1
	case "quorum":
		if config.RequiredSuccesses < 1 || config.RequiredSuccesses > len(config.Members) {
			return nil, fmt.Errorf("agent_group node %s required_successes must be between 1 and %d", node.Key, len(config.Members))
		}
	default:
		return nil, fmt.Errorf("agent_group node %s strategy must be all, any or quorum", node.Key)
	}
	for index, reference := range config.InputFrom {
		config.InputFrom[index] = strings.TrimSpace(reference)
		if config.InputFrom[index] == "" {
			return nil, fmt.Errorf("agent_group node %s input_from contains an empty step", node.Key)
		}
		if config.InputFrom[index] == node.Key {
			return nil, fmt.Errorf("agent_group node %s input_from cannot reference itself", node.Key)
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
		var inputFrom []string
		switch node.Type {
		case "agent":
			config, err := ParseAgentNodeConfig(node)
			if err != nil {
				return err
			}
			inputFrom = config.InputFrom
		case "agent_group":
			config, err := ParseAgentGroupNodeConfig(node)
			if err != nil {
				return err
			}
			inputFrom = config.InputFrom
		case "tool":
			config, err := ParseToolNodeConfig(node)
			if err != nil {
				return err
			}
			inputFrom = config.InputFrom
		case "interrupt":
			if _, err := ParseInterruptNodeConfig(node); err != nil {
				return err
			}
		default:
			continue
		}
		for _, reference := range inputFrom {
			if _, exists := nodeSet[reference]; !exists {
				return fmt.Errorf("%s node %s input_from node not found: %s", node.Type, node.Key, reference)
			}
		}
	}
	interruptIDs := make(map[string]string)
	for _, node := range nodeSet {
		if node.Type != "interrupt" {
			continue
		}
		config, err := ParseInterruptNodeConfig(node)
		if err != nil {
			return err
		}
		if previous, exists := interruptIDs[config.InterruptID]; exists {
			return fmt.Errorf("duplicate interrupt_id %s in nodes %s and %s", config.InterruptID, previous, node.Key)
		}
		interruptIDs[config.InterruptID] = node.Key
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
