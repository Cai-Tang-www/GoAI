import dagre from '@dagrejs/dagre'
import type { RunStep, WorkflowDefinition, WorkflowNode } from '../api/types'
import { pick } from './format'

export const WORKFLOW_NODE_WIDTH = 224
export const WORKFLOW_NODE_HEIGHT = 76

// 已执行步骤的归一化状态；未出现在 steps 里的节点没有 status（渲染为待执行）。
export type WorkflowNodeRunStatus = 'success' | 'failed' | 'cancelled' | 'waiting' | 'running'

export interface WorkflowGraphNodeData {
  nodeKey: string
  nodeType: string
  isEntry: boolean
  summary: string
  status?: WorkflowNodeRunStatus
  latencyMs?: number
  attempt?: number
  errorMessage?: string
  [key: string]: unknown
}

export interface WorkflowGraphNode {
  id: string
  position: { x: number; y: number }
  data: WorkflowGraphNodeData
}

export interface WorkflowGraphEdge {
  id: string
  source: string
  target: string
  executed: boolean
}

export interface WorkflowGraph {
  nodes: WorkflowGraphNode[]
  edges: WorkflowGraphEdge[]
}

// summarizeNodeConfig 把节点 config 压缩成一行人类可读的摘要，用于画布节点副标题。
export function summarizeNodeConfig(node: WorkflowNode): string {
  const config = node.config || {}
  const text = (value: unknown) => String(value ?? '').trim()
  switch (node.type) {
    case 'agent': {
      const target = text(config.target_agent) || 'registry 路由'
      const capability = text(config.capability)
      return capability ? `${target} · ${capability}` : target
    }
    case 'agent_tool': {
      const target = text(config.target_agent) || 'registry 路由'
      const tool = text(config.tool_name)
      const capability = text(config.capability)
      return [target, capability, tool && `tool:${tool}`].filter(Boolean).join(' · ')
    }
    case 'agent_group': {
      const members = Array.isArray(config.members) ? config.members.length : 0
      const strategy = text(config.strategy)
      return `${members} members${strategy ? ` · ${strategy}` : ''}`
    }
    case 'tool': {
      const server = text(config.server_code)
      const tool = text(config.tool_name)
      return server && tool ? `${server}.${tool}` : server || tool
    }
    case 'interrupt':
      return text(config.reason)
    case 'llm':
      return text(config.model) || text(config.prompt).slice(0, 40)
    default:
      return ''
  }
}

// normalizeStepStatus 把后端 step 状态归一化为画布可渲染的枚举。
function normalizeStepStatus(raw: string | undefined): WorkflowNodeRunStatus | undefined {
  const status = (raw || '').trim().toLowerCase()
  if (!status) return undefined
  if (status === 'success') return 'success'
  if (status === 'failed') return 'failed'
  if (status === 'cancelled') return 'cancelled'
  if (status.startsWith('waiting')) return 'waiting'
  return 'running'
}

interface StepOverlay {
  status: WorkflowNodeRunStatus
  latencyMs?: number
  attempt?: number
  errorMessage?: string
}

// latestStepByKey 同一节点可能有多次 attempt，取最后一条记录作为该节点的当前状态。
function latestStepByKey(steps: RunStep[]): Map<string, StepOverlay> {
  const byKey = new Map<string, StepOverlay>()
  for (const step of steps) {
    const key = pick<string>(step, 'step_key', 'StepKey')
    const status = normalizeStepStatus(pick<string>(step, 'status', 'Status'))
    if (!key || !status) continue
    byKey.set(key, {
      status,
      latencyMs: pick<number>(step, 'latency_ms', 'LatencyMS'),
      attempt: pick<number>(step, 'attempt', 'Attempt'),
      errorMessage: pick<string>(step, 'error_message', 'ErrorMessage'),
    })
  }
  return byKey
}

// buildWorkflowGraph 把 Workflow 定义（可选叠加 Run steps）转换成带 dagre 坐标的图结构。
export function buildWorkflowGraph(definition: WorkflowDefinition, steps?: RunStep[]): WorkflowGraph {
  const graph = new dagre.graphlib.Graph()
  graph.setGraph({ rankdir: 'LR', nodesep: 36, ranksep: 90, marginx: 16, marginy: 16 })
  graph.setDefaultEdgeLabel(() => ({}))
  for (const node of definition.nodes) {
    graph.setNode(node.key, { width: WORKFLOW_NODE_WIDTH, height: WORKFLOW_NODE_HEIGHT })
  }
  for (const edge of definition.edges) {
    graph.setEdge(edge.from, edge.to)
  }
  dagre.layout(graph)

  const overlays = steps?.length ? latestStepByKey(steps) : new Map<string, StepOverlay>()
  const nodes: WorkflowGraphNode[] = definition.nodes.map((node) => {
    const layout = graph.node(node.key)
    const overlay = overlays.get(node.key)
    return {
      id: node.key,
      position: { x: layout.x - WORKFLOW_NODE_WIDTH / 2, y: layout.y - WORKFLOW_NODE_HEIGHT / 2 },
      data: {
        nodeKey: node.key,
        nodeType: node.type,
        isEntry: node.key === definition.entry_node,
        summary: summarizeNodeConfig(node),
        status: overlay?.status,
        latencyMs: overlay?.latencyMs,
        attempt: overlay?.attempt,
        errorMessage: overlay?.errorMessage,
      },
    }
  })
  const edges: WorkflowGraphEdge[] = definition.edges.map((edge, index) => ({
    id: `${edge.from}->${edge.to}#${index}`,
    source: edge.from,
    target: edge.to,
    executed: overlays.has(edge.from) && overlays.has(edge.to),
  }))
  return { nodes, edges }
}
