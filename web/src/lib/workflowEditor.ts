import type { WorkflowDefinition, WorkflowNode } from '../api/types'
import { validateWorkflowDefinition } from './workflow'
import { buildWorkflowGraph } from './workflowGraph'

export interface NodePosition {
  x: number
  y: number
}

export type PositionMap = Record<string, NodePosition>

interface EditorSnapshot {
  definition: WorkflowDefinition
  positions: PositionMap
}

export interface EditorState extends EditorSnapshot {
  selectedKey: string | null
  past: EditorSnapshot[]
  future: EditorSnapshot[]
  dirty: boolean
}

export type EditorAction =
  | { type: 'reset'; definition: WorkflowDefinition; positions?: PositionMap }
  | { type: 'add-node'; nodeType: string; position: NodePosition }
  | { type: 'remove-node'; key: string }
  | { type: 'update-config'; key: string; config: Record<string, unknown> | undefined }
  | { type: 'rename-node'; key: string; nextKey: string }
  | { type: 'add-edge'; from: string; to: string }
  | { type: 'remove-edge'; from: string; to: string }
  | { type: 'set-entry'; key: string }
  | { type: 'move-node'; key: string; position: NodePosition }
  | { type: 'auto-layout' }
  | { type: 'select'; key: string | null }
  | { type: 'undo' }
  | { type: 'redo' }

export const EDITOR_NODE_TYPES = ['noop', 'llm', 'agent', 'agent_tool', 'agent_group', 'tool', 'interrupt'] as const

const HISTORY_LIMIT = 50
const RESERVED_KEYS = new Set(['start', 'end'])

// defaultNodeConfig 给新节点一个能通过表单渲染的最小 config；缺失的必填项由校验面板提示。
export function defaultNodeConfig(nodeType: string, key: string): Record<string, unknown> | undefined {
  switch (nodeType) {
    case 'agent':
      return { capability: '' }
    case 'agent_tool':
      return { capability: '' }
    case 'agent_group':
      return { members: [], strategy: 'all' }
    case 'tool':
      return { server_code: '', tool_name: '', input: {} }
    case 'interrupt':
      return { interrupt_id: key, reason: '' }
    default:
      return undefined
  }
}

// uniqueNodeKey 生成不与现有节点和保留字冲突的 key，如 llm_1、llm_2。
export function uniqueNodeKey(nodeType: string, existing: Iterable<string>): string {
  const taken = new Set(existing)
  for (let index = 1; ; index += 1) {
    const candidate = `${nodeType}_${index}`
    if (!taken.has(candidate) && !RESERVED_KEYS.has(candidate)) return candidate
  }
}

// validateNodeKey 返回重命名的错误信息；null 表示合法。
export function validateNodeKey(key: string, existing: Iterable<string>): string | null {
  const trimmed = key.trim()
  if (!trimmed) return '节点 key 不能为空'
  if (trimmed.length > 64) return '节点 key 最多 64 个字符'
  if (RESERVED_KEYS.has(trimmed)) return `"${trimmed}" 是编排引擎保留字`
  for (const other of existing) if (other === trimmed) return `节点 key 已存在：${trimmed}`
  return null
}

// renameNodeKey 级联重命名：entry_node、edges 两端、所有节点 config 里的 input_from 引用。
export function renameNodeKey(definition: WorkflowDefinition, key: string, nextKey: string): WorkflowDefinition {
  const renameRef = (value: string) => (value === key ? nextKey : value)
  return {
    entry_node: renameRef(definition.entry_node),
    nodes: definition.nodes.map((node) => {
      const renamed: WorkflowNode = { ...node, key: renameRef(node.key) }
      if (node.config && Array.isArray(node.config.input_from)) {
        renamed.config = {
          ...node.config,
          input_from: (node.config.input_from as unknown[]).map((ref) => (typeof ref === 'string' ? renameRef(ref) : ref)),
        }
      }
      return renamed
    }),
    edges: definition.edges.map((edge) => ({ from: renameRef(edge.from), to: renameRef(edge.to) })),
  }
}

// removeNodeCascade 删除节点：同时删除相连的边、其他节点 input_from 里的引用；entry 被删则回退到第一个剩余节点。
export function removeNodeCascade(definition: WorkflowDefinition, key: string): WorkflowDefinition {
  const nodes = definition.nodes
    .filter((node) => node.key !== key)
    .map((node) => {
      if (node.config && Array.isArray(node.config.input_from)) {
        return { ...node, config: { ...node.config, input_from: (node.config.input_from as unknown[]).filter((ref) => ref !== key) } }
      }
      return node
    })
  let entry = definition.entry_node
  if (entry === key) entry = nodes[0]?.key || ''
  return {
    entry_node: entry,
    nodes,
    edges: definition.edges.filter((edge) => edge.from !== key && edge.to !== key),
  }
}

export interface WorkflowAnalysis {
  errors: string[]
  warnings: string[]
}

// analyzeWorkflow 汇总结构校验错误与运行时相关的告警（可保存但可能跑不起来的形态）。
export function analyzeWorkflow(definition: WorkflowDefinition): WorkflowAnalysis {
  const errors: string[] = []
  const warnings: string[] = []
  if (definition.nodes.length === 0) {
    return { errors: ['画布为空：至少需要一个节点'], warnings }
  }
  try {
    validateWorkflowDefinition(definition)
  } catch (error) {
    errors.push(error instanceof Error ? error.message : String(error))
  }

  const keys = new Set(definition.nodes.map((node) => node.key))
  const adjacency = new Map<string, string[]>()
  for (const edge of definition.edges) {
    if (!keys.has(edge.from) || !keys.has(edge.to)) continue
    adjacency.set(edge.from, [...(adjacency.get(edge.from) || []), edge.to])
  }

  // entry 可达集
  const reachable = new Set<string>()
  if (keys.has(definition.entry_node)) {
    const stack = [definition.entry_node]
    while (stack.length) {
      const current = stack.pop()!
      if (reachable.has(current)) continue
      reachable.add(current)
      for (const next of adjacency.get(current) || []) stack.push(next)
    }
  }
  const unreachable = definition.nodes.filter((node) => !reachable.has(node.key)).map((node) => node.key)
  if (unreachable.length > 0) {
    warnings.push(`以下节点从入口不可达，运行时不会执行：${unreachable.join('、')}`)
  }

  // 可达子图环检测（镜像后端 ResolveExecutionOrder 语义）
  if (reachable.size > 0) {
    const inDegree = new Map<string, number>()
    for (const key of reachable) inDegree.set(key, 0)
    for (const [from, targets] of adjacency) {
      if (!reachable.has(from)) continue
      for (const to of targets) if (reachable.has(to)) inDegree.set(to, (inDegree.get(to) || 0) + 1)
    }
    const queue = [...reachable].filter((key) => (inDegree.get(key) || 0) === 0)
    let visited = 0
    while (queue.length) {
      const current = queue.shift()!
      visited += 1
      for (const next of adjacency.get(current) || []) {
        if (!reachable.has(next)) continue
        const degree = (inDegree.get(next) || 0) - 1
        inDegree.set(next, degree)
        if (degree === 0) queue.push(next)
      }
    }
    if (visited !== reachable.size) errors.push('入口可达的子图中存在环，无法确定执行顺序')
  }

  // 运行时要求显式串行边：多条出边的图能保存但执行会失败，并行请用 agent_group。
  const fanout = [...adjacency.entries()].filter(([, targets]) => targets.length > 1).map(([from]) => from)
  if (fanout.length > 0) {
    warnings.push(`节点 ${fanout.join('、')} 有多条出边：运行时要求串行边，并行委派请改用 agent_group 节点`)
  }
  return { errors, warnings }
}

// fillMissingPositions 给缺失坐标的节点补 dagre 自动布局的位置（打开旧数据或 JSON 粘贴场景）。
export function fillMissingPositions(definition: WorkflowDefinition, positions: PositionMap): PositionMap {
  const missing = definition.nodes.some((node) => !positions[node.key])
  if (!missing) return positions
  if (definition.nodes.length === 0) return positions
  const layout = buildWorkflowGraph(definition)
  const filled: PositionMap = { ...positions }
  for (const node of layout.nodes) {
    if (!filled[node.id]) filled[node.id] = node.position
  }
  return filled
}

function snapshot(state: EditorState): EditorSnapshot {
  return { definition: state.definition, positions: state.positions }
}

function commit(state: EditorState, next: EditorSnapshot, selectedKey: string | null = state.selectedKey): EditorState {
  return {
    definition: next.definition,
    positions: next.positions,
    selectedKey,
    past: [...state.past.slice(-(HISTORY_LIMIT - 1)), snapshot(state)],
    future: [],
    dirty: true,
  }
}

export function createEditorState(definition: WorkflowDefinition, positions?: PositionMap): EditorState {
  return {
    definition,
    positions: fillMissingPositions(definition, positions || {}),
    selectedKey: null,
    past: [],
    future: [],
    dirty: false,
  }
}

export function editorReducer(state: EditorState, action: EditorAction): EditorState {
  switch (action.type) {
    case 'reset':
      return createEditorState(action.definition, action.positions)
    case 'select':
      return { ...state, selectedKey: action.key }
    case 'move-node':
      // 拖动只改坐标：不进撤销历史，避免一次拖拽产生一堆历史步。
      return { ...state, positions: { ...state.positions, [action.key]: action.position }, dirty: true }
    case 'add-node': {
      const key = uniqueNodeKey(action.nodeType, state.definition.nodes.map((node) => node.key))
      const node: WorkflowNode = { key, type: action.nodeType }
      const config = defaultNodeConfig(action.nodeType, key)
      if (config) node.config = config
      const definition: WorkflowDefinition = {
        entry_node: state.definition.nodes.length === 0 ? key : state.definition.entry_node,
        nodes: [...state.definition.nodes, node],
        edges: state.definition.edges,
      }
      return commit(state, { definition, positions: { ...state.positions, [key]: action.position } }, key)
    }
    case 'remove-node': {
      const definition = removeNodeCascade(state.definition, action.key)
      const positions = { ...state.positions }
      delete positions[action.key]
      return commit(state, { definition, positions }, state.selectedKey === action.key ? null : state.selectedKey)
    }
    case 'update-config': {
      const definition: WorkflowDefinition = {
        ...state.definition,
        nodes: state.definition.nodes.map((node) => {
          if (node.key !== action.key) return node
          const next: WorkflowNode = { key: node.key, type: node.type }
          if (action.config !== undefined) next.config = action.config
          return next
        }),
      }
      return commit(state, { definition, positions: state.positions })
    }
    case 'rename-node': {
      const nextKey = action.nextKey.trim()
      if (nextKey === action.key || validateNodeKey(nextKey, state.definition.nodes.map((node) => node.key)) !== null) return state
      const definition = renameNodeKey(state.definition, action.key, nextKey)
      const positions = { ...state.positions }
      if (positions[action.key]) {
        positions[nextKey] = positions[action.key]
        delete positions[action.key]
      }
      return commit(state, { definition, positions }, state.selectedKey === action.key ? nextKey : state.selectedKey)
    }
    case 'add-edge': {
      if (action.from === action.to) return state
      const exists = state.definition.edges.some((edge) => edge.from === action.from && edge.to === action.to)
      if (exists) return state
      const definition: WorkflowDefinition = {
        ...state.definition,
        edges: [...state.definition.edges, { from: action.from, to: action.to }],
      }
      return commit(state, { definition, positions: state.positions })
    }
    case 'remove-edge': {
      const definition: WorkflowDefinition = {
        ...state.definition,
        edges: state.definition.edges.filter((edge) => !(edge.from === action.from && edge.to === action.to)),
      }
      return commit(state, { definition, positions: state.positions })
    }
    case 'set-entry': {
      if (!state.definition.nodes.some((node) => node.key === action.key)) return state
      return commit(state, { definition: { ...state.definition, entry_node: action.key }, positions: state.positions })
    }
    case 'auto-layout': {
      if (state.definition.nodes.length === 0) return state
      const layout = buildWorkflowGraph(state.definition)
      const positions: PositionMap = {}
      for (const node of layout.nodes) positions[node.id] = node.position
      return commit(state, { definition: state.definition, positions })
    }
    case 'undo': {
      const previous = state.past[state.past.length - 1]
      if (!previous) return state
      return {
        ...previous,
        selectedKey: state.selectedKey && previous.definition.nodes.some((node) => node.key === state.selectedKey) ? state.selectedKey : null,
        past: state.past.slice(0, -1),
        future: [snapshot(state), ...state.future],
        dirty: true,
      }
    }
    case 'redo': {
      const next = state.future[0]
      if (!next) return state
      return {
        ...next,
        selectedKey: state.selectedKey && next.definition.nodes.some((node) => node.key === state.selectedKey) ? state.selectedKey : null,
        past: [...state.past, snapshot(state)],
        future: state.future.slice(1),
        dirty: true,
      }
    }
    default:
      return state
  }
}
