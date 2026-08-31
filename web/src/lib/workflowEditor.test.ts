import { describe, expect, it } from 'vitest'
import type { NodeChange } from '@xyflow/react'
import type { WorkflowDefinition } from '../api/types'
import {
  analyzeWorkflow,
  collectNodePositions,
  createEditorState,
  editorReducer,
  removeNodeCascade,
  renameNodeKey,
  uniqueNodeKey,
  validateNodeKey,
  type EditorState,
} from './workflowEditor'

const serial: WorkflowDefinition = {
  entry_node: 'a',
  nodes: [
    { key: 'a', type: 'noop' },
    { key: 'b', type: 'agent', config: { target_agent: 'x', capability: 'y', input_from: ['a'] } },
    { key: 'c', type: 'noop', config: { input_from: ['b'] } },
  ],
  edges: [
    { from: 'a', to: 'b' },
    { from: 'b', to: 'c' },
  ],
}

function apply(state: EditorState, ...actions: Parameters<typeof editorReducer>[1][]): EditorState {
  return actions.reduce(editorReducer, state)
}

describe('uniqueNodeKey / validateNodeKey', () => {
  it('生成不冲突的 key', () => {
    expect(uniqueNodeKey('llm', ['llm_1', 'llm_2'])).toBe('llm_3')
    expect(uniqueNodeKey('noop', [])).toBe('noop_1')
  })
  it('拒绝保留字、空值和重复', () => {
    expect(validateNodeKey('start', [])).toContain('保留字')
    expect(validateNodeKey('  ', [])).toContain('不能为空')
    expect(validateNodeKey('a', ['a'])).toContain('已存在')
    expect(validateNodeKey('fresh', ['a'])).toBeNull()
  })
})

describe('renameNodeKey', () => {
  it('级联更新 entry、edges 和 input_from', () => {
    const renamed = renameNodeKey(serial, 'a', 'prepare')
    expect(renamed.entry_node).toBe('prepare')
    expect(renamed.edges[0]).toEqual({ from: 'prepare', to: 'b' })
    const nodeB = renamed.nodes.find((node) => node.key === 'b')
    expect(nodeB?.config?.input_from).toEqual(['prepare'])
  })
})

describe('removeNodeCascade', () => {
  it('删除节点及其边和引用，入口被删则回退', () => {
    const removed = removeNodeCascade(serial, 'a')
    expect(removed.entry_node).toBe('b')
    expect(removed.edges).toEqual([{ from: 'b', to: 'c' }])
    const nodeB = removed.nodes.find((node) => node.key === 'b')
    expect(nodeB?.config?.input_from).toEqual([])
  })
})

describe('analyzeWorkflow', () => {
  it('合法串行图无错误', () => {
    const analysis = analyzeWorkflow(serial)
    expect(analysis.errors).toEqual([])
    expect(analysis.warnings).toEqual([])
  })
  it('检出环', () => {
    const cyclic: WorkflowDefinition = {
      ...serial,
      edges: [...serial.edges, { from: 'c', to: 'a' }],
    }
    expect(analyzeWorkflow(cyclic).errors.some((error) => error.includes('环'))).toBe(true)
  })
  it('不可达节点是告警', () => {
    const withOrphan: WorkflowDefinition = {
      ...serial,
      nodes: [...serial.nodes, { key: 'orphan', type: 'noop' }],
    }
    const analysis = analyzeWorkflow(withOrphan)
    expect(analysis.warnings.some((warning) => warning.includes('orphan'))).toBe(true)
  })
  it('多条出边给出串行边告警', () => {
    const fanout: WorkflowDefinition = {
      ...serial,
      edges: [...serial.edges, { from: 'a', to: 'c' }],
    }
    expect(analyzeWorkflow(fanout).warnings.some((warning) => warning.includes('agent_group'))).toBe(true)
  })
  it('空画布是错误', () => {
    expect(analyzeWorkflow({ entry_node: '', nodes: [], edges: [] }).errors.length).toBeGreaterThan(0)
  })
})

describe('editorReducer', () => {
  it('add-node：第一个节点自动成为入口', () => {
    const state = apply(createEditorState({ entry_node: '', nodes: [], edges: [] }), {
      type: 'add-node',
      nodeType: 'noop',
      position: { x: 10, y: 20 },
    })
    expect(state.definition.entry_node).toBe('noop_1')
    expect(state.positions.noop_1).toEqual({ x: 10, y: 20 })
    expect(state.selectedKey).toBe('noop_1')
    expect(state.dirty).toBe(true)
  })

  it('add-edge：拒绝自环和重复边', () => {
    const base = createEditorState(serial)
    expect(editorReducer(base, { type: 'add-edge', from: 'a', to: 'a' })).toBe(base)
    expect(editorReducer(base, { type: 'add-edge', from: 'a', to: 'b' })).toBe(base)
    const added = editorReducer(base, { type: 'add-edge', from: 'a', to: 'c' })
    expect(added.definition.edges).toHaveLength(3)
  })

  it('rename-node：非法目标名不改变状态', () => {
    const base = createEditorState(serial)
    expect(editorReducer(base, { type: 'rename-node', key: 'a', nextKey: 'b' })).toBe(base)
    expect(editorReducer(base, { type: 'rename-node', key: 'a', nextKey: 'start' })).toBe(base)
  })

  it('rename-node：坐标跟随新 key，选中态保持', () => {
    const base = apply(createEditorState(serial), { type: 'select', key: 'a' })
    const renamed = editorReducer(base, { type: 'rename-node', key: 'a', nextKey: 'prepare' })
    expect(renamed.positions.prepare).toBeDefined()
    expect(renamed.positions.a).toBeUndefined()
    expect(renamed.selectedKey).toBe('prepare')
  })

  it('undo/redo 往返恢复定义与坐标', () => {
    const base = createEditorState(serial)
    const edited = apply(
      base,
      { type: 'add-node', nodeType: 'llm', position: { x: 5, y: 5 } },
      { type: 'add-edge', from: 'c', to: 'llm_1' },
    )
    expect(edited.definition.nodes).toHaveLength(4)
    const undone = apply(edited, { type: 'undo' }, { type: 'undo' })
    expect(undone.definition).toEqual(base.definition)
    expect(undone.positions.llm_1).toBeUndefined()
    const redone = apply(undone, { type: 'redo' }, { type: 'redo' })
    expect(redone.definition).toEqual(edited.definition)
  })

  it('move-node 不进历史', () => {
    const base = createEditorState(serial)
    const moved = editorReducer(base, { type: 'move-node', key: 'a', position: { x: 99, y: 99 } })
    expect(moved.past).toHaveLength(0)
    expect(moved.positions.a).toEqual({ x: 99, y: 99 })
    expect(moved.dirty).toBe(true)
  })

  it('collectNodePositions 提取拖动中的 position changes', () => {
    const changes: NodeChange[] = [
      { id: 'a', type: 'position', position: { x: 24, y: 36 }, dragging: true },
      { id: 'a', type: 'select', selected: true },
      { id: 'b', type: 'position', position: { x: 140, y: 80 } },
    ]
    expect(collectNodePositions(changes)).toEqual({
      a: { x: 24, y: 36 },
      b: { x: 140, y: 80 },
    })
  })

  it('move-nodes 批量落盘坐标且不生成拖动历史', () => {
    const base = createEditorState(serial)
    const moved = editorReducer(base, { type: 'move-nodes', positions: { a: { x: 99, y: 99 }, b: { x: 120, y: 40 } } })
    expect(moved.positions).toMatchObject({ a: { x: 99, y: 99 }, b: { x: 120, y: 40 } })
    expect(moved.past).toHaveLength(0)
    expect(moved.dirty).toBe(true)
  })

  it('remove-node 清除选中态', () => {
    const base = apply(createEditorState(serial), { type: 'select', key: 'b' })
    const removed = editorReducer(base, { type: 'remove-node', key: 'b' })
    expect(removed.selectedKey).toBeNull()
    expect(removed.definition.edges).toHaveLength(0)
  })

  it('remove-nodes 批量级联删除只生成一个历史快照', () => {
    const base = apply(createEditorState(serial), { type: 'select', key: 'b' })
    const removed = editorReducer(base, { type: 'remove-nodes', keys: ['b', 'b'] })
    expect(removed.definition.nodes.map((node) => node.key)).toEqual(['a', 'c'])
    expect(removed.definition.edges).toEqual([])
    expect(removed.past).toHaveLength(1)
    expect(removed.selectedKey).toBeNull()
  })

  it('set-entry 只接受存在的节点', () => {
    const base = createEditorState(serial)
    expect(editorReducer(base, { type: 'set-entry', key: 'ghost' })).toBe(base)
    expect(editorReducer(base, { type: 'set-entry', key: 'c' }).definition.entry_node).toBe('c')
  })
})
