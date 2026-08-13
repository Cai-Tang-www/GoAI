import { describe, expect, it } from 'vitest'
import type { RunStep, WorkflowDefinition } from '../api/types'
import { buildWorkflowGraph, summarizeNodeConfig } from './workflowGraph'

const diamond: WorkflowDefinition = {
  entry_node: 'prepare',
  nodes: [
    { key: 'prepare', type: 'noop', config: {} },
    { key: 'draft', type: 'llm', config: { model: 'deepseek-chat' } },
    { key: 'review', type: 'agent', config: { target_agent: 'reviewer', capability: 'review' } },
    { key: 'merge', type: 'noop', config: {} },
  ],
  edges: [
    { from: 'prepare', to: 'draft' },
    { from: 'prepare', to: 'review' },
    { from: 'draft', to: 'merge' },
    { from: 'review', to: 'merge' },
  ],
}

describe('buildWorkflowGraph', () => {
  it('positions every node and keeps all edges', () => {
    const graph = buildWorkflowGraph(diamond)
    expect(graph.nodes).toHaveLength(4)
    expect(graph.edges).toHaveLength(4)
    for (const node of graph.nodes) {
      expect(Number.isFinite(node.position.x)).toBe(true)
      expect(Number.isFinite(node.position.y)).toBe(true)
    }
    const positions = new Set(graph.nodes.map((node) => `${node.position.x},${node.position.y}`))
    expect(positions.size).toBe(4)
  })

  it('lays out left-to-right following edge direction', () => {
    const graph = buildWorkflowGraph(diamond)
    const byId = new Map(graph.nodes.map((node) => [node.id, node]))
    expect(byId.get('prepare')!.position.x).toBeLessThan(byId.get('draft')!.position.x)
    expect(byId.get('draft')!.position.x).toBeLessThan(byId.get('merge')!.position.x)
  })

  it('marks entry node and derives config summaries', () => {
    const graph = buildWorkflowGraph(diamond)
    const byId = new Map(graph.nodes.map((node) => [node.id, node]))
    expect(byId.get('prepare')!.data.isEntry).toBe(true)
    expect(byId.get('draft')!.data.isEntry).toBe(false)
    expect(byId.get('draft')!.data.summary).toBe('deepseek-chat')
    expect(byId.get('review')!.data.summary).toBe('reviewer · review')
  })

  it('overlays the latest attempt status per step and flags executed edges', () => {
    const steps: RunStep[] = [
      { step_key: 'prepare', status: 'success', latency_ms: 3, attempt: 1 },
      { step_key: 'draft', status: 'failed', latency_ms: 10, attempt: 1, error_message: 'boom' },
      { step_key: 'draft', status: 'success', latency_ms: 8, attempt: 2 },
    ]
    const graph = buildWorkflowGraph(diamond, steps)
    const byId = new Map(graph.nodes.map((node) => [node.id, node]))
    expect(byId.get('prepare')!.data.status).toBe('success')
    expect(byId.get('draft')!.data.status).toBe('success')
    expect(byId.get('draft')!.data.attempt).toBe(2)
    expect(byId.get('review')!.data.status).toBeUndefined()
    const executed = graph.edges.filter((edge) => edge.executed).map((edge) => `${edge.source}->${edge.target}`)
    expect(executed).toEqual(['prepare->draft'])
  })

  it('normalizes waiting and unknown statuses', () => {
    const steps: RunStep[] = [
      { step_key: 'prepare', status: 'waiting_input' },
      { step_key: 'draft', status: 'dispatched' },
    ]
    const graph = buildWorkflowGraph(diamond, steps)
    const byId = new Map(graph.nodes.map((node) => [node.id, node]))
    expect(byId.get('prepare')!.data.status).toBe('waiting')
    expect(byId.get('draft')!.data.status).toBe('running')
  })

  it('reads PascalCase step fields from raw model payloads', () => {
    const steps: RunStep[] = [{ StepKey: 'review', Status: 'success', LatencyMS: 42, Attempt: 1 }]
    const graph = buildWorkflowGraph(diamond, steps)
    const review = graph.nodes.find((node) => node.id === 'review')!
    expect(review.data.status).toBe('success')
    expect(review.data.latencyMs).toBe(42)
  })
})

describe('summarizeNodeConfig', () => {
  it('summarizes each node type', () => {
    expect(summarizeNodeConfig({ key: 'a', type: 'agent', config: { capability: 'write' } })).toBe('registry 路由 · write')
    expect(summarizeNodeConfig({ key: 'b', type: 'agent_tool', config: { target_agent: 'x', capability: 'c', tool_name: 't' } })).toBe('x · c · tool:t')
    expect(summarizeNodeConfig({ key: 'c', type: 'agent_group', config: { members: [{}, {}], strategy: 'all' } })).toBe('2 members · all')
    expect(summarizeNodeConfig({ key: 'd', type: 'tool', config: { server_code: 'docs', tool_name: 'search' } })).toBe('docs.search')
    expect(summarizeNodeConfig({ key: 'e', type: 'interrupt', config: { reason: '需要审批' } })).toBe('需要审批')
    expect(summarizeNodeConfig({ key: 'f', type: 'noop' })).toBe('')
  })
})
