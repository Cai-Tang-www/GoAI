import { describe, expect, it } from 'vitest'
import { validateWorkflowDefinition } from './workflow'

describe('validateWorkflowDefinition', () => {
  it('accepts a minimal reachable workflow', () => {
    expect(() => validateWorkflowDefinition({ entry_node: 'start', nodes: [{ key: 'start', type: 'noop', config: {} }], edges: [] })).not.toThrow()
  })

  it('rejects missing edge targets', () => {
    expect(() => validateWorkflowDefinition({ entry_node: 'start', nodes: [{ key: 'start', type: 'noop' }], edges: [{ from: 'start', to: 'missing' }] })).toThrow('不存在的节点')
  })

  it('validates interrupt identity and reason', () => {
    expect(() => validateWorkflowDefinition({ entry_node: 'approval', nodes: [{ key: 'approval', type: 'interrupt', config: { interrupt_id: 'approval' } }], edges: [] })).toThrow('缺少 reason')
  })

  it('rejects missing and self-referencing input_from nodes', () => {
    expect(() => validateWorkflowDefinition({
      entry_node: 'delegate',
      nodes: [{ key: 'delegate', type: 'agent', config: { target_agent: 'writer', capability: 'write', input_from: ['missing'] } }],
      edges: [],
    })).toThrow('引用了不存在的节点 missing')
    expect(() => validateWorkflowDefinition({
      entry_node: 'delegate',
      nodes: [{ key: 'delegate', type: 'agent', config: { target_agent: 'writer', capability: 'write', input_from: ['delegate'] } }],
      edges: [],
    })).toThrow('不能引用自身')
  })

  it('requires exactly one tool input source and validates timeout', () => {
    const tool = (config: Record<string, unknown>) => ({ entry_node: 'tool', nodes: [{ key: 'tool', type: 'tool', config }], edges: [] })
    expect(() => validateWorkflowDefinition(tool({ server_code: 'docs', tool_name: 'search' }))).toThrow('input 或 input_from')
    expect(() => validateWorkflowDefinition(tool({ server_code: 'docs', tool_name: 'search', input: {}, input_from: ['source'] }))).toThrow('不能同时使用')
    expect(() => validateWorkflowDefinition(tool({ server_code: 'docs', tool_name: 'search', input: {}, timeout_ms: 300001 }))).toThrow('0 到 300000')
  })

  it('validates agent group members and quorum', () => {
    const group = (config: Record<string, unknown>) => ({ entry_node: 'review', nodes: [{ key: 'review', type: 'agent_group', config }], edges: [] })
    const members = [
      { key: 'security', target_agent: 'security', capability: 'review' },
      { key: 'quality', target_agent: 'quality', capability: 'review' },
    ]
    expect(() => validateWorkflowDefinition(group({ members, strategy: 'quorum', required_successes: 3 }))).toThrow('required_successes')
    expect(() => validateWorkflowDefinition(group({ members, strategy: 'race' }))).toThrow('all、any 或 quorum')
  })

  it('enforces interrupt field lengths and object schemas', () => {
    expect(() => validateWorkflowDefinition({
      entry_node: 'approval',
      nodes: [{ key: 'approval', type: 'interrupt', config: { interrupt_id: 'x'.repeat(129), reason: 'approval' } }],
      edges: [],
    })).toThrow('最多 128 个字符')
    expect(() => validateWorkflowDefinition({
      entry_node: 'approval',
      nodes: [{ key: 'approval', type: 'interrupt', config: { interrupt_id: 'approval', reason: 'approval', response_schema: [] } }],
      edges: [],
    })).toThrow('response_schema 必须是对象')
  })
})
