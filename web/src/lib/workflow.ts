import type { WorkflowDefinition } from '../api/types'

function isObject(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

function validateTimeout(nodeKey: string, value: unknown) {
  if (value === undefined) return
  if (!Number.isInteger(value) || Number(value) < 0 || Number(value) > 300000) throw new Error(`节点 ${nodeKey} 的 timeout_ms 必须是 0 到 300000 之间的整数`)
}

function validateInputFrom(nodeKey: string, value: unknown, keys: Set<string>) {
  if (value === undefined) return
  if (!Array.isArray(value)) throw new Error(`节点 ${nodeKey} 的 input_from 必须是数组`)
  const seen = new Set<string>()
  for (const rawReference of value) {
    const reference = typeof rawReference === 'string' ? rawReference.trim() : ''
    if (!reference) throw new Error(`节点 ${nodeKey} 的 input_from 不能包含空节点`)
    if (reference === nodeKey) throw new Error(`节点 ${nodeKey} 的 input_from 不能引用自身`)
    if (seen.has(reference)) throw new Error(`节点 ${nodeKey} 的 input_from 重复引用 ${reference}`)
    if (!keys.has(reference)) throw new Error(`节点 ${nodeKey} 的 input_from 引用了不存在的节点 ${reference}`)
    seen.add(reference)
  }
}

export function validateWorkflowDefinition(value: unknown): asserts value is WorkflowDefinition {
  if (!value || typeof value !== 'object') throw new Error('Definition 必须是 JSON 对象')
  const definition = value as Partial<WorkflowDefinition>
  if (!definition.entry_node?.trim()) throw new Error('entry_node 不能为空')
  if (!Array.isArray(definition.nodes) || definition.nodes.length === 0) throw new Error('nodes 至少包含一个节点')
  if (!Array.isArray(definition.edges)) throw new Error('edges 必须是数组')

  const keys = new Set<string>()
  for (const node of definition.nodes) {
    if (!node?.key?.trim()) throw new Error('每个 node.key 都必须非空')
    if (keys.has(node.key)) throw new Error(`node.key 重复：${node.key}`)
    if (node.key === 'start' || node.key === 'end') throw new Error(`node.key "${node.key}" 是编排引擎保留字，请换一个名字`)
    keys.add(node.key)
    if (!node.type?.trim()) throw new Error(`节点 ${node.key} 缺少 type`)
    if (node.config !== undefined && !isObject(node.config)) throw new Error(`节点 ${node.key} 的 config 必须是对象`)
  }
  if (!keys.has(definition.entry_node)) throw new Error(`entry_node ${definition.entry_node} 不存在于 nodes`)

  const interruptIds = new Set<string>()
  for (const node of definition.nodes) {
    const config = node.config || {}
    if (node.type === 'interrupt') {
      const id = String(config.interrupt_id || '').trim()
      if (!id) throw new Error(`interrupt 节点 ${node.key} 缺少 interrupt_id`)
      if (id.length > 128) throw new Error(`interrupt 节点 ${node.key} 的 interrupt_id 最多 128 个字符`)
      if (interruptIds.has(id)) throw new Error(`interrupt_id 重复：${id}`)
      interruptIds.add(id)
      const reason = String(config.reason || '').trim()
      if (!reason) throw new Error(`interrupt 节点 ${node.key} 缺少 reason`)
      if (reason.length > 128) throw new Error(`interrupt 节点 ${node.key} 的 reason 最多 128 个字符`)
      if (config.response_schema !== undefined && !isObject(config.response_schema)) throw new Error(`interrupt 节点 ${node.key} 的 response_schema 必须是对象`)
      if (config.metadata !== undefined && !isObject(config.metadata)) throw new Error(`interrupt 节点 ${node.key} 的 metadata 必须是对象`)
    }
    if (node.type === 'tool') {
      if (!String(config.server_code || '').trim() || !String(config.tool_name || '').trim()) throw new Error(`tool 节点 ${node.key} 需要 server_code 和 tool_name`)
      const hasInput = config.input !== undefined
      const hasInputFrom = Array.isArray(config.input_from) && config.input_from.length > 0
      if (!hasInput && !hasInputFrom) throw new Error(`tool 节点 ${node.key} 必须提供 input 或 input_from`)
      if (hasInput && hasInputFrom) throw new Error(`tool 节点 ${node.key} 的 input 和 input_from 不能同时使用`)
      if (hasInput && !isObject(config.input)) throw new Error(`tool 节点 ${node.key} 的 input 必须是对象`)
      validateTimeout(node.key, config.timeout_ms)
    }
    if (['agent', 'agent_tool'].includes(node.type)) {
      if (!String(config.capability || '').trim()) throw new Error(`${node.type} 节点 ${node.key} 缺少 capability`)
      const targetAgent = String(config.target_agent || '').trim()
      const routingPolicy = String(config.routing_policy || '').trim()
      if (!targetAgent && routingPolicy !== 'registry') throw new Error(`${node.type} 节点 ${node.key} 需要 target_agent 或 routing_policy=registry`)
      if (routingPolicy && routingPolicy !== 'registry') throw new Error(`${node.type} 节点 ${node.key} 的 routing_policy 只能是 registry`)
      if (node.type === 'agent_tool' && String(config.tool_name || '').length > 64) throw new Error(`agent_tool 节点 ${node.key} 的 tool_name 最多 64 个字符`)
      validateTimeout(node.key, config.timeout_ms)
    }
    if (node.type === 'agent_group') {
      if (!Array.isArray(config.members) || config.members.length < 2 || config.members.length > 16) throw new Error(`agent_group 节点 ${node.key} 需要 2 到 16 个 member`)
      const memberKeys = new Set<string>()
      const targets = new Set<string>()
      for (const [index, rawMember] of config.members.entries()) {
        if (!isObject(rawMember)) throw new Error(`agent_group 节点 ${node.key} 的 member ${index + 1} 必须是对象`)
        const memberKey = String(rawMember.key || '').trim()
        const target = String(rawMember.target_agent || '').trim()
        const capability = String(rawMember.capability || '').trim()
        if (!memberKey || memberKey.length > 64) throw new Error(`agent_group 节点 ${node.key} 的 member key 必填且最多 64 个字符`)
        if (memberKeys.has(memberKey)) throw new Error(`agent_group 节点 ${node.key} 的 member key 重复：${memberKey}`)
        if (!target || !capability) throw new Error(`agent_group 节点 ${node.key} 的 member ${memberKey} 需要 target_agent 和 capability`)
        const targetKey = `${target}\0${capability}`
        if (targets.has(targetKey)) throw new Error(`agent_group 节点 ${node.key} 重复委派 ${target}/${capability}`)
        validateTimeout(`${node.key}.${memberKey}`, rawMember.timeout_ms)
        memberKeys.add(memberKey)
        targets.add(targetKey)
      }
      const strategy = String(config.strategy || '').trim()
      if (!['all', 'any', 'quorum'].includes(strategy)) throw new Error(`agent_group 节点 ${node.key} 的 strategy 必须是 all、any 或 quorum`)
      if (strategy === 'quorum' && (!Number.isInteger(config.required_successes) || Number(config.required_successes) < 1 || Number(config.required_successes) > config.members.length)) throw new Error(`agent_group 节点 ${node.key} 的 required_successes 必须在 1 到 member 数量之间`)
    }
    validateInputFrom(node.key, config.input_from, keys)
  }
  for (const edge of definition.edges) {
    if (!edge || typeof edge.from !== 'string' || typeof edge.to !== 'string') throw new Error('每条 edge 都必须包含 from 和 to')
    if (!keys.has(edge.from) || !keys.has(edge.to)) throw new Error(`边 ${edge.from} → ${edge.to} 引用了不存在的节点`)
    if (edge.from === edge.to) throw new Error(`节点 ${edge.from} 不能连接自身`)
  }
}
