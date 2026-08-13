import { useQuery } from '@tanstack/react-query'
import { Alert, AutoComplete, Button, Input, InputNumber, Popconfirm, Radio, Select, Tooltip } from 'antd'
import { CornerUpLeft, Plus, Trash2 } from 'lucide-react'
import { useMemo, useState } from 'react'
import { apiRequest } from '../api/client'
import type { Agent, MCPServer, MCPTool, WorkflowNode } from '../api/types'
import { validateNodeKey } from '../lib/workflowEditor'

type Config = Record<string, unknown>

interface NodeConfigPanelProps {
  node: WorkflowNode
  entryKey: string
  allKeys: string[]
  onConfigChange: (config: Config | undefined) => void
  onRename: (nextKey: string) => void
  onSetEntry: () => void
  onRemove: () => void
}

function asText(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

function asNumber(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined
}

function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : []
}

// JsonField 让自由 JSON 字段可编辑：本地暂存文本，仅在解析合法时向上提交。
function JsonField({ label, value, onChange, rows = 4 }: { label: string; value: unknown; onChange: (value: unknown) => void; rows?: number }) {
  const serialized = useMemo(() => JSON.stringify(value ?? {}, null, 2), [value])
  const [text, setText] = useState(serialized)
  const [invalid, setInvalid] = useState(false)
  // 外部值变化（撤销、切换输入模式等）时在渲染期同步本地草稿。
  const [lastSerialized, setLastSerialized] = useState(serialized)
  if (serialized !== lastSerialized) {
    setLastSerialized(serialized)
    setText(serialized)
    setInvalid(false)
  }
  return (
    <div className="config-field">
      <label>{label}</label>
      <Input.TextArea
        className="code-input"
        rows={rows}
        value={text}
        status={invalid ? 'error' : undefined}
        onChange={(event) => setText(event.target.value)}
        onBlur={() => {
          try {
            onChange(JSON.parse(text))
            setInvalid(false)
          } catch {
            setInvalid(true)
          }
        }}
      />
      {invalid && <span className="config-field-error">JSON 不合法，未保存该字段</span>}
    </div>
  )
}

// useAgentCardSkills 从公开的 A2A Agent Card 拉取目标 Agent 的能力列表，用于 capability 联动。
function useAgentCardSkills(agentCode: string) {
  return useQuery({
    queryKey: ['agent-card-skills', agentCode],
    enabled: Boolean(agentCode),
    retry: false,
    queryFn: async () => {
      const response = await fetch(`/a2a/agents/${encodeURIComponent(agentCode)}/.well-known/agent-card.json`)
      if (!response.ok) return [] as string[]
      const card = (await response.json()) as { skills?: Array<{ id?: string; name?: string }> }
      return (card.skills || []).map((skill) => skill.id || skill.name || '').filter(Boolean)
    },
  })
}

function DelegationFields({ config, set, apply, isAgentTool }: { config: Config; set: (field: string, value: unknown) => void; apply: (patch: Config) => void; isAgentTool: boolean }) {
  const agentsQuery = useQuery({ queryKey: ['published-agents'], queryFn: () => apiRequest<Agent[]>('/api/agents/published') })
  const targetAgent = asText(config.target_agent)
  const useRegistry = asText(config.routing_policy) === 'registry'
  const skillsQuery = useAgentCardSkills(useRegistry ? '' : targetAgent)
  const agentOptions = (agentsQuery.data || []).map((agent) => ({ value: agent.agent_code, label: `${agent.name}（${agent.agent_code}）` }))
  return (
    <>
      <div className="config-field">
        <label>路由方式</label>
        <Radio.Group
          value={useRegistry ? 'registry' : 'direct'}
          onChange={(event) => {
            if (event.target.value === 'registry') {
              apply({ routing_policy: 'registry', target_agent: undefined })
            } else {
              set('routing_policy', undefined)
            }
          }}
          options={[{ label: '指定 Agent', value: 'direct' }, { label: 'Registry 按能力路由', value: 'registry' }]}
        />
      </div>
      {!useRegistry && (
        <div className="config-field">
          <label>目标 Agent</label>
          <AutoComplete
            value={targetAgent}
            options={agentOptions}
            placeholder="选择已发布 Agent 或输入 agent_code"
            filterOption={(input, option) => String(option?.label ?? '').toLowerCase().includes(input.toLowerCase())}
            onChange={(value) => set('target_agent', value || undefined)}
          />
        </div>
      )}
      <div className="config-field">
        <label>Capability</label>
        <AutoComplete
          value={asText(config.capability)}
          options={(skillsQuery.data || []).map((skill) => ({ value: skill }))}
          placeholder={useRegistry ? '按能力在 Registry 中路由' : '目标 Agent 的能力 code'}
          onChange={(value) => set('capability', value)}
        />
      </div>
      {isAgentTool && (
        <div className="config-field">
          <label>Tool 名称（可选，暴露给 LLM 的工具名）</label>
          <Input value={asText(config.tool_name)} maxLength={64} onChange={(event) => set('tool_name', event.target.value || undefined)} />
        </div>
      )}
    </>
  )
}

function GroupMembersField({ config, set }: { config: Config; set: (field: string, value: unknown) => void }) {
  const agentsQuery = useQuery({ queryKey: ['published-agents'], queryFn: () => apiRequest<Agent[]>('/api/agents/published') })
  const members = Array.isArray(config.members) ? (config.members as Config[]) : []
  const strategy = asText(config.strategy) || 'all'
  const agentOptions = (agentsQuery.data || []).map((agent) => ({ value: agent.agent_code, label: `${agent.name}（${agent.agent_code}）` }))
  const updateMember = (index: number, field: string, value: unknown) => {
    const next = members.map((member, i) => (i === index ? { ...member, [field]: value === '' || value === undefined ? undefined : value } : member))
    set('members', next)
  }
  return (
    <>
      <div className="config-field">
        <label>并行成员（2–16 个）</label>
        <div className="group-members">
          {members.map((member, index) => (
            <div key={index} className="group-member">
              <div className="group-member-head">
                <Input size="small" placeholder="member key" value={asText(member.key)} onChange={(event) => updateMember(index, 'key', event.target.value)} />
                <Button size="small" type="text" danger icon={<Trash2 size={13} />} onClick={() => set('members', members.filter((_, i) => i !== index))} />
              </div>
              <AutoComplete size="small" placeholder="target_agent" value={asText(member.target_agent)} options={agentOptions}
                filterOption={(input, option) => String(option?.label ?? '').toLowerCase().includes(input.toLowerCase())}
                onChange={(value) => updateMember(index, 'target_agent', value)} />
              <Input size="small" placeholder="capability" value={asText(member.capability)} onChange={(event) => updateMember(index, 'capability', event.target.value)} />
            </div>
          ))}
          <Button size="small" icon={<Plus size={13} />} onClick={() => set('members', [...members, { key: `member_${members.length + 1}`, target_agent: '', capability: '' }])}>
            添加成员
          </Button>
        </div>
      </div>
      <div className="config-field">
        <label>聚合策略</label>
        <Select
          value={strategy}
          options={[{ value: 'all', label: 'all · 全部成功' }, { value: 'any', label: 'any · 任一成功' }, { value: 'quorum', label: 'quorum · 达到法定数' }]}
          onChange={(value) => set('strategy', value)}
        />
      </div>
      {strategy === 'quorum' && (
        <div className="config-field">
          <label>required_successes</label>
          <InputNumber min={1} max={Math.max(members.length, 1)} value={asNumber(config.required_successes)} onChange={(value) => set('required_successes', value ?? undefined)} />
        </div>
      )}
    </>
  )
}

function ToolFields({ config, set, apply }: { config: Config; set: (field: string, value: unknown) => void; apply: (patch: Config) => void }) {
  const serversQuery = useQuery({ queryKey: ['mcp-servers'], queryFn: () => apiRequest<MCPServer[]>('/api/mcp/servers') })
  const serverCode = asText(config.server_code)
  const toolsQuery = useQuery({
    queryKey: ['mcp-tools', serverCode],
    enabled: Boolean(serverCode),
    queryFn: () => apiRequest<MCPTool[]>(`/api/mcp/servers/${encodeURIComponent(serverCode)}/tools`),
    retry: false,
  })
  // tool 节点 input 与 input_from 互斥：以静态 input 是否存在判定当前模式。
  const usesUpstream = config.input === undefined
  return (
    <>
      <div className="config-field">
        <label>MCP Server</label>
        <AutoComplete
          value={serverCode}
          options={(serversQuery.data || []).map((server) => ({ value: server.server_code, label: `${server.name}（${server.server_code}）` }))}
          filterOption={(input, option) => String(option?.label ?? '').toLowerCase().includes(input.toLowerCase())}
          onChange={(value) => set('server_code', value)}
        />
      </div>
      <div className="config-field">
        <label>Tool</label>
        <AutoComplete
          value={asText(config.tool_name)}
          options={(toolsQuery.data || []).map((tool) => ({ value: tool.tool_name }))}
          onChange={(value) => set('tool_name', value)}
        />
      </div>
      <div className="config-field">
        <label>输入来源（二选一）</label>
        <Radio.Group
          value={usesUpstream ? 'upstream' : 'static'}
          onChange={(event) => {
            if (event.target.value === 'upstream') {
              set('input', undefined)
            } else {
              apply({ input_from: undefined, input: {} })
            }
          }}
          options={[{ label: '静态 input', value: 'static' }, { label: '上游节点输出', value: 'upstream' }]}
        />
      </div>
      {!usesUpstream && <JsonField label="静态 input（JSON）" value={config.input} onChange={(value) => set('input', value)} />}
    </>
  )
}

// NodeConfigPanel 是选中节点后的右侧配置面板：key 重命名、入口设置和按类型的表单。
export function NodeConfigPanel({ node, entryKey, allKeys, onConfigChange, onRename, onSetEntry, onRemove }: NodeConfigPanelProps) {
  const config: Config = node.config || {}
  // 面板以 node.key 作为 React key 挂载，重命名/切换选择都会重挂载并重置草稿。
  const [keyDraft, setKeyDraft] = useState(node.key)
  const keyError = keyDraft === node.key ? null : validateNodeKey(keyDraft, allKeys.filter((key) => key !== node.key))

  // apply 支持一次提交多个字段，避免连续两次 set 基于旧 config 互相覆盖。
  const apply = (patch: Config) => {
    const next: Config = { ...config }
    for (const [field, value] of Object.entries(patch)) {
      if (value === undefined || value === '' || (Array.isArray(value) && value.length === 0 && field === 'input_from')) {
        delete next[field]
      } else {
        next[field] = value
      }
    }
    onConfigChange(Object.keys(next).length > 0 ? next : undefined)
  }
  const set = (field: string, value: unknown) => apply({ [field]: value })

  const inputFromOptions = allKeys.filter((key) => key !== node.key).map((key) => ({ value: key }))
  const showInputFrom = ['agent', 'agent_tool', 'agent_group', 'tool'].includes(node.type)
  const showTimeout = ['agent', 'agent_tool', 'tool'].includes(node.type)

  return (
    <aside className="node-config-panel">
      <div className="config-head">
        <span className="wf-node-type">{node.type}</span>
        <div className="config-head-actions">
          {node.key !== entryKey && (
            <Tooltip title="设为入口节点"><Button size="small" icon={<CornerUpLeft size={13} />} onClick={onSetEntry}>设为入口</Button></Tooltip>
          )}
          <Popconfirm title="删除该节点及其连线？" onConfirm={onRemove}>
            <Button size="small" danger icon={<Trash2 size={13} />} />
          </Popconfirm>
        </div>
      </div>
      <div className="config-field">
        <label>节点 key</label>
        <Input
          value={keyDraft}
          status={keyError ? 'error' : undefined}
          onChange={(event) => setKeyDraft(event.target.value)}
          onBlur={() => { if (!keyError && keyDraft !== node.key) onRename(keyDraft) }}
          onPressEnter={() => { if (!keyError && keyDraft !== node.key) onRename(keyDraft) }}
        />
        {keyError && <span className="config-field-error">{keyError}</span>}
      </div>
      {node.key === entryKey && <Alert type="info" showIcon message="当前入口节点" />}

      {['agent', 'agent_tool'].includes(node.type) && <DelegationFields config={config} set={set} apply={apply} isAgentTool={node.type === 'agent_tool'} />}
      {node.type === 'agent_group' && <GroupMembersField config={config} set={set} />}
      {node.type === 'tool' && <ToolFields config={config} set={set} apply={apply} />}
      {node.type === 'interrupt' && (
        <>
          <div className="config-field"><label>interrupt_id</label><Input value={asText(config.interrupt_id)} maxLength={128} onChange={(event) => set('interrupt_id', event.target.value)} /></div>
          <div className="config-field"><label>原因（reason）</label><Input value={asText(config.reason)} maxLength={128} onChange={(event) => set('reason', event.target.value)} /></div>
          <div className="config-field"><label>提示消息（可选）</label><Input.TextArea rows={2} value={asText(config.message)} onChange={(event) => set('message', event.target.value || undefined)} /></div>
          <JsonField label="response_schema（可选）" value={config.response_schema} onChange={(value) => set('response_schema', value)} />
        </>
      )}
      {['llm', 'noop'].includes(node.type) && (
        <JsonField label="节点 config（JSON）" value={config} rows={6} onChange={(value) => onConfigChange(value && typeof value === 'object' && Object.keys(value as Config).length > 0 ? (value as Config) : undefined)} />
      )}

      {showInputFrom && (
        <div className="config-field">
          <label>input_from（上游输出来源，可选）</label>
          <Select
            mode="multiple"
            allowClear
            value={asStringArray(config.input_from)}
            options={inputFromOptions}
            placeholder="选择上游节点"
            onChange={(value) => set('input_from', value)}
          />
        </div>
      )}
      {showTimeout && (
        <div className="config-field">
          <label>timeout_ms（0–300000，可选）</label>
          <InputNumber min={0} max={300000} value={asNumber(config.timeout_ms)} onChange={(value) => set('timeout_ms', value ?? undefined)} style={{ width: '100%' }} />
        </div>
      )}
    </aside>
  )
}
