import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, Descriptions, Form, Input, InputNumber, Modal, Popconfirm, Select, Table, Tabs, message } from 'antd'
import { Activity, ArrowLeft, CheckCircle2, Circle, Edit3, ExternalLink, HeartPulse, Plus, Power } from 'lucide-react'
import { useMemo, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { ApiError, apiRequest } from '../api/client'
import type { Agent, AgentEndpoint, Capability, Workflow } from '../api/types'
import { CodeEditor } from '../components/CodeEditor'
import { ErrorState } from '../components/ErrorState'
import { JsonView } from '../components/JsonView'
import { PageHeader } from '../components/PageHeader'
import { RequestErrorAlert } from '../components/RequestErrorAlert'
import { StatusTag } from '../components/StatusTag'
import { formatTime, prettyJSON, shortId } from '../lib/format'
import { isFormValidationError, notifyRequestError } from '../lib/notify'
import { validateWorkflowDefinition } from '../lib/workflow'

const emptyWorkflow = { entry_node: 'prepare', nodes: [{ key: 'prepare', type: 'noop', config: {} }], edges: [] }

export function AgentDetailPage() {
  const { agentCode = '' } = useParams()
  const navigate = useNavigate()
  const client = useQueryClient()
  const [modal, setModal] = useState<'agent' | 'capability' | 'endpoint' | 'workflow' | null>(null)
  const [editing, setEditing] = useState<Capability | AgentEndpoint | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [formError, setFormError] = useState<unknown>(null)
  const [jsonValue, setJsonValue] = useState(prettyJSON(emptyWorkflow))
  const [form] = Form.useForm()
  const agentQuery = useQuery({ queryKey: ['agent', agentCode], queryFn: () => apiRequest<Agent>(`/api/agents/${encodeURIComponent(agentCode)}`) })
  const workflowsQuery = useQuery({ queryKey: ['workflows', agentCode], queryFn: () => apiRequest<Workflow[]>(`/api/agents/${encodeURIComponent(agentCode)}/workflows`) })
  const cardQuery = useQuery({ queryKey: ['agent-card', agentCode], queryFn: () => fetch(`/a2a/agents/${encodeURIComponent(agentCode)}/.well-known/agent-card.json`).then(async (response) => { if (!response.ok) throw new ApiError(response.status === 404 ? 'Agent 尚未发布' : `Agent Card 请求失败（HTTP ${response.status}）`, response.status, 'AGENT_CARD_FAILED', response.headers.get('X-Trace-ID') || '') ; return response.json() }), retry: false })

  const refresh = async () => {
    await Promise.all([
      client.invalidateQueries({ queryKey: ['agent', agentCode] }),
      client.invalidateQueries({ queryKey: ['workflows', agentCode] }),
      client.invalidateQueries({ queryKey: ['agent-card', agentCode] }),
    ])
  }
  const agent = agentQuery.data
  const capabilities = useMemo(() => agent?.capabilities || [], [agent?.capabilities])
  const endpoints = useMemo(() => agent?.endpoints || [], [agent?.endpoints])
  const workflows = useMemo(() => workflowsQuery.data || [], [workflowsQuery.data])
  const gates = useMemo(() => {
    const activeWorkflows = workflows.filter((workflow) => workflow.is_active)
    return [
      { label: '存在 active Workflow 版本', ok: activeWorkflows.length > 0 },
      { label: '存在版本一致的 active Workflow Capability', ok: capabilities.some((capability) => capability.status === 'active' && capability.capability_type === 'workflow' && activeWorkflows.some((workflow) => workflow.id === capability.workflow_id && String(workflow.version) === capability.version)) },
      { label: '存在健康的 active A2A Endpoint', ok: endpoints.some((endpoint) => endpoint.status === 'active' && endpoint.last_healthy_at) },
    ]
  }, [capabilities, endpoints, workflows])

  const openModal = (type: typeof modal, item?: Capability | AgentEndpoint) => {
    setModal(type)
    setEditing(item || null)
    setFormError(null)
    form.resetFields()
    if (type === 'agent' && agent) form.setFieldsValue({ name: agent.name, description: agent.description })
    if (type === 'capability') form.setFieldsValue(item || { capability_type: 'workflow', version: '1', status: 'inactive', input_schema_json: '{}', output_schema_json: '{}', config_json: '{}' })
    if (type === 'endpoint') form.setFieldsValue(item || { protocol: 'a2a', transport: 'http', auth_type: 'goai_hmac_sha256', config_json: '{}' })
    if (type === 'workflow') { form.setFieldsValue({ version: (workflows[0]?.version || 0) + 1 }); setJsonValue(prettyJSON(emptyWorkflow)) }
  }

  const submit = async (values: Record<string, unknown>) => {
    setFormError(null)
    setSubmitting(true)
    try {
      if (modal === 'agent') await apiRequest(`/api/agents/${encodeURIComponent(agentCode)}`, { method: 'PUT', body: { ...values, agent_code: agentCode } })
      if (modal === 'capability') {
        const code = String(editing && 'capability_code' in editing ? editing.capability_code : values.capability_code)
        const path = editing ? `/api/agents/${encodeURIComponent(agentCode)}/capabilities/${encodeURIComponent(code)}` : `/api/agents/${encodeURIComponent(agentCode)}/capabilities`
        await apiRequest(path, { method: editing ? 'PUT' : 'POST', body: values })
      }
      if (modal === 'endpoint') {
        const code = String(editing && 'endpoint_code' in editing ? editing.endpoint_code : values.endpoint_code)
        const path = editing ? `/api/agents/${encodeURIComponent(agentCode)}/endpoints/${encodeURIComponent(code)}` : `/api/agents/${encodeURIComponent(agentCode)}/endpoints`
        await apiRequest(path, { method: editing ? 'PUT' : 'POST', body: values })
      }
      if (modal === 'workflow') {
        const definition: unknown = JSON.parse(jsonValue)
        validateWorkflowDefinition(definition)
        await apiRequest(`/api/agents/${encodeURIComponent(agentCode)}/workflows`, { method: 'POST', body: { version: values.version, definition } })
      }
      message.success('保存成功')
      setModal(null)
      await refresh()
    } catch (error) {
      if (isFormValidationError(error)) setFormError(error)
      else notifyRequestError(error)
    } finally { setSubmitting(false) }
  }

  const action = async (path: string, success: string) => {
    try { await apiRequest(path, { method: 'POST' }); message.success(success); await refresh() } catch (error) { notifyRequestError(error) }
  }

  if (agentQuery.error) return <div className="page-shell"><ErrorState error={agentQuery.error} onRetry={() => agentQuery.refetch()} /></div>

  const tabs = [
    { key: 'overview', label: '概览', children: <div className="detail-grid"><section className="content-section span-two"><div className="section-heading"><div><h2>Agent 信息</h2><span>Registry record</span></div><Button icon={<Edit3 size={15} />} onClick={() => openModal('agent')}>编辑</Button></div><Descriptions column={{ xs: 1, md: 2 }} items={[{ key: 'code', label: 'Agent Code', children: <code>{agent?.agent_code}</code> }, { key: 'status', label: '状态', children: <StatusTag status={agent?.status} /> }, { key: 'owner', label: 'Owner', children: <code>#{agent?.owner_user_id}</code> }, { key: 'workflow', label: 'Active Workflow', children: agent?.active_workflow ? `v${agent.active_workflow.version}` : '—' }, { key: 'description', label: '描述', span: 2, children: agent?.description || '—' }, { key: 'created', label: '创建时间', children: formatTime(agent?.created_at) }, { key: 'updated', label: '更新时间', children: formatTime(agent?.updated_at) }]} /></section><aside className="content-section"><div className="section-heading"><div><h2>Agent Card</h2><span>A2A discovery</span></div>{!cardQuery.error && <ExternalLink size={16} />}</div>{cardQuery.isLoading ? '正在获取…' : cardQuery.error instanceof ApiError && cardQuery.error.status === 404 ? <Alert type="info" showIcon message="尚未发布" description="发布后会公开 Agent Card。" /> : cardQuery.error ? <ErrorState error={cardQuery.error} onRetry={() => cardQuery.refetch()} /> : <JsonView value={cardQuery.data} maxHeight={360} />}</aside></div> },
    { key: 'capabilities', label: `Capabilities ${capabilities.length}`, children: <section className="content-section"><div className="section-heading"><div><h2>能力</h2><span>对 Runtime 和其他 Agent 暴露的业务能力</span></div><Button type="primary" icon={<Plus size={15} />} onClick={() => openModal('capability')}>新建</Button></div><Table rowKey="capability_code" dataSource={capabilities} scroll={{ x: 800 }} pagination={false} columns={[{ title: '能力', render: (_, item) => <div className="table-primary"><span><strong>{item.name}</strong><code>{item.capability_code}</code></span></div> }, { title: '类型', dataIndex: 'capability_type', render: (value) => <code>{value}</code> }, { title: 'Workflow', render: (_, item) => item.workflow_id ? <span><code>#{item.workflow_id}</code> · v{item.version}</span> : '—' }, { title: '状态', dataIndex: 'status', render: (value) => <StatusTag status={value} /> }, { title: '操作', width: 150, render: (_, item) => <div className="row-actions"><Button size="small" onClick={() => openModal('capability', item)}>编辑</Button>{item.status === 'active' && <Popconfirm title="确认停用此 Capability？" onConfirm={() => action(`/api/agents/${encodeURIComponent(agentCode)}/capabilities/${encodeURIComponent(item.capability_code)}/deactivate`, 'Capability 已停用')}><Button size="small" danger>停用</Button></Popconfirm>}</div> }]} /></section> },
    { key: 'endpoints', label: `Endpoints ${endpoints.length}`, children: <section className="content-section"><div className="section-heading"><div><h2>A2A Endpoints</h2><span>健康检查通过后才能参与发布</span></div><Button type="primary" icon={<Plus size={15} />} onClick={() => openModal('endpoint')}>新建</Button></div><Table rowKey="endpoint_code" dataSource={endpoints} scroll={{ x: 980 }} pagination={false} columns={[{ title: 'Endpoint', render: (_, item) => <div className="table-primary"><span><strong>{item.endpoint_code}</strong><code>{item.transport} · {item.protocol}</code></span></div> }, { title: '地址', dataIndex: 'address', ellipsis: true, render: (value) => <code>{value}</code> }, { title: '健康状态', render: (_, item) => <div><StatusTag status={item.status} /><small className="table-note">{item.last_healthy_at ? formatTime(item.last_healthy_at) : '尚未检查'}</small></div> }, { title: '操作', width: 260, render: (_, item) => <div className="row-actions"><Button size="small" icon={<HeartPulse size={14} />} onClick={() => action(`/api/agents/${encodeURIComponent(agentCode)}/endpoints/${encodeURIComponent(item.endpoint_code)}/health-check`, '健康检查通过')}>健康检查</Button><Button size="small" onClick={() => openModal('endpoint', item)}>编辑</Button>{item.status === 'active' && <Popconfirm title="确认停用此 Endpoint？" onConfirm={() => action(`/api/agents/${encodeURIComponent(agentCode)}/endpoints/${encodeURIComponent(item.endpoint_code)}/deactivate`, 'Endpoint 已停用')}><Button size="small" danger>停用</Button></Popconfirm>}</div> }]} /></section> },
    { key: 'workflows', label: `Workflows ${workflows.length}`, children: <section className="content-section"><div className="section-heading"><div><h2>Workflow 版本</h2><span>active 版本不可原地编辑</span></div><Button type="primary" icon={<Plus size={15} />} onClick={() => openModal('workflow')}>新建版本</Button></div><Table rowKey="version" loading={workflowsQuery.isLoading} dataSource={workflows} pagination={false} columns={[{ title: '版本', render: (_, item) => <Link to={`/agents/${agentCode}/workflows/${item.version}`}><strong>v{item.version}</strong></Link> }, { title: 'Checksum', dataIndex: 'checksum', render: (value) => <code>{shortId(value, 16)}</code> }, { title: '状态', dataIndex: 'is_active', render: (value) => <StatusTag status={value ? 'active' : 'inactive'} /> }, { title: 'Capability 引用', dataIndex: 'capabilities', render: (values) => values?.length ? values.map((value: { capability_code: string }) => <code key={value.capability_code}>{value.capability_code} </code>) : '—' }, { title: '操作', render: (_, item) => <div className="row-actions"><Link to={`/agents/${agentCode}/workflows/${item.version}`}><Button size="small">查看</Button></Link>{item.is_active ? <Popconfirm title="确认停用此版本？" onConfirm={() => action(`/api/agents/${encodeURIComponent(agentCode)}/workflows/${item.version}/deactivate`, 'Workflow 已停用')}><Button size="small">停用</Button></Popconfirm> : <Button size="small" type="primary" onClick={() => action(`/api/agents/${encodeURIComponent(agentCode)}/workflows/${item.version}/activate`, 'Workflow 已激活')}>激活</Button>}</div> }]} /></section> },
    { key: 'publish', label: '发布', children: <div className="publish-layout"><section className="content-section"><div className="section-heading"><div><h2>发布门禁</h2><span>所有协议资产都必须准备就绪</span></div><Activity size={18} /></div><div className="gate-list">{gates.map((gate) => <div className={gate.ok ? 'passed' : ''} key={gate.label}>{gate.ok ? <CheckCircle2 size={21} /> : <Circle size={21} />}<span>{gate.label}</span><strong>{gate.ok ? '通过' : '未满足'}</strong></div>)}</div><div className="publish-actions"><Alert type="info" showIcon message="发布后 Agent 将进入 Registry Router，并公开 Agent Card。" /><Button type="primary" size="large" icon={<Power size={17} />} disabled={!gates.every((gate) => gate.ok) || agent?.status === 'active'} onClick={() => action(`/api/agents/${encodeURIComponent(agentCode)}/activate`, 'Agent 已发布')}>发布 Agent</Button>{agent?.status === 'active' && <Popconfirm title="停用后将不再接受新委派，确认继续？" onConfirm={() => action(`/api/agents/${encodeURIComponent(agentCode)}/deactivate`, 'Agent 已停用')}><Button danger size="large">停用 Agent</Button></Popconfirm>}</div></section></div> },
  ]

  return (
    <div className="page-shell">
      <Button className="back-button" type="text" icon={<ArrowLeft size={16} />} onClick={() => navigate('/agents')}>Agents</Button>
      <PageHeader eyebrow="AGENT REGISTRY" title={agent?.name || agentCode} description={agent?.description || '未填写描述'} actions={<StatusTag status={agent?.status} />} />
      <Tabs className="resource-tabs" items={tabs} />
      <Modal title={modal === 'agent' ? '编辑 Agent' : modal === 'capability' ? `${editing ? '编辑' : '新建'} Capability` : modal === 'endpoint' ? `${editing ? '编辑' : '新建'} Endpoint` : '新建 Workflow 版本'} width={modal === 'workflow' ? 820 : 620} open={Boolean(modal)} onCancel={() => setModal(null)} onOk={() => form.submit()} confirmLoading={submitting} okText="保存">
        <Form form={form} layout="vertical" onFinish={submit} requiredMark={false}>
          {Boolean(formError) && <RequestErrorAlert error={formError} />}
          {modal === 'agent' && <><Form.Item name="name" label="名称" rules={[{ required: true }]}><Input /></Form.Item><Form.Item name="description" label="描述"><Input.TextArea rows={4} /></Form.Item></>}
          {modal === 'capability' && <><Form.Item name="capability_code" label="Capability Code" rules={[{ required: true }]}><Input disabled={Boolean(editing)} className="code-input" /></Form.Item><Form.Item name="name" label="名称" rules={[{ required: true }]}><Input /></Form.Item><Form.Item name="description" label="描述"><Input.TextArea rows={2} /></Form.Item><div className="form-grid"><Form.Item name="capability_type" label="类型" extra="V1 只有 workflow 类型可参与 Agent 发布。" rules={[{ required: true }]}><Select options={['workflow', 'remote', 'tool', 'custom'].map((value) => ({ value }))} /></Form.Item><Form.Item name="status" label="状态"><Select options={['active', 'inactive'].map((value) => ({ value }))} /></Form.Item><Form.Item name="workflow_id" label="Workflow" dependencies={['capability_type']} rules={[{ validator: async (_, value) => { if (form.getFieldValue('capability_type') === 'workflow' && !value) throw new Error('workflow 类型必须选择 Workflow') } }]}><Select allowClear options={workflows.map((workflow) => ({ value: workflow.id, label: `v${workflow.version} · #${workflow.id}` }))} /></Form.Item><Form.Item name="version" label="版本" dependencies={['capability_type', 'workflow_id']} rules={[{ validator: async (_, value) => { if (form.getFieldValue('capability_type') !== 'workflow') return; const selected = workflows.find((workflow) => workflow.id === form.getFieldValue('workflow_id')); if (!value) throw new Error('workflow 类型必须填写版本'); if (selected && String(value) !== String(selected.version)) throw new Error(`版本必须与 Workflow v${selected.version} 一致`) } }]}><Input /></Form.Item></div>{['input_schema_json', 'output_schema_json', 'config_json'].map((field) => <Form.Item key={field} name={field} label={field} rules={[{ validator: async (_, value) => { if (value) JSON.parse(value) } }]}><Input.TextArea className="code-input" rows={3} /></Form.Item>)}</>}
          {modal === 'endpoint' && <><Form.Item name="endpoint_code" label="Endpoint Code" rules={[{ required: true }]}><Input disabled={Boolean(editing)} className="code-input" /></Form.Item><div className="form-grid"><Form.Item name="protocol" label="协议"><Input disabled /></Form.Item><Form.Item name="transport" label="传输"><Select options={['http', 'https'].map((value) => ({ value }))} /></Form.Item></div><Form.Item name="address" label="地址" rules={[{ required: true }, { type: 'url' }]}><Input placeholder="http://127.0.0.1:8080/a2a/agents/..." /></Form.Item><div className="form-grid"><Form.Item name="auth_type" label="认证类型"><Input /></Form.Item><Form.Item name="credential_ref" label="凭据引用"><Input placeholder="仅填写引用名，不要填写密钥" /></Form.Item></div><Form.Item name="config_json" label="非敏感配置 JSON" rules={[{ validator: async (_, value) => { if (value) JSON.parse(value) } }]}><Input.TextArea className="code-input" rows={4} /></Form.Item></>}
          {modal === 'workflow' && <><Form.Item name="version" label="版本号" rules={[{ required: true }]}><InputNumber min={1} precision={0} /></Form.Item><Form.Item label="Definition JSON" required><CodeEditor value={jsonValue} onChange={setJsonValue} height="400px" /></Form.Item></>}
        </Form>
      </Modal>
    </div>
  )
}
