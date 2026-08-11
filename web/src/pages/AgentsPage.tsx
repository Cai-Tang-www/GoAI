import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Button, Form, Input, Modal, Popconfirm, Table, message } from 'antd'
import { Bot, Edit3, Plus, Power } from 'lucide-react'
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { apiRequest } from '../api/client'
import type { Agent } from '../api/types'
import { EmptyPanel } from '../components/EmptyPanel'
import { ErrorState } from '../components/ErrorState'
import { PageHeader } from '../components/PageHeader'
import { RequestErrorAlert } from '../components/RequestErrorAlert'
import { StatusTag } from '../components/StatusTag'
import { formatTime } from '../lib/format'
import { isFormValidationError, notifyRequestError } from '../lib/notify'

export function AgentsPage() {
  const query = useQuery({ queryKey: ['agents'], queryFn: () => apiRequest<Agent[]>('/api/agents') })
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [open, setOpen] = useState(false)
  const [editing, setEditing] = useState<Agent | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [actioning, setActioning] = useState('')
  const [formError, setFormError] = useState<unknown>(null)
  const [form] = Form.useForm()

  const openModal = (agent?: Agent) => {
    setEditing(agent || null)
    setFormError(null)
    form.setFieldsValue(agent ? { agent_code: agent.agent_code, name: agent.name, description: agent.description } : { agent_code: '', name: '', description: '' })
    setOpen(true)
  }

  const save = async (values: { agent_code: string; name: string; description?: string }) => {
    setFormError(null)
    setSubmitting(true)
    try {
      const path = editing ? `/api/agents/${encodeURIComponent(editing.agent_code)}` : '/api/agents'
      await apiRequest(path, { method: editing ? 'PUT' : 'POST', body: editing ? { agent_code: editing.agent_code, name: values.name, description: values.description } : values })
      message.success(editing ? 'Agent 已更新' : 'Agent 已创建')
      setOpen(false)
      form.resetFields()
      await queryClient.invalidateQueries({ queryKey: ['agents'] })
      if (!editing) navigate(`/agents/${values.agent_code}`)
    } catch (error) {
      if (isFormValidationError(error)) setFormError(error)
      else notifyRequestError(error)
    } finally { setSubmitting(false) }
  }

  const changeStatus = async (agent: Agent) => {
    const operation = agent.status === 'active' ? 'deactivate' : 'activate'
    setActioning(agent.agent_code)
    try {
      await apiRequest(`/api/agents/${encodeURIComponent(agent.agent_code)}/${operation}`, { method: 'POST' })
      message.success(operation === 'activate' ? 'Agent 已发布' : 'Agent 已停用')
      await queryClient.invalidateQueries({ queryKey: ['agents'] })
    } catch (error) { notifyRequestError(error) } finally { setActioning('') }
  }

  return (
    <div className="page-shell">
      <PageHeader title="Agents" description="注册、配置并发布可被 Runtime 发现的执行主体。" actions={<Button type="primary" icon={<Plus size={16} />} onClick={() => openModal()}>新建 Agent</Button>} />
      {query.error ? <ErrorState error={query.error} onRetry={() => query.refetch()} /> : (
        <section className="content-section">
          <div className="section-heading"><div><h2>Registry</h2><span>{query.data?.length || 0} 个 Agent</span></div><Bot size={18} /></div>
          {!query.isLoading && query.data?.length === 0 ? <EmptyPanel title="Registry 为空" description="创建第一个 Agent，然后配置 Workflow、Capability 和 Endpoint。" action={<Button type="primary" onClick={() => openModal()}>新建 Agent</Button>} /> : (
            <Table rowKey="agent_code" loading={query.isLoading} dataSource={query.data} onRow={(record) => ({ onClick: () => navigate(`/agents/${record.agent_code}`) })} pagination={{ pageSize: 10, hideOnSinglePage: true }} scroll={{ x: 960 }} columns={[
              { title: 'Agent', dataIndex: 'name', render: (name: string, record: Agent) => <div className="table-primary"><span className="agent-glyph"><Bot size={16} /></span><span><strong>{name}</strong><code>{record.agent_code}</code></span></div> },
              { title: '描述', dataIndex: 'description', ellipsis: true, render: (value: string) => value || '—' },
              { title: '状态', dataIndex: 'status', width: 120, render: (value: string) => <StatusTag status={value} /> },
              { title: '所有者', dataIndex: 'owner_user_id', width: 100, render: (value: number) => <code>#{value}</code> },
              { title: '更新时间', dataIndex: 'updated_at', width: 180, render: formatTime },
              { title: '操作', width: 190, render: (_: unknown, agent: Agent) => <div className="row-actions" onClick={(event) => event.stopPropagation()}><Button size="small" icon={<Edit3 size={14} />} onClick={() => openModal(agent)}>编辑</Button><Popconfirm title={agent.status === 'active' ? '确认停用此 Agent？' : '尝试发布此 Agent？'} onConfirm={() => changeStatus(agent)}><Button size="small" danger={agent.status === 'active'} type={agent.status === 'active' ? 'default' : 'primary'} loading={actioning === agent.agent_code} icon={<Power size={14} />}>{agent.status === 'active' ? '停用' : '发布'}</Button></Popconfirm></div> },
            ]} />
          )}
        </section>
      )}
      <Modal title={editing ? '编辑 Agent' : '新建 Agent'} open={open} onCancel={() => setOpen(false)} onOk={() => form.submit()} confirmLoading={submitting} okText={editing ? '保存' : '创建'}>
        <Form form={form} layout="vertical" onFinish={save} requiredMark={false}>
          {Boolean(formError) && <RequestErrorAlert error={formError} />}
          <Form.Item name="agent_code" label="Agent Code" extra="创建后不可修改，用于协议发现和 Workflow 引用。" rules={[{ required: true }, { pattern: /^[a-zA-Z0-9_-]+$/, message: '仅支持字母、数字、下划线和短横线' }]}><Input disabled={Boolean(editing)} className="code-input" placeholder="planner" /></Form.Item>
          <Form.Item name="name" label="名称" rules={[{ required: true }]}><Input placeholder="Planning Agent" /></Form.Item>
          <Form.Item name="description" label="描述"><Input.TextArea rows={3} /></Form.Item>
        </Form>
      </Modal>
    </div>
  )
}
