import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Button, Form, Input, Modal, Table, message } from 'antd'
import { Bot, Plus } from 'lucide-react'
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { apiRequest, errorDescription } from '../api/client'
import type { Agent } from '../api/types'
import { EmptyPanel } from '../components/EmptyPanel'
import { ErrorState } from '../components/ErrorState'
import { PageHeader } from '../components/PageHeader'
import { StatusTag } from '../components/StatusTag'
import { formatTime } from '../lib/format'

export function AgentsPage() {
  const query = useQuery({ queryKey: ['agents'], queryFn: () => apiRequest<Agent[]>('/api/agents') })
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const [open, setOpen] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [form] = Form.useForm()

  const create = async (values: { agent_code: string; name: string; description?: string }) => {
    setSubmitting(true)
    try {
      await apiRequest('/api/agents', { method: 'POST', body: values })
      message.success('Agent 已创建')
      setOpen(false)
      form.resetFields()
      await queryClient.invalidateQueries({ queryKey: ['agents'] })
      navigate(`/agents/${values.agent_code}`)
    } catch (error) { message.error(errorDescription(error)) } finally { setSubmitting(false) }
  }

  return (
    <div className="page-shell">
      <PageHeader title="Agents" description="注册、配置并发布可被 Runtime 发现的执行主体。" actions={<Button type="primary" icon={<Plus size={16} />} onClick={() => setOpen(true)}>新建 Agent</Button>} />
      {query.error ? <ErrorState error={query.error} onRetry={() => query.refetch()} /> : (
        <section className="content-section">
          <div className="section-heading"><div><h2>Registry</h2><span>{query.data?.length || 0} 个 Agent</span></div><Bot size={18} /></div>
          {!query.isLoading && query.data?.length === 0 ? <EmptyPanel title="Registry 为空" description="创建第一个 Agent，然后配置 Workflow、Capability 和 Endpoint。" action={<Button type="primary" onClick={() => setOpen(true)}>新建 Agent</Button>} /> : (
            <Table rowKey="agent_code" loading={query.isLoading} dataSource={query.data} onRow={(record) => ({ onClick: () => navigate(`/agents/${record.agent_code}`) })} pagination={{ pageSize: 10, hideOnSinglePage: true }} columns={[
              { title: 'Agent', dataIndex: 'name', render: (name: string, record: Agent) => <div className="table-primary"><span className="agent-glyph"><Bot size={16} /></span><span><strong>{name}</strong><code>{record.agent_code}</code></span></div> },
              { title: '描述', dataIndex: 'description', ellipsis: true, render: (value: string) => value || '—' },
              { title: '状态', dataIndex: 'status', width: 120, render: (value: string) => <StatusTag status={value} /> },
              { title: '所有者', dataIndex: 'owner_user_id', width: 100, render: (value: number) => <code>#{value}</code> },
              { title: '更新时间', dataIndex: 'updated_at', width: 180, render: formatTime },
            ]} />
          )}
        </section>
      )}
      <Modal title="新建 Agent" open={open} onCancel={() => setOpen(false)} onOk={() => form.submit()} confirmLoading={submitting} okText="创建">
        <Form form={form} layout="vertical" onFinish={create} requiredMark={false}>
          <Form.Item name="agent_code" label="Agent Code" extra="创建后不可修改，用于协议发现和 Workflow 引用。" rules={[{ required: true }, { pattern: /^[a-zA-Z0-9_-]+$/, message: '仅支持字母、数字、下划线和短横线' }]}><Input className="code-input" placeholder="planner" /></Form.Item>
          <Form.Item name="name" label="名称" rules={[{ required: true }]}><Input placeholder="Planning Agent" /></Form.Item>
          <Form.Item name="description" label="描述"><Input.TextArea rows={3} /></Form.Item>
        </Form>
      </Modal>
    </div>
  )
}
