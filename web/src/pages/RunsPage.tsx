import { useQuery } from '@tanstack/react-query'
import { Button, Form, Input, InputNumber, Modal, Select, Table } from 'antd'
import { Play, RotateCw, Search } from 'lucide-react'
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { apiRequest } from '../api/client'
import type { Agent, RunListItem } from '../api/types'
import { EmptyPanel } from '../components/EmptyPanel'
import { PageHeader } from '../components/PageHeader'
import { RequestErrorAlert } from '../components/RequestErrorAlert'
import { StatusTag } from '../components/StatusTag'
import { formatTime, shortId } from '../lib/format'
import { isFormValidationError, notifyRequestError } from '../lib/notify'

const statusOptions = ['pending', 'queued', 'running', 'waiting_external', 'waiting_input', 'success', 'failed', 'cancelled']

export function RunsPage() {
  const [statusFilter, setStatusFilter] = useState<string>()
  const [agentFilter, setAgentFilter] = useState<string>()
  const [createOpen, setCreateOpen] = useState(false)
  const [creating, setCreating] = useState(false)
  const [formError, setFormError] = useState<unknown>(null)
  const [form] = Form.useForm()
  const navigate = useNavigate()

  const agentsQuery = useQuery({ queryKey: ['published-agents'], queryFn: () => apiRequest<Agent[]>('/api/agents/published') })
  const runsQuery = useQuery({
    queryKey: ['runs', statusFilter, agentFilter],
    queryFn: () => {
      const params = new URLSearchParams({ limit: '100' })
      if (statusFilter) params.set('status', statusFilter)
      if (agentFilter) params.set('agent_code', agentFilter)
      return apiRequest<RunListItem[]>(`/api/runs?${params.toString()}`)
    },
    refetchInterval: 10_000,
  })
  const runs = runsQuery.data || []
  const agents = agentsQuery.data || []

  const createRun = async (values: { agent_code: string; workflow_version?: number; input?: string }) => {
    setFormError(null)
    setCreating(true)
    try {
      let input: unknown = {}
      if (values.input?.trim()) input = JSON.parse(values.input)
      const result = await apiRequest<{ run_id: string; status: string }>('/api/runs', {
        method: 'POST',
        headers: { 'Idempotency-Key': crypto.randomUUID() },
        body: { agent_code: values.agent_code, workflow_version: Number(values.workflow_version ?? 0), trigger_type: 'manual', input },
      })
      setCreateOpen(false)
      navigate(`/runs/${result.run_id}`)
    } catch (error) {
      if (isFormValidationError(error)) setFormError(error)
      else notifyRequestError(error)
    } finally {
      setCreating(false)
    }
  }

  return (
    <div className="page-shell">
      <PageHeader
        title="运行记录"
        description="当前账号可见的全部 Run，由服务端持久化，每 10 秒自动刷新。"
        actions={<Button type="primary" icon={<Play size={16} />} onClick={() => { setFormError(null); setCreateOpen(true) }}>手动触发</Button>}
      />
      <div className="run-search-strip">
        <Search size={17} />
        <Input.Search placeholder="输入完整 Run ID 直接打开" enterButton="打开" onSearch={(value) => value.trim() && navigate(`/runs/${encodeURIComponent(value.trim())}`)} />
        <Select
          allowClear
          placeholder="按状态过滤"
          style={{ minWidth: 150 }}
          value={statusFilter}
          onChange={setStatusFilter}
          options={statusOptions.map((status) => ({ value: status, label: status }))}
        />
        <Select
          allowClear
          showSearch
          placeholder="按 Agent 过滤"
          style={{ minWidth: 190 }}
          value={agentFilter}
          onChange={setAgentFilter}
          options={agents.map((agent) => ({ value: agent.agent_code, label: `${agent.name} (${agent.agent_code})` }))}
        />
        <Button icon={<RotateCw size={15} />} loading={runsQuery.isFetching} onClick={() => void runsQuery.refetch()}>刷新</Button>
      </div>
      <section className="content-section">
        <div className="section-heading"><div><h2>Run 列表</h2><span>{runs.length} 条（最多显示 100 条）</span></div></div>
        {runsQuery.isLoading ? null : runs.length === 0 ? (
          <EmptyPanel title="还没有运行记录" description="从对话工作台发起任务，或手动触发一个 Run。" action={<Button type="primary" onClick={() => navigate('/chat')}>前往对话</Button>} />
        ) : (
          <Table
            rowKey="run_id"
            dataSource={runs}
            loading={runsQuery.isLoading}
            pagination={{ pageSize: 15, hideOnSinglePage: true }}
            onRow={(record) => ({ onClick: () => navigate(`/runs/${record.run_id}`) })}
            columns={[
              { title: 'Run ID', dataIndex: 'run_id', render: (value: string) => <code title={value}>{shortId(value, 18)}</code> },
              { title: 'Agent', dataIndex: 'agent_code', render: (value: string, record) => value ? <span className="run-agent-cell"><strong>{record.agent_name || value}</strong><code>{value}</code></span> : '—' },
              { title: '触发方式', dataIndex: 'trigger_type', render: (value: string) => <code>{value || '—'}</code> },
              { title: '状态', dataIndex: 'status', render: (value: string) => <StatusTag status={value} /> },
              { title: '当前步骤', dataIndex: 'current_step', render: (value: string, record) => record.error_message ? <span className="table-error" title={record.error_message}>{record.error_message.slice(0, 40)}</span> : (value ? <code>{value}</code> : '—') },
              { title: '创建时间', dataIndex: 'created_at', render: formatTime },
              { title: '结束时间', dataIndex: 'finished_at', render: (value: string | null) => value ? formatTime(value) : '—' },
            ]}
          />
        )}
      </section>
      <Modal title="手动触发 Run" open={createOpen} onCancel={() => setCreateOpen(false)} onOk={() => form.submit()} confirmLoading={creating} okText="触发运行">
        <Form form={form} layout="vertical" onFinish={createRun} initialValues={{ workflow_version: 0, input: '{}' }}>
          {Boolean(formError) && <RequestErrorAlert error={formError} />}
          <Form.Item name="agent_code" label="Agent" rules={[{ required: true }]}>
            <Select showSearch options={agents.map((agent) => ({ value: agent.agent_code, label: `${agent.name} (${agent.agent_code})` }))} />
          </Form.Item>
          <Form.Item name="workflow_version" label="Workflow 版本"><InputNumber min={0} precision={0} style={{ width: '100%' }} /></Form.Item>
          <Form.Item name="input" label="输入 JSON" rules={[{ validator: async (_, value) => { if (value) JSON.parse(value) } }]}><Input.TextArea className="code-input" rows={7} /></Form.Item>
        </Form>
      </Modal>
    </div>
  )
}
