import { Button, Form, Input, Modal, Select, Table, message } from 'antd'
import { Eraser, Play, Search } from 'lucide-react'
import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { apiRequest, errorDescription } from '../api/client'
import type { Agent, RecentRun } from '../api/types'
import { EmptyPanel } from '../components/EmptyPanel'
import { PageHeader } from '../components/PageHeader'
import { StatusTag } from '../components/StatusTag'
import { formatTime, shortId } from '../lib/format'
import { clearRecentRuns, loadRecentRuns, rememberRun } from '../lib/storage'

export function RunsPage() {
  const [runs, setRuns] = useState<RecentRun[]>(loadRecentRuns)
  const [agents, setAgents] = useState<Agent[]>([])
  const [createOpen, setCreateOpen] = useState(false)
  const [creating, setCreating] = useState(false)
  const [form] = Form.useForm()
  const navigate = useNavigate()

  useEffect(() => {
    const sync = () => setRuns(loadRecentRuns())
    window.addEventListener('goai:runs-changed', sync)
    apiRequest<Agent[]>('/api/agents').then(setAgents).catch(() => undefined)
    return () => window.removeEventListener('goai:runs-changed', sync)
  }, [])

  const createRun = async (values: { agent_code: string; workflow_version?: number; input?: string }) => {
    setCreating(true)
    try {
      let input: unknown = {}
      if (values.input?.trim()) input = JSON.parse(values.input)
      const result = await apiRequest<{ run_id: string; status: string }>('/api/runs', {
        method: 'POST',
        headers: { 'Idempotency-Key': crypto.randomUUID() },
        body: { agent_code: values.agent_code, workflow_version: values.workflow_version || 0, trigger_type: 'manual', input },
      })
      rememberRun({ runId: result.run_id, agentCode: values.agent_code, status: result.status, title: '手动触发', visitedAt: new Date().toISOString() })
      setCreateOpen(false)
      navigate(`/runs/${result.run_id}`)
    } catch (error) {
      message.error(errorDescription(error))
    } finally {
      setCreating(false)
    }
  }

  return (
    <div className="page-shell">
      <PageHeader
        title="运行记录"
        description="后端不提供 Run 列表，此处保存由当前浏览器发起或访问过的运行。"
        actions={<Button type="primary" icon={<Play size={16} />} onClick={() => setCreateOpen(true)}>手动触发</Button>}
      />
      <div className="run-search-strip">
        <Search size={17} />
        <Input.Search placeholder="输入完整 Run ID 直接打开" enterButton="打开" onSearch={(value) => value.trim() && navigate(`/runs/${encodeURIComponent(value.trim())}`)} />
      </div>
      <section className="content-section">
        <div className="section-heading"><div><h2>最近访问</h2><span>{runs.length} 条本地记录</span></div>{runs.length > 0 && <Button type="text" danger icon={<Eraser size={15} />} onClick={clearRecentRuns}>清空</Button>}</div>
        {runs.length === 0 ? (
          <EmptyPanel title="还没有运行记录" description="从对话工作台发起任务，或通过 Run ID 直接打开。" action={<Button type="primary" onClick={() => navigate('/chat')}>前往对话</Button>} />
        ) : (
          <Table
            rowKey="runId"
            dataSource={runs}
            pagination={{ pageSize: 10, hideOnSinglePage: true }}
            onRow={(record) => ({ onClick: () => navigate(`/runs/${record.runId}`) })}
            columns={[
              { title: 'Run ID', dataIndex: 'runId', render: (value: string) => <code title={value}>{shortId(value, 18)}</code> },
              { title: '来源', dataIndex: 'title', render: (value: string) => value || '运行' },
              { title: 'Agent', dataIndex: 'agentCode', render: (value: string) => value ? <code>{value}</code> : '—' },
              { title: '状态', dataIndex: 'status', render: (value: string) => <StatusTag status={value} /> },
              { title: '最近访问', dataIndex: 'visitedAt', render: formatTime },
            ]}
          />
        )}
      </section>
      <Modal title="手动触发 Run" open={createOpen} onCancel={() => setCreateOpen(false)} onOk={() => form.submit()} confirmLoading={creating} okText="触发运行">
        <Form form={form} layout="vertical" onFinish={createRun} initialValues={{ workflow_version: 0, input: '{}' }}>
          <Form.Item name="agent_code" label="Agent" rules={[{ required: true }]}>
            <Select showSearch options={agents.map((agent) => ({ value: agent.agent_code, label: `${agent.name} (${agent.agent_code})` }))} />
          </Form.Item>
          <Form.Item name="workflow_version" label="Workflow 版本"><Input type="number" min={0} /></Form.Item>
          <Form.Item name="input" label="输入 JSON" rules={[{ validator: async (_, value) => { if (value) JSON.parse(value) } }]}><Input.TextArea className="code-input" rows={7} /></Form.Item>
        </Form>
      </Modal>
    </div>
  )
}
