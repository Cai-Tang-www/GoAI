import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Button, Form, Input, Modal, Select, Table, message } from 'antd'
import { Plus, Server, Wrench } from 'lucide-react'
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { apiRequest, errorDescription } from '../api/client'
import type { MCPServer } from '../api/types'
import { EmptyPanel } from '../components/EmptyPanel'
import { ErrorState } from '../components/ErrorState'
import { PageHeader } from '../components/PageHeader'
import { StatusTag } from '../components/StatusTag'
import { formatTime } from '../lib/format'

export function MCPPage() {
  const query = useQuery({ queryKey: ['mcp-servers'], queryFn: () => apiRequest<MCPServer[]>('/api/mcp/servers') })
  const client = useQueryClient()
  const navigate = useNavigate()
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [form] = Form.useForm()

  const create = async (values: Record<string, unknown>) => {
    setLoading(true)
    try { await apiRequest('/api/mcp/servers', { method: 'POST', body: values }); message.success('MCP Server 已创建'); setOpen(false); await client.invalidateQueries({ queryKey: ['mcp-servers'] }); navigate(`/mcp/${values.server_code}`) } catch (error) { message.error(errorDescription(error)) } finally { setLoading(false) }
  }

  return <div className="page-shell"><PageHeader title="MCP 服务" description="管理 Agent 可调用的外部工具服务和 discovery 快照。" actions={<Button type="primary" icon={<Plus size={16} />} onClick={() => { form.resetFields(); form.setFieldsValue({ transport: 'streamable_http', auth_type: 'none', config_json: '{}' }); setOpen(true) }}>接入服务</Button>} />{query.error ? <ErrorState error={query.error} onRetry={() => query.refetch()} /> : <section className="content-section"><div className="section-heading"><div><h2>Servers</h2><span>{query.data?.length || 0} 个接入点</span></div><Wrench size={18} /></div>{!query.isLoading && query.data?.length === 0 ? <EmptyPanel title="还没有 MCP 服务" description="接入支持 Streamable HTTP 的 MCP Server。" /> : <Table rowKey="server_code" loading={query.isLoading} dataSource={query.data} onRow={(record) => ({ onClick: () => navigate(`/mcp/${record.server_code}`) })} columns={[{ title: '服务', render: (_, item) => <div className="table-primary"><span className="agent-glyph"><Server size={16} /></span><span><strong>{item.name}</strong><code>{item.server_code}</code></span></div> }, { title: 'Endpoint', dataIndex: 'endpoint', ellipsis: true, render: (value) => <code>{value}</code> }, { title: '状态', dataIndex: 'status', render: (value) => <StatusTag status={value} /> }, { title: '工具版本', dataIndex: 'config_version', render: (value) => `v${value}` }, { title: '健康检查', dataIndex: 'last_healthy_at', render: formatTime }]} />}</section>}<Modal title="接入 MCP Server" open={open} onCancel={() => setOpen(false)} onOk={() => form.submit()} confirmLoading={loading} okText="创建"><Form form={form} layout="vertical" onFinish={create} initialValues={{ transport: 'streamable_http', auth_type: 'none', config_json: '{}' }} requiredMark={false}><Form.Item name="server_code" label="Server Code" rules={[{ required: true }]}><Input className="code-input" /></Form.Item><Form.Item name="name" label="名称" rules={[{ required: true }]}><Input /></Form.Item><Form.Item name="description" label="描述"><Input.TextArea rows={2} /></Form.Item><Form.Item name="endpoint" label="Endpoint" rules={[{ required: true }, { type: 'url' }]}><Input placeholder="https://mcp.example.com/mcp" /></Form.Item><div className="form-grid"><Form.Item name="transport" label="Transport"><Select options={[{ value: 'streamable_http' }]} /></Form.Item><Form.Item name="auth_type" label="认证"><Select options={['none', 'bearer'].map((value) => ({ value }))} /></Form.Item></div><Form.Item name="credential_ref" label="凭据引用"><Input placeholder="服务端凭据引用，不填写真实 token" /></Form.Item><Form.Item name="config_json" label="非敏感配置 JSON" extra="不要在这里填写 secret、token 或 password。" rules={[{ validator: async (_, value) => { if (value) JSON.parse(value) } }]}><Input.TextArea className="code-input" rows={4} /></Form.Item></Form></Modal></div>
}
