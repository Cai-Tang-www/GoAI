import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, Form, Input, Modal, Popconfirm, Table, message } from 'antd'
import { Plus, Trash2, Users } from 'lucide-react'
import { useState } from 'react'
import { ApiError, apiRequest } from '../api/client'
import type { User } from '../api/types'
import { ErrorState } from '../components/ErrorState'
import { PageHeader } from '../components/PageHeader'
import { RequestErrorAlert } from '../components/RequestErrorAlert'
import { formatTime } from '../lib/format'
import { isFormValidationError, notifyRequestError } from '../lib/notify'

export function UsersPage() {
  const query = useQuery({ queryKey: ['users'], queryFn: () => apiRequest<User[]>('/api/users'), retry: false })
  const client = useQueryClient()
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [formError, setFormError] = useState<unknown>(null)
  const [form] = Form.useForm()
  const create = async (values: Record<string, unknown>) => { setFormError(null); setLoading(true); try { await apiRequest('/api/users', { method: 'POST', body: values }); message.success('用户已创建'); setOpen(false); form.resetFields(); await client.invalidateQueries({ queryKey: ['users'] }) } catch (error) { if (isFormValidationError(error)) setFormError(error); else notifyRequestError(error) } finally { setLoading(false) } }
  const remove = async (id: number) => { try { await apiRequest(`/api/users/${id}`, { method: 'DELETE' }); message.success('用户已删除'); await client.invalidateQueries({ queryKey: ['users'] }) } catch (error) { notifyRequestError(error) } }
  const forbiddenError = query.error instanceof ApiError && query.error.status === 403 ? query.error : null
  return <div className="page-shell"><PageHeader title="用户管理" description="管理平台登录用户。member 账户没有此页面的操作权限。" actions={!forbiddenError && <Button type="primary" icon={<Plus size={16} />} onClick={() => { setFormError(null); setOpen(true) }}>新建用户</Button>} />{forbiddenError ? <Alert className="permission-panel" type="warning" showIcon message="没有用户管理权限" description={<span>当前账户可以继续使用对话和自己名下的 Agent、Workflow、MCP 资源。<br /><code>{forbiddenError.traceId}</code></span>} /> : query.error ? <ErrorState error={query.error} onRetry={() => query.refetch()} /> : <section className="content-section"><div className="section-heading"><div><h2>Users</h2><span>{query.data?.length || 0} 个账户</span></div><Users size={18} /></div><Table rowKey="id" loading={query.isLoading} dataSource={query.data} columns={[{ title: '用户', render: (_, item) => <div className="table-primary"><span><strong>{item.username}</strong><code>#{item.id}</code></span></div> }, { title: '邮箱', dataIndex: 'email' }, { title: '创建时间', dataIndex: 'created_at', render: formatTime }, { title: '操作', width: 100, render: (_, item) => <Popconfirm title={`删除用户 ${item.username}？`} onConfirm={() => remove(item.id)}><Button aria-label={`删除用户 ${item.username}`} type="text" danger icon={<Trash2 size={15} />} /></Popconfirm> }]} /></section>}<Modal title="新建用户" open={open} onCancel={() => setOpen(false)} onOk={() => form.submit()} confirmLoading={loading}><Form form={form} layout="vertical" onFinish={create}>{Boolean(formError) && <RequestErrorAlert error={formError} />}<Form.Item name="username" label="用户名" rules={[{ required: true }, { min: 3 }]}><Input /></Form.Item><Form.Item name="email" label="邮箱" rules={[{ required: true }, { type: 'email' }]}><Input /></Form.Item><Form.Item name="password" label="初始密码" rules={[{ required: true }, { min: 8 }]}><Input.Password /></Form.Item></Form></Modal></div>
}
