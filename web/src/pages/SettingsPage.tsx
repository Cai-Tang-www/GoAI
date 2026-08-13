import { Alert, Avatar, Button, Form, Input, message } from 'antd'
import { KeyRound, Save, UserRound } from 'lucide-react'
import { useEffect, useState } from 'react'
import { apiRequest } from '../api/client'
import { useAuth } from '../auth/AuthContext'
import { PageHeader } from '../components/PageHeader'
import { RequestErrorAlert } from '../components/RequestErrorAlert'
import { formatTime } from '../lib/format'
import { isFormValidationError, notifyRequestError } from '../lib/notify'

export function SettingsPage() {
  const { user, refreshUser } = useAuth()
  const [loading, setLoading] = useState(false)
  const [formError, setFormError] = useState<unknown>(null)
  const [form] = Form.useForm()
  useEffect(() => { form.setFieldsValue({ email: user?.email }) }, [form, user])
  const save = async (values: { email: string; password?: string }) => { if (!user) return; setLoading(true); setFormError(null); try { await apiRequest(`/api/users/${user.id}`, { method: 'PUT', body: { email: values.email, ...(values.password ? { password: values.password } : {}) } }); message.success('个人资料已更新'); form.setFieldValue('password', ''); await refreshUser() } catch (error) { if (isFormValidationError(error)) setFormError(error); else notifyRequestError(error) } finally { setLoading(false) } }
  return <div className="page-shell settings-page"><PageHeader title="个人设置" description="更新当前账户资料和登录密码。" /><div className="settings-layout"><aside className="settings-profile"><Avatar size={72} icon={<UserRound size={30} />} /><h2>{user?.username}</h2><code>User #{user?.id}</code><dl><div><dt>邮箱</dt><dd>{user?.email}</dd></div><div><dt>创建时间</dt><dd>{formatTime(user?.created_at)}</dd></div></dl></aside><section className="content-section"><div className="section-heading"><div><h2>账户资料</h2><span>用户名不可在当前版本修改</span></div></div><Form form={form} layout="vertical" onFinish={save} requiredMark={false}>{Boolean(formError) && <RequestErrorAlert error={formError} />}<Form.Item label="用户名"><Input value={user?.username} disabled /></Form.Item><Form.Item name="email" label="邮箱" rules={[{ required: true }, { type: 'email' }]}><Input /></Form.Item><Form.Item name="password" label="新密码" extra="留空表示不修改密码" rules={[{ min: 8, message: '至少 8 个字符' }]}><Input.Password prefix={<KeyRound size={15} />} /></Form.Item><Alert type="info" showIcon message="JWT 有效期为 24 小时；修改密码不会自动退出其他已签发会话。" /><Button className="settings-save" type="primary" htmlType="submit" icon={<Save size={15} />} loading={loading}>保存修改</Button></Form></section></div></div>
}
