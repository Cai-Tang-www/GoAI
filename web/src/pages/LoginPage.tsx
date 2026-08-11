import { Alert, Button, Form, Input, Segmented } from 'antd'
import { ArrowRight, Braces, KeyRound, Mail, UserRound } from 'lucide-react'
import { useState } from 'react'
import { Navigate, useLocation, useNavigate } from 'react-router-dom'
import { errorDescription } from '../api/client'
import { useAuth } from '../auth/AuthContext'

export function LoginPage() {
  const [mode, setMode] = useState<'login' | 'register'>('login')
  const [error, setError] = useState<string>('')
  const [submitting, setSubmitting] = useState(false)
  const { token, login, register } = useAuth()
  const navigate = useNavigate()
  const location = useLocation()

  if (token) return <Navigate to="/chat" replace />

  const submit = async (values: { username: string; email?: string; password: string }) => {
    setSubmitting(true)
    setError('')
    try {
      if (mode === 'login') await login(values.username, values.password)
      else await register({ username: values.username, email: values.email || '', password: values.password })
      const from = (location.state as { from?: { pathname?: string } } | null)?.from?.pathname
      navigate(from || '/chat', { replace: true })
    } catch (caught) {
      setError(errorDescription(caught))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="auth-page">
      <section className="auth-context">
        <div className="auth-brand"><Braces size={22} /><span>GoAI</span></div>
        <div className="auth-copy">
          <span className="auth-kicker">MULTI-AGENT RUNTIME</span>
          <h1>让协作过程<br />清晰可见。</h1>
          <p>连接 Agent，观察每一次委派、等待与恢复，在同一个工作台里完成构建和运行。</p>
        </div>
        <div className="auth-signal" aria-hidden="true">
          <div className="signal-line"><i /><span>AG-UI Gateway</span><b>online</b></div>
          <div className="signal-line"><i /><span>A2A Runtime</span><b>ready</b></div>
          <div className="signal-line"><i /><span>Workflow Engine</span><b>listening</b></div>
        </div>
      </section>
      <main className="auth-form-wrap">
        <div className="auth-form">
          <div className="auth-form-heading">
            <div className="brand-mark"><Braces size={19} /></div>
            <div><h2>{mode === 'login' ? '登录控制台' : '创建账户'}</h2><p>{mode === 'login' ? '继续管理你的 Agent 运行时' : '注册后默认获得 member 权限'}</p></div>
          </div>
          <Segmented block value={mode} options={[{ label: '登录', value: 'login' }, { label: '注册', value: 'register' }]} onChange={(value) => { setMode(value as 'login' | 'register'); setError('') }} />
          {error && <Alert type="error" showIcon message={error} />}
          <Form layout="vertical" requiredMark={false} onFinish={submit} autoComplete="on">
            <Form.Item name="username" label="用户名" rules={[{ required: true, message: '请输入用户名' }, { min: 3, message: '至少 3 个字符' }]}>
              <Input size="large" prefix={<UserRound size={16} />} placeholder="your-name" autoComplete="username" />
            </Form.Item>
            {mode === 'register' && (
              <Form.Item name="email" label="邮箱" rules={[{ required: true, message: '请输入邮箱' }, { type: 'email', message: '邮箱格式不正确' }]}>
                <Input size="large" prefix={<Mail size={16} />} placeholder="name@example.com" autoComplete="email" />
              </Form.Item>
            )}
            <Form.Item name="password" label="密码" rules={[{ required: true, message: '请输入密码' }, { min: 8, message: '至少 8 个字符' }]}>
              <Input.Password size="large" prefix={<KeyRound size={16} />} placeholder="至少 8 个字符" autoComplete={mode === 'login' ? 'current-password' : 'new-password'} />
            </Form.Item>
            <Button className="auth-submit" type="primary" htmlType="submit" size="large" loading={submitting} icon={<ArrowRight size={17} />} iconPosition="end">
              {mode === 'login' ? '进入控制台' : '注册并登录'}
            </Button>
          </Form>
          <div className="auth-footnote">API 经本地代理访问 · 凭据仅保存在当前浏览器</div>
        </div>
      </main>
    </div>
  )
}
