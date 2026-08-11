import { useQuery } from '@tanstack/react-query'
import { Alert, Avatar, Button, Dropdown, Input, Select, Skeleton, Tooltip, message } from 'antd'
import {
  Bot,
  Check,
  ChevronDown,
  CircleStop,
  CircleSlash2,
  Clock3,
  MoreHorizontal,
  PanelLeft,
  Plus,
  Send,
  Trash2,
  UserRound,
  X,
} from 'lucide-react'
import { useEffect, useMemo, useRef, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import { Link } from 'react-router-dom'
import { ApiError, apiRequest, errorDescription } from '../api/client'
import { buildInterruptResume, streamAgent } from '../api/agui'
import type { InterruptDecision } from '../api/agui'
import type { Agent, AGUIEvent, ChatMessage, ChatSession } from '../api/types'
import { StatusTag } from '../components/StatusTag'
import { formatTime, prettyJSON } from '../lib/format'
import { loadSessions, rememberRun, saveSessions } from '../lib/storage'
import { notifyRequestError } from '../lib/notify'

interface PendingInterrupt {
  id: string
  reason?: string
  message?: string
  responseSchema?: unknown
  metadata?: Record<string, unknown>
  payloadText: string
  submitted?: boolean
}

interface SessionRuntime {
  running: boolean
  status: string
  steps: Array<{ name: string; status: string }>
  interrupts: PendingInterrupt[]
  stopNotice: boolean
}

const emptyRuntime: SessionRuntime = { running: false, status: 'idle', steps: [], interrupts: [], stopNotice: false }

function createSession(agentCode = ''): ChatSession {
  const now = new Date().toISOString()
  return { id: crypto.randomUUID(), title: '新对话', agentCode, threadId: '', messages: [], updatedAt: now }
}

export function ChatPage() {
  const agentsQuery = useQuery({ queryKey: ['active-agents'], queryFn: () => apiRequest<Agent[]>('/api/agents') })
  const activeAgents = useMemo(() => (agentsQuery.data || []).filter((agent) => agent.status === 'active'), [agentsQuery.data])
  const [sessions, setSessions] = useState<ChatSession[]>(loadSessions)
  const [activeId, setActiveId] = useState<string>(() => loadSessions()[0]?.id || '')
  const [input, setInput] = useState('')
  const [sessionPanelOpen, setSessionPanelOpen] = useState(() => window.innerWidth > 991)
  const [runtimeBySession, setRuntimeBySession] = useState<Record<string, SessionRuntime>>({})
  const controllersRef = useRef<Record<string, AbortController>>({})
  const messageViewportRef = useRef<HTMLDivElement>(null)
  const active = sessions.find((session) => session.id === activeId)
  const runtime = runtimeBySession[activeId] || emptyRuntime

  useEffect(() => { saveSessions(sessions) }, [sessions])
  useEffect(() => {
    const viewport = messageViewportRef.current
    if (viewport) viewport.scrollTo({ top: viewport.scrollHeight, behavior: 'smooth' })
  }, [active?.messages, runtime.steps, runtime.interrupts])
  useEffect(() => {
    const controllers = controllersRef.current
    return () => { Object.values(controllers).forEach((controller) => controller.abort()) }
  }, [])

  useEffect(() => {
    if (sessions.length === 0 && activeAgents.length > 0) {
      const session = createSession(activeAgents[0].agent_code)
      setSessions([session])
      setActiveId(session.id)
      setRuntimeBySession({ [session.id]: emptyRuntime })
    }
  }, [activeAgents, sessions.length])

  const updateSession = (id: string, updater: (session: ChatSession) => ChatSession) => {
    setSessions((current) => current.map((session) => session.id === id ? updater(session) : session))
  }

  const updateRuntime = (id: string, updater: (current: SessionRuntime) => SessionRuntime) => {
    setRuntimeBySession((current) => ({ ...current, [id]: updater(current[id] || emptyRuntime) }))
  }

  const newSession = () => {
    const session = createSession(activeAgents[0]?.agent_code || '')
    setSessions((current) => [session, ...current])
    setActiveId(session.id)
    setRuntimeBySession((current) => ({ ...current, [session.id]: emptyRuntime }))
  }

  const removeSession = (id: string) => {
    controllersRef.current[id]?.abort()
    delete controllersRef.current[id]
    setSessions((current) => current.filter((session) => session.id !== id))
    setRuntimeBySession((current) => { const next = { ...current }; delete next[id]; return next })
    if (id === activeId) setActiveId(sessions.find((session) => session.id !== id)?.id || '')
  }

  const handleEvent = (session: ChatSession, event: AGUIEvent, streamRunId = '') => {
    const sessionId = session.id
    if (event.type === 'RUN_STARTED') {
      updateRuntime(sessionId, (current) => ({ ...current, status: 'running' }))
      updateSession(sessionId, (session) => ({ ...session, threadId: event.threadId || session.threadId, currentRunId: event.runId || session.currentRunId, updatedAt: new Date().toISOString() }))
      if (event.runId) rememberRun({ runId: event.runId, threadId: event.threadId, agentCode: session.agentCode, status: 'running', title: session.title, visitedAt: new Date().toISOString() })
    }
    if (event.type === 'STEP_STARTED' && event.stepName) updateRuntime(sessionId, (current) => ({ ...current, steps: [...current.steps, { name: event.stepName!, status: 'running' }] }))
    if (event.type === 'STEP_FINISHED' && event.stepName) updateRuntime(sessionId, (current) => ({ ...current, steps: current.steps.map((step) => step.name === event.stepName ? { ...step, status: 'success' } : step) }))
    if (event.type === 'TEXT_MESSAGE_START' && event.messageId) {
      updateSession(sessionId, (session) => ({ ...session, messages: [...session.messages, { id: event.messageId!, role: 'assistant', content: '', pending: true, runId: event.runId || streamRunId || session.currentRunId, createdAt: new Date().toISOString() }] }))
    }
    if (event.type === 'TEXT_MESSAGE_CONTENT' && event.messageId) {
      updateSession(sessionId, (session) => {
        const exists = session.messages.some((item) => item.id === event.messageId)
        const messages = exists ? session.messages.map((item) => item.id === event.messageId ? { ...item, content: item.content + (event.delta || '') } : item) : [...session.messages, { id: event.messageId!, role: 'assistant' as const, content: event.delta || '', pending: true, runId: event.runId || streamRunId || session.currentRunId, createdAt: new Date().toISOString() }]
        return { ...session, messages }
      })
    }
    if (event.type === 'TEXT_MESSAGE_END' && event.messageId) updateSession(sessionId, (session) => ({ ...session, messages: session.messages.map((item) => item.id === event.messageId ? { ...item, pending: false } : item) }))
    if (event.type === 'RUN_FINISHED') {
      const status = event.outcome?.type === 'interrupt' ? 'waiting_input' : 'success'
      updateRuntime(sessionId, (current) => ({
        ...current,
        status,
        interrupts: (event.outcome?.interrupts || []).map((interrupt) => ({
          ...interrupt,
          payloadText: prettyJSON(interrupt.metadata?.defaultPayload || {}),
        })),
      }))
      const runId = event.runId || streamRunId || session.currentRunId
      if (runId) rememberRun({ runId, threadId: event.threadId || session.threadId, agentCode: session.agentCode, status, title: session.title, visitedAt: new Date().toISOString() })
    }
    if (event.type === 'RUN_ERROR') {
      updateRuntime(sessionId, (current) => ({ ...current, status: 'failed' }))
      const errorMessage: ChatMessage = { id: crypto.randomUUID(), role: 'assistant', content: event.message || 'Agent 运行失败', runId: event.runId || streamRunId || session.currentRunId, failed: true, createdAt: new Date().toISOString() }
      updateSession(sessionId, (session) => ({ ...session, messages: [...session.messages, errorMessage] }))
    }
  }

  const runStream = async (session: ChatSession, body: Parameters<typeof streamAgent>[1]) => {
    const controller = new AbortController()
    let streamRunId = body.runId || session.currentRunId || ''
    controllersRef.current[session.id] = controller
    updateRuntime(session.id, (current) => ({ ...current, running: true, stopNotice: false }))
    try {
      await streamAgent(session.agentCode, body, controller.signal, (event) => {
        streamRunId = event.runId || streamRunId
        handleEvent(session, event, streamRunId)
      })
    } catch (error) {
      if (controller.signal.aborted) return false
      updateRuntime(session.id, (current) => ({ ...current, status: 'failed' }))
      notifyRequestError(error)
      if (error instanceof ApiError) {
        updateSession(session.id, (current) => ({ ...current, messages: [...current.messages, { id: crypto.randomUUID(), role: 'assistant', content: errorDescription(error), runId: streamRunId || undefined, failed: true, createdAt: new Date().toISOString() }] }))
      }
      return false
    } finally {
      updateRuntime(session.id, (current) => ({ ...current, running: false }))
      if (controllersRef.current[session.id] === controller) delete controllersRef.current[session.id]
    }
    return true
  }

  const send = async () => {
    if (!active || !input.trim() || runtime.running || !active.agentCode) return
    const content = input.trim()
    setInput('')
    updateRuntime(active.id, (current) => ({ ...current, steps: [], interrupts: [], status: 'idle', stopNotice: false }))
    const userMessage: ChatMessage = { id: crypto.randomUUID(), role: 'user', content, createdAt: new Date().toISOString() }
    const nextMessages = [...active.messages.filter((item) => !item.failed), userMessage]
    const title = active.messages.length === 0 ? content.slice(0, 24) : active.title
    updateSession(active.id, (session) => ({ ...session, title, messages: nextMessages, updatedAt: new Date().toISOString() }))
    await runStream({ ...active, title, messages: nextMessages }, {
      threadId: active.threadId,
      runId: '',
      state: {},
      tools: [],
      context: [],
      messages: nextMessages.map(({ id, role, content: text }) => ({ id, role, content: text })),
    })
  }

  const stop = () => {
    controllersRef.current[activeId]?.abort()
    updateRuntime(activeId, (current) => ({ ...current, running: false, stopNotice: true, status: 'detached' }))
  }

  const resume = async (interrupt: PendingInterrupt, decision: InterruptDecision) => {
    if (!active?.currentRunId) return
    let resumeItem: ReturnType<typeof buildInterruptResume>
    try { resumeItem = buildInterruptResume(interrupt.id, interrupt.payloadText, decision) }
    catch { message.error('Payload 必须是合法 JSON 对象'); return }
    updateRuntime(active.id, (current) => ({ ...current, interrupts: current.interrupts.map((item) => item.id === interrupt.id ? { ...item, submitted: true } : item) }))
    const accepted = await runStream(active, { runId: active.currentRunId, resume: [resumeItem] })
    if (!accepted) updateRuntime(active.id, (current) => ({ ...current, interrupts: current.interrupts.map((item) => item.id === interrupt.id ? { ...item, submitted: false } : item) }))
  }

  const agentMenu = active ? (
    <Select
      className="agent-select"
      value={active.agentCode || undefined}
      placeholder="选择 Agent"
      loading={agentsQuery.isLoading}
      options={activeAgents.map((agent) => ({ value: agent.agent_code, label: agent.name }))}
      onChange={(value) => updateSession(active.id, (session) => ({ ...session, agentCode: value }))}
      suffixIcon={<ChevronDown size={14} />}
      disabled={runtime.running}
    />
  ) : null

  return (
    <div className="chat-page">
      <aside className={`chat-sessions ${sessionPanelOpen ? 'open' : ''}`}>
        <div className="chat-session-head"><div><span>CONVERSATIONS</span><strong>对话</strong></div><Tooltip title="新建对话"><Button aria-label="新建对话" type="text" icon={<Plus size={17} />} onClick={newSession} /></Tooltip></div>
        <div className="session-list">
          {sessions.map((session) => (
            <div key={session.id} className={`session-item ${session.id === activeId ? 'active' : ''}`}>
              <button className="session-open" type="button" aria-current={session.id === activeId ? 'page' : undefined} onClick={() => setActiveId(session.id)}>
                <span className="session-icon"><Bot size={15} /></span>
                <span><strong>{session.title}</strong><small>{session.agentCode || '未选择 Agent'} · {formatTime(session.updatedAt).slice(5, 16)}</small></span>
              </button>
              <Dropdown trigger={['click']} menu={{ items: [{ key: 'delete', label: '删除对话', icon: <Trash2 size={14} />, danger: true }], onClick: ({ domEvent }) => { domEvent.stopPropagation(); removeSession(session.id) } }}>
                <Tooltip title="会话操作"><Button aria-label={`管理会话 ${session.title}`} type="text" size="small" icon={<MoreHorizontal size={15} />} /></Tooltip>
              </Dropdown>
            </div>
          ))}
        </div>
        <div className="session-local-note"><Clock3 size={14} /><span>会话仅保存在当前浏览器</span></div>
      </aside>
      <main className="chat-workspace">
        <header className="chat-toolbar">
          <Button aria-label="切换会话列表" className="session-toggle" type="text" icon={<PanelLeft size={18} />} onClick={() => setSessionPanelOpen((open) => !open)} />
          <div className="chat-agent"><span>当前 Agent</span>{agentMenu}</div>
          <div className="chat-run-state">{runtime.status !== 'idle' && <StatusTag status={runtime.status === 'detached' ? 'running' : runtime.status} />}{active?.currentRunId && <Link to={`/runs/${active.currentRunId}`}>查看 Run</Link>}</div>
        </header>
        <div ref={messageViewportRef} className="message-viewport">
          {!active || active.messages.length === 0 ? (
            agentsQuery.isLoading ? <Skeleton active /> : (
              <div className="chat-empty">
                <div className="chat-empty-mark"><Bot size={30} /></div>
                <h1>向 Agent 发起一次运行</h1>
                <p>选择已发布的 Agent，输入任务。步骤、委派和人工介入会在这里实时呈现。</p>
                {activeAgents.length === 0 && <Alert type="warning" showIcon message="暂无已启用 Agent" description="请先到 Agent 管理页完成 Workflow、Capability 和 Endpoint 配置并发布。" action={<Link to="/agents">前往配置</Link>} />}
              </div>
            )
          ) : (
            <div className="message-list">
              {active.messages.map((item) => (
                <article key={item.id} className={`chat-message ${item.role} ${item.failed ? 'failed' : ''}`}>
                  <Avatar className="message-avatar" icon={item.role === 'user' ? <UserRound size={16} /> : <Bot size={16} />} />
                  <div className="message-body">
                    <div className="message-meta"><strong>{item.role === 'user' ? '你' : active.agentCode}</strong><span>{formatTime(item.createdAt).slice(11)}</span>{item.runId && <Link to={`/runs/${item.runId}`}>Run</Link>}</div>
                    <div className="message-content"><ReactMarkdown>{item.content || (item.pending ? '正在生成…' : '')}</ReactMarkdown>{item.pending && <i className="typing-caret" />}</div>
                  </div>
                </article>
              ))}
              {runtime.steps.length > 0 && (
                <div className="step-ribbon">{runtime.steps.map((step, index) => <div key={`${step.name}-${index}`}><span className={step.status}><Check size={12} /></span><code>{step.name}</code></div>)}</div>
              )}
              {runtime.interrupts.map((interrupt) => (
                <div className="interrupt-card" key={interrupt.id}>
                  <div>
                    <span>HUMAN INPUT</span>
                    <h3>{interrupt.message || '此运行需要人工确认'}</h3>
                    <p>{interrupt.reason || `Interrupt · ${interrupt.id}`}</p>
                    {(interrupt.responseSchema || interrupt.metadata) && <details><summary>查看响应约束</summary><pre>{prettyJSON({ responseSchema: interrupt.responseSchema, metadata: interrupt.metadata })}</pre></details>}
                    <label className="interrupt-payload-label" htmlFor={`interrupt-${interrupt.id}`}>响应 Payload (JSON)</label>
                    <Input.TextArea id={`interrupt-${interrupt.id}`} className="code-input" value={interrupt.payloadText} onChange={(event) => updateRuntime(active!.id, (current) => ({ ...current, interrupts: current.interrupts.map((item) => item.id === interrupt.id ? { ...item, payloadText: event.target.value } : item) }))} rows={4} disabled={interrupt.submitted} />
                  </div>
                  <div className="interrupt-actions">
                    <Button icon={<CircleSlash2 size={15} />} disabled={interrupt.submitted} onClick={() => resume(interrupt, 'cancelled')}>取消节点</Button>
                    <Button danger icon={<X size={15} />} disabled={interrupt.submitted} onClick={() => resume(interrupt, 'rejected')}>拒绝</Button>
                    <Button type="primary" icon={<Check size={15} />} disabled={interrupt.submitted} onClick={() => resume(interrupt, 'approved')}>批准</Button>
                  </div>
                </div>
              ))}
              {runtime.stopNotice && <Alert type="info" showIcon message="已停止接收事件" description="浏览器已断开观察流，但后端任务仍可能继续运行。可从 Run 详情查看最终状态。" />}
            </div>
          )}
        </div>
        <footer className="chat-composer-wrap">
          <div className="chat-composer">
            <Input.TextArea
              value={input}
              onChange={(event) => setInput(event.target.value)}
              onPressEnter={(event) => { if (!event.shiftKey) { event.preventDefault(); void send() } }}
              placeholder={active?.agentCode ? `发送任务给 ${active.agentCode}` : '请先选择 Agent'}
              autoSize={{ minRows: 1, maxRows: 6 }}
              disabled={!active?.agentCode}
            />
            {runtime.running ? <Tooltip title="停止接收，后端任务不会被取消"><Button aria-label="停止接收事件" danger type="text" icon={<CircleStop size={20} />} onClick={stop} /></Tooltip> : <Tooltip title="发送"><Button aria-label="发送消息" type="primary" icon={<Send size={18} />} disabled={!input.trim() || !active?.agentCode} onClick={() => void send()} /></Tooltip>}
          </div>
          <span>Enter 发送 · Shift + Enter 换行</span>
        </footer>
      </main>
    </div>
  )
}
