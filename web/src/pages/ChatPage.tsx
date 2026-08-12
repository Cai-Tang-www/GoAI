import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Avatar, Button, Input, Select, Skeleton, Tooltip, message } from 'antd'
import {
  Bot,
  Check,
  ChevronDown,
  CircleStop,
  CircleSlash2,
  Plus,
  PanelLeft,
  Send,
  UserRound,
  X,
} from 'lucide-react'
import { useEffect, useMemo, useRef, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import { Link } from 'react-router-dom'
import { ApiError, apiRequest, errorDescription } from '../api/client'
import { buildInterruptResume, streamAgent } from '../api/agui'
import type { InterruptDecision, RunAgentInput } from '../api/agui'
import type { Agent, AGUIEvent, ChatMessage, ThreadMessage, ThreadSummary } from '../api/types'
import { StatusTag } from '../components/StatusTag'
import { formatTime, prettyJSON } from '../lib/format'
import { notifyRequestError } from '../lib/notify'

const DRAFT_KEY = 'draft'

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
  currentRunId?: string
  agentCode?: string
  live: ChatMessage[]
}

const emptyRuntime: SessionRuntime = { running: false, status: 'idle', steps: [], interrupts: [], stopNotice: false, live: [] }

function toChatMessage(item: ThreadMessage): ChatMessage | null {
  if (item.message_type !== 'input' && item.message_type !== 'result') return null
  if (item.role !== 'user' && item.role !== 'assistant') return null
  return {
    id: item.message_id,
    role: item.role,
    content: item.content,
    runId: item.run_id || undefined,
    createdAt: item.created_at,
  }
}

export function ChatPage() {
  const queryClient = useQueryClient()
  const agentsQuery = useQuery({ queryKey: ['published-agents'], queryFn: () => apiRequest<Agent[]>('/api/agents/published') })
  const threadsQuery = useQuery({ queryKey: ['threads'], queryFn: () => apiRequest<ThreadSummary[]>('/api/threads') })
  const publishedAgents = agentsQuery.data || []
  const threads = useMemo(() => threadsQuery.data || [], [threadsQuery.data])

  const [activeKey, setActiveKey] = useState<string>(DRAFT_KEY)
  const [draftAgent, setDraftAgent] = useState('')
  const [input, setInput] = useState('')
  const [sessionPanelOpen, setSessionPanelOpen] = useState(() => window.innerWidth > 991)
  const [runtimeByKey, setRuntimeByKey] = useState<Record<string, SessionRuntime>>({})
  const controllersRef = useRef<Record<string, AbortController>>({})
  const messageViewportRef = useRef<HTMLDivElement>(null)

  const activeThread = threads.find((thread) => thread.thread_id === activeKey)
  const runtime = runtimeByKey[activeKey] || emptyRuntime
  const isDraft = activeKey === DRAFT_KEY
  const agentCode = runtime.agentCode || (isDraft ? draftAgent : activeThread?.agent_code) || ''

  const messagesQuery = useQuery({
    queryKey: ['thread-messages', activeKey],
    queryFn: () => apiRequest<ThreadMessage[]>(`/api/threads/${encodeURIComponent(activeKey)}/messages`),
    enabled: !isDraft,
    staleTime: 30_000,
  })

  const displayMessages = useMemo(() => {
    const live = runtime.live
    const liveIds = new Set(live.map((item) => item.id))
    const liveRunIds = new Set(live.filter((item) => item.role === 'assistant' && item.runId).map((item) => item.runId))
    const history = (messagesQuery.data || [])
      .map(toChatMessage)
      .filter((item): item is ChatMessage => item !== null)
      .filter((item) => !liveIds.has(item.id) && !(item.role === 'assistant' && item.runId && liveRunIds.has(item.runId)))
    return [...history, ...live]
  }, [messagesQuery.data, runtime.live])

  useEffect(() => {
    const viewport = messageViewportRef.current
    if (viewport) viewport.scrollTo({ top: viewport.scrollHeight, behavior: 'smooth' })
  }, [displayMessages, runtime.steps, runtime.interrupts])

  useEffect(() => {
    const controllers = controllersRef.current
    return () => { Object.values(controllers).forEach((controller) => controller.abort()) }
  }, [])

  useEffect(() => {
    if (!draftAgent && publishedAgents.length > 0) setDraftAgent(publishedAgents[0].agent_code)
  }, [draftAgent, publishedAgents])

  const updateRuntime = (key: string, updater: (current: SessionRuntime) => SessionRuntime) => {
    setRuntimeByKey((current) => ({ ...current, [key]: updater(current[key] || emptyRuntime) }))
  }

  const newConversation = () => {
    setActiveKey(DRAFT_KEY)
    setRuntimeByKey((current) => ({ ...current, [DRAFT_KEY]: { ...emptyRuntime, agentCode: current[DRAFT_KEY]?.agentCode } }))
  }

  // 草稿会话在 RUN_STARTED 拿到 threadId 后迁移为服务端会话，运行态与控制器一并转移。
  const migrateDraft = (threadId: string) => {
    setRuntimeByKey((current) => {
      const next = { ...current }
      next[threadId] = current[DRAFT_KEY] || emptyRuntime
      next[DRAFT_KEY] = emptyRuntime
      return next
    })
    const controller = controllersRef.current[DRAFT_KEY]
    if (controller) {
      controllersRef.current[threadId] = controller
      delete controllersRef.current[DRAFT_KEY]
    }
    setActiveKey((current) => (current === DRAFT_KEY ? threadId : current))
  }

  const handleEvent = (keyRef: { current: string }, agent: string, event: AGUIEvent, streamRunId: string) => {
    if (event.type === 'RUN_STARTED') {
      if (keyRef.current === DRAFT_KEY && event.threadId) {
        migrateDraft(event.threadId)
        keyRef.current = event.threadId
      }
      updateRuntime(keyRef.current, (current) => ({ ...current, status: 'running', agentCode: agent, currentRunId: event.runId || current.currentRunId }))
      void queryClient.invalidateQueries({ queryKey: ['threads'] })
    }
    const key = keyRef.current
    if (event.type === 'STEP_STARTED' && event.stepName) updateRuntime(key, (current) => ({ ...current, steps: [...current.steps, { name: event.stepName!, status: 'running' }] }))
    if (event.type === 'STEP_FINISHED' && event.stepName) updateRuntime(key, (current) => ({ ...current, steps: current.steps.map((step) => step.name === event.stepName ? { ...step, status: 'success' } : step) }))
    if (event.type === 'TEXT_MESSAGE_START' && event.messageId) {
      updateRuntime(key, (current) => ({ ...current, live: [...current.live, { id: event.messageId!, role: 'assistant', content: '', pending: true, runId: event.runId || streamRunId || current.currentRunId, createdAt: new Date().toISOString() }] }))
    }
    if (event.type === 'TEXT_MESSAGE_CONTENT' && event.messageId) {
      updateRuntime(key, (current) => {
        const exists = current.live.some((item) => item.id === event.messageId)
        const live = exists
          ? current.live.map((item) => item.id === event.messageId ? { ...item, content: item.content + (event.delta || '') } : item)
          : [...current.live, { id: event.messageId!, role: 'assistant' as const, content: event.delta || '', pending: true, runId: event.runId || streamRunId || current.currentRunId, createdAt: new Date().toISOString() }]
        return { ...current, live }
      })
    }
    if (event.type === 'TEXT_MESSAGE_END' && event.messageId) updateRuntime(key, (current) => ({ ...current, live: current.live.map((item) => item.id === event.messageId ? { ...item, pending: false } : item) }))
    if (event.type === 'RUN_FINISHED') {
      const status = event.outcome?.type === 'interrupt' ? 'waiting_input' : 'success'
      updateRuntime(key, (current) => ({
        ...current,
        status,
        interrupts: (event.outcome?.interrupts || []).map((interrupt) => ({
          ...interrupt,
          payloadText: prettyJSON(interrupt.metadata?.defaultPayload || {}),
        })),
      }))
      void queryClient.invalidateQueries({ queryKey: ['threads'] })
    }
    if (event.type === 'RUN_ERROR') {
      updateRuntime(key, (current) => ({
        ...current,
        status: 'failed',
        live: [...current.live, { id: crypto.randomUUID(), role: 'assistant', content: event.message || 'Agent 运行失败', runId: event.runId || streamRunId || current.currentRunId, failed: true, createdAt: new Date().toISOString() }],
      }))
      void queryClient.invalidateQueries({ queryKey: ['threads'] })
    }
  }

  const runStream = async (startKey: string, agent: string, body: RunAgentInput) => {
    const controller = new AbortController()
    const keyRef = { current: startKey }
    let streamRunId = body.runId || ''
    controllersRef.current[startKey] = controller
    updateRuntime(startKey, (current) => ({ ...current, running: true, stopNotice: false }))
    try {
      await streamAgent(agent, body, controller.signal, (event) => {
        streamRunId = event.runId || streamRunId
        handleEvent(keyRef, agent, event, streamRunId)
      })
    } catch (error) {
      if (controller.signal.aborted) return false
      updateRuntime(keyRef.current, (current) => ({ ...current, status: 'failed' }))
      notifyRequestError(error)
      if (error instanceof ApiError) {
        updateRuntime(keyRef.current, (current) => ({ ...current, live: [...current.live, { id: crypto.randomUUID(), role: 'assistant', content: errorDescription(error), runId: streamRunId || undefined, failed: true, createdAt: new Date().toISOString() }] }))
      }
      return false
    } finally {
      // 仅当自己仍是该会话的活跃流时才清理运行态，避免停止后重发的新流被旧流误关。
      const key = keyRef.current
      if (controllersRef.current[key] === controller) {
        updateRuntime(key, (current) => ({ ...current, running: false }))
        delete controllersRef.current[key]
      }
    }
    return true
  }

  const send = async () => {
    if (!input.trim() || runtime.running || !agentCode) return
    const content = input.trim()
    setInput('')
    const userMessage: ChatMessage = { id: crypto.randomUUID(), role: 'user', content, createdAt: new Date().toISOString() }
    const startKey = activeKey
    const historyBefore = displayMessages.filter((item) => !item.failed)
    updateRuntime(startKey, (current) => ({
      ...current,
      steps: [],
      interrupts: [],
      status: 'idle',
      agentCode,
      live: [...current.live.filter((item) => !item.failed), userMessage],
    }))
    await runStream(startKey, agentCode, {
      threadId: isDraft ? '' : activeKey,
      runId: '',
      state: {},
      tools: [],
      context: [],
      messages: [...historyBefore, userMessage].map(({ id, role, content: text }) => ({ id, role, content: text })),
    })
  }

  const stop = () => {
    controllersRef.current[activeKey]?.abort()
    delete controllersRef.current[activeKey]
    updateRuntime(activeKey, (current) => ({ ...current, running: false, stopNotice: true, status: 'detached' }))
  }

  const resume = async (interrupt: PendingInterrupt, decision: InterruptDecision) => {
    if (!runtime.currentRunId || !agentCode) return
    let resumeItem: ReturnType<typeof buildInterruptResume>
    try { resumeItem = buildInterruptResume(interrupt.id, interrupt.payloadText, decision) }
    catch { message.error('Payload 必须是合法 JSON 对象'); return }
    const key = activeKey
    updateRuntime(key, (current) => ({ ...current, interrupts: current.interrupts.map((item) => item.id === interrupt.id ? { ...item, submitted: true } : item) }))
    const accepted = await runStream(key, agentCode, { runId: runtime.currentRunId, resume: [resumeItem] })
    if (!accepted) updateRuntime(key, (current) => ({ ...current, interrupts: current.interrupts.map((item) => item.id === interrupt.id ? { ...item, submitted: false } : item) }))
  }

  const selectThread = (threadId: string) => {
    setActiveKey(threadId)
    setSessionPanelOpen(window.innerWidth > 991)
  }

  const currentRunId = runtime.currentRunId || (!isDraft ? activeThread?.last_run_id : undefined)
  const emptyState = displayMessages.length === 0 && !(!isDraft && messagesQuery.isLoading)

  return (
    <div className="chat-page">
      <aside className={`chat-sessions ${sessionPanelOpen ? 'open' : ''}`}>
        <div className="chat-session-head">
          <div><span>CONVERSATIONS</span><strong>对话</strong></div>
          <Tooltip title="新建对话"><Button aria-label="新建对话" type="text" icon={<Plus size={17} />} onClick={newConversation} /></Tooltip>
        </div>
        <div className="session-list">
          <button type="button" className={`session-item session-draft ${isDraft ? 'active' : ''}`} onClick={newConversation}>
            <span className="session-icon"><Plus size={15} /></span>
            <span><strong>新对话</strong><small>{draftAgent || '选择 Agent 后发起'}</small></span>
          </button>
          {threadsQuery.isLoading && <Skeleton active paragraph={{ rows: 4 }} style={{ padding: 12 }} />}
          {threads.map((thread) => {
            const threadRuntime = runtimeByKey[thread.thread_id]
            const status = threadRuntime?.running ? 'running' : thread.last_run_status
            return (
              <button
                key={thread.thread_id}
                type="button"
                className={`session-item ${thread.thread_id === activeKey ? 'active' : ''}`}
                aria-current={thread.thread_id === activeKey ? 'page' : undefined}
                onClick={() => selectThread(thread.thread_id)}
              >
                <span className="session-icon"><Bot size={15} /></span>
                <span>
                  <strong>{thread.title || '未命名对话'}</strong>
                  <small>
                    <i className={`session-dot ${status || 'idle'}`} />
                    {thread.agent_name || thread.agent_code || '—'} · {formatTime(thread.updated_at).slice(5, 16)}
                  </small>
                </span>
              </button>
            )
          })}
          {!threadsQuery.isLoading && threads.length === 0 && (
            <p className="session-empty">历史会话保存在服务端，发起第一次对话后会出现在这里。</p>
          )}
        </div>
        <div className="session-local-note"><Check size={13} /><span>会话与消息由服务端持久化</span></div>
      </aside>
      <main className="chat-workspace">
        <header className="chat-toolbar">
          <Button aria-label="切换会话列表" className="session-toggle" type="text" icon={<PanelLeft size={18} />} onClick={() => setSessionPanelOpen((open) => !open)} />
          <div className="chat-agent">
            <span>当前 Agent</span>
            <Select
              className="agent-select"
              value={agentCode || undefined}
              placeholder="选择 Agent"
              loading={agentsQuery.isLoading}
              options={publishedAgents.map((agent) => ({ value: agent.agent_code, label: agent.name }))}
              onChange={(value) => {
                if (isDraft) setDraftAgent(value)
                updateRuntime(activeKey, (current) => ({ ...current, agentCode: value }))
              }}
              suffixIcon={<ChevronDown size={14} />}
              disabled={runtime.running}
            />
          </div>
          <div className="chat-run-state">
            {runtime.status !== 'idle' && <StatusTag status={runtime.status === 'detached' ? 'running' : runtime.status} />}
            {currentRunId && <Link to={`/runs/${currentRunId}`}>查看 Run</Link>}
          </div>
        </header>
        <div ref={messageViewportRef} className="message-viewport">
          {!isDraft && messagesQuery.isLoading ? (
            <div className="message-list"><Skeleton active avatar paragraph={{ rows: 3 }} /><Skeleton active avatar paragraph={{ rows: 2 }} /></div>
          ) : emptyState ? (
            agentsQuery.isLoading ? <Skeleton active style={{ padding: 40 }} /> : (
              <div className="chat-empty">
                <div className="chat-empty-mark"><Bot size={30} /></div>
                <h1>向 Agent 发起一次运行</h1>
                <p>选择已发布的 Agent，输入任务。步骤、委派和人工介入会在这里实时呈现，历史会话由服务端持久化。</p>
                {publishedAgents.length === 0 && <Alert type="warning" showIcon message="暂无已发布 Agent" description="请先到 Agent 管理页完成 Workflow、Capability 和 Endpoint 配置并发布。" action={<Link to="/agents">前往配置</Link>} />}
              </div>
            )
          ) : (
            <div className="message-list">
              {displayMessages.map((item) => (
                <article key={item.id} className={`chat-message ${item.role} ${item.failed ? 'failed' : ''}`}>
                  <Avatar className="message-avatar" icon={item.role === 'user' ? <UserRound size={16} /> : <Bot size={16} />} />
                  <div className="message-body">
                    <div className="message-meta"><strong>{item.role === 'user' ? '你' : agentCode || 'Agent'}</strong><span>{formatTime(item.createdAt).slice(11)}</span>{item.runId && <Link to={`/runs/${item.runId}`}>Run</Link>}</div>
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
                    <Input.TextArea id={`interrupt-${interrupt.id}`} className="code-input" value={interrupt.payloadText} onChange={(event) => updateRuntime(activeKey, (current) => ({ ...current, interrupts: current.interrupts.map((item) => item.id === interrupt.id ? { ...item, payloadText: event.target.value } : item) }))} rows={4} disabled={interrupt.submitted} />
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
              placeholder={agentCode ? `发送任务给 ${agentCode}` : '请先选择 Agent'}
              autoSize={{ minRows: 1, maxRows: 6 }}
              disabled={!agentCode}
            />
            {runtime.running ? <Tooltip title="停止接收，后端任务不会被取消"><Button aria-label="停止接收事件" danger type="text" icon={<CircleStop size={20} />} onClick={stop} /></Tooltip> : <Tooltip title="发送"><Button aria-label="发送消息" type="primary" icon={<Send size={18} />} disabled={!input.trim() || !agentCode} onClick={() => void send()} /></Tooltip>}
          </div>
          <span>Enter 发送 · Shift + Enter 换行</span>
        </footer>
      </main>
    </div>
  )
}
