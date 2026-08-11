import { fetchEventSource } from '@microsoft/fetch-event-source'
import { ApiError, TOKEN_KEY } from './client'
import type { AGUIEvent } from './types'

export interface RunAgentInput {
  threadId?: string
  runId?: string
  parentRunId?: string
  state?: Record<string, never>
  messages?: Array<{ id: string; role: string; content: string }>
  tools?: never[]
  context?: never[]
  resume?: Array<{ interruptId: string; status: 'resolved' | 'cancelled'; payload?: unknown }>
}

export async function streamAgent(
  agentCode: string,
  input: RunAgentInput,
  signal: AbortSignal,
  onEvent: (event: AGUIEvent) => void,
) {
  const token = localStorage.getItem(TOKEN_KEY)
  await fetchEventSource(`/api/agents/${encodeURIComponent(agentCode)}/agui`, {
    method: 'POST',
    headers: {
      Accept: 'text/event-stream',
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token || ''}`,
      'X-Trace-ID': `web_${crypto.randomUUID()}`,
    },
    body: JSON.stringify(input),
    signal,
    openWhenHidden: true,
    async onopen(response) {
      const contentType = response.headers.get('Content-Type') || ''
      if (!response.ok || !contentType.includes('text/event-stream')) {
        let body: { code?: string; message?: string; trace_id?: string } = {}
        try {
          body = (await response.json()) as typeof body
        } catch {
          body = { message: `服务返回异常（HTTP ${response.status}）` }
        }
        if (response.status === 401) window.dispatchEvent(new CustomEvent('goai:unauthorized'))
        throw new ApiError(body.message || '无法启动 Agent', response.status, body.code, body.trace_id || response.headers.get('X-Trace-ID') || '')
      }
    },
    onmessage(frame) {
      if (!frame.data) return
      try {
        onEvent(JSON.parse(frame.data) as AGUIEvent)
      } catch {
        throw new ApiError('收到无法解析的 AG-UI 事件', 0, 'INVALID_SSE_EVENT')
      }
    },
    onerror(error) {
      throw error
    },
  })
}
