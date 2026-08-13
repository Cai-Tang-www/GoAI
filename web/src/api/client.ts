import type { Envelope } from './types'

export const TOKEN_KEY = 'goai.console.token'

export class ApiError extends Error {
  status: number
  code: string
  traceId: string
  details?: unknown

  constructor(message: string, status = 0, code = 'REQUEST_FAILED', traceId = '', details?: unknown) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
    this.traceId = traceId
    this.details = details
  }
}

export interface RequestOptions extends Omit<RequestInit, 'body'> {
  body?: unknown
  anonymous?: boolean
}

function makeTraceId() {
  return `web_${crypto.randomUUID()}`
}

export async function apiRequest<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const token = localStorage.getItem(TOKEN_KEY)
  const headers = new Headers(options.headers)
  headers.set('Accept', 'application/json')
  headers.set('X-Trace-ID', headers.get('X-Trace-ID') || makeTraceId())
  if (options.body !== undefined) headers.set('Content-Type', 'application/json')
  if (!options.anonymous && token) headers.set('Authorization', `Bearer ${token}`)

  let response: Response
  try {
    response = await fetch(path, {
      ...options,
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
    })
  } catch (error) {
    throw new ApiError(error instanceof Error ? error.message : '无法连接服务')
  }

  if (response.status === 401) {
    window.dispatchEvent(new CustomEvent('goai:unauthorized'))
  }
  const traceHeader = response.headers.get('X-Trace-ID') || ''
  let envelope: Envelope<T>
  try {
    envelope = (await response.json()) as Envelope<T>
  } catch {
    throw new ApiError(`服务返回了无法解析的响应（HTTP ${response.status}）`, response.status, 'INVALID_RESPONSE', traceHeader)
  }

  if (!response.ok || envelope.code !== 'OK') {
    throw new ApiError(
      envelope.message || `请求失败（HTTP ${response.status}）`,
      response.status,
      envelope.code,
      envelope.trace_id || traceHeader,
      envelope.data,
    )
  }
  return envelope.data
}

export function errorDescription(error: unknown) {
  if (!(error instanceof ApiError)) return error instanceof Error ? error.message : '未知错误'
  return error.traceId ? `${error.message} · Trace ${error.traceId}` : error.message
}

export function errorTitle(error: unknown) {
  if (!(error instanceof ApiError)) return '加载失败'
  if (error.status === 0) return '无法连接服务'
  if (error.status === 403) return '没有访问权限'
  if (error.status === 404) return '资源不存在'
  if (error.status === 409) return '资源状态冲突'
  if (error.status >= 500) return '服务暂时不可用'
  return '请求失败'
}

export function decodeUserId(token: string): number | null {
  try {
    const payload = token.split('.')[1]
    const normalized = payload.replace(/-/g, '+').replace(/_/g, '/')
    const decoded = JSON.parse(decodeURIComponent(escape(atob(normalized)))) as { user_id?: number; exp?: number }
    if (!decoded.user_id || (decoded.exp && decoded.exp * 1000 < Date.now())) return null
    return decoded.user_id
  } catch {
    return null
  }
}
