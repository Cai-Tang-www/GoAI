import type { ChatSession, RecentRun } from '../api/types'
import { decodeUserId, TOKEN_KEY } from '../api/client'

const SESSIONS_KEY = 'goai.console.sessions'
const RUNS_KEY = 'goai.console.recent-runs'

function scopedKey(key: string) {
  const token = localStorage.getItem(TOKEN_KEY)
  const userId = token ? decodeUserId(token) : null
  return `${key}.${userId ?? 'anonymous'}`
}

function read<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key)
    return raw ? (JSON.parse(raw) as T) : fallback
  } catch {
    return fallback
  }
}

export function loadSessions() {
  return read<ChatSession[]>(scopedKey(SESSIONS_KEY), [])
}

export function saveSessions(sessions: ChatSession[]) {
  localStorage.setItem(scopedKey(SESSIONS_KEY), JSON.stringify(sessions.slice(0, 40)))
}

export function loadRecentRuns() {
  return read<RecentRun[]>(scopedKey(RUNS_KEY), [])
}

export function rememberRun(run: RecentRun) {
  const next = [run, ...loadRecentRuns().filter((item) => item.runId !== run.runId)].slice(0, 50)
  localStorage.setItem(scopedKey(RUNS_KEY), JSON.stringify(next))
  window.dispatchEvent(new CustomEvent('goai:runs-changed'))
}

export function clearRecentRuns() {
  localStorage.removeItem(scopedKey(RUNS_KEY))
  window.dispatchEvent(new CustomEvent('goai:runs-changed'))
}
