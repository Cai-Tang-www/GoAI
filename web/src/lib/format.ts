import dayjs from 'dayjs'

export function formatTime(value?: string | null) {
  return value ? dayjs(value).format('YYYY-MM-DD HH:mm:ss') : '—'
}

export function shortId(value?: string | null, length = 12) {
  if (!value) return '—'
  return value.length > length ? `${value.slice(0, length)}…` : value
}

export function pick<T>(object: Record<string, unknown> | undefined, ...keys: string[]): T | undefined {
  if (!object) return undefined
  for (const key of keys) {
    if (object[key] !== undefined && object[key] !== null) return object[key] as T
  }
  return undefined
}

export function prettyJSON(value: unknown) {
  if (typeof value === 'string') {
    try {
      return JSON.stringify(JSON.parse(value), null, 2)
    } catch {
      return value
    }
  }
  return JSON.stringify(value ?? {}, null, 2)
}
