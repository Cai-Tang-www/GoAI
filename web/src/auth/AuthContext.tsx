import { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'
import { apiRequest, decodeUserId, TOKEN_KEY } from '../api/client'
import type { User } from '../api/types'

interface AuthContextValue {
  token: string | null
  user: User | null
  loading: boolean
  login: (username: string, password: string) => Promise<void>
  register: (values: { username: string; email: string; password: string }) => Promise<void>
  logout: () => void
  refreshUser: () => Promise<void>
}

const AuthContext = createContext<AuthContextValue | null>(null)

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [token, setToken] = useState<string | null>(() => localStorage.getItem(TOKEN_KEY))
  const [user, setUser] = useState<User | null>(null)
  const [loading, setLoading] = useState(Boolean(token))

  const logout = useCallback(() => {
    localStorage.removeItem(TOKEN_KEY)
    window.dispatchEvent(new CustomEvent('goai:logout'))
    setToken(null)
    setUser(null)
    setLoading(false)
  }, [])

  const refreshUser = useCallback(async () => {
    const current = localStorage.getItem(TOKEN_KEY)
    if (!current) return
    const userId = decodeUserId(current)
    if (!userId) {
      logout()
      return
    }
    setUser(await apiRequest<User>(`/api/users/${userId}`))
  }, [logout])

  useEffect(() => {
    const unauthorized = () => logout()
    window.addEventListener('goai:unauthorized', unauthorized)
    return () => window.removeEventListener('goai:unauthorized', unauthorized)
  }, [logout])

  useEffect(() => {
    if (!token) return
    const timer = window.setTimeout(() => {
      refreshUser().catch(logout).finally(() => setLoading(false))
    }, 0)
    return () => window.clearTimeout(timer)
  }, [token, refreshUser, logout])

  const login = useCallback(async (username: string, password: string) => {
    const result = await apiRequest<{ token: string }>('/auth/login', {
      method: 'POST',
      anonymous: true,
      body: { username, password },
    })
    localStorage.setItem(TOKEN_KEY, result.token)
    setToken(result.token)
  }, [])

  const register = useCallback(async (values: { username: string; email: string; password: string }) => {
    await apiRequest('/auth/register', { method: 'POST', anonymous: true, body: values })
    await login(values.username, values.password)
  }, [login])

  const value = useMemo(
    () => ({ token, user, loading, login, register, logout, refreshUser }),
    [token, user, loading, login, register, logout, refreshUser],
  )

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

// eslint-disable-next-line react-refresh/only-export-components
export function useAuth() {
  const context = useContext(AuthContext)
  if (!context) throw new Error('useAuth must be used inside AuthProvider')
  return context
}
