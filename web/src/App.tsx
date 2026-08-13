import { Spin } from 'antd'
import { lazy, Suspense } from 'react'
import { Navigate, Route, Routes, useLocation } from 'react-router-dom'
import { useAuth } from './auth/AuthContext'
import { AppShell } from './components/AppShell'
import { LoginPage } from './pages/LoginPage'

const ChatPage = lazy(() => import('./pages/ChatPage').then((module) => ({ default: module.ChatPage })))
const AgentsPage = lazy(() => import('./pages/AgentsPage').then((module) => ({ default: module.AgentsPage })))
const AgentDetailPage = lazy(() => import('./pages/AgentDetailPage').then((module) => ({ default: module.AgentDetailPage })))
const WorkflowDetailPage = lazy(() => import('./pages/WorkflowDetailPage').then((module) => ({ default: module.WorkflowDetailPage })))
const RunsPage = lazy(() => import('./pages/RunsPage').then((module) => ({ default: module.RunsPage })))
const RunDetailPage = lazy(() => import('./pages/RunDetailPage').then((module) => ({ default: module.RunDetailPage })))
const MCPPage = lazy(() => import('./pages/MCPPage').then((module) => ({ default: module.MCPPage })))
const MCPDetailPage = lazy(() => import('./pages/MCPDetailPage').then((module) => ({ default: module.MCPDetailPage })))
const UsersPage = lazy(() => import('./pages/UsersPage').then((module) => ({ default: module.UsersPage })))
const SettingsPage = lazy(() => import('./pages/SettingsPage').then((module) => ({ default: module.SettingsPage })))

function PageLoader() {
  return <div className="page-loader"><Spin /><span>加载工作区</span></div>
}

function ProtectedLayout() {
  const { token, loading } = useAuth()
  const location = useLocation()
  if (loading) return <div className="app-loading"><Spin size="large" /><span>正在恢复会话</span></div>
  if (!token) return <Navigate to="/login" replace state={{ from: location }} />
  return <AppShell />
}

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route element={<ProtectedLayout />}>
        <Route index element={<Navigate to="/chat" replace />} />
        <Route path="/chat" element={<Suspense fallback={<PageLoader />}><ChatPage /></Suspense>} />
        <Route path="/agents" element={<Suspense fallback={<PageLoader />}><AgentsPage /></Suspense>} />
        <Route path="/agents/:agentCode" element={<Suspense fallback={<PageLoader />}><AgentDetailPage /></Suspense>} />
        <Route path="/agents/:agentCode/workflows/:version" element={<Suspense fallback={<PageLoader />}><WorkflowDetailPage /></Suspense>} />
        <Route path="/runs" element={<Suspense fallback={<PageLoader />}><RunsPage /></Suspense>} />
        <Route path="/runs/:runId" element={<Suspense fallback={<PageLoader />}><RunDetailPage /></Suspense>} />
        <Route path="/mcp" element={<Suspense fallback={<PageLoader />}><MCPPage /></Suspense>} />
        <Route path="/mcp/:serverCode" element={<Suspense fallback={<PageLoader />}><MCPDetailPage /></Suspense>} />
        <Route path="/users" element={<Suspense fallback={<PageLoader />}><UsersPage /></Suspense>} />
        <Route path="/settings" element={<Suspense fallback={<PageLoader />}><SettingsPage /></Suspense>} />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}
