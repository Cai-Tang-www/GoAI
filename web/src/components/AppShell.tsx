import { Avatar, Button, Drawer, Dropdown, Layout, Menu, Tooltip } from 'antd'
import {
  Bot,
  Braces,
  ChevronDown,
  LogOut,
  Menu as MenuIcon,
  MessageSquare,
  Network,
  Settings,
  Users,
  Wrench,
} from 'lucide-react'
import { useState } from 'react'
import { Outlet, useLocation, useNavigate } from 'react-router-dom'
import { useAuth } from '../auth/AuthContext'

const { Sider, Content } = Layout

const navItems = [
  { key: '/chat', icon: <MessageSquare size={17} />, label: '对话工作台' },
  { key: '/runs', icon: <Network size={17} />, label: '运行记录' },
  { type: 'group' as const, label: '构建与接入', children: [
    { key: '/agents', icon: <Bot size={17} />, label: 'Agents' },
    { key: '/mcp', icon: <Wrench size={17} />, label: 'MCP 服务' },
  ] },
  { type: 'group' as const, label: '平台治理', children: [
    { key: '/users', icon: <Users size={17} />, label: '用户管理' },
    { key: '/settings', icon: <Settings size={17} />, label: '个人设置' },
  ] },
]

function Navigation({ onSelect }: { onSelect?: () => void }) {
  const navigate = useNavigate()
  const location = useLocation()
  const routeKeys = ['/chat', '/runs', '/agents', '/mcp', '/users', '/settings']
  const selected = routeKeys.filter((key) => location.pathname.startsWith(key)).sort((a, b) => b.length - a.length)[0]

  return (
    <Menu
      mode="inline"
      items={navItems}
      selectedKeys={[selected || '/chat']}
      onClick={({ key }) => {
        navigate(key)
        onSelect?.()
      }}
    />
  )
}

export function AppShell() {
  const [drawerOpen, setDrawerOpen] = useState(false)
  const { user, logout } = useAuth()
  const navigate = useNavigate()

  const profileMenu = {
    items: [
      { key: 'settings', label: '个人设置', icon: <Settings size={15} /> },
      { type: 'divider' as const },
      { key: 'logout', label: '退出登录', icon: <LogOut size={15} />, danger: true },
    ],
    onClick: ({ key }: { key: string }) => key === 'logout' ? logout() : navigate('/settings'),
  }

  return (
    <Layout className="app-layout">
      <Sider className="app-sider" width={224} breakpoint="lg" collapsedWidth={0} trigger={null}>
        <div className="brand">
          <div className="brand-mark"><Braces size={19} /></div>
          <div><strong>GoAI</strong><span>Runtime Console</span></div>
        </div>
        <Navigation />
        <div className="sider-foot">
          <Dropdown menu={profileMenu} trigger={['click']} placement="topLeft">
            <button className="profile-button" aria-label="打开账户菜单">
              <Avatar size={32}>{user?.username?.slice(0, 1).toUpperCase() || 'U'}</Avatar>
              <span><strong>{user?.username || '用户'}</strong><small>ID {user?.id || '—'}</small></span>
              <ChevronDown size={15} />
            </button>
          </Dropdown>
        </div>
      </Sider>
      <Layout>
        <header className="mobile-header">
          <Button aria-label="打开导航菜单" type="text" icon={<MenuIcon size={20} />} onClick={() => setDrawerOpen(true)} />
          <div className="mobile-brand"><Braces size={18} /> GoAI</div>
          <Tooltip title="个人设置"><Button aria-label="打开个人设置" type="text" icon={<Avatar size={28}>{user?.username?.slice(0, 1)}</Avatar>} onClick={() => navigate('/settings')} /></Tooltip>
        </header>
        <Content className="app-content"><Outlet /></Content>
      </Layout>
      <Drawer className="mobile-drawer" placement="left" width={264} open={drawerOpen} onClose={() => setDrawerOpen(false)} closable={false}>
        <div className="brand"><div className="brand-mark"><Braces size={19} /></div><div><strong>GoAI</strong><span>Runtime Console</span></div></div>
        <Navigation onSelect={() => setDrawerOpen(false)} />
      </Drawer>
    </Layout>
  )
}
