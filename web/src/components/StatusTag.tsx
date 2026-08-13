import { Tag } from 'antd'

const colors: Record<string, string> = {
  active: 'green',
  success: 'green',
  completed: 'green',
  healthy: 'green',
  running: 'blue',
  working: 'blue',
  waiting_external: 'cyan',
  waiting_input: 'gold',
  queued: 'default',
  pending: 'default',
  inactive: 'default',
  failed: 'red',
  unhealthy: 'red',
  rejected: 'red',
  cancelled: 'orange',
  canceled: 'orange',
  skipped: 'default',
}

const labels: Record<string, string> = {
  active: '已启用',
  inactive: '未启用',
  success: '成功',
  failed: '失败',
  running: '运行中',
  waiting_external: '等待外部 Agent',
  waiting_input: '等待人工输入',
  queued: '排队中',
  pending: '待处理',
  cancelled: '已取消',
  canceled: '已取消',
  unhealthy: '不健康',
  healthy: '健康',
  skipped: '已跳过',
}

export function StatusTag({ status }: { status?: string | null }) {
  const value = status || 'unknown'
  return <Tag color={colors[value] || 'default'}>{labels[value] || value}</Tag>
}
