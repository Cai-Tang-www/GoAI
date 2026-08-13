import { Empty } from 'antd'
import type { ReactNode } from 'react'

export function EmptyPanel({ title, description, action }: { title: string; description: string; action?: ReactNode }) {
  return (
    <div className="empty-panel">
      <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={null} />
      <h3>{title}</h3>
      <p>{description}</p>
      {action}
    </div>
  )
}
