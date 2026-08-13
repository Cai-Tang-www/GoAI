import { Alert, Button, message } from 'antd'
import { Copy, RefreshCw } from 'lucide-react'
import { ApiError, errorDescription, errorTitle } from '../api/client'

export function ErrorState({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const traceId = error instanceof ApiError ? error.traceId : ''
  const copyTrace = async () => {
    if (!traceId) return
    await navigator.clipboard.writeText(traceId)
    message.success('Trace ID 已复制')
  }
  return (
    <Alert
      type="error"
      showIcon
      message={errorTitle(error)}
      description={errorDescription(error)}
      action={<div className="error-actions">{traceId && <Button icon={<Copy size={14} />} onClick={() => void copyTrace()}>复制 Trace</Button>}{onRetry && <Button icon={<RefreshCw size={14} />} onClick={onRetry}>重试</Button>}</div>}
    />
  )
}
