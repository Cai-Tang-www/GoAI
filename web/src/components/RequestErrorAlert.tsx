import { Alert, Button } from 'antd'
import { Copy } from 'lucide-react'
import { ApiError, errorDescription, errorTitle } from '../api/client'

export function RequestErrorAlert({ error }: { error: unknown }) {
  const traceId = error instanceof ApiError ? error.traceId : ''
  const copyAction = traceId ? <Button aria-label="复制 Trace ID" size="small" icon={<Copy size={13} />} onClick={() => void navigator.clipboard.writeText(traceId)}>复制 Trace</Button> : undefined
  return <Alert className="form-request-error" type="error" showIcon message={errorTitle(error)} description={errorDescription(error)} action={copyAction} />
}
