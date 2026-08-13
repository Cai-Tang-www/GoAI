import { Button, message } from 'antd'
import { Copy } from 'lucide-react'
import { createElement } from 'react'
import { ApiError, errorDescription, errorTitle } from '../api/client'

export function isFormValidationError(error: unknown) {
  return error instanceof ApiError && ['VALIDATION_FAILED', 'INVALID_ID'].includes(error.code)
}

export function notifyRequestError(error: unknown) {
  const traceId = error instanceof ApiError ? error.traceId : ''
  const copyButton = traceId ? createElement(Button, {
    'aria-label': '复制 Trace ID',
    size: 'small',
    icon: createElement(Copy, { size: 13 }),
    onClick: () => void navigator.clipboard.writeText(traceId),
  }, '复制 Trace') : null

  void message.error({
    duration: 8,
    content: createElement(
      'span',
      { className: 'request-error-toast' },
      createElement('span', null, createElement('strong', null, errorTitle(error)), createElement('small', null, errorDescription(error))),
      copyButton,
    ),
  })
}
