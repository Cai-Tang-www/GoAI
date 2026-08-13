import { Button, Tooltip } from 'antd'
import { Check, Copy } from 'lucide-react'
import { useState } from 'react'
import { prettyJSON } from '../lib/format'

export function JsonView({ value, maxHeight = 420 }: { value: unknown; maxHeight?: number }) {
  const [copied, setCopied] = useState(false)
  const text = prettyJSON(value)

  const copy = async () => {
    await navigator.clipboard.writeText(text)
    setCopied(true)
    window.setTimeout(() => setCopied(false), 1500)
  }

  return (
    <div className="json-view">
      <Tooltip title="复制 JSON">
        <Button
          className="json-copy"
          aria-label="复制 JSON"
          type="text"
          size="small"
          icon={copied ? <Check size={15} /> : <Copy size={15} />}
          onClick={copy}
        />
      </Tooltip>
      <pre style={{ maxHeight }}>{text}</pre>
    </div>
  )
}
