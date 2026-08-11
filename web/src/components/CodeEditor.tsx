import CodeMirror from '@uiw/react-codemirror'

export function CodeEditor({ value, onChange, height = '280px', readOnly = false }: { value: string; onChange?: (value: string) => void; height?: string; readOnly?: boolean }) {
  return (
    <div className="code-editor">
      <CodeMirror value={value} height={height} readOnly={readOnly} onChange={onChange} basicSetup={{ lineNumbers: true, foldGutter: true, highlightActiveLine: !readOnly }} />
    </div>
  )
}
