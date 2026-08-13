import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Background,
  Controls,
  MarkerType,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Edge,
  type Node,
  type OnSelectionChangeParams,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { Alert, Button, InputNumber, Segmented, Skeleton, Tooltip, message } from 'antd'
import { ArrowLeft, Bot, Boxes, CircleDot, Hammer, LayoutGrid, PauseCircle, Redo2, Save, Sparkles, Undo2, Wrench } from 'lucide-react'
import { useCallback, useMemo, useReducer, useState, type DragEvent } from 'react'
import { useNavigate, useParams, useSearchParams } from 'react-router-dom'
import { apiRequest } from '../api/client'
import type { Workflow, WorkflowDefinition } from '../api/types'
import { CodeEditor } from '../components/CodeEditor'
import { ErrorState } from '../components/ErrorState'
import { NodeConfigPanel } from '../components/NodeConfigPanel'
import { WORKFLOW_NODE_TYPES } from '../components/WorkflowGraph'
import { prettyJSON } from '../lib/format'
import { notifyRequestError } from '../lib/notify'
import {
  EDITOR_NODE_TYPES,
  analyzeWorkflow,
  createEditorState,
  editorReducer,
  fillMissingPositions,
  type EditorState,
} from '../lib/workflowEditor'
import { WORKFLOW_NODE_HEIGHT, WORKFLOW_NODE_WIDTH, summarizeNodeConfig, type WorkflowGraphNodeData } from '../lib/workflowGraph'

const DND_MIME = 'application/goai-node-type'
const EMPTY_DEFINITION: WorkflowDefinition = { entry_node: '', nodes: [], edges: [] }

const PALETTE: Array<{ type: (typeof EDITOR_NODE_TYPES)[number]; label: string; hint: string; icon: typeof Bot }> = [
  { type: 'noop', label: 'noop', hint: '透传节点', icon: CircleDot },
  { type: 'llm', label: 'llm', hint: '模型调用', icon: Sparkles },
  { type: 'agent', label: 'agent', hint: 'A2A 委派', icon: Bot },
  { type: 'agent_tool', label: 'agent_tool', hint: 'Agent 作为工具', icon: Hammer },
  { type: 'agent_group', label: 'agent_group', hint: '并行委派 fan-out', icon: Boxes },
  { type: 'tool', label: 'tool', hint: 'MCP 工具', icon: Wrench },
  { type: 'interrupt', label: 'interrupt', hint: '等待用户输入', icon: PauseCircle },
]

function EditorCanvas({ state, dispatch }: { state: EditorState; dispatch: React.Dispatch<Parameters<typeof editorReducer>[1]> }) {
  const { screenToFlowPosition } = useReactFlow()
  const nodes: Node<WorkflowGraphNodeData>[] = useMemo(
    () =>
      state.definition.nodes.map((node) => ({
        id: node.key,
        type: 'workflowNode',
        position: state.positions[node.key] || { x: 0, y: 0 },
        selected: node.key === state.selectedKey,
        data: {
          nodeKey: node.key,
          nodeType: node.type,
          isEntry: node.key === state.definition.entry_node,
          summary: summarizeNodeConfig(node),
        },
      })),
    [state.definition, state.positions, state.selectedKey],
  )
  const edges: Edge[] = useMemo(
    () =>
      state.definition.edges.map((edge, index) => ({
        id: `${edge.from}->${edge.to}#${index}`,
        source: edge.from,
        target: edge.to,
        markerEnd: { type: MarkerType.ArrowClosed, color: '#65837f' },
        style: { stroke: '#65837f', strokeWidth: 1.5 },
      })),
    [state.definition.edges],
  )

  const onDrop = useCallback(
    (event: DragEvent) => {
      event.preventDefault()
      const nodeType = event.dataTransfer.getData(DND_MIME)
      if (!nodeType) return
      const point = screenToFlowPosition({ x: event.clientX, y: event.clientY })
      dispatch({ type: 'add-node', nodeType, position: { x: point.x - WORKFLOW_NODE_WIDTH / 2, y: point.y - WORKFLOW_NODE_HEIGHT / 2 } })
    },
    [dispatch, screenToFlowPosition],
  )

  const onSelectionChange = useCallback(
    ({ nodes: selected }: OnSelectionChangeParams) => {
      dispatch({ type: 'select', key: selected.length === 1 ? selected[0].id : null })
    },
    [dispatch],
  )

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      nodeTypes={WORKFLOW_NODE_TYPES}
      fitView
      minZoom={0.3}
      maxZoom={1.6}
      deleteKeyCode={['Backspace', 'Delete']}
      proOptions={{ hideAttribution: true }}
      onDragOver={(event) => { event.preventDefault(); event.dataTransfer.dropEffect = 'move' }}
      onDrop={onDrop}
      onConnect={(connection) => { if (connection.source && connection.target) dispatch({ type: 'add-edge', from: connection.source, to: connection.target }) }}
      onNodeDragStop={(_, node) => dispatch({ type: 'move-node', key: node.id, position: node.position })}
      onSelectionChange={onSelectionChange}
      onNodesDelete={(deleted) => deleted.forEach((node) => dispatch({ type: 'remove-node', key: node.id }))}
      onEdgesDelete={(deleted) => deleted.forEach((edge) => dispatch({ type: 'remove-edge', from: edge.source, to: edge.target }))}
    >
      <Background color="#d6dddb" gap={24} size={1} />
      <Controls showInteractive={false} />
      {state.definition.nodes.length > 6 && <MiniMap pannable zoomable className="wf-minimap" />}
    </ReactFlow>
  )
}

function EditorInner({ agentCode, mode, workflow }: { agentCode: string; mode: 'new' | 'edit'; workflow?: Workflow }) {
  const navigate = useNavigate()
  const client = useQueryClient()
  const [searchParams] = useSearchParams()
  const [state, dispatch] = useReducer(
    editorReducer,
    undefined,
    () => createEditorState(workflow?.definition || EMPTY_DEFINITION, workflow?.layout?.positions),
  )
  const [view, setView] = useState<'canvas' | 'json'>('canvas')
  const [jsonDraft, setJsonDraft] = useState('')
  const [jsonError, setJsonError] = useState<string | null>(null)
  const [version, setVersion] = useState<number>(() => {
    const fromQuery = Number(searchParams.get('version'))
    if (Number.isInteger(fromQuery) && fromQuery > 0) return fromQuery
    return (workflow?.version || 0) + 1
  })

  const analysis = useMemo(() => analyzeWorkflow(state.definition), [state.definition])
  const selectedNode = state.definition.nodes.find((node) => node.key === state.selectedKey) || null
  const allKeys = state.definition.nodes.map((node) => node.key)

  const switchView = (next: 'canvas' | 'json') => {
    if (next === view) return
    if (next === 'json') {
      setJsonDraft(prettyJSON(state.definition))
      setJsonError(null)
      setView('json')
      return
    }
    try {
      const parsed = JSON.parse(jsonDraft) as WorkflowDefinition
      if (!parsed || typeof parsed !== 'object' || !Array.isArray(parsed.nodes) || !Array.isArray(parsed.edges)) throw new Error('definition 需要包含 entry_node / nodes / edges')
      dispatch({ type: 'reset', definition: parsed, positions: fillMissingPositions(parsed, state.positions) })
      setJsonError(null)
      setView('canvas')
    } catch (error) {
      setJsonError(error instanceof Error ? error.message : String(error))
    }
  }

  const saveMutation = useMutation({
    mutationFn: async () => {
      const payload = { definition: state.definition, layout: { positions: state.positions } }
      if (mode === 'edit' && workflow) {
        await apiRequest(`/api/agents/${encodeURIComponent(agentCode)}/workflows/${workflow.version}`, { method: 'PUT', body: payload })
        return workflow.version
      }
      await apiRequest(`/api/agents/${encodeURIComponent(agentCode)}/workflows`, { method: 'POST', body: { version, ...payload } })
      return version
    },
    onSuccess: async (savedVersion) => {
      message.success(mode === 'edit' ? 'Workflow 已更新' : `Workflow v${savedVersion} 已创建`)
      await client.invalidateQueries({ queryKey: ['workflows', agentCode] })
      await client.invalidateQueries({ queryKey: ['workflow', agentCode, String(savedVersion)] })
      navigate(`/agents/${agentCode}/workflows/${savedVersion}`)
    },
    onError: notifyRequestError,
  })

  const canSave = analysis.errors.length === 0 && state.definition.nodes.length > 0 && (view === 'canvas' || !jsonError)

  return (
    <div className="page-shell workflow-editor-page">
      <Button className="back-button" type="text" icon={<ArrowLeft size={16} />} onClick={() => navigate(`/agents/${agentCode}`)}>Agent 详情</Button>
      <div className="editor-toolbar">
        <div className="editor-toolbar-left">
          <h1>{mode === 'edit' ? `编辑 Workflow v${workflow?.version}` : '新建 Workflow 版本'}</h1>
          {mode === 'new' && (
            <label className="editor-version">版本号<InputNumber min={1} precision={0} value={version} onChange={(value) => setVersion(value || 1)} /></label>
          )}
        </div>
        <div className="editor-toolbar-actions">
          <Segmented value={view} onChange={(value) => switchView(value as 'canvas' | 'json')} options={[{ label: '画布', value: 'canvas' }, { label: 'JSON', value: 'json' }]} />
          <Tooltip title="撤销"><Button icon={<Undo2 size={15} />} disabled={state.past.length === 0} onClick={() => dispatch({ type: 'undo' })} /></Tooltip>
          <Tooltip title="重做"><Button icon={<Redo2 size={15} />} disabled={state.future.length === 0} onClick={() => dispatch({ type: 'redo' })} /></Tooltip>
          <Tooltip title="自动布局"><Button icon={<LayoutGrid size={15} />} onClick={() => dispatch({ type: 'auto-layout' })} /></Tooltip>
          <Button type="primary" icon={<Save size={15} />} loading={saveMutation.isPending} disabled={!canSave} onClick={() => saveMutation.mutate()}>
            保存
          </Button>
        </div>
      </div>

      {(analysis.errors.length > 0 || analysis.warnings.length > 0) && (
        <div className="editor-issues">
          {analysis.errors.map((error, index) => <Alert key={`e${index}`} type="error" showIcon message={error} />)}
          {analysis.warnings.map((warning, index) => <Alert key={`w${index}`} type="warning" showIcon message={warning} />)}
        </div>
      )}
      {jsonError && <Alert type="error" showIcon message={`JSON 不合法：${jsonError}`} />}

      {view === 'json' ? (
        <CodeEditor value={jsonDraft} onChange={setJsonDraft} height="600px" />
      ) : (
        <div className="editor-layout">
          <aside className="node-palette">
            <span className="palette-title">节点类型（拖入画布）</span>
            {PALETTE.map(({ type, label, hint, icon: Icon }) => (
              <div
                key={type}
                className={`palette-item wf-type-${type}`}
                draggable
                role="button"
                title={`点击或拖入画布以添加 ${label} 节点`}
                onDragStart={(event) => { event.dataTransfer.setData(DND_MIME, type); event.dataTransfer.effectAllowed = 'move' }}
                onClick={() => {
                  const count = state.definition.nodes.length
                  dispatch({ type: 'add-node', nodeType: type, position: { x: 80 + (count % 3) * (WORKFLOW_NODE_WIDTH + 60), y: 60 + Math.floor(count / 3) * (WORKFLOW_NODE_HEIGHT + 60) } })
                }}
              >
                <div className="wf-node-icon"><Icon size={15} /></div>
                <div><strong>{label}</strong><span>{hint}</span></div>
              </div>
            ))}
            <span className="palette-note">点击或拖拽添加节点；连线：拖动节点两侧圆点；删除：选中后按 Delete</span>
          </aside>
          <div className="editor-canvas workflow-canvas">
            <ReactFlowProvider>
              <EditorCanvas state={state} dispatch={dispatch} />
            </ReactFlowProvider>
            {state.definition.nodes.length === 0 && <div className="editor-empty-hint">从左侧拖入第一个节点开始编排</div>}
          </div>
          {selectedNode && (
            <NodeConfigPanel
              key={selectedNode.key}
              node={selectedNode}
              entryKey={state.definition.entry_node}
              allKeys={allKeys}
              onConfigChange={(config) => dispatch({ type: 'update-config', key: selectedNode.key, config })}
              onRename={(nextKey) => dispatch({ type: 'rename-node', key: selectedNode.key, nextKey })}
              onSetEntry={() => dispatch({ type: 'set-entry', key: selectedNode.key })}
              onRemove={() => dispatch({ type: 'remove-node', key: selectedNode.key })}
            />
          )}
        </div>
      )}
    </div>
  )
}

// WorkflowEditorPage 支持两种模式：/workflows/new 新建版本、/workflows/:version/edit 编辑 inactive 版本。
export function WorkflowEditorPage({ mode }: { mode: 'new' | 'edit' }) {
  const { agentCode = '', version = '' } = useParams()
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const templateVersion = searchParams.get('from') || ''
  const query = useQuery({
    queryKey: ['workflow', agentCode, version],
    enabled: mode === 'edit',
    queryFn: () => apiRequest<Workflow>(`/api/agents/${encodeURIComponent(agentCode)}/workflows/${version}`),
  })
  const templateQuery = useQuery({
    queryKey: ['workflow', agentCode, templateVersion],
    enabled: mode === 'new' && Boolean(templateVersion),
    queryFn: () => apiRequest<Workflow>(`/api/agents/${encodeURIComponent(agentCode)}/workflows/${templateVersion}`),
  })

  if (mode === 'edit') {
    if (query.isLoading) return <div className="page-shell"><Skeleton active /></div>
    if (query.error) return <div className="page-shell"><ErrorState error={query.error} onRetry={() => query.refetch()} /></div>
    if (query.data?.is_active) {
      return (
        <div className="page-shell">
          <Alert
            type="warning"
            showIcon
            message="Active 版本不可原地编辑"
            description="请基于此版本创建一个新版本再修改。"
            action={<Button type="primary" onClick={() => navigate(`/agents/${agentCode}/workflows/new?from=${query.data?.version}`)}>基于 v{query.data?.version} 新建</Button>}
          />
        </div>
      )
    }
    return <EditorInner agentCode={agentCode} mode="edit" workflow={query.data} />
  }

  if (templateQuery.isLoading) return <div className="page-shell"><Skeleton active /></div>
  const template = templateQuery.data
  return <EditorInner key={template ? `from-${template.version}` : 'blank'} agentCode={agentCode} mode="new" workflow={template} />
}
