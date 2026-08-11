import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Background, Controls, MarkerType, Position, ReactFlow, type Edge, type Node } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { Alert, Button, Segmented, message } from 'antd'
import { ArrowLeft, Save } from 'lucide-react'
import { useMemo, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { apiRequest, errorDescription } from '../api/client'
import type { Workflow } from '../api/types'
import { CodeEditor } from '../components/CodeEditor'
import { ErrorState } from '../components/ErrorState'
import { PageHeader } from '../components/PageHeader'
import { StatusTag } from '../components/StatusTag'
import { prettyJSON } from '../lib/format'
import { validateWorkflowDefinition } from '../lib/workflow'

export function WorkflowDetailPage() {
  const { agentCode = '', version = '' } = useParams()
  const navigate = useNavigate()
  const client = useQueryClient()
  const [view, setView] = useState<'graph' | 'json'>('graph')
  const [draft, setDraft] = useState('')
  const query = useQuery({ queryKey: ['workflow', agentCode, version], queryFn: async () => { const workflow = await apiRequest<Workflow>(`/api/agents/${encodeURIComponent(agentCode)}/workflows/${version}`); setDraft(prettyJSON(workflow.definition)); return workflow } })
  const graph = useMemo(() => {
    const definition = query.data?.definition
    if (!definition) return { nodes: [], edges: [] }
    const levels = new Map<string, number>([[definition.entry_node, 0]])
    for (let pass = 0; pass < definition.nodes.length; pass += 1) for (const edge of definition.edges) if (levels.has(edge.from)) levels.set(edge.to, Math.max(levels.get(edge.to) || 0, (levels.get(edge.from) || 0) + 1))
    const grouped = new Map<number, string[]>()
    definition.nodes.forEach((node) => { const level = levels.get(node.key) || 0; grouped.set(level, [...(grouped.get(level) || []), node.key]) })
    const nodes: Node[] = definition.nodes.map((node) => {
      const level = levels.get(node.key) || 0
      const row = grouped.get(level) || []
      return { id: node.key, position: { x: level * 260, y: row.indexOf(node.key) * 120 }, sourcePosition: Position.Right, targetPosition: Position.Left, data: { label: <div className="workflow-node"><span>{node.type}</span><strong>{node.key}</strong>{node.key === definition.entry_node && <small>ENTRY</small>}</div> }, className: 'workflow-flow-node' }
    })
    const edges: Edge[] = definition.edges.map((edge, index) => ({ id: `${edge.from}-${edge.to}-${index}`, source: edge.from, target: edge.to, markerEnd: { type: MarkerType.ArrowClosed }, style: { stroke: '#65837f', strokeWidth: 1.5 } }))
    return { nodes, edges }
  }, [query.data])

  const save = async () => {
    try { const definition: unknown = JSON.parse(draft); validateWorkflowDefinition(definition); await apiRequest(`/api/agents/${encodeURIComponent(agentCode)}/workflows/${version}`, { method: 'PUT', body: { definition } }); message.success('Workflow 已更新'); await client.invalidateQueries({ queryKey: ['workflow', agentCode, version] }) } catch (error) { message.error(errorDescription(error)) }
  }

  if (query.error) return <div className="page-shell"><ErrorState error={query.error} onRetry={() => query.refetch()} /></div>
  return <div className="page-shell workflow-page"><Button className="back-button" type="text" icon={<ArrowLeft size={16} />} onClick={() => navigate(`/agents/${agentCode}`)}>Agent 详情</Button><PageHeader eyebrow={`WORKFLOW · ${agentCode}`} title={`Version ${version}`} description={query.data?.checksum ? `Checksum ${query.data.checksum}` : '加载中'} actions={<><StatusTag status={query.data?.is_active ? 'active' : 'inactive'} />{!query.data?.is_active && <Button type="primary" icon={<Save size={15} />} onClick={save}>保存</Button>}</>} /><div className="workflow-toolbar"><Segmented value={view} onChange={setView} options={[{ label: 'DAG', value: 'graph' }, { label: 'JSON', value: 'json' }]} />{query.data?.is_active && <Alert type="info" showIcon message="Active 版本只读；请新建版本后修改。" />}</div>{view === 'graph' ? <div className="workflow-canvas"><ReactFlow nodes={graph.nodes} edges={graph.edges} fitView minZoom={0.4} maxZoom={1.5} nodesDraggable={false} nodesConnectable={false}><Background color="#d6dddb" gap={24} size={1} /><Controls showInteractive={false} /></ReactFlow></div> : <CodeEditor value={draft} onChange={setDraft} readOnly={Boolean(query.data?.is_active)} height="620px" />}</div>
}
