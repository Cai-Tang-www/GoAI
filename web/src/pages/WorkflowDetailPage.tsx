import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Alert, Button, Segmented, message } from 'antd'
import { ArrowLeft, Save } from 'lucide-react'
import { useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import { apiRequest } from '../api/client'
import type { Workflow } from '../api/types'
import { CodeEditor } from '../components/CodeEditor'
import { ErrorState } from '../components/ErrorState'
import { PageHeader } from '../components/PageHeader'
import { RequestErrorAlert } from '../components/RequestErrorAlert'
import { StatusTag } from '../components/StatusTag'
import { WorkflowGraph } from '../components/WorkflowGraph'
import { prettyJSON } from '../lib/format'
import { isFormValidationError, notifyRequestError } from '../lib/notify'
import { validateWorkflowDefinition } from '../lib/workflow'

export function WorkflowDetailPage() {
  const { agentCode = '', version = '' } = useParams()
  const navigate = useNavigate()
  const client = useQueryClient()
  const [view, setView] = useState<'graph' | 'json'>('graph')
  const [draft, setDraft] = useState('')
  const [saveError, setSaveError] = useState<unknown>(null)
  const query = useQuery({ queryKey: ['workflow', agentCode, version], queryFn: async () => { const workflow = await apiRequest<Workflow>(`/api/agents/${encodeURIComponent(agentCode)}/workflows/${version}`); setDraft(prettyJSON(workflow.definition)); return workflow } })

  const save = async () => {
    setSaveError(null)
    try { const definition: unknown = JSON.parse(draft); validateWorkflowDefinition(definition); await apiRequest(`/api/agents/${encodeURIComponent(agentCode)}/workflows/${version}`, { method: 'PUT', body: { definition } }); message.success('Workflow 已更新'); await client.invalidateQueries({ queryKey: ['workflow', agentCode, version] }) } catch (error) { if (isFormValidationError(error)) setSaveError(error); else notifyRequestError(error) }
  }

  if (query.error) return <div className="page-shell"><ErrorState error={query.error} onRetry={() => query.refetch()} /></div>
  return <div className="page-shell workflow-page"><Button className="back-button" type="text" icon={<ArrowLeft size={16} />} onClick={() => navigate(`/agents/${agentCode}`)}>Agent 详情</Button><PageHeader eyebrow={`WORKFLOW · ${agentCode}`} title={`Version ${version}`} description={query.data?.checksum ? `Checksum ${query.data.checksum}` : '加载中'} actions={<><StatusTag status={query.data?.is_active ? 'active' : 'inactive'} />{!query.data?.is_active && <Button type="primary" icon={<Save size={15} />} onClick={save}>保存</Button>}</>} />{Boolean(saveError) && <RequestErrorAlert error={saveError} />}<div className="workflow-toolbar"><Segmented value={view} onChange={setView} options={[{ label: 'DAG', value: 'graph' }, { label: 'JSON', value: 'json' }]} />{query.data?.is_active && <Alert type="info" showIcon message="Active 版本只读；请新建版本后修改。" />}</div>{view === 'graph' ? (query.data?.definition ? <WorkflowGraph definition={query.data.definition} height="calc(100vh - 250px)" /> : null) : <CodeEditor value={draft} onChange={setDraft} readOnly={Boolean(query.data?.is_active)} height="620px" />}</div>
}
