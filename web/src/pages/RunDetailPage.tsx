import { useQuery } from '@tanstack/react-query'
import { Alert, Button, Collapse, Descriptions, Segmented, Skeleton, Table, Timeline, Tooltip, Tree } from 'antd'
import type { TreeDataNode } from 'antd'
import { ArrowLeft, Copy, GitBranch, History, MessagesSquare, RefreshCw, RotateCcw } from 'lucide-react'
import { useMemo, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { apiRequest } from '../api/client'
import type { Run, RunStep, RunTrace } from '../api/types'
import { ErrorState } from '../components/ErrorState'
import { JsonView } from '../components/JsonView'
import { PageHeader } from '../components/PageHeader'
import { StatusTag } from '../components/StatusTag'
import { formatTime, pick, shortId } from '../lib/format'
import { notifyRequestError } from '../lib/notify'

type TraceRecord = Record<string, unknown>

function runValue<T>(run: Run | undefined, ...keys: string[]) { return pick<T>(run as TraceRecord | undefined, ...keys) }
function recordValue<T>(record: TraceRecord | undefined, ...keys: string[]) { return pick<T>(record, ...keys) }
function isTerminal(status?: string) { return Boolean(status && ['success', 'failed', 'cancelled'].includes(status)) }
function parseJSON(value: unknown) {
  if (typeof value !== 'string') return value
  try { return JSON.parse(value) as unknown } catch { return value }
}

function buildRunTree(runs: Run[], rootRunId: string): TreeDataNode[] {
  const children = new Map<string, Run[]>()
  for (const run of runs) {
    const parentId = runValue<string>(run, 'parent_run_id', 'ParentRunID') || ''
    const bucket = children.get(parentId) || []
    bucket.push(run)
    children.set(parentId, bucket)
  }
  const renderNode = (run: Run): TreeDataNode => {
    const id = runValue<string>(run, 'run_id', 'RunID') || 'unknown'
    const status = runValue<string>(run, 'status', 'Status')
    return {
      key: id,
      title: <span className="run-tree-title"><Link to={`/runs/${id}`}>{shortId(id, 22)}</Link><StatusTag status={status} /></span>,
      children: (children.get(id) || []).map(renderNode),
    }
  }
  const root = runs.find((run) => runValue<string>(run, 'run_id', 'RunID') === rootRunId) || runs[0]
  return root ? [renderNode(root)] : []
}

function LoopDetailPanel({ loop }: { loop: TraceRecord }) {
  const loopId = recordValue<string>(loop, 'loop_id', 'LoopID') || ''
  const detailQuery = useQuery({ queryKey: ['loop', loopId], queryFn: () => apiRequest<TraceRecord>(`/api/loops/${encodeURIComponent(loopId)}`), enabled: Boolean(loopId) })
  const evaluationsQuery = useQuery({ queryKey: ['loop-evaluations', loopId], queryFn: () => apiRequest<TraceRecord[]>(`/api/loops/${encodeURIComponent(loopId)}/evaluations`), enabled: Boolean(loopId) })
  if (detailQuery.isLoading || evaluationsQuery.isLoading) return <Skeleton active paragraph={{ rows: 3 }} />
  if (detailQuery.error || evaluationsQuery.error) return <ErrorState error={detailQuery.error || evaluationsQuery.error} onRetry={() => { detailQuery.refetch(); evaluationsQuery.refetch() }} />
  return <div className="loop-detail"><JsonView value={detailQuery.data} maxHeight={260} /><Table rowKey={(row) => `${loopId}:${recordValue<string>(row, 'evaluator_code', 'EvaluatorCode') || 'unknown'}`} size="small" pagination={false} dataSource={evaluationsQuery.data || []} columns={[{ title: 'Evaluator', render: (_, row) => <code>{recordValue<string>(row, 'evaluator_code', 'EvaluatorCode') || '—'}</code> }, { title: '状态', render: (_, row) => <StatusTag status={recordValue<string>(row, 'status', 'Status')} /> }, { title: 'Score', render: (_, row) => recordValue<number>(row, 'score', 'Score') ?? '—' }, { title: '结果', render: (_, row) => <code>{shortId(String(recordValue(row, 'result_json', 'ResultJSON') || '—'), 32)}</code> }]} /></div>
}

export function RunDetailPage() {
  const { runId = '' } = useParams()
  const navigate = useNavigate()
  const [view, setView] = useState<'overview' | 'trace' | 'json'>('overview')
  const [replaying, setReplaying] = useState<'run' | 'thread' | null>(null)
  const runQuery = useQuery({
    queryKey: ['run', runId],
    queryFn: () => apiRequest<Run>(`/api/runs/${encodeURIComponent(runId)}`),
    refetchInterval: (query) => isTerminal(runValue<string>(query.state.data, 'status', 'Status')) ? false : 4000,
  })
  const stepsQuery = useQuery({
    queryKey: ['run-steps', runId],
    queryFn: () => apiRequest<RunStep[]>(`/api/runs/${encodeURIComponent(runId)}/steps`),
    refetchInterval: () => isTerminal(runValue<string>(runQuery.data, 'status', 'Status')) ? false : 4000,
  })
  const traceQuery = useQuery({
    queryKey: ['run-trace', runId],
    queryFn: () => apiRequest<RunTrace>(`/api/runs/${encodeURIComponent(runId)}/trace`),
    enabled: view !== 'overview',
    refetchInterval: () => view !== 'overview' && !isTerminal(runValue<string>(runQuery.data, 'status', 'Status')) ? 4000 : false,
  })

  const run = runQuery.data
  const status = runValue<string>(run, 'status', 'Status')
  const threadId = runValue<string>(run, 'thread_id', 'ThreadID')
  const traceId = runValue<string>(run, 'trace_id', 'TraceID')

  const replayRun = async () => {
    if (replaying) return
    setReplaying('run')
    try {
      const result = await apiRequest<{ run_id: string; status: string }>(`/api/runs/${encodeURIComponent(runId)}/replay`, { method: 'POST', headers: { 'Idempotency-Key': crypto.randomUUID() } })
      navigate(`/runs/${result.run_id}`)
    } catch (error) { notifyRequestError(error) } finally { setReplaying(null) }
  }

  const replayThread = async () => {
    if (!threadId || replaying) return
    setReplaying('thread')
    try {
      const result = await apiRequest<{ run_id: string; status: string }>(`/api/threads/${encodeURIComponent(threadId)}/replay`, { method: 'POST', headers: { 'Idempotency-Key': crypto.randomUUID() }, body: { source_run_id: runId } })
      navigate(`/runs/${result.run_id}`)
    } catch (error) { notifyRequestError(error) } finally { setReplaying(null) }
  }

  const delegationRows = useMemo(() => traceQuery.data?.delegations || [], [traceQuery.data])
  const runTree = useMemo(() => buildRunTree(traceQuery.data?.runs || [], runId), [traceQuery.data, runId])

  if (runQuery.isLoading) return <div className="page-shell"><Skeleton active /></div>
  if (runQuery.error) return <div className="page-shell"><Button type="text" icon={<ArrowLeft size={16} />} onClick={() => navigate('/runs')}>返回</Button><ErrorState error={runQuery.error} onRetry={() => runQuery.refetch()} /></div>

  return (
    <div className="page-shell">
      <Button className="back-button" type="text" icon={<ArrowLeft size={16} />} onClick={() => navigate('/runs')}>运行记录</Button>
      <PageHeader
        eyebrow="RUN DETAIL"
        title={shortId(runId, 32)}
        description={`触发方式：${runValue<string>(run, 'trigger_type', 'TriggerType') || '—'}`}
        actions={<><Button icon={<RefreshCw size={15} />} onClick={() => { runQuery.refetch(); stepsQuery.refetch(); if (view !== 'overview') traceQuery.refetch() }}>刷新</Button><Button loading={replaying === 'run'} disabled={Boolean(replaying)} icon={<History size={15} />} onClick={replayRun}>Replay Run</Button><Button loading={replaying === 'thread'} disabled={!threadId || Boolean(replaying)} type="primary" icon={<RotateCcw size={15} />} onClick={replayThread}>Replay Thread</Button></>}
      />
      <div className="run-status-band">
        <div><span>状态</span><StatusTag status={status} /></div>
        <div><span>当前步骤</span><strong>{runValue<string>(run, 'current_step', 'CurrentStep') || '—'}</strong></div>
        <div><span>Thread</span><code>{shortId(threadId, 20)}</code></div>
        <div><span>Trace</span><Tooltip title="复制 Trace ID"><button aria-label="复制 Trace ID" className="copy-id" onClick={() => navigator.clipboard.writeText(traceId || '')}><code>{shortId(traceId, 20)}</code><Copy size={13} /></button></Tooltip></div>
      </div>
      {runValue<string>(run, 'error_message', 'ErrorMessage') && <Alert type="error" showIcon message="运行失败" description={runValue<string>(run, 'error_message', 'ErrorMessage')} />}
      <Segmented className="detail-segment" value={view} onChange={setView} options={[{ label: '步骤与信息', value: 'overview' }, { label: '协作 Trace', value: 'trace' }, { label: '原始 JSON', value: 'json' }]} />
      {view === 'overview' && (
        <div className="detail-grid">
          <section className="content-section span-two">
            <div className="section-heading"><div><h2>执行步骤</h2><span>{stepsQuery.data?.length || 0} 个 checkpoint</span></div></div>
            {stepsQuery.error ? <ErrorState error={stepsQuery.error} /> : <Timeline items={(stepsQuery.data || []).map((step) => {
              const stepStatus = pick<string>(step, 'status', 'Status')
              return { color: stepStatus === 'success' ? 'green' : stepStatus === 'failed' ? 'red' : stepStatus?.startsWith('waiting') ? 'orange' : 'blue', children: <div className="step-item"><div><strong>{pick<string>(step, 'step_key', 'StepKey')}</strong><StatusTag status={stepStatus} /></div><p><code>{pick<string>(step, 'step_type', 'StepType')}</code> · attempt {pick<number>(step, 'attempt', 'Attempt') || 0} · {pick<number>(step, 'latency_ms', 'LatencyMS') || 0}ms</p><p>开始 {formatTime(pick<string>(step, 'started_at', 'StartedAt'))} · 结束 {formatTime(pick<string>(step, 'finished_at', 'FinishedAt'))}</p>{pick<string>(step, 'error_message', 'ErrorMessage') && <Alert type="error" message={pick<string>(step, 'error_message', 'ErrorMessage')} />}<Collapse ghost items={[{ key: 'payload', label: '输入 / 输出', children: <div className="step-payload"><div><span>Input</span><JsonView value={parseJSON(pick<string>(step, 'input_json', 'InputJSON'))} maxHeight={180} /></div><div><span>Output</span><JsonView value={parseJSON(pick<string>(step, 'output_json', 'OutputJSON'))} maxHeight={180} /></div></div> }]} /></div> }
            })} />}
          </section>
          <aside className="content-section">
            <div className="section-heading"><div><h2>运行信息</h2><span>持久化快照</span></div></div>
            <Descriptions column={1} size="small" items={[{ key: 'run', label: 'Run ID', children: <code>{runId}</code> }, { key: 'thread', label: 'Thread ID', children: <code>{threadId || '—'}</code> }, { key: 'parent', label: 'Parent Run', children: runValue<string>(run, 'parent_run_id', 'ParentRunID') ? <Link to={`/runs/${runValue<string>(run, 'parent_run_id', 'ParentRunID')}`}>{shortId(runValue<string>(run, 'parent_run_id', 'ParentRunID'))}</Link> : '—' }, { key: 'start', label: '开始', children: formatTime(runValue<string>(run, 'started_at', 'StartedAt')) }, { key: 'finish', label: '结束', children: formatTime(runValue<string>(run, 'finished_at', 'FinishedAt')) }, { key: 'model', label: '模型', children: runValue<string>(run, 'model', 'Model') || '—' }]} />
            {run?.resume && <Collapse ghost items={[{ key: 'resume', label: '恢复租约诊断', children: <JsonView value={run.resume} maxHeight={260} /> }]} />}
          </aside>
        </div>
      )}
      {view === 'trace' && (traceQuery.isLoading ? <Skeleton active /> : traceQuery.error ? <ErrorState error={traceQuery.error} onRetry={() => traceQuery.refetch()} /> : (
        <div className="trace-layout">
          <section className="content-section">
            <div className="section-heading"><div><h2>Run 委派树</h2><span>父子 Run 执行关系</span></div><GitBranch size={18} /></div>
            {runTree.length ? <Tree showLine defaultExpandAll treeData={runTree} /> : <span>暂无 Run 关系</span>}
            <div className="trace-divider" />
            <div className="section-heading"><div><h2>委派记录</h2><span>{delegationRows.length} 条 A2A 委派</span></div></div>
            <Table rowKey={(row) => String(recordValue(row, 'delegation_id', 'DelegationID', 'id', 'ID') || '')} dataSource={delegationRows} pagination={false} scroll={{ x: 780 }} columns={[{ title: '来源 Agent', render: (_, row) => <code>#{String(recordValue(row, 'source_agent_id', 'SourceAgentID') || '—')}</code> }, { title: '目标 Agent', render: (_, row) => <code>#{String(recordValue(row, 'target_agent_id', 'TargetAgentID') || '—')}</code> }, { title: 'Capability', render: (_, row) => <code>{recordValue<string>(row, 'capability_code', 'CapabilityCode') || '—'}</code> }, { title: 'Child Run', render: (_, row) => { const id = recordValue<string>(row, 'child_run_id', 'ChildRunID') || ''; return id ? <Link to={`/runs/${id}`}>{shortId(id)}</Link> : '—' } }, { title: '状态', render: (_, row) => <StatusTag status={recordValue<string>(row, 'status', 'Status')} /> }]} />
          </section>
          <aside className="content-section"><div className="section-heading"><div><h2>Trace 概览</h2><span>关联资源数量</span></div></div><div className="metric-list">{[['Runs', traceQuery.data?.runs.length], ['Steps', traceQuery.data?.steps.length], ['Messages', traceQuery.data?.messages.length], ['Loops', traceQuery.data?.loops.length], ['Evaluations', traceQuery.data?.evaluations.length]].map(([label, value]) => <div key={String(label)}><span>{label}</span><strong>{value ?? '—'}</strong></div>)}</div></aside>
          <section className="content-section trace-wide">
            <div className="section-heading"><div><h2>消息</h2><span>Thread 与 Delegation 消息</span></div><MessagesSquare size={18} /></div>
            <Table rowKey={(row) => String(recordValue(row, 'message_id', 'MessageID', 'id', 'ID') || '')} dataSource={traceQuery.data?.messages || []} pagination={{ pageSize: 8, hideOnSinglePage: true }} scroll={{ x: 900 }} expandable={{ expandedRowRender: (row) => <JsonView value={{ content: parseJSON(recordValue(row, 'content_json', 'ContentJSON')), metadata: parseJSON(recordValue(row, 'metadata_json', 'MetadataJSON')) }} maxHeight={280} /> }} columns={[{ title: '消息', render: (_, row) => <code>{shortId(recordValue<string>(row, 'message_id', 'MessageID'), 22)}</code> }, { title: '发送方', render: (_, row) => `${recordValue<string>(row, 'sender_type', 'SenderType') || '—'} · ${recordValue<string>(row, 'sender_id', 'SenderID') || '—'}` }, { title: '类型', render: (_, row) => <code>{recordValue<string>(row, 'message_type', 'MessageType') || '—'}</code> }, { title: 'Run', render: (_, row) => { const id = recordValue<string>(row, 'run_id', 'RunID') || ''; return id ? <Link to={`/runs/${id}`}>{shortId(id)}</Link> : '—' } }, { title: '状态', render: (_, row) => <StatusTag status={recordValue<string>(row, 'status', 'Status')} /> }, { title: '时间', render: (_, row) => formatTime(recordValue<string>(row, 'created_at', 'CreatedAt')) }]} />
          </section>
          <section className="content-section trace-wide">
            <div className="section-heading"><div><h2>Loops 与 Evaluations</h2><span>展开查看 Loop 快照和独立评估结果</span></div></div>
            <Table rowKey={(row) => String(recordValue(row, 'loop_id', 'LoopID', 'id', 'ID') || '')} dataSource={traceQuery.data?.loops || []} pagination={false} expandable={{ expandedRowRender: (row) => <LoopDetailPanel loop={row} /> }} columns={[{ title: 'Loop', render: (_, row) => <code>{shortId(recordValue<string>(row, 'loop_id', 'LoopID'), 24)}</code> }, { title: '类型', render: (_, row) => <code>{recordValue<string>(row, 'loop_type', 'LoopType') || '—'}</code> }, { title: 'Run', render: (_, row) => { const id = recordValue<string>(row, 'run_id', 'RunID') || ''; return id ? <Link to={`/runs/${id}`}>{shortId(id)}</Link> : '—' } }, { title: '状态', render: (_, row) => <StatusTag status={recordValue<string>(row, 'status', 'Status')} /> }, { title: 'Token', render: (_, row) => recordValue<number>(row, 'total_tokens', 'TotalTokens') ?? '—' }, { title: '延迟', render: (_, row) => `${recordValue<number>(row, 'latency_ms', 'LatencyMS') || 0}ms` }]} />
          </section>
        </div>
      ))}
      {view === 'json' && <Collapse className="raw-json-collapse" items={[{ key: 'raw', label: '展开原始 JSON', children: <JsonView value={{ run, steps: stepsQuery.data, trace: traceQuery.data }} maxHeight={680} /> }]} />}
    </div>
  )
}
