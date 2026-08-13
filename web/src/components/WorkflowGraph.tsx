import {
  Background,
  Controls,
  Handle,
  MarkerType,
  MiniMap,
  Position,
  ReactFlow,
  type Edge,
  type Node,
  type NodeProps,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { Tooltip } from 'antd'
import { Bot, Boxes, CircleDot, Hammer, PauseCircle, Sparkles, Wrench } from 'lucide-react'
import { useMemo } from 'react'
import type { RunStep, WorkflowDefinition } from '../api/types'
import { buildWorkflowGraph, type WorkflowGraphNodeData } from '../lib/workflowGraph'

const NODE_ICONS: Record<string, typeof Bot> = {
  llm: Sparkles,
  agent: Bot,
  agent_tool: Hammer,
  agent_group: Boxes,
  tool: Wrench,
  interrupt: PauseCircle,
}

function WorkflowGraphNode({ data }: NodeProps<Node<WorkflowGraphNodeData>>) {
  const Icon = NODE_ICONS[data.nodeType] || CircleDot
  const statusClass = data.status ? ` wf-status-${data.status}` : ''
  const body = (
    <div className={`wf-node wf-type-${data.nodeType}${statusClass}`}>
      <Handle type="target" position={Position.Left} className="wf-handle" />
      <div className="wf-node-icon"><Icon size={17} /></div>
      <div className="wf-node-body">
        <div className="wf-node-head">
          <span className="wf-node-type">{data.nodeType}</span>
          {data.isEntry && <span className="wf-node-entry">ENTRY</span>}
          {data.status && <span className="wf-node-dot" />}
        </div>
        <strong className="wf-node-key">{data.nodeKey}</strong>
        {data.summary && <span className="wf-node-summary">{data.summary}</span>}
        {data.status && (
          <span className="wf-node-run">
            {data.status}
            {typeof data.latencyMs === 'number' && ` · ${data.latencyMs}ms`}
            {typeof data.attempt === 'number' && data.attempt > 1 && ` · 第 ${data.attempt} 次`}
          </span>
        )}
      </div>
      <Handle type="source" position={Position.Right} className="wf-handle" />
    </div>
  )
  return data.errorMessage ? <Tooltip title={data.errorMessage}>{body}</Tooltip> : body
}

export const WORKFLOW_NODE_TYPES = { workflowNode: WorkflowGraphNode }
const NODE_TYPES = WORKFLOW_NODE_TYPES

export interface WorkflowGraphProps {
  definition: WorkflowDefinition
  steps?: RunStep[]
  height?: number | string
  onNodeClick?: (nodeKey: string) => void
}

// WorkflowGraph 是只读的工作流 DAG 画布：渲染定义结构，可选叠加 Run 步骤的执行状态。
export function WorkflowGraph({ definition, steps, height = 520, onNodeClick }: WorkflowGraphProps) {
  const graph = useMemo(() => buildWorkflowGraph(definition, steps), [definition, steps])
  const nodes: Node<WorkflowGraphNodeData>[] = useMemo(
    () => graph.nodes.map((node) => ({ ...node, type: 'workflowNode' })),
    [graph.nodes],
  )
  const edges: Edge[] = useMemo(
    () =>
      graph.edges.map((edge) => ({
        id: edge.id,
        source: edge.source,
        target: edge.target,
        markerEnd: { type: MarkerType.ArrowClosed, color: edge.executed ? '#2f6d62' : '#9fb0ac' },
        animated: edge.executed,
        style: edge.executed
          ? { stroke: '#2f6d62', strokeWidth: 2 }
          : { stroke: '#b3c2be', strokeWidth: 1.5 },
      })),
    [graph.edges],
  )
  return (
    <div className="workflow-canvas" style={{ height }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={NODE_TYPES}
        fitView
        minZoom={0.3}
        maxZoom={1.6}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={Boolean(onNodeClick)}
        proOptions={{ hideAttribution: true }}
        onNodeClick={onNodeClick ? (_, node) => onNodeClick(node.id) : undefined}
      >
        <Background color="#d6dddb" gap={24} size={1} />
        <Controls showInteractive={false} />
        {graph.nodes.length > 6 && <MiniMap pannable zoomable className="wf-minimap" />}
      </ReactFlow>
    </div>
  )
}
