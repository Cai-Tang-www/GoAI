export interface Envelope<T = unknown> {
  code: string
  message: string
  data: T
  trace_id: string
}

export interface User {
  id: number
  username: string
  email: string
  created_at?: string
  updated_at?: string
}

export interface Agent {
  agent_code: string
  name: string
  description: string
  owner_user_id: number
  status: string
  created_at: string
  updated_at: string
  capabilities?: Capability[]
  endpoints?: AgentEndpoint[]
  active_workflow?: { id: number; version: number; checksum: string } | null
}

export interface Capability {
  capability_code: string
  name: string
  description: string
  capability_type: 'workflow' | 'remote' | 'tool' | 'custom'
  workflow_id: number | null
  version: string
  input_schema_json: string
  output_schema_json: string
  config_json: string
  status: string
  created_at?: string
  updated_at?: string
}

export interface AgentEndpoint {
  endpoint_code: string
  protocol: string
  transport: string
  address: string
  auth_type: string
  credential_ref: string
  config_json: string
  status: string
  last_healthy_at?: string | null
  created_at?: string
  updated_at?: string
}

export interface WorkflowNode {
  key: string
  type: string
  config?: Record<string, unknown>
}

export interface WorkflowEdge {
  from: string
  to: string
}

export interface WorkflowDefinition {
  entry_node: string
  nodes: WorkflowNode[]
  edges: WorkflowEdge[]
}

export interface Workflow {
  id: number
  agent_code: string
  version: number
  definition: WorkflowDefinition
  checksum: string
  is_active: boolean
  created_by: number
  capabilities: Array<{ capability_code: string; version: string; status: string }>
  created_at: string
  updated_at: string
}

export interface Run {
  ID?: number
  RunID?: string
  ThreadID?: string
  ParentRunID?: string | null
  TraceID?: string
  Status?: string
  CurrentStep?: string
  ErrorMessage?: string
  TriggerType?: string
  Provider?: string
  Model?: string
  StartedAt?: string | null
  FinishedAt?: string | null
  CreatedAt?: string
  UpdatedAt?: string
  run_id?: string
  thread_id?: string
  parent_run_id?: string | null
  trace_id?: string
  status?: string
  current_step?: string
  error_message?: string
  trigger_type?: string
  provider?: string
  model?: string
  started_at?: string | null
  finished_at?: string | null
  created_at?: string
  updated_at?: string
  resume?: Record<string, unknown>
  delegation_groups?: unknown[]
  [key: string]: unknown
}

export interface RunStep {
  ID?: number
  RunID?: string
  StepKey?: string
  StepType?: string
  Attempt?: number
  Status?: string
  InputJSON?: string
  OutputJSON?: string
  LatencyMS?: number
  ErrorMessage?: string
  StartedAt?: string | null
  FinishedAt?: string | null
  step_key?: string
  step_type?: string
  attempt?: number
  status?: string
  input_json?: string
  output_json?: string
  latency_ms?: number
  error_message?: string
  started_at?: string | null
  finished_at?: string | null
  [key: string]: unknown
}

export interface RunTrace {
  root_run: Run
  runs: Run[]
  steps: RunStep[]
  loops: Array<Record<string, unknown>>
  delegations: Array<Record<string, unknown>>
  delegation_groups: Array<Record<string, unknown>>
  messages: Array<Record<string, unknown>>
  evaluations: Array<Record<string, unknown>>
}

export interface MCPServer {
  server_code: string
  name: string
  description: string
  owner_user_id: number
  transport: string
  endpoint: string
  auth_type: string
  credential_ref?: string
  config_json?: string
  status: string
  config_version: number
  last_error?: string
  last_healthy_at?: string | null
  created_at: string
  updated_at: string
}

export interface MCPTool {
  tool_name: string
  description: string
  input_schema_json: string
  output_schema_json?: string
  created_at: string
  updated_at: string
}

export interface AGUIEvent {
  type: string
  threadId?: string
  runId?: string
  stepName?: string
  messageId?: string
  delta?: string
  message?: string
  outcome?: {
    type?: string
    interrupts?: Array<{ id: string; reason?: string; message?: string; responseSchema?: unknown; metadata?: Record<string, unknown> }>
  }
  [key: string]: unknown
}

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant' | 'system' | 'developer'
  content: string
  runId?: string
  pending?: boolean
  failed?: boolean
  createdAt: string
}

export interface ChatSession {
  id: string
  title: string
  agentCode: string
  threadId: string
  currentRunId?: string
  messages: ChatMessage[]
  updatedAt: string
}

export interface RecentRun {
  runId: string
  threadId?: string
  agentCode?: string
  status?: string
  title?: string
  visitedAt: string
}
