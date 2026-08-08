# GoAI API / Protocol Contract

本文档描述 GoAI 当前 V1 对外可消费的 HTTP、AG-UI 和 A2A 契约。协议 Gateway 只负责解析和编码外部协议，内部统一落成 `Thread / Message / Run / Delegation`，不把外部字段直接扩散到服务层。

## 1. Common Conventions

### Base URL

以下示例假定服务地址为 `http://127.0.0.1:8080`。生产环境的远程 Agent Endpoint 必须使用 HTTPS；本地开发才允许 loopback HTTP。

### Authentication

- `/ping`、`/auth/register`、`/auth/login` 不需要 JWT。
- `/api/*` 需要 `Authorization: Bearer <jwt>`。
- 启用 RBAC 时，`/auth/register` 与管理员 `POST /api/users` 创建的用户会自动获得 `member` 角色；用户创建和角色绑定在同一事务内完成。
- `/api/agents/:agent_code/agui` 和 `/api/runs` 需要 `run:create`。
- Agent Registry 管理接口分别使用 `agent:create`、`agent:read`、`agent:update`、`agent:activate`；`agent:manage` 只提供跨 owner 管理能力，不替代路由权限。
- MCP Server 管理接口使用 `mcp:create`、`mcp:read`、`mcp:update`；`mcp:manage` 允许管理员跨 owner 管理，Tool Runtime 的调用权限为 `mcp:invoke`。
- Loop / Trace 查询接口使用 `loop:read`；member 只能查询自己 Run 关联的数据，admin 可以跨 owner 查询。
- A2A Agent Card discovery 公开；`message:send`、Task 查询/取消、终态 callback 及其他业务路由默认要求 `GoAI-HMAC-SHA256` 机器身份认证。

### Trace and Errors

普通 JSON API 使用 `code / message / data / trace_id` envelope。请求可以携带 `X-Trace-ID`，服务端会在响应头和响应体中复用同一个值。

完整错误码和 SSE 契约见 [统一响应契约](RESPONSE_CONTRACT.md)。AG-UI 与 A2A 保持各自协议的 wire format，不把平台 JSON envelope 套在协议事件或 A2A JSON-RPC 结果外层。

## 2. HTTP API Matrix

| Method | Path | Auth | Result |
| --- | --- | --- | --- |
| `GET` | `/ping` | public | JSON envelope |
| `POST` | `/auth/register` | public | JSON envelope |
| `POST` | `/auth/login` | public | JSON envelope with JWT |
| `POST` | `/api/chat` | JWT + `chat:use` | debug SSE |
| `POST` | `/api/agents/:agent_code/agui` | JWT + `run:create` | AG-UI SSE |
| `POST/GET` | `/api/agents` | JWT + `agent:create` / `agent:read` | create or list managed Agents |
| `GET/PUT` | `/api/agents/:agent_code` | JWT + `agent:read` / `agent:update` | Agent detail or metadata update |
| `POST` | `/api/agents/:agent_code/activate` | JWT + `agent:activate` | validate and publish Agent |
| `POST` | `/api/agents/:agent_code/deactivate` | JWT + `agent:activate` | deactivate Agent |
| `POST/GET/PUT` | `/api/agents/:agent_code/capabilities[...]` | JWT + `agent:update` / `agent:read` | manage Capability assets |
| `POST/GET/PUT` | `/api/agents/:agent_code/endpoints[...]` | JWT + `agent:update` / `agent:read` | manage and health-check A2A Endpoints |
| `POST/GET/PUT` | `/api/mcp/servers[...]` | JWT + `mcp:create` / `mcp:read` / `mcp:update` | manage MCP Servers and discovery snapshots |
| `POST` | `/api/runs` | JWT + `run:create` | JSON envelope, `202` or idempotent `200` |
| `GET` | `/api/runs/:run_id` | JWT + `run:read` | JSON envelope |
| `GET` | `/api/runs/:run_id/steps` | JWT + `run:read` | JSON envelope |
| `GET` | `/api/runs/:run_id/trace` | JWT + `loop:read` | Run/Step/Loop/A2A trace snapshot |
| `GET` | `/api/runs/:run_id/loops` | JWT + `loop:read` | Loop list |
| `GET` | `/api/loops/:loop_id` | JWT + `loop:read` | Loop detail with evaluations |
| `GET` | `/api/loops/:loop_id/evaluations` | JWT + `loop:read` | Evaluation list |
| `POST` | `/api/runs/:run_id/replay` | JWT + `run:replay` | JSON envelope, `202` or idempotent `200` |
| `GET` | `/a2a/agents/:agent_code/.well-known/agent-card.json` | protocol gateway | A2A Agent Card |
| `POST` | `/a2a/agents/:agent_code/message:send` | protocol gateway | A2A Task |
| `GET` | `/a2a/agents/:agent_code/tasks/:task_id` | protocol gateway | A2A Task |
| `POST` | `/a2a/agents/:agent_code/tasks/:task_id:cancel` | protocol gateway | canceled A2A Task |
| `POST` | `/a2a/agents/:source_agent_code/callbacks/tasks/:task_id` | protocol gateway | callback accepted |

## 3. Authentication Examples

### Register

```http
POST /auth/register HTTP/1.1
Content-Type: application/json
X-Trace-ID: trace-register-001

{
  "username": "alice",
  "email": "alice@example.com",
  "password": "change-me-in-local-development"
}
```

```json
{
  "code": "OK",
  "message": "register success",
  "data": {"user_id": 1, "username": "alice"},
  "trace_id": "trace-register-001"
}
```

### Login

```http
POST /auth/login HTTP/1.1
Content-Type: application/json

{"username":"alice","password":"change-me-in-local-development"}
```

```json
{
  "code": "OK",
  "message": "login success",
  "data": {"token": "<jwt returned by the service>"},
  "trace_id": "trace-login-001"
}
```

不要把真实 JWT、密码或 API Key 写入仓库、日志和文档。

## 4. Run API Examples

### Create a Run

```http
POST /api/runs HTTP/1.1
Authorization: Bearer <jwt>
Content-Type: application/json
Idempotency-Key: create-demo-001
X-Trace-ID: trace-run-001

{
  "agent_code": "planner",
  "workflow_version": 1,
  "thread_id": "thread-demo",
  "trigger_type": "api",
  "input": {"prompt": "拆解这个需求"},
  "provider": "deepseek",
  "model": "deepseek-chat"
}
```

```json
{
  "code": "OK",
  "message": "success",
  "data": {"run_id": "run-demo", "status": "queued"},
  "trace_id": "trace-run-001"
}
```

首次创建返回 `202 Accepted`。同一用户、operation 和 `Idempotency-Key` 对应相同请求时返回 `200 OK` 和原 Run；请求哈希不同返回 `409 IDEMPOTENCY_KEY_REUSED`。

### Query and Replay

```http
GET /api/runs/run-demo HTTP/1.1
Authorization: Bearer <jwt>
```

```http
GET /api/runs/run-demo/steps HTTP/1.1
Authorization: Bearer <jwt>
```

```http
POST /api/runs/run-demo/replay HTTP/1.1
Authorization: Bearer <jwt>
Idempotency-Key: replay-demo-001
```

Replay 使用原 Run 的输入和 Workflow 创建新 Run；管理员可以跨用户查询或回放，普通用户只能访问自己的 Run。

### Loop / Trace 查询

Loop / Trace 查询是只读管理接口，不改变 Run、Delegation 或 Evaluation 状态。`GET /api/runs/:run_id/trace` 返回根 Run、可访问的 A2A 子 Run、RunStep、LoopRecord、Delegation、DelegationGroup、Message 和 LoopEvaluation；集合字段始终返回空数组而不是 `null`。

```http
GET /api/runs/run-demo/trace HTTP/1.1
Authorization: Bearer <jwt>
X-Trace-ID: trace-observe-001
```

```json
{
  "code": "OK",
  "message": "success",
  "data": {
    "root_run": {"RunID": "run-demo", "Status": "success"},
    "runs": [],
    "steps": [],
    "loops": [],
    "delegations": [],
    "delegation_groups": [],
    "messages": [],
    "evaluations": []
  },
  "trace_id": "trace-observe-001"
}
```

`/api/runs/:run_id/loops` 只返回指定 Run 的 Loop；`/api/loops/:loop_id` 返回单个 Loop 和其评估结果；`/api/loops/:loop_id/evaluations` 只返回评估记录。普通用户访问其他 owner 的 Run 或 Loop 返回 `403 AUTH_FORBIDDEN`，不存在的 Loop 返回 `404 LOOP_NOT_FOUND`。

Run 查询保持既有 Run 字段格式，并在 Parent resume 存在时增加可选的 `resume` 对象：

```json
{
  "code": "OK",
  "message": "success",
  "data": {
    "RunID": "run-parent-001",
    "Status": "running",
    "CurrentStep": "summarize",
    "resume": {
      "delegation_id": "delegation-review-001",
      "status": "claimed",
      "error": "resume execution lease expired before completion; recovery event scheduled",
      "publish_attempts": 2,
      "execution_attempt": 3,
      "lease_owner": "resume-worker-2",
      "lease_claimed_at": "2026-08-03T12:00:00Z",
      "lease_heartbeat_at": "2026-08-03T12:00:05Z",
      "lease_expires_at": "2026-08-03T12:00:30Z"
    }
  },
  "trace_id": "trace-parent"
}
```

`resume` 是受 JWT、RBAC 和 owner/admin 约束的管理诊断信息。租约字段不会进入公开 A2A Task metadata；没有恢复记录时该字段省略。

## 5. Agent Registry Management API

Agent Registry is the management plane. It controls which Agent identities, capabilities, and protocol endpoints are eligible for discovery. It never replaces A2A execution: local Agents still use loopback HTTP and remote Agents use HTTPS through the same A2A Gateway contract.

### Create and publish an Agent

1. POST /api/agents creates an inactive Agent owned by the current user.
2. POST /api/agents/:agent_code/capabilities adds a capability asset. V1 publication requires at least one active Workflow capability backed by the same Agent's active Workflow; Tool and Custom assets are not advertised as executable Agent Card skills yet.
3. POST /api/agents/:agent_code/endpoints adds an inactive A2A endpoint.
4. POST /api/agents/:agent_code/endpoints/:endpoint_code/health-check discovers the Agent Card, validates its declared agentCode, transport, binding, and Delegation extension, then marks the endpoint active.
5. POST /api/agents/:agent_code/activate validates all publication invariants and publishes the Agent.

~~~http
POST /api/agents HTTP/1.1
Authorization: Bearer <jwt>
Content-Type: application/json

{"agent_code":"writer","name":"Writer","description":"Generates articles"}
~~~

~~~http
POST /api/agents/writer/capabilities HTTP/1.1
Authorization: Bearer <jwt>
Content-Type: application/json

{
  "capability_code": "write",
  "name": "Write",
  "capability_type": "workflow",
  "workflow_id": 42,
  "version": "1",
  "input_schema_json": "{\"type\":\"object\"}",
  "output_schema_json": "{\"type\":\"object\"}"
}
~~~

~~~http
POST /api/agents/writer/endpoints HTTP/1.1
Authorization: Bearer <jwt>
Content-Type: application/json

{
  "endpoint_code": "primary",
  "protocol": "a2a",
  "transport": "https",
  "address": "https://agents.example.com/a2a/agents/writer",
  "auth_type": "goai_hmac_sha256",
  "credential_ref": "writer-a2a-key"
}
~~~

The API returns only credential_ref; the referenced secret is never stored in or returned by Registry records. Endpoint config_json is restricted to non-sensitive transport metadata; secret, password, token, private-key, authorization, and credential fields are rejected. Endpoint updates reset health to inactive. An active Agent cannot lose its last executable Workflow Capability or healthy Endpoint without first being deactivated.

Member access is owner-scoped. A caller with the separate agent:manage permission can bypass ownership, but still needs the route-specific action permission.

## 6. AG-UI Gateway

### Request

入口：`POST /api/agents/:agent_code/agui`。

```http
POST /api/agents/planner/agui HTTP/1.1
Authorization: Bearer <jwt>
Content-Type: application/json
X-Trace-ID: trace-agui-001

{
  "threadId": "thread-demo",
  "runId": "run-demo",
  "state": {},
  "messages": [
    {
      "id": "message-demo",
      "role": "user",
      "content": "请把需求拆成三个子任务"
    }
  ],
  "tools": [],
  "context": []
}
```

V1 接受普通文本 `user / assistant / system / developer` 消息。非空 `state`、`tools`、`context` 和 `forwardedProps` 仍会在进入流式阶段前拒绝；多模态和高级消息字段也不在 V1 范围内。`parentRunId` 与 `resume` 已支持，分别用于创建 AG-UI 分支 Run 和恢复等待用户输入的 Run。

`parentRunId` 只表达 AG-UI Thread 内的 Run lineage：父 Run 必须属于当前用户，子 Run 默认继承父 Run 的 Thread；显式传入的 `threadId` 必须与父 Run 一致。它不等价于 A2A Delegation 的 Parent/Child Run 关系。

当 Run 进入 `waiting_input` 时，服务端会在 `RUN_FINISHED` 事件中返回 interrupt outcome。客户端使用同一个 `runId` 和 `resume` 数组重新调用本入口；每个 entry 必须包含 `interruptId`、`status`（`resolved` 或 `cancelled`）和 JSON `payload`。所有 pending interrupt 处理完成后，Run 会重新排队，并从持久化的后继节点继续执行。

### Event Stream

成功进入流式阶段后，HTTP 状态为 `200`，响应类型为 `text/event-stream`。每个 `data:` 是官方 AG-UI 事件 JSON：

```text
data: {"type":"RUN_STARTED","threadId":"thread-demo","runId":"run-demo"}

data: {"type":"STEP_STARTED","stepName":"prepare"}

data: {"type":"TEXT_MESSAGE_START","messageId":"message-result","role":"assistant"}

data: {"type":"TEXT_MESSAGE_CONTENT","messageId":"message-result","delta":"已拆分"}

data: {"type":"TEXT_MESSAGE_END","messageId":"message-result"}

data: {"type":"STEP_FINISHED","stepName":"prepare"}

data: {"type":"RUN_FINISHED","threadId":"thread-demo","runId":"run-demo"}
```

暂停等待用户输入时：

```text
data: {"type":"RUN_FINISHED","threadId":"thread-demo","runId":"run-demo","outcome":{"type":"interrupt","interrupts":[{"id":"approval","reason":"approval_required","message":"Approve this action?"}]}}
```

恢复请求示例：

```http
POST /api/agents/planner/agui HTTP/1.1
Authorization: Bearer <jwt>
Content-Type: application/json

{
  "runId": "run-demo",
  "resume": [
    {
      "interruptId": "approval",
      "status": "resolved",
      "payload": {"approved": true}
    }
  ]
}
```

当前可能出现的事件包括 `RUN_STARTED`、`STEP_STARTED`、`STEP_FINISHED`、`TEXT_MESSAGE_START`、`TEXT_MESSAGE_CONTENT`、`TEXT_MESSAGE_END`、`RUN_FINISHED` 和 `RUN_ERROR`。流开始前的参数错误使用普通 JSON envelope；流开始后的失败使用 AG-UI `RUN_ERROR`。

## 7. A2A Gateway

A2A 是 Agent 与 Agent 之间的语义通信协议。Kafka 只能承载 GoAI 内部异步执行消息，不能替代 A2A；本地 Agent 和远程 Agent 必须经过同一 A2A Gateway 契约。

### Agent Card

```http
GET /a2a/agents/writer/.well-known/agent-card.json HTTP/1.1
Accept: application/json
```

最小响应形状如下，实际 skills 来自目标 Agent 当前启用的 Capability：

```json
{
  "name": "Writer",
  "description": "负责文档写作",
  "version": "1.0",
  "supportedInterfaces": [
    {
      "url": "http://127.0.0.1:8080/a2a/agents/writer",
      "protocolBinding": "HTTP+JSON",
      "protocolVersion": "0.3.0"
    }
  ],
  "capabilities": {
    "pushNotifications": true,
    "extensions": [
      {
        "uri": "https://goai.dev/extensions/delegation/v1",
        "description": "GoAI multi-agent delegation metadata",
        "required": true
      }
    ]
  },
  "defaultInputModes": ["text/plain", "application/json"],
  "defaultOutputModes": ["application/json"],
  "skills": [
    {
      "id": "write",
      "name": "Write",
      "description": "生成文档",
      "tags": ["text", "v1"],
      "inputModes": ["text/plain", "application/json"],
      "outputModes": ["application/json"]
    }
  ]
}
```

本地 HTTP Endpoint 必须是 loopback 地址；远程 Endpoint 必须使用 HTTPS。

### Delegate with `message:send`

```http
POST /a2a/agents/writer/message:send HTTP/1.1
Content-Type: application/json

{
  "message": {
    "messageId": "msg-001",
    "contextId": "thread-demo",
    "taskId": "run-child-001",
    "extensions": ["https://goai.dev/extensions/delegation/v1"],
    "metadata": {
      "https://goai.dev/extensions/delegation/v1": {
        "sourceAgentCode": "planner",
        "capabilityCode": "write",
        "parentRunId": "run-parent-001"
      }
    },
    "parts": [{"text": "把需求整理成文档"}],
    "role": "ROLE_USER"
  }
}
```

出站 Runtime 会在官方 SendMessageConfig 中设置 `returnImmediately=true`，并携带由 `A2A_CALLBACK_BASE_URL`、源 Agent code 和稳定 Task ID 生成的 PushConfig。远程 callback URL 必须使用 HTTPS，本地开发只允许 loopback HTTP。

成功响应是官方 A2A Task；accepted/working 表示目标已持久化委派，不表示业务已经完成：

```json
{
  "id": "run-child-001",
  "contextId": "thread-demo",
  "status": {"state": "TASK_STATE_SUBMITTED"},
  "history": []
}
```

同一协议重试使用相同标识时会复用已有协作记录；复用标识但改变请求内容会返回 A2A invalid request 错误。

### Query Task

```http
GET /a2a/agents/writer/tasks/run-child-001?historyLength=10 HTTP/1.1
Accept: application/json
```

返回的 Task 状态由 Child Run 和 Delegation 快照映射而来。Task 查询保留为诊断与兼容接口；Parent Worker 不轮询该接口等待终态，主链路使用 Push Notification callback。

### Cancel Task

来源 Agent 只能取消自己发起且目标 Agent 匹配的 Task：

```http
POST /a2a/agents/writer/tasks/run-child-001:cancel HTTP/1.1
Authorization: GoAI-HMAC-SHA256 ...
X-GoAI-Agent-Code: planner
X-GoAI-Timestamp: 2026-08-08T10:00:00Z
X-GoAI-Nonce: nonce-cancel-001
X-GoAI-Content-SHA256: <sha256-of-empty-body>
```

目标 Runtime 在事务中将可取消的 Child Run 推进为 `cancelled`，将活动 RunStep 推进为 `skipped`，再通过已有 PushConfig callback 回送取消终态。已处于 `completed`、`failed` 或 `canceled` 的 Task 重复取消直接返回当前终态快照；来源不匹配、目标不匹配或 Task 不存在分别映射为 `Unauthorized` 或 `TaskNotFound`。

### Terminal Callback and Parent Resume

Target Child Run 进入 `completed / failed / canceled / rejected` 后，将官方 A2A `StreamResponse` 事件发送到：

```http
POST /a2a/agents/planner/callbacks/tasks/run-child-001 HTTP/1.1
Authorization: GoAI-HMAC-SHA256 ...
X-GoAI-Agent-Code: writer
A2A-Notification-Token: <task-bound-token>
X-Trace-ID: trace-parent
Content-Type: application/json
```

callback 处理规则：

- Gateway 先校验 HMAC 机器身份，再校验 PushConfig 中绑定的 notification token、来源/目标 Agent、Task ID 和 Delegation。
- 只接受终态事件；相同终态事件通过事件哈希幂等返回 `202`，同一 Task 的冲突终态返回 `409`。
- 成功事件幂等写入跨 Runtime 一致的 Result Message，并发布内部 Kafka `run_resume` 消息。
- 失败或取消事件终结 Parent Run，不发布成功 resume。
- Resume Worker 原子 claim `waiting_external` Parent Run，并从 Delegation 保存的后继节点游标继续 Eino Graph；重复消息安全 no-op。
- callback 或 resume 发布失败由 RecoveryWorker 基于持久化状态恢复，进程重启不丢失恢复意图。

Parent Run 等待 callback 时，AG-UI SSE 继续观察持久化快照；AG-UI 客户端断开不会取消已持久化的 Child Run 或 Delegation。

### A2A Errors

A2A 错误使用官方 JSON-RPC/HTTP+JSON 错误结构，不使用 GoAI 普通 HTTP envelope：

- 目标 Agent 或 capability 不存在：`InvalidParams`
- Task/Run 不存在：`TaskNotFound`
- 稳定 Task、Message 或 Delegation 标识冲突：`InvalidRequest`
- 未映射 Runtime 错误：`InternalError`

## 8. Protocol-to-Domain Mapping

| External concept | Internal model |
| --- | --- |
| AG-UI `threadId` | `Thread.thread_id` |
| AG-UI input message | `Message` with `message_type=input` |
| AG-UI run | `Run` with `trigger_type=agui` |
| A2A message:send | `Message + Delegation + Child Run` |
| A2A Task | Child `Run`/`Delegation` snapshot |
| Eino Workflow/Graph | Agent capability execution, not communication transport |

协议 Gateway 不允许通过进程内 Service 直调绕过 A2A。Workflow 的 Agent 节点必须经过 A2A Client；这样本地和远程 Agent 执行的通信语义一致。

## 9. Compatibility and Current Limits

- V1 只做文本消息和 callback 驱动的 Child suspend/resume；显式 `agent_group` 已支持多个 Child Run 的 `all`、`any`、`quorum` fan-out/fan-in 和部分失败聚合。不在当前范围内的是多模态和任意并行 DAG。
- AG-UI `parentRunId` 分支与用户主动 resume 已在 Issue #61 落地；它们与 A2A Delegation 的 Parent/Child Run 不是同一语义。当前仍不支持多模态消息和任意并行 DAG。
- 远程来源 Delegation 当前可能使用远程 A2A Task ID 填充 `ChildRunID`；后续可独立建模 `RemoteTaskID`，不改变当前协议字段。
- A2A 业务请求必须携带 `Authorization`、`X-GoAI-Agent-Code`、`X-GoAI-Timestamp`、`X-GoAI-Nonce`、`X-GoAI-Content-SHA256`；来源身份必须匹配委派 metadata，Task 查询仅允许委派来源 Agent。
- 外部协议版本升级时，先更新 Gateway 适配和本文件，再变更内部领域模型；不要把 SDK 类型直接作为 Service 接口。
