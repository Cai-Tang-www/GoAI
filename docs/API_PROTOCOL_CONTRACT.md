# GoAI API / Protocol Contract

本文档描述 GoAI 当前 V1 对外可消费的 HTTP、AG-UI 和 A2A 契约。协议 Gateway 只负责解析和编码外部协议，内部统一落成 `Thread / Message / Run / Delegation`，不把外部字段直接扩散到服务层。

## 1. Common Conventions

### Base URL

以下示例假定服务地址为 `http://127.0.0.1:8080`。生产环境的远程 Agent Endpoint 必须使用 HTTPS；本地开发才允许 loopback HTTP。

### Authentication

- `/ping`、`/auth/register`、`/auth/login` 不需要 JWT。
- `/api/*` 需要 `Authorization: Bearer <jwt>`。
- `/api/agents/:agent_code/agui` 和 `/api/runs` 需要 `run:create`。
- A2A Agent Card discovery 公开；`message:send`、Task 查询及其他业务路由默认要求 `GoAI-HMAC-SHA256` 机器身份认证。

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
| `POST` | `/api/runs` | JWT + `run:create` | JSON envelope, `202` or idempotent `200` |
| `GET` | `/api/runs/:run_id` | JWT + `run:read` | JSON envelope |
| `GET` | `/api/runs/:run_id/steps` | JWT + `run:read` | JSON envelope |
| `POST` | `/api/runs/:run_id/replay` | JWT + `run:replay` | JSON envelope, `202` or idempotent `200` |
| `GET` | `/a2a/agents/:agent_code/.well-known/agent-card.json` | protocol gateway | A2A Agent Card |
| `POST` | `/a2a/agents/:agent_code/message:send` | protocol gateway | A2A Task |
| `GET` | `/a2a/agents/:agent_code/tasks/:task_id` | protocol gateway | A2A Task |

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

## 5. AG-UI Gateway

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

V1 接受普通文本 `user / assistant / system / developer` 消息。非空 `state`、`tools`、`context`、`forwardedProps`、`parentRunId` 和 `resume` 会在进入流式阶段前拒绝；多模态和高级消息字段也不在 V1 范围内。

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

当前可能出现的事件包括 `RUN_STARTED`、`STEP_STARTED`、`STEP_FINISHED`、`TEXT_MESSAGE_START`、`TEXT_MESSAGE_CONTENT`、`TEXT_MESSAGE_END`、`RUN_FINISHED` 和 `RUN_ERROR`。流开始前的参数错误使用普通 JSON envelope；流开始后的失败使用 AG-UI `RUN_ERROR`。

## 6. A2A Gateway

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

成功响应是官方 A2A Task：

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

返回的 Task 状态由 Child Run 和 Delegation 快照映射而来。当前 V1 支持查询和轮询，不支持取消、Push Notification 或流式 A2A 消息。

### A2A Errors

A2A 错误使用官方 JSON-RPC/HTTP+JSON 错误结构，不使用 GoAI 普通 HTTP envelope：

- 目标 Agent 或 capability 不存在：`InvalidParams`
- Task/Run 不存在：`TaskNotFound`
- 稳定 Task、Message 或 Delegation 标识冲突：`InvalidRequest`
- 未映射 Runtime 错误：`InternalError`

## 7. Protocol-to-Domain Mapping

| External concept | Internal model |
| --- | --- |
| AG-UI `threadId` | `Thread.thread_id` |
| AG-UI input message | `Message` with `message_type=input` |
| AG-UI run | `Run` with `trigger_type=agui` |
| A2A message:send | `Message + Delegation + Child Run` |
| A2A Task | Child `Run`/`Delegation` snapshot |
| Eino Workflow/Graph | Agent capability execution, not communication transport |

协议 Gateway 不允许通过进程内 Service 直调绕过 A2A。Workflow 的 Agent 节点必须经过 A2A Client；这样本地和远程 Agent 执行的通信语义一致。

## 8. Compatibility and Current Limits

- V1 只做文本消息和同步轮询式 A2A 结果收敛，不做多模态、Push、Cancel、callback suspend/resume 或多个 Child Run 并行聚合。
- A2A 业务请求必须携带 `Authorization`、`X-GoAI-Agent-Code`、`X-GoAI-Timestamp`、`X-GoAI-Nonce`、`X-GoAI-Content-SHA256`；来源身份必须匹配委派 metadata，Task 查询仅允许委派来源 Agent。
- 外部协议版本升级时，先更新 Gateway 适配和本文件，再变更内部领域模型；不要把 SDK 类型直接作为 Service 接口。
