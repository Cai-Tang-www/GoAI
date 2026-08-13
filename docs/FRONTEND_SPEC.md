# GoAI 前端建设参考（交给 AI 生成用）

本文档是 GoAI 管理控制台 + 聊天工作台前端的需求参考。目标读者是负责生成前端代码的 AI 或工程师。后端已完整实现，本文所有 API 均真实可用；机器可读契约见同目录 [`openapi.yaml`](openapi.yaml)，协议细节见 [`API_PROTOCOL_CONTRACT.md`](API_PROTOCOL_CONTRACT.md)，错误码见 [`RESPONSE_CONTRACT.md`](RESPONSE_CONTRACT.md)。

---

## 1. 后端事实（生成前必读的硬约束）

1. **Base URL**：本地开发 `http://localhost:8080`。
2. **后端没有 CORS 中间件**。前端开发必须用 dev server 代理（如 Vite `server.proxy` 把 `/api`、`/auth`、`/a2a`、`/ping` 转发到 8080），生产建议同域部署（Nginx 反代）。不要假设可以跨域直连。
3. **统一响应 envelope**：除 SSE 和 A2A 协议路由外，所有 JSON 响应都是：

```json
{ "code": "OK", "message": "success", "data": {}, "trace_id": "trace_xxx" }
```

`code != "OK"` 即业务失败。`trace_id` 必须在前端错误提示中展示（供排查）。请求可携带 `X-Trace-ID` 头，服务端会原样回传。

4. **认证**：`POST /auth/login` 返回 `data.token`（JWT）。此后所有 `/api/*` 请求携带 `Authorization: Bearer <token>`。JWT payload 中只有 `user_id` 和标准 claims；**没有 `/api/users/me` 接口**，前端需 base64 解码 JWT 取 `user_id`，再调 `GET /api/users/:id` 获取本人资料（member 有 `user:read_self` 权限，只能查自己）。
5. **RBAC**：**没有返回当前用户权限列表的接口**。前端不要尝试预取权限做菜单隐藏；正确做法是所有功能可见，收到 `403 AUTH_FORBIDDEN` 时在 UI 上明确提示无权限。预置角色：`admin`（全部权限）、`member`（默认，无 `user:manage`，其余管理均为 owner-scoped——只能看到/管理自己创建的资源）。
6. **SSE 是 POST 请求**。浏览器原生 `EventSource` 只支持 GET，必须使用 `@microsoft/fetch-event-source` 或手写 `fetch` + `ReadableStream` 解析 `text/event-stream`。
7. **列表接口无分页参数**，直接返回全量数组，前端本地分页/搜索即可。
8. **幂等键**：`POST /api/runs`、两个 replay 接口支持 `Idempotency-Key` 头。前端为每次用户主动操作生成一个 UUID 作为幂等键，网络重试时复用同一个键；收到 `409 IDEMPOTENCY_KEY_REUSED` 说明键被不同请求复用，应换新键。
9. **A2A 路由（`/a2a/*`）不是给前端用的**，那是 Agent 间机器通信（HMAC 签名）。前端最多只读展示 Agent Card（`GET /a2a/agents/:agent_code/.well-known/agent-card.json`，公开无鉴权）。

---

## 2. 推荐技术栈

- React 18 + TypeScript + Vite
- 路由：React Router；状态：TanStack Query（服务端状态）+ 少量 Zustand/Context（token、当前用户）
- UI 组件库：Ant Design（管理台密集表单/表格场景合适）
- SSE：`@microsoft/fetch-event-source`
- Workflow 图可视化：React Flow（只读渲染 DAG 必须有；可视化编辑为加分项，JSON 编辑器兜底）
- JSON 编辑：Monaco Editor 或 CodeMirror（Workflow definition、Capability schema、trace 展示都需要）

---

## 3. 信息架构与页面清单

```text
/login                        登录/注册
/                             重定向到 /chat
/chat                         聊天工作台（AG-UI，核心页面）
/agents                       Agent 列表
/agents/:agentCode            Agent 详情（Tab：概览 | Capability | Endpoint | Workflow | 发布）
/agents/:agentCode/workflows/:version   Workflow 版本详情（DAG 渲染 + JSON）
/runs                         Run 列表（GET /api/runs，支持 status/agent_code/thread_id 过滤）
/runs/:runId                  Run 详情（步骤时间线 + Trace + Loop + Replay）
/mcp                          MCP Server 列表
/mcp/:serverCode              MCP Server 详情（健康检查 + Tool 快照）
/users                        用户管理（admin 专属）
/settings                     个人资料（改邮箱/密码）
```

优先级：P0 = `/login`、`/chat`、`/runs/:runId`；P1 = Agent/Workflow 管理全套；P2 = MCP、用户管理、设置。

---

## 4. 各页面 API 与逻辑

### 4.1 登录/注册 `/login`

- `POST /auth/register`：`{username(3-64), email, password(>=8)}`，成功 201。
- `POST /auth/login`：`{username, password}` → `data.token`。
- 登录后：解码 JWT 取 `user_id` → `GET /api/users/:id` 拿资料存入全局状态。token 存 localStorage；任何请求收到 401（`AUTH_MISSING_TOKEN`/`AUTH_INVALID_TOKEN`）时清空 token 跳登录页。

### 4.2 聊天工作台 `/chat`（核心页面）

左侧：服务端持久化的会话列表（`GET /api/threads`，返回标题、归属 Agent、最近 Run 状态；历史消息用 `GET /api/threads/:thread_id/messages` 恢复）。顶部：Agent 选择器（`GET /api/agents/published`，跨 owner 的已发布 Agent 公开目录）。主区：消息流。

**发起对话**：

```text
POST /api/agents/:agent_code/agui        （JWT，Content-Type: application/json，响应 text/event-stream）
```

请求体（首次对话 threadId/runId 传空字符串，后端会生成并在事件中回传）：

```json
{
  "threadId": "",
  "runId": "",
  "state": {},
  "messages": [{ "id": "<uuid>", "role": "user", "content": "用户输入" }],
  "tools": [],
  "context": []
}
```

约束：V1 只支持 `user/assistant/system/developer` 纯文本消息；`state/tools/context/forwardedProps` 必须为空对象/空数组，否则进流前返回 JSON envelope 错误（此时响应是 `application/json` 而非 SSE，**前端必须先看响应 Content-Type 再决定按 SSE 还是按 envelope 解析**）。续聊时带上已有 `threadId`，`messages` 里带全量历史 + 新消息（后端会去重）。

**SSE 事件状态机**（每个 frame 是 `data: {json}`，按 `type` 字段分发）：

| 事件 | 前端动作 |
| --- | --- |
| `RUN_STARTED` | 记录 `threadId`/`runId`（首次对话在此拿到真实 ID），消息区显示"运行中" |
| `STEP_STARTED` / `STEP_FINISHED` | 渲染步骤进度条/时间线（`stepName`） |
| `TEXT_MESSAGE_START` | 创建一条 assistant 消息占位（`messageId`） |
| `TEXT_MESSAGE_CONTENT` | 按 `messageId` 追加 `delta` 增量文本（打字机效果） |
| `TEXT_MESSAGE_END` | 该消息定稿 |
| `RUN_FINISHED` | 结束。**检查 `outcome`**：若 `outcome.type == "interrupt"` 进入人工介入流程（见下） |
| `RUN_ERROR` | 展示错误消息（流开始后的失败走这里，不走 envelope） |

**Interrupt / Resume（人工介入）**：`RUN_FINISHED` 带 `outcome.interrupts[]`（每项含 `id`、`reason`、`message`）时，渲染一张审批卡片（展示 message，提供"批准/拒绝/取消"和可选 JSON payload 输入）。用户处理后用**同一个 runId** 再次调用同一入口：

```json
{
  "runId": "<原runId>",
  "resume": [{ "interruptId": "approval", "status": "resolved", "payload": { "approved": true } }]
}
```

`status` 取 `resolved`（携带结果继续）或 `cancelled`(跳过该节点)。响应仍是 SSE 流，按同一状态机处理。重复/并发 resume 后端幂等，前端无需特殊防护，但应禁用已提交的卡片。

**分支对话（可选 P2）**：请求带 `parentRunId` 可从某次历史 Run 分支（同 Thread、父 Run 须属于当前用户）。

每条 assistant 消息旁提供"查看 Run 详情"链接跳 `/runs/:runId`。

### 4.3 Agent 列表 `/agents`

- `GET /api/agents`：表格列 = agent_code、name、status（active/inactive 徽标）、描述、操作。
- 新建：`POST /api/agents` `{agent_code, name, description}`（创建后默认 inactive）。
- 行操作：激活 `POST /api/agents/:code/activate`（失败时后端 envelope 会说明未满足的发布门禁，直接展示 message）、停用 `.../deactivate`、编辑 `PUT /api/agents/:code`。

### 4.4 Agent 详情 `/agents/:agentCode`（Tab 页）

**概览 Tab**：`GET /api/agents/:code` + 公开 Agent Card 预览（`GET /a2a/agents/:code/.well-known/agent-card.json`，404 表示未发布，如实展示"未发布"）。

**Capability Tab**：
- `GET /api/agents/:code/capabilities` 列表。
- 新建/编辑表单字段：`capability_code`、`name`、`description`、`capability_type`（枚举 `workflow/remote/tool/custom`，V1 只有 `workflow` 可支撑发布，UI 需注明）、`workflow_id`（type=workflow 时必填，从 Workflow Tab 的列表选择）、`version`（字符串，须与所选 Workflow 版本一致）、`input_schema_json`/`output_schema_json`（JSON 编辑器）、`status`。
- `POST /api/agents/:code/capabilities`、`PUT .../capabilities/:capability_code`、`POST .../capabilities/:capability_code/deactivate`。

**Endpoint Tab**：
- `GET /api/agents/:code/endpoints` 列表，展示健康状态。
- 表单：`endpoint_code`、`protocol`（固定 `a2a`）、`transport`（`http` 仅限 loopback 地址 / `https` 远程）、`address`、`auth_type`（默认 `goai_hmac_sha256`）、`credential_ref`（只是引用名，UI 注明"真实密钥由服务端配置解析，此处不填密钥"）、`config_json`（注明禁止放 secret/token/password 字段，后端会拒绝）。
- 关键操作：**健康检查** `POST .../endpoints/:endpoint_code/health-check`（Endpoint 新建/更新后为 inactive，必须健康检查通过才 active；UI 上健康检查按钮要醒目，结果直接展示）。

**Workflow Tab**：
- `GET /api/agents/:code/workflows`：版本列表（版本号、active 状态、checksum、被哪些 Capability 引用）。
- 新建版本：`POST /api/agents/:code/workflows` `{version: <int>, definition: <object>}`，definition 用 JSON 编辑器 + 客户端预校验（结构见第 6 节）。
- 版本详情页 `/agents/:agentCode/workflows/:version`：`GET .../workflows/:version`，用 React Flow 渲染 nodes/edges 只读 DAG + JSON 视图。
- inactive 版本可编辑：`PUT .../workflows/:version`；激活/停用：`POST .../workflows/:version/activate|deactivate`。**active 版本禁止编辑**（编辑按钮置灰），停用被 active Capability 引用的版本会被后端拒绝，如实展示错误。

**发布 Tab（发布向导）**：把发布门禁做成 checklist UI，逐项显示是否满足：
1. 至少一个 active Workflow 版本
2. 至少一个 `capability_type=workflow` 且引用该 Workflow、版本一致的 active Capability
3. 至少一个健康检查通过的 active A2A Endpoint
4. 全部满足后"发布"按钮可用 → `POST /api/agents/:code/activate`

### 4.5 Run 详情 `/runs/:runId`

- `GET /api/runs/:run_id`：状态徽标（`queued/running/waiting_external/waiting_input/success/failed/cancelled`）、CurrentStep、可选 `resume` 诊断对象（租约信息，折叠展示）。
- `GET /api/runs/:run_id/steps`：步骤时间线（节点 key、状态、起止时间、输入输出）。
- `GET /api/runs/:run_id/trace`：完整快照，含 `root_run/runs/steps/loops/delegations/delegation_groups/messages/evaluations`。用它渲染**父子 Run 委派树**（Delegation 连接 parent/child）+ 消息列表；原始 JSON 折叠可查。
- `GET /api/runs/:run_id/loops` → 每个 Loop 可点开 `GET /api/loops/:loop_id`、`GET /api/loops/:loop_id/evaluations`。
- 操作：**Replay** `POST /api/runs/:run_id/replay`（带 Idempotency-Key，202 返回新 run_id 后跳转新详情页）；**Thread Replay** `POST /api/threads/:thread_id/replay`（body 可选 `{source_run_id}`）。
- Run 处于非终态时前端轮询（3~5s）刷新详情与步骤。

### 4.6 Run 入口说明

Run 列表使用 `GET /api/runs`（owner 隔离，admin 跨 owner；支持 `thread_id`/`agent_code`/`status`/`limit` 过滤，默认 50 条、最多 200 条，按创建时间倒序）。`/runs` 页直接渲染服务端数据，配合手动输入 runId 跳转的搜索框。

（可选）技术调试用创建入口：`POST /api/runs` `{agent_code, workflow_version?, thread_id?, input?, provider?, model?}` → 202 `data.run_id`。可做成"手动触发 Run"的调试表单，非主流程。

### 4.7 MCP 管理 `/mcp`

- `GET /api/mcp/servers` 列表；`POST /api/mcp/servers` 新建：`{server_code, name, description, transport: "streamable_http", endpoint, auth_type, credential_ref, config_json}`。
- 详情页：`GET /api/mcp/servers/:server_code`；`PUT` 更新；`POST .../deactivate` 停用。
- `POST /api/mcp/servers/:server_code/health-check`：健康检查并刷新 Tool 快照。
- `GET /api/mcp/servers/:server_code/tools`：Tool discovery 快照表格（名称、描述、input schema 折叠展示）。

### 4.8 用户管理 `/users`（admin）与设置 `/settings`

- `GET /api/users`、`POST /api/users`（同注册字段）、`GET/PUT/DELETE /api/users/:id`。member 调 `GET /api/users` 会得到 403，页面如实提示无权限。
- `/settings`：对本人 `GET /api/users/:id` + `PUT /api/users/:id` `{email?, password?}`。

### 4.9 （可选）Provider 调试聊天

`POST /api/chat` `{provider?, model?, messages:[{role, content}]}`，SSE 格式与 AG-UI 不同：`event: chunk`（`data.data.content` 增量）、`event: done`、`event: error`（envelope）。可做成独立的"模型直连调试"小页面，不与 `/chat` 混用。

---

## 5. 错误处理规范

统一 axios/fetch 拦截器：

| 情形 | UI 行为 |
| --- | --- |
| HTTP 401 | 清 token 跳登录 |
| `AUTH_FORBIDDEN` (403) | toast "无权限执行此操作"，不跳转 |
| `VALIDATION_FAILED` / `INVALID_ID` | 表单内联展示 message |
| `*_NOT_FOUND` | 页面级空状态 |
| `IDEMPOTENCY_KEY_REUSED` (409) | 提示后换新幂等键重试 |
| `WORKFLOW_INVALID_STATE` / `MCP_SERVER_INVALID_STATE` 等 409 | 直接展示后端 message（后端消息已可读） |
| `INTERNAL_ERROR` 等 5xx | toast "服务异常" + 展示 `trace_id` 并提供复制按钮 |

所有错误提示都附带 `trace_id`。

---

## 6. Workflow DSL 参考（编辑器/渲染器需要）

Definition 结构：

```json
{
  "entry_node": "prepare",
  "nodes": [{ "key": "prepare", "type": "noop", "config": {} }],
  "edges": [{ "from": "prepare", "to": "next" }]
}
```

约束：node key 唯一非空，且不得使用编排引擎保留字 `start` / `end`；`entry_node` 必须存在于 nodes；执行是**串行/可达顺序**（只有 `agent_group` 内部并行）。节点类型与 config：

| type | config 字段 | 说明 |
| --- | --- | --- |
| `noop` | 无 | 空节点 |
| `llm` 等其他 | 自由 | 校验器不强制 config |
| `agent` | `target_agent`、`capability`(必填)、`routing_policy`("registry" 时可省 target_agent 由注册中心选路)、`input_from[]`、`timeout_ms` | A2A 委派另一个 Agent |
| `agent_tool` | `target_agent`、`capability`(必填)、`tool_name`、`routing_policy`、`input_from[]`、`timeout_ms` | 把 Agent 能力包装为工具调用 |
| `agent_group` | `members[]`(每项 `key/target_agent/capability/timeout_ms`)、`strategy`("all"/"any"/"quorum")、`required_successes`(quorum 时)、`input_from[]` | 多 Agent 并行 fan-out/fan-in |
| `tool` | `server_code`(必填)、`tool_name`(必填)、`input` 或 `input_from[]`(二选一必填)、`timeout_ms`(0~300000) | MCP 工具调用 |
| `interrupt` | `interrupt_id`(必填,全图唯一,<=128)、`reason`(必填,<=128)、`message`、`response_schema`、`metadata` | 暂停等待用户输入 |

`input_from` 引用的节点 key 必须存在且不能引用自己。前端 JSON 编辑器应做这些预校验，最终以后端校验错误为准（创建/更新接口会返回具体校验失败原因）。

---

## 7. 交付要求

1. 全部 API 调用以 `docs/openapi.yaml` 为准；envelope 的 `data` 字段结构以实际响应为准（列表返回数组，详情返回对象），前端类型定义宽松处理未知字段。
2. 中文 UI，暗色/亮色主题不强制。整体是开发者工具审美：等宽字体展示 code/id/JSON，状态徽标色彩语义统一（active/success=绿，inactive/queued=灰，running/waiting=蓝，failed=红，cancelled=橙）。ai味不要那么重，减少圆框或者颜色对比。
3. SSE 处理必须支持用户中断（AbortController）：用户离开聊天页或点"停止"时中止 fetch；注意中止只是断开观察流，后端 Run 不会因此取消，UI 文案要如实（"已停止接收，任务仍在后台运行"）。
4. 提供 `.env` 配置后端地址；Vite dev proxy 配置 `/api`、`/auth`、`/a2a`、`/ping`。
5. 附 README：安装、启动、代理说明、与后端联调步骤（先 `docker-compose up -d` + `go run .` 起后端）。
