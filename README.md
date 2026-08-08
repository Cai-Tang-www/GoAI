# GoAI

GoAI 是一个 **基于 Go 的多 Agent 协议运行时平台 / AI 中台开发框架**，不是单 Agent Chat 后端，也不是以接入模型厂商数量为目标的 Provider 平台。

平台主线是：

- 使用 **AG-UI** 承载 `前端 / 用户 <-> Agent Runtime` 的交互协议
- 使用 **A2A** 承载 `Agent <-> Agent` 的任务委派、状态更新和结果回流
- 使用 **Eino Graph / Workflow** 表达单个 Agent 的执行能力
- 使用 **MCP** 接入工具与外部资源
- 使用 **JWT / RBAC / MySQL / Redis / Kafka / Trace / Replay / Eval** 提供可复用的运行时底座

> GoAI / FORGE 的目标是让开发者聚焦 Agent 能力、Graph 编排、Tool 接入和协作策略，而不是重复搭建通信、权限、异步执行、回放与观测基础设施。

## 产品边界

### V1 目标

V1 聚焦多 Agent 协议运行时的最小闭环：

- AG-UI Gateway
- A2A Gateway
- 统一内部领域模型：`Thread / Message / Run / Delegation`
- Agent Registry 管理面：Agent、Capability、Endpoint 的注册、健康检查、发布与发现
- Eino Graph 作为 Agent 的执行能力接入
- Run / RunStep / Message / Delegation 持久化
- Replay / Trace / Loop 基础能力
- JWT / RBAC / Kafka / Redis / MySQL 等工程底座

### V1 非目标

当前版本不优先建设：

- 大而全的多模态平台
- 每家模型的深度定制 SDK
- 完整前端管理控制台
- 企业级租户、组织与计费体系
- 高级自治规划和复杂调度算法

Provider、Workflow 和 `/api/chat` 都是平台能力或调试入口，不是产品主线。

## 核心领域概念

- `Thread`：一次用户会话或多 Agent 协作上下文
- `Message`：Thread 内的用户、Agent、Tool 或系统通信单元
- `Run`：一次完整业务回合，可由 AG-UI、A2A 或系统调度触发
- `RunStep`：Run 内部一个可观测执行步骤
- `Delegation`：源 Agent 将子任务委派给目标 Agent 的协作记录，关联 Parent Run 与唯一 Child Run
- `AgentEndpoint`：Agent 的 A2A 协议入口；远程使用 HTTPS，本地开发使用同一 Gateway 的 loopback HTTP，均执行相同的 A2A 契约；业务请求使用 HMAC-SHA256 机器身份认证、时间窗与 nonce 防重放，Agent Card discovery 保持公开
- `AgentCapability`：Agent 对 Runtime 和其他 Agent 暴露的可发现业务能力
- `Agent Registry`：管理 Agent、Capability、Endpoint、健康状态与发布状态；所有可委派 Agent 必须先进入 Registry
- `Workflow / Graph`：某个 Agent 的执行模板或能力，不代表整个平台
- `Loop`：用于 Trace、Replay、Eval 和成本分析的执行片段

## 目标主链路
```text
AG-UI Client
    -> AG-UI Gateway
    -> Thread / Message / Parent Run
    -> Coordinator Agent Eino Graph
    -> Workflow agent node
    -> AgentAsTool / A2A Client
    -> loopback HTTP or remote HTTPS
    -> Target Agent A2A Gateway
    -> Delegation / Child Run
    -> Target Eino Graph / Tool / LLM
    -> authenticated A2A callback
    -> Parent Run resume from persisted cursor
    -> RunStep / Message persistence
    -> AG-UI SSE
    -> Trace / Replay / Eval
```

### A2A 通信约束

A2A 是 Agent 协作的协议语义，HTTPS 只是远程传输方式，Kafka 只是异步基础设施。

- 远程 Agent 通过 HTTPS A2A transport 通信
- 本地 Agent 通过 loopback HTTP 访问同一 A2A Gateway，不提供进程内 transport adapter 或 Service 直调旁路
- 本地与远程 transport 必须使用相同的 A2A 请求、状态和结果契约
- 每次跨 Agent 调用都必须形成 `Message + Delegation + Child Run + Trace`
- Workflow 的 Agent 节点必须交给 Runtime 发起 Delegation，不能直接调用另一个 Agent 的 service
- Kafka 可以承载投递、重试和回调事件，但不能代替 A2A 协议或 Runtime 业务决策
- Workflow 不接受任意未注册 URL；Runtime 只能发现并委派已发布、具备 active Capability 和健康 A2A Endpoint 的 Agent

### Agent Registry 管理面

Agent Registry 管理“谁可以被调用”，A2A Runtime 管理“这次如何调用和如何推进”。管理 API 不执行跨 Agent 业务调用，也不会绕过 A2A Gateway。

- Agent 创建后默认 `inactive`；V1 发布前必须至少有一个由当前 active Workflow 支撑且版本一致的 active Capability，以及一个通过 Agent Card 健康检查的 active A2A Endpoint
- 本地 Endpoint 只允许 loopback HTTP，远程 Endpoint 必须使用 HTTPS
- Endpoint 更新后会重置为 `inactive`，需要重新健康检查
- member 只能管理自己的 Agent；拥有 `agent:manage` 的管理员可以跨 owner 管理
- API 只返回 `credential_ref`，真实 HMAC secret 只由配置驱动的 CredentialResolver 解析；Endpoint `config_json` 仅允许非敏感传输元数据，密钥、Token、密码等字段会被拒绝

## 分层职责

- `main.go`：配置加载、依赖装配和进程生命周期
- `config`：环境变量、默认值与启动校验
- `db`：数据库连接、迁移和基础初始化
- `redis`：Redis 客户端与缓存基础能力
- `kafka`：消息生产、消费与消息结构
- `worker`：异步入口，将消息分发给运行时或应用服务
- `routers`：路由和中间件装配
- `handlers`：HTTP 请求解析、鉴权上下文读取和响应映射
- `services`：应用服务与 Runtime 协调逻辑
- `domain/workflow`：Workflow DSL、图校验和执行顺序
- `domain/runstate`：Run 与 RunStep 状态机
- `models`：持久化模型和状态常量
- `ai`：Provider、模型请求与流式输出收敛

## 当前实现状态

### 已实现

- JWT 登录与 DB 实时 RBAC
- 统一响应 envelope、稳定错误码和 `trace_id`
- Run / RunStep 状态机与持久化
- `POST /api/runs`、Run 查询、步骤查询和 Replay
- Run 创建幂等与 Kafka 消费原子 claim
- Kafka 异步 Run 执行链路
- Workflow DSL 基础校验与拓扑排序
- Provider Registry 与 OpenAI-compatible 调试通道
- Agent Registry 管理 API：Agent / Capability / Endpoint 注册、ownership、健康检查、发布校验与发现
- MCP Tool Runtime：MCP Server owner-scoped Registry、官方 SDK 健康检查、Tool discovery snapshot 与 Eino `tool` 节点调用
- HTTP / SSE / Kafka / Worker 优雅关闭
- 服务治理：进程内限流、下游超时、按 target 熔断、快速失败和恢复观测（见 `docs/SERVICE_GOVERNANCE.md`）
- 旧 Task 模型、空包和误导性文件名清理
- db / redis / kafka 显式依赖装配
- `Thread / Message / Delegation / AgentEndpoint / AgentCapability` 统一领域模型与迁移
- 官方 Go SDK 驱动的 AG-UI Gateway：请求映射、Thread 创建/复用、Run 触发、Step/Message SSE 回传
- 官方 A2A Go SDK 驱动的 A2A Gateway：Agent Card、入站委派、Task 查询/取消、Push Notification callback、Child Run 与结果 Artifact 回流
- Workflow `agent` 节点的出站 A2A Client：Agent Card discovery、能力校验、带 PushConfig 的 `message:send`、Task cancel 和 accepted 结果收敛
- Eino Graph 执行器：在单个 Agent 内执行串行/可达 Workflow 节点，并将 `agent` 节点统一交给 A2A Client 委派
- `agent_group` fan-out/fan-in：显式把一个 Workflow 节点分派给多个 Agent，每个成员都创建独立 Delegation、Child Run、A2A Task 和 Message
- 本地 Agent 使用 loopback HTTP，远程 Agent 强制 HTTPS；跨 Agent 不提供进程内 Service 直调旁路
- A2A 调用使用稳定的 TaskID/MessageID/DelegationID，节点重试不会重复创建协议任务，并透传 `traceId` 关联父子 Run
- Target 返回 accepted 后，Parent Run 与当前 RunStep 持久化为 `waiting_external`，释放执行 Worker，不进行 Task polling
- Target 终态通过认证 A2A callback 回流；Runtime 幂等落库后发布 Kafka `run_resume`，Resume Worker 原子 claim 并从持久化游标继续 Eino Graph
- callback 与 resume 发布失败由 RecoveryWorker 扫描恢复，重复 callback、重复 Kafka 消息和进程重启不会重复执行后继节点
- Parent Run resume 使用带 owner、heartbeat、expires_at 与 fencing attempt 的执行租约；过期租约可由新 Worker 原子接管，并根据成功 RunStep checkpoint 跳过已完成节点
- `agent_group` 支持 `all`、`any`、`quorum` 三种 fan-in 策略；成员通过 A2A HTTP(S) 并行委派，结果回流后由 group coordinator 统一恢复 Parent Run
- Supervisor 的 `agent` 节点支持 `routing_policy=registry`，Router 只从 active Agent、版本一致的 Workflow Capability 和健康 A2A Endpoint 中按稳定顺序选择 Worker；选路结果写入 RunStep，实际委派仍通过 A2A HTTP(S)
- 来源 Agent 可通过 A2A `CancelTask` 取消 Child Run；目标 Runtime 原子收敛 Child Run/RunStep、停止本地执行上下文并通过认证 callback 回送取消终态，重复取消幂等

### 在建

- 任意并行 DAG、条件分支、流式节点和节点级恢复
- 更复杂的 Eino Graph 能力扩展：并行 DAG、条件分支、流式节点和节点级恢复
- Loop / Trace / Replay / Eval / Cost 的完整观测与评估能力

文档中的“在建”能力不能视为当前已经可用。


## AG-UI Gateway

当前协议入口：

```text
POST /api/agents/:agent_code/agui
```

该入口需要 JWT 和 `run:create` 权限，接收官方 `RunAgentInput` JSON。V1 只接受 `user / assistant / system / developer` 普通文本消息；多模态内容、`tool / activity / reasoning` 消息、ToolCall/加密/命名消息字段，以及非空 `state / tools / context / forwardedProps`、`parentRunId` 与 `resume` 都会在进入流式阶段前返回参数错误。SDK 默认产生的空对象或空数组允许传入，但不会渗透到内部 Run 输入。AG-UI `parentRunId` 表示同一 Thread 的分支 lineage，A2A Delegation 的 Parent/Child Run 表示 Agent 委派关系，二者不共用语义；V1 暂不开放分支与恢复能力，也不会静默降级。

最小请求示例：

```json
{
  "threadId": "thread-demo",
  "runId": "run-demo",
  "state": {},
  "messages": [
    {
      "id": "message-demo",
      "role": "user",
      "content": "请执行当前 Agent 的工作流"
    }
  ],
  "tools": [],
  "context": []
}
```

`threadId` 为空时由 Runtime 创建新 Thread；传入本人 active Thread 时复用。`runId` 为空时由 Runtime 生成；相同 `runId` 与相同请求可安全复用，不同请求会返回冲突。

成功进入执行阶段后响应为 `text/event-stream`。官方 SDK 当前通过每个 SSE frame 的 `data:` JSON 中的 `type` 区分事件：

```text
data: {"type":"RUN_STARTED","threadId":"thread-demo","runId":"run-demo"}

data: {"type":"STEP_STARTED","stepName":"prepare"}

data: {"type":"TEXT_MESSAGE_CONTENT","messageId":"message-result","delta":"执行结果"}

data: {"type":"RUN_FINISHED","threadId":"thread-demo","runId":"run-demo"}
```

当前可回传事件包括：

- `RUN_STARTED`
- `STEP_STARTED` / `STEP_FINISHED`
- `TEXT_MESSAGE_START` / `TEXT_MESSAGE_CONTENT` / `TEXT_MESSAGE_END`
- `RUN_FINISHED` / `RUN_ERROR`

协议 Gateway 只负责协议解析、内部命令映射和事件编码；Thread、Message、Run 的一致性由 Runtime 保证，Workflow 执行由 Run Worker 负责。

## A2A Gateway

当前实现提供官方 A2A HTTP+JSON 入站协议入口：

```text
GET  /a2a/agents/:agent_code/.well-known/agent-card.json
POST /a2a/agents/:agent_code/message:send
GET  /a2a/agents/:agent_code/tasks/:task_id
POST /a2a/agents/:agent_code/tasks/:task_id:cancel
POST /a2a/agents/:source_agent_code/callbacks/tasks/:task_id
```

Agent Card 从目标 Agent 当前可执行的 active Workflow Capability 与健康 A2A Endpoint 构造；`tool/custom` 在 V1 仅作为管理资产，不会被广告为 Runtime 尚不能执行的 Skill。Endpoint 必须满足以下传输边界：

- 本地开发 Endpoint 只能使用 loopback 主机的 HTTP 地址
- 远程 Endpoint 必须使用 HTTPS 地址
- 本地与远程 Agent 都执行同一套 A2A 请求、Task 状态和结果契约

GoAI 使用下面的 A2A Message metadata 扩展表达委派语义：

```json
{
  "https://goai.dev/extensions/delegation/v1": {
    "sourceAgentCode": "planner",
    "capabilityCode": "write",
    "parentRunId": "run-parent",
    "traceId": "trace-parent",
    "delegationId": "dlg-parent",
    "delegationGroupId": "group-parent",
    "groupMemberKey": "security"
  }
}
```

`message:send` 会把协议请求映射为内部 `Thread + request Message + Delegation + Child Run`，并在事务提交后投递 Child Run。重复发送相同 A2A 请求会返回同一个 Child Run；复用相同标识但改变请求内容会返回冲突。Child Run 完成后，Runtime 将结果写为目标 Agent 返回给源 Agent 的 Result Message；A2A Task 查询会把结果映射为 Artifact，失败响应不会暴露 Provider 或数据库原始错误。

来源 Agent 可以通过 A2A `CancelTask` 取消自己发起的 Child Run。目标 Runtime 会校验来源、目标和 Task 归属，在事务内将 Child Run 推进为 `cancelled`、关闭当前活动 RunStep，并通过已有认证 callback 回送取消终态；重复取消是幂等操作。取消不通过进程内 Service 直调，也不使用 Kafka 冒充 A2A 控制消息。

当前实现已经覆盖 callback 驱动的 A2A 异步闭环：Workflow `agent` 节点先由 Registry Router 选出满足能力、Workflow 版本和健康 Endpoint 门禁的 Worker，再通过 Agent Card discovery 和官方 A2A HTTP+JSON Client 携带 PushConfig 发起 `message:send`。目标返回 accepted 后，Parent Run 与当前 RunStep 进入 `waiting_external`，Worker 立即释放；目标 Child Run 独立执行并在终态发送带机器身份签名和 notification token 的 callback。源 Runtime 幂等收敛 Result Message，发布 Kafka `run_resume`，Resume Worker 从 Delegation 保存的后继节点游标继续执行 Graph。`agent_group` 是显式的多 Agent 并行边界：每个成员都通过 A2A HTTP(S) 创建独立 Delegation、Child Run、A2A Task 和 Message，支持 `all`、`any`、`quorum` 聚合及失败收敛；group coordinator 负责一次性恢复 Parent Run。Kafka 只承担内部 `run_execute/run_resume` 调度与恢复，不承载 Agent 委派语义；本地和远程调用都不能绕过 A2A HTTP(S)。Parent Workflow 仍保持串行，只有 `agent_group` 节点内部允许 fan-out/fan-in。

> 安全边界：A2A 业务路由默认要求 `goai_hmac_sha256` 机器身份签名。数据库仅保存 `credential_ref`，真实 secret 由 `A2A_AUTH_CREDENTIALS_JSON` 在部署侧解析；Endpoint `config_json` 只能保存非敏感元数据；远程 Endpoint 仍必须使用 HTTPS。

## 当前 HTTP API

- `POST /auth/register`
- `POST /auth/login`
- `POST /api/chat`：单 Agent / Provider 流式调试入口
- `POST /api/agents/:agent_code/agui`：AG-UI 标准协议入口
- `/a2a/agents/:agent_code/*`：A2A Agent Card、委派、Task 查询与终态 callback 入口
- `POST /api/runs`
- `GET /api/runs/:run_id`
- `GET /api/runs/:run_id/steps`
- `POST /api/runs/:run_id/replay`
- `POST/GET/PUT`、停用、健康检查和 Tool discovery：`/api/mcp/servers/*`
- `/api/users/*`：用户管理接口

### 统一响应

普通 JSON API 使用：

```json
{
  "code": "OK",
  "message": "success",
  "data": {},
  "trace_id": "trace_xxx"
}
```

常用错误码包括：

- 认证：`AUTH_MISSING_TOKEN` `AUTH_INVALID_TOKEN` `AUTH_INVALID_CREDENTIALS`
- 授权：`AUTH_FORBIDDEN`
- 参数：`VALIDATION_FAILED` `INVALID_ID`
- 资源：`USER_NOT_FOUND` `USER_ALREADY_EXISTS` `RUN_NOT_FOUND`
- MCP：`MCP_SERVER_NOT_FOUND` `MCP_SERVER_ALREADY_EXISTS` `MCP_SERVER_INVALID_STATE` `MCP_SERVER_UNHEALTHY` `MCP_TOOL_NOT_FOUND` `MCP_TOOL_INVOCATION_FAILED` `MCP_INVALID_CONFIG` `MCP_CREDENTIAL_NOT_FOUND` `MCP_TRANSPORT_FAILED` `MCP_PROTOCOL_FAILED` `MCP_TOOL_REPORTED_ERROR`
- 幂等：`IDEMPOTENCY_KEY_REUSED`
- Provider：`PROVIDER_NOT_FOUND` `PROVIDER_DRIVER_NOT_FOUND` `PROVIDER_INVALID_CONFIG` `MODEL_NOT_CONFIGURED` `STREAM_INTERRUPTED`
- 系统：`INTERNAL_ERROR` `RBAC_PERMISSION_LOAD_FAILED` `KAFKA_PUBLISH_FAILED`

### SSE 调试协议

`POST /api/chat` 保持 `Content-Type: text/event-stream`：

- `event: chunk`：`data.content` 为增量文本
- `event: done`：`data.done=true`
- `event: error`：返回统一 envelope
- 客户端断开或服务关闭时，请求 context 会取消 Provider 上游流

### RBAC

预置角色：

- `admin`：拥有全部预置权限
- `member`：默认角色，不具备 `user:manage`

预置权限：

- `run:create` `run:read` `run:replay`
- `user:read_self` `user:update_self` `user:manage`
- `chat:use`
- `agent:create` `agent:read` `agent:update` `agent:activate` `agent:manage`
- `mcp:create` `mcp:read` `mcp:update` `mcp:invoke` `mcp:manage`

请求通过 JWT 认证但权限不足时，返回 `403` 和 `AUTH_FORBIDDEN`。

启用 RBAC 时，公开注册和管理员创建用户都会在同一数据库事务中自动绑定 `member` 角色；角色绑定失败会回滚用户创建。关闭 `RBAC_ENABLE` 时保持紧急旁路语义，不执行角色绑定。

## 配置

基础运行时配置：

- `MYSQL_HOST` `MYSQL_PORT` `MYSQL_USER` `MYSQL_ROOT_PASSWORD` `MYSQL_DATABASE`
- `REDIS_HOST` `REDIS_PORT` `REDIS_PASSWORD`
- `KAFKA_BOOTSTRAP_SERVERS` `KAFKA_RUN_TOPIC` `KAFKA_RUN_GROUP_ID`
- `KAFKA_RUN_RESUME_TOPIC` `KAFKA_RUN_RESUME_GROUP_ID`
- `A2A_CALLBACK_BASE_URL`：源 Runtime 暴露给目标 Agent 的 callback 基地址；本地默认 loopback HTTP，远程部署必须配置 HTTPS
- `JWT_SECRET`
- `SERVER_PORT`
- `SERVER_SHUTDOWN_TIMEOUT_SECONDS`，默认 15 秒且必须大于 0

RBAC 配置：

- `RBAC_ENABLE`
- `RBAC_BOOTSTRAP_ADMIN_USERNAME`

Provider 调试配置：

- `MODEL_PROVIDER_DEFAULT`
- `MODEL_DRIVER_<PROVIDER>`
- `MODEL_BASE_URL_<PROVIDER>`
- `MODEL_API_KEY_<PROVIDER>`
- `MODEL_NAME_DEFAULT_<PROVIDER>`
- `MODEL_ENDPOINT_PATH_<PROVIDER>`

完整示例见 [`.env.example`](.env.example)。示例文件不得填写真实密钥。

## 服务生命周期

进程使用标准 `http.Server` 并监听 `SIGINT` / `SIGTERM`。`SERVER_SHUTDOWN_TIMEOUT_SECONDS` 是整个关闭流程共享的总预算，不会为每个阶段重复计算：

1. 取消 AG-UI/Chat SSE 请求，并调用 HTTP `Shutdown` 停止接收新连接
2. 关闭 Run execute / resume Kafka consumer 与 RecoveryWorker，停止拉取新消息和恢复扫描；已经进入 handler 的消息保留 worker context 继续处理
3. 在共享 drain 窗口内并行等待普通 HTTP 请求、当前 worker、consumer close 和恢复循环完成
4. drain 超时后取消 worker context，并强制关闭仍存活的 HTTP 连接
5. 只有 HTTP、consumer 和 worker 都已退出，才在剩余预算内依次关闭 Kafka producer、Redis、数据库连接池和 observability exporter
6. 聚合并记录所有关闭错误；如果工作仍未退出，则跳过仍可能被使用的共享依赖，避免 use-after-close

第一次信号进入上述优雅关闭流程；信号处理随后恢复系统默认行为，第二次 `SIGINT` / `SIGTERM` 可直接强制退出。

Kafka producer、Redis 和 MySQL 的第三方 `Close()` API 不接收 context，运行时通过受总预算约束的适配器调用它们。预算耗尽后进程停止等待并跳过后续依赖关闭；Go 无法强制中断已经进入的第三方 `Close()`，最终由进程退出回收剩余描述符。

## 文档

- [产品设计提案](docs/PRODUCT_PROPOSAL.md)
- [实施路线图](docs/IMPLEMENTATION_ROADMAP.md)
- [统一响应契约](docs/RESPONSE_CONTRACT.md)
- [MCP Tool Runtime](docs/MCP_RUNTIME.md)
- [HTTP / AG-UI / A2A 协议契约](docs/API_PROTOCOL_CONTRACT.md)
- [HTTP 压测基线](bench/README.md)

## 启动与验证

```bash
go run .
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
go build ./...
docker build -t goai:local .
```

GitHub Actions 会执行全量 Go 质量检查和 Docker 构建。CI 未通过的 PR 不应合并。
