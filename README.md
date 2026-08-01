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
- Agent Endpoint 与 Capability 管理
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
- `AgentEndpoint`：Agent 的 A2A 协议入口；远程使用 HTTPS，本地开发使用同一 Gateway 的 loopback HTTP，均执行相同的 A2A 契约；Agent 身份认证与授权仍在后续版本建设
- `AgentCapability`：Agent 对 Runtime 和其他 Agent 暴露的可发现业务能力
- `Workflow / Graph`：某个 Agent 的执行模板或能力，不代表整个平台
- `Loop`：用于 Trace、Replay、Eval 和成本分析的执行片段

## 目标主链路
```text
AG-UI / A2A Request
        -> Protocol Gateway
        -> Thread / Message / Run
        -> Runtime Coordination
        -> Delegation / Child Run
        -> Eino Graph / MCP Tool / LLM
        -> RunStep / Message persistence
        -> Protocol event / callback
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
- HTTP / SSE / Kafka / Worker 优雅关闭
- 服务治理：进程内限流、下游超时、按 target 熔断、快速失败和恢复观测（见 `docs/SERVICE_GOVERNANCE.md`）
- 旧 Task 模型、空包和误导性文件名清理
- db / redis / kafka 显式依赖装配
- `Thread / Message / Delegation / AgentEndpoint / AgentCapability` 统一领域模型与迁移
- 官方 Go SDK 驱动的 AG-UI Gateway：请求映射、Thread 创建/复用、Run 触发、Step/Message SSE 回传
- 官方 A2A Go SDK 驱动的 A2A Gateway：Agent Card、入站委派、Child Run、Task 状态查询与结果 Artifact 回流
- Workflow `agent` 节点的出站 A2A Client：Agent Card discovery、能力校验、`message:send`、Task polling 和结果收敛
- 本地 Agent 使用 loopback HTTP，远程 Agent 强制 HTTPS；跨 Agent 不提供进程内 Service 直调旁路
- A2A 调用使用稳定的 TaskID/MessageID，节点重试不会重复创建协议任务

### 在建

- A2A Agent 身份认证、授权与凭据管理
- callback 驱动的 Parent Run suspend/resume，当前 V1 使用 Worker 内阻塞轮询
- 多 Child Run 并行执行、结果聚合与部分失败策略
- 多 Agent Runtime 的 Supervisor / Router / Worker 协作策略
- Eino Graph 能力化接入
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
```

Agent Card 从目标 Agent 的活跃 `AgentCapability` 与 A2A `AgentEndpoint` 构造。Endpoint 必须满足以下传输边界：

- 本地开发 Endpoint 只能使用 loopback 主机的 HTTP 地址
- 远程 Endpoint 必须使用 HTTPS 地址
- 本地与远程 Agent 都执行同一套 A2A 请求、Task 状态和结果契约

GoAI 使用下面的 A2A Message metadata 扩展表达委派语义：

```json
{
  "https://goai.dev/extensions/delegation/v1": {
    "sourceAgentCode": "planner",
    "capabilityCode": "write",
    "parentRunId": "run-parent"
  }
}
```

`message:send` 会把协议请求映射为内部 `Thread + request Message + Delegation + Child Run`，并在事务提交后投递 Child Run。重复发送相同 A2A 请求会返回同一个 Child Run；复用相同标识但改变请求内容会返回冲突。Child Run 完成后，Runtime 将结果写为目标 Agent 返回给源 Agent 的 Result Message；A2A Task 查询会把结果映射为 Artifact，失败响应不会暴露 Provider 或数据库原始错误。

当前实现已经覆盖入站与出站的 A2A 最小闭环：Workflow `agent` 节点通过 Agent Card discovery 找到目标 Agent，通过官方 A2A HTTP+JSON Client 发起 `message:send`，再轮询 Task 直到终态并将 Artifact/Message 收敛为父 RunStep 输出。当前父 Worker 在节点执行期间阻塞轮询，尚未实现 callback 驱动的 suspend/resume，也尚未支持多个 Child Run 的并行聚合；这些属于后续运行时增强。无论后续如何扩展，都不允许通过进程内 service 直调或 Kafka 投递绕过 A2A。

> 安全提示：当前尚未实现 A2A Agent 身份认证、授权和凭据管理。A2A 路由只适用于受控开发网络，不应直接暴露到公网。

## 当前 HTTP API

- `POST /auth/register`
- `POST /auth/login`
- `POST /api/chat`：单 Agent / Provider 流式调试入口
- `POST /api/agents/:agent_code/agui`：AG-UI 标准协议入口
- `/a2a/agents/:agent_code/*`：A2A Agent Card、委派与 Task 查询入口
- `POST /api/runs`
- `GET /api/runs/:run_id`
- `GET /api/runs/:run_id/steps`
- `POST /api/runs/:run_id/replay`
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

请求通过 JWT 认证但权限不足时，返回 `403` 和 `AUTH_FORBIDDEN`。

## 配置

基础运行时配置：

- `MYSQL_HOST` `MYSQL_PORT` `MYSQL_USER` `MYSQL_ROOT_PASSWORD` `MYSQL_DATABASE`
- `REDIS_HOST` `REDIS_PORT` `REDIS_PASSWORD`
- `KAFKA_BOOTSTRAP_SERVERS` `KAFKA_RUN_TOPIC` `KAFKA_RUN_GROUP_ID`
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

进程使用标准 `http.Server` 并监听 `SIGINT` / `SIGTERM`：

1. HTTP Server 停止接收新连接，并在配置窗口内等待普通请求和 SSE 结束
2. drain 超时后强制关闭仍存活的连接
3. 取消 worker context，并关闭 Kafka consumer 解除阻塞读取
4. 等待 worker 退出
5. 依次关闭 Kafka producer、Redis 和数据库连接池
6. 聚合并记录所有关闭错误，不因单个资源失败跳过后续清理

## 文档

- [产品设计提案](docs/PRODUCT_PROPOSAL.md)
- [实施路线图](docs/IMPLEMENTATION_ROADMAP.md)
- [统一响应契约](docs/RESPONSE_CONTRACT.md)
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
