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
- `Delegation`：源 Agent 将子任务委派给目标 Agent 的协作记录
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
- 本地 Agent 可以使用进程内 transport adapter，避免无意义的网络回环
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
- 旧 Task 模型、空包和误导性文件名清理
- db / redis / kafka 显式依赖装配

### 在建
- `Thread / Message / Delegation / AgentEndpoint / AgentCapability` 领域模型
- AG-UI Gateway
- A2A Gateway 与 transport
- 多 Agent Runtime、父子 Run 与结果聚合
- Eino Graph 能力化接入
- Loop / Trace / Replay / Eval / Cost

文档中的“在建”能力不能视为当前已经可用。

## 当前 HTTP API

- `POST /auth/register`
- `POST /auth/login`
- `POST /api/chat`：单 Agent / Provider 流式调试入口
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
