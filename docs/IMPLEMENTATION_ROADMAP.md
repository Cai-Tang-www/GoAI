# GoAI 实施路线图

更新时间：2026-08-03

## 当前判断

GoAI 当前主线已经从“Run/Workflow 后端”升级为“多 Agent 协议运行时平台”。

因此，路线图不再以 `Provider -> Workflow` 为中心，而改为：

`Protocol -> Thread/Message -> Runtime Coordination -> Run/Delegation -> Graph/Tool/LLM -> Trace/Replay/Eval`

## P0：主线纠偏与运行时基础

### 1. 服务优雅关闭（Issue #13 建立基线，Issue #41 完成补强）
- HTTP / SSE / Kafka / Worker 生命周期已经收口
- SSE、新 Kafka 拉取、在途普通 HTTP 与当前 Worker 具备明确的 drain / force-stop 顺序
- 关闭流程共享一个总超时预算；未退出的工作不会继续访问已关闭的 Kafka、Redis、MySQL 或 observability 依赖

### 2. 命名与地基清理（已完成，Issue #14）
- 旧 Task 模型与入口已经清理
- 仓库术语统一到 `Thread / Message / Run / Delegation / Workflow`
- 误导性的空包、旧文件名和旧模型已经删除

### 3. 基础设施显式装配（已完成，Issue #15）
- `db / redis / kafka` 已从全局可变单例收口为构造实例并由 `main` 显式装配
- 已为协议 gateway 与 runtime 演进建立可测试的依赖边界

### 4. 内部统一领域模型（已完成，Issue #18）
- `Thread / Message / Delegation / AgentEndpoint / AgentCapability` 已建模并纳入迁移
- Delegation 明确关联 Parent Run、唯一 Child Run、请求 Message 与结果 Message
- Endpoint 使用统一 A2A 协议语义；本地开发通过同一 Gateway 的 loopback HTTP，远程通过 HTTPS，不提供 Service 直调旁路
- 内部模型不直接复制 AG-UI/A2A 外部协议字段

### 5. AG-UI Gateway（已完成，Issue #19）
- 使用官方 AG-UI Go SDK 接收 `RunAgentInput`
- 将协议请求映射为内部 `Thread / Message / Run`
- 支持 Thread 创建与本人 active Thread 复用
- 通过官方 SSE 事件回传 Run、RunStep 与 Result Message
- 支持请求取消、稳定错误事件和历史消息去重
- V1 仅支持普通文本消息，多模态、高级消息字段、非空 `state / tools / context / forwardedProps`、`parentRunId` 和 `resume` 显式拒绝；空对象/空数组不渗透到内部模型。AG-UI `parentRunId` 的 Thread 分支 lineage 与 A2A Delegation Parent/Child Run 分开建模

### 6. A2A Gateway（已完成，Issue #20）
- 使用官方 A2A Go SDK 提供 Agent Card、`message:send` 与 Task 状态查询
- 将入站委派原子映射为 `Thread + request Message + Delegation + Child Run`
- Child Run 终态回写 Delegation，并将成功结果映射为 Result Message 与 A2A Artifact
- 相同请求支持幂等复用，冲突请求显式拒绝，重复 Kafka 消费不会重复执行或重复生成结果
- 远程 Agent Endpoint 强制 HTTPS，本地开发 Endpoint 强制 loopback HTTP；两者都访问同一 A2A Gateway，不提供 Service 直调旁路
- 入站/出站 A2A 委派闭环已完成；A2A HMAC 机器身份、Endpoint 凭据引用、时间窗和 nonce 防重放已完成

## P1：多 Agent 运行时闭环

### Agent Registry 管理面（Issue #49，已实现）
- Agent、Capability、A2A Endpoint 的注册、查询、更新、启停和 owner-scoped 管理已落地
- Agent 发布前强制校验至少一个可执行的 active Workflow Capability、健康 A2A Endpoint、Workflow 归属/版本和凭据引用；`tool/custom` 暂不满足 V1 发布门禁
- Endpoint 健康检查使用 Agent Card discovery 并绑定声明的 `agentCode`，更新后必须重新检查；`config_json` 只允许非敏感元数据
- 本地只允许 loopback HTTP，远程强制 HTTPS；管理 API 不提供 Service 直调或 Kafka 通信旁路
- member 拥有自己的 Agent 管理权限，`agent:manage` 仅作为管理员跨 owner bypass

### MCP Tool Runtime（Issue #51，已实现）
- 使用官方 `github.com/modelcontextprotocol/go-sdk` 的 Streamable HTTP Client 完成 initialize、`tools/list` 和 `tools/call`
- `mcp_servers` / `mcp_tools` 提供 owner-scoped Registry、健康状态、配置版本 fencing 和 discovery snapshot
- member 只能管理自己的 MCP Server，admin 通过 `mcp:manage` 跨 owner 管理；真实凭据只保存 `credential_ref`
- Workflow 显式 `tool` 节点通过 `ToolInvoker` 调用 MCP，不把 MCP 变成 A2A 的替代品；Agent 间通信仍固定走 A2A HTTP(S)
- Tool 调用纳入 Eino Graph、RunStep、超时、重试、Replay checkpoint 和重复 Kafka 消费去重
- 管理 API 使用统一 envelope，MCP 下游错误和敏感信息不会进入响应、数据库错误摘要或日志

### 7. 多 Agent 协作运行时（Issue #37 + Issue #43 + Issue #45，可靠异步闭环已落地）
已落地：
- Workflow `agent` 节点通过 `AgentInvoker` 发起出站 A2A 调用，不允许进程内 Agent Service 直调
- 官方 A2A Go SDK Client 完成 Agent Card discovery、能力/扩展/Push Notification 校验，并使用 `ReturnImmediately + PushConfig` 提交委派
- 本地目标只允许 loopback HTTP，远程目标必须使用 HTTPS；Kafka 只承载 `run_execute / run_resume` 内部调度消息，不承担 Agent 协议通信
- `input_from` 支持从成功的上游 RunStep 聚合输入；TaskID/MessageID/DelegationID 基于 Parent Run + 节点稳定生成，重试不重复创建子任务
- Target 返回 accepted 后，Parent Run/RunStep 持久化为 `waiting_external` 并释放 Worker，不进行 Task polling
- Target 终态通过认证 A2A callback 回流；源 Runtime 幂等写 Result Message，发布 `run_resume`，并从持久化游标继续 Eino Graph
- callback/resume 发布状态、重复消息 no-op 与 RecoveryWorker 已覆盖进程重启恢复
- Parent resume 使用带 heartbeat、expires_at 和递增 execution attempt fencing token 的执行租约，多实例并发只能有一个 worker 接管
- 持久化 Workflow/Delegation 游标与成功 RunStep checkpoint 支持从 crash point 继续；遗留 running Step、再次 A2A 挂起和终态残留租约均可幂等收敛
- **Issue #47 已落地**：显式 `agent_group` 支持多 Child Run fan-out/fan-in、`all`/`any`/`quorum` 聚合、部分失败收敛、group coordinator resume lease 和真实 A2A HTTP 多 Runtime 闭环

后续补齐：
- 多副本共享 nonce store、凭据轮换与 mTLS/OIDC 增强
- AG-UI `parentRunId` 分支和用户主动 resume
- Supervisor / Router / Worker 协作策略
### 8. Eino Graph 能力化接入（Issue #37，V1 已落地）
- 把 Graph 定位成 Agent 的一种执行能力，而不是平台的 Agent 间通信机制
- 已使用 Eino Graph 执行单个 Agent 内部的串行/可达 Workflow 节点
- `agent` 节点必须经由 A2A Client 与目标 Agent Gateway 通信，不允许进程内 Service 直调
- `agent_group` 是显式的并行能力边界；Parent Workflow 仍串行，后续再扩展任意并行 DAG、条件分支、流式节点和节点级恢复
- `tool` 节点通过 MCP `ToolInvoker` 执行 Agent -> Tool 调用；MCP 不替代 Agent -> Agent 的 A2A 通信

### 9. Replay / Loop / Trace
- Thread Replay
- Run Replay
- LoopID / OTel Trace
- Prompt 快照与性能成本基础字段

### 10. 文档与契约
- OpenAPI / Swagger
- AG-UI / A2A 使用说明
- 示例请求、响应和回放说明

## P2：工程化与平台增强

### 11. 可观测性
- 结构化日志
- Prometheus 指标
- OTel Trace
- Loop / Cost / Eval 数据看板准备

### 12. 服务治理
- 限流
- 熔断
- 降级
- Redis 计数与缓存落地

### 13. CI / Docker / Bench
- test / vet / build / docker
- 压测脚本与基线

### 14. 审计与运营聚合
- 审计日志
- Dashboard 聚合接口

## 这条路线保留什么

以下能力继续保留并逐步内建化：

- JWT
- RBAC
- Kafka
- Redis
- MySQL
- trace_id
- 统一错误码
- Provider Registry

这些不是支线，而是平台底座。

## 这条路线暂时不做什么

- 过度追求支持所有模型厂商
- 先做花哨前端而后端主线不稳
- 先做复杂多模态和企业级租户
- 把所有未来可能需要的抽象一次性做完

## 成功标志

当下面这些能力成立时，说明 GoAI 已经真正进入目标轨道：

- 支持 AG-UI 进入平台并创建 Thread
- 支持 A2A 发起 Agent 协作
- 支持统一 Run / RunStep / Message / Delegation 落库
- 支持 Replay、Trace、Loop 观测
- 保留 JWT / RBAC / Kafka / Redis 等后端工程化亮点
