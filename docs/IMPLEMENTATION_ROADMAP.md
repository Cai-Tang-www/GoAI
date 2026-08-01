# GoAI 实施路线图

更新时间：2026-07-31

## 当前判断

GoAI 当前主线已经从“Run/Workflow 后端”升级为“多 Agent 协议运行时平台”。

因此，路线图不再以 `Provider -> Workflow` 为中心，而改为：

`Protocol -> Thread/Message -> Runtime Coordination -> Run/Delegation -> Graph/Tool/LLM -> Trace/Replay/Eval`

## P0：主线纠偏与运行时基础

### 1. 服务优雅关闭（已完成，Issue #13）
- HTTP / SSE / Kafka / Worker 生命周期已经收口
- 在途请求、流式响应与异步消费者具备超时关闭语义

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
- 当前仅完成入站委派；A2A Agent 身份认证、授权与凭据管理尚未实现，路由只适用于受控开发网络

## P1：多 Agent 运行时闭环

### 7. 多 Agent 协作运行时（Issue #21，核心闭环进行中）
已落地：
- Workflow `agent` 节点通过 `AgentInvoker` 发起出站 A2A 调用，不允许进程内 Agent Service 直调
- 官方 A2A Go SDK Client 完成 Agent Card discovery、能力/扩展校验、`message:send`、Task polling 和 Artifact/Message 结果收敛
- 本地目标只允许 loopback HTTP，远程目标必须使用 HTTPS；Kafka 只承载 Run 执行消息，不承担 Agent 协议通信
- `input_from` 支持从成功的上游 RunStep 聚合输入；TaskID/MessageID 基于 Parent Run + 节点稳定生成，重试不重复创建子任务

后续补齐：
- callback 驱动的 Parent Run suspend/resume；当前 V1 是 Worker 内阻塞轮询
- 多 Child Run 并行执行、结果聚合与部分失败策略
- A2A Agent 身份认证、授权和 Endpoint 凭据管理
- Supervisor / Router / Worker 协作策略
### 8. Eino Graph 能力化接入
- 把 Graph 定位成 Agent 的一种执行能力
- 保持 Workflow DSL 基础校验与执行顺序解析

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
