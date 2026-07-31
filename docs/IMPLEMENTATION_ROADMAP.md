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

### 4. 内部统一领域模型
- `threads`
- `messages`
- `delegations`
- `agent_endpoints`
- `agent_capabilities`

### 5. AG-UI Gateway
- 基础协议接入
- Thread / Run 创建
- 流式回传

### 6. A2A Gateway
- Agent 间委派入口与统一 A2A 消息模型
- 远程 Agent 使用 HTTPS transport，本地 Agent 使用遵守相同语义的进程内 transport
- 回调 / 结果通知
- 协议映射到 Message + Delegation + Child Run + Trace

## P1：多 Agent 运行时闭环

### 7. 多 Agent 协作运行时
- Supervisor / Router / Worker 模式
- Workflow Agent 节点通过 Runtime 创建 Delegation 与 Child Run
- Delegation 状态推进、父 Run 等待与恢复
- 子任务结果汇总

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
