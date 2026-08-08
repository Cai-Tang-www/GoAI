# GoAI / FORGE 产品设计提案

更新时间：2026-08-01

## 1. 背景

AI 应用开发中，真正反复出现且成本高的工作，不只是“编排一个 Agent 图”，而是：

- 前端如何和 Agent 做标准化、流式、可观测的交互
- Agent 如何把任务委派给另一个 Agent
- 一次完整业务回合如何被追踪、回放、评估和审计
- Workflow、Tool、LLM Provider 如何被统一纳入同一个运行时
- JWT、RBAC、Kafka、Redis、Trace、Replay 等基础设施如何即开即用

现有开发方式常常是：

- 一边用编排框架写图
- 一边自己补 Web 服务、协议适配、消息状态、回放追踪、异步消费
- 每个项目重复搭相似的底座

GoAI / FORGE 的目标就是把这部分通用复杂度平台化。

## 2. 一句话定位

> GoAI / FORGE 是一个基于 Go 的多 Agent 协议运行时平台与 AI 中台开发框架，集成 AG-UI、A2A、Eino 和 Infra，让开发者更聚焦 Agent 能力与编排本身。

## 3. 为什么要做这个

### 3.1 对开发者

开发者应该主要关注：

- Agent 的能力边界
- Workflow / Graph 编排
- Tool 与业务逻辑
- 协作策略

而不该不断重复处理：

- 前后端协议适配
- Agent 间通信协议
- Thread / Run / Replay / Trace / Eval
- JWT / RBAC / Kafka / Redis / 配置装配

### 3.2 对项目价值

这个平台做成后，GoAI 不只是一个“能跑的 AI Demo 后端”，而是：

- 一个可讲清架构的多 Agent 运行时平台
- 一个可沉淀模板和组件的 AI 中台
- 一个能体现 Go 后端工程能力的简历项目

## 4. 产品目标

### 4.1 总目标

提供一套通用的 Go AI 开发框架，使开发者只需主要关注 Agent 节点编排与能力定义，而无需重复搭建客户端交互、远程工具调用、Agent 协作、Trace、Replay、Eval 等通用底座。

### 4.2 V1 目标

V1 只做“多 Agent 协议运行时最小闭环”，不做过度平台化。

V1 需要完成：

- AG-UI Gateway
- A2A Gateway
- 统一内部领域模型：`Thread / Message / Run / Delegation`
- Eino Graph 作为 Agent 执行能力接入
- Run / RunStep / Message / Delegation 持久化
- Replay / Trace / Loop 基础能力
- JWT / RBAC / Kafka / Redis / MySQL 等基础设施保留并纳入统一架构

## 5. 非目标（当前不做）

以下内容不进入当前 V1 核心范围：

- 大而全的多模态平台
- 每家模型做深度定制 SDK 接入
- 完整前端控制台
- 高级任务调度算法和复杂自治规划
- 复杂租户/工作区/组织体系
- 企业级计费与商业化能力

原因：

- 会抢走 V1 的主线资源
- 容易让项目重新退化成“功能堆砌”
- 无法尽快形成协议运行时闭环

## 6. 核心用户与使用方式

### 6.1 核心用户

- 使用 Go 开发 AI 后端的开发者
- 希望快速做出多 Agent 应用原型的团队
- 需要保留工程化底座（JWT、RBAC、Kafka、Redis、Trace、Replay）的项目

### 6.2 使用方式

开发者通过 GoAI / FORGE：

1. 注册或定义一个 Agent
2. 配置 Agent 的通信端点、能力与图编排
3. 从 AG-UI 或 A2A 发起请求
4. 交给平台统一创建 Thread / Run
5. 由 Runtime 协调执行、委派、回流消息与结果
6. 在平台内完成 Trace、Replay、Eval、成本观测

## 7. 核心概念

### 7.1 Thread

一次用户会话或多 Agent 协作上下文。Thread 是顶层会话容器，承载多个 Message 与多个 Run。

### 7.2 Run

一次 AG-UI / A2A -> Runtime -> Eino Graph 的完整业务回合。每次 Run 有唯一 RunID，并可追踪状态、重试、回放。

### 7.3 Message

Thread 内的通信单元，既可以是用户消息，也可以是 Agent 间消息、工具响应、系统事件。

### 7.4 Delegation

一个 Agent 将子任务委派给另一个 Agent 的记录。它描述协作关系，而不是简单的函数调用。

### 7.5 Workflow / Graph

某个 Agent 的执行模板或 Graph 能力。Workflow 是能力层，而不是平台本体。

### 7.6 Loop

一个可观测执行片段，用于串联 Trace、Prompt 快照、Replay、Eval、成本统计。

## 8. 产品边界与层次

### 8.1 Management Plane

负责管理哪些 Agent 可以被发现和委派。

包含：

- Agent Registry
- Capability 与 Endpoint 管理
- 健康检查与发布校验
- owner / RBAC 治理

管理面只维护身份、能力、协议入口和发布状态，不直接执行跨 Agent 任务。所有本地与远程 Agent 都必须先注册，Workflow 不能填写任意未治理 URL；真正的协作请求仍统一进入 A2A Gateway。

### 8.2 Protocol Layer

负责协议接入与协议适配。

包含：

- AG-UI Gateway
- A2A Gateway
- MCP Adapter
- AgentAsTool Adapter

职责：

- 解析外部协议
- 校验协议请求
- 映射到内部领域模型
- 输出统一流式响应/回调

不负责：

- 业务编排决策
- 持久化状态机逻辑

A2A 的约束：

- A2A 定义 Agent 协作语义，HTTPS 只是远程 transport，Kafka 只是异步基础设施
- 远程 Agent 使用 HTTPS transport；本地 Agent 通过 loopback HTTP 访问同一 A2A Gateway，不提供进程内 transport adapter
- 不同 transport 必须共享同一套 A2A 请求、状态与结果模型
- 每次跨 Agent 调用必须持久化 Message、Delegation、Child Run，并关联 Trace
- 不允许用 service 直接调用或只投递一个 Kafka run_id 来替代 A2A 协作语义

### 8.3 Runtime Layer

负责 Thread / Run / Delegation 的业务运行时协调。

包含：

- Thread 管理
- Run 创建与推进
- 消息持久化
- 协作调度
- Replay
- 异步执行边界

这是平台中心。

### 8.4 Capability Layer

负责 Agent 真正会做什么。

包含：

- Eino Graph Runner
- Workflow DSL
- Tool 执行
- LLM Provider
- Retrieval / Memory（后续）

这里的能力可以扩展，但不应把能力层反客为主变成产品本体。

### 8.5 Infra Layer

负责基础设施与观测底座。

包含：

- MySQL
- Redis
- Kafka
- JWT / RBAC
- OpenTelemetry
- Trace / Loop / Eval / Replay
- 成本与性能指标

## 9. V1 详细功能范围

### 9.1 必做功能

#### A. 协议接入
- AG-UI 基础 Gateway
- A2A 基础 Gateway
- 协议请求到内部模型的映射
- 流式响应 / 回调基础能力

#### B. 统一领域模型
- Thread
- Message
- Run
- RunStep
- Delegation
- AgentEndpoint
- AgentCapability

#### C. 运行时闭环
- 创建 Thread
- 创建 Run
- Agent 任务委派
- 异步执行
- 状态推进
- 结果回流
- Replay
- 本地 loopback HTTP / 远程 HTTPS 的 A2A 出站调用
- Agent Card discovery、Capability/Push Notification 校验、异步委派与 callback 驱动恢复
#### D. 编排能力
- Eino Graph 接入
- Workflow DSL 基础校验
- 拓扑排序
- 节点串行执行
- Tool/LLM 基础能力接入

#### E. Infra 底座
- JWT
- RBAC
- MySQL
- Kafka
- Redis
- TraceID
- 基础日志
- 统一错误码

### 9.2 保留但降级为“能力/底座”的功能

这些功能继续保留，但不再作为产品主线：

- Provider 多厂商接入
- `/api/chat` 调试接口
- 单纯面向 Workflow 的叙事

原因：

- 它们仍然是运行时需要的能力
- 但不应压过 AG-UI / A2A / Runtime 主线

## 10. V1 不做的功能

- 高级多模态输入输出
- 复杂多 Agent 自主规划
- 复杂并行图优化
- 完整可视化管理后台
- 企业级多租户与计费
- 完整知识库平台

## 11. 数据模型提案

### 11.1 agents

表示平台中的一个 Agent 实体。

关键字段建议：
- `agent_code`
- `name`
- `description`
- `owner_user_id`
- `status`

### 11.2 agent_endpoints

表示 Agent 对平台暴露的协议入口。V1 的 Agent 协作协议固定为 `a2a`，传输方式通过独立字段区分；本地 loopback HTTP 与远程 HTTPS 都访问统一 A2A Gateway，不提供进程内调用旁路。

关键字段：
- `agent_id`
- `endpoint_code`，在同一 Agent 内唯一
- `protocol`，V1 为 `a2a`
- `transport`（本地开发使用 loopback HTTP，远程使用 HTTPS；两者都经过同一 A2A Gateway，不允许退化为 Service 直调）
- `address`
- `auth_type`
- `credential_ref`，只保存凭据引用，不保存真实密钥
- `config_json`，仅保存非敏感传输元数据；认证材料必须使用 `credential_ref`
- `status`
- `last_healthy_at`

### 11.3 agent_capabilities

表示 Agent 对 Runtime 和其他 Agent 暴露的业务能力资产。能力可以由 Workflow、Tool 或自定义执行器实现，但 Provider 不作为平台主能力对外暴露；V1 只有由当前 Agent active Workflow 支撑且版本一致的 active Workflow Capability 会进入 Agent Card 并满足发布门禁，`tool/custom` 暂只参与管理。

关键字段：
- `agent_id`
- `capability_code`，在同一 Agent 内唯一
- `name` / `description`
- `capability_type` (`workflow` / `tool` / `custom`)
- `workflow_id`，V1 的 Workflow 类型能力必须关联当前 Agent 的 active Workflow
- `version`
- `input_schema_json` / `output_schema_json`
- `config_json`
- `status`

### 11.4 threads

一次用户会话或多 Agent 协作上下文，是 Message、Run 与 Delegation 的顶层容器。

关键字段：
- `thread_id`
- `owner_user_id`
- `title`
- `status`
- `metadata_json`

Thread 不直接保存 AG-UI/A2A 的协议字段；协议来源由 Gateway 映射为内部命令，必要的扩展信息进入 metadata。

### 11.5 messages

Thread 内的稳定通信单元，可以表示用户输入、Agent 委派、Agent 结果、工具结果、状态更新或系统事件。

关键字段：
- `message_id`
- `thread_id`
- `run_id`
- `delegation_id`
- `parent_message_id`
- `sender_type` / `sender_id`
- `receiver_type` / `receiver_id`
- `sender_id / receiver_id` 的标识符命名空间由对应 type 决定
- `message_type`
- `content_type` / `content_json`
- `metadata_json`
- `status`

Message 不直接复制外部 AG-UI/A2A 消息结构，协议层需要先完成输入与事件映射。

### 11.6 runs

一次完整业务回合，可由 AG-UI 用户请求、A2A 委派或系统调度触发，不等价于一次 Workflow 节点调用。

关键字段：
- `run_id`
- `thread_id`
- `agent_id`
- `workflow_id`
- `trigger_type`
- `status`
- `current_step`
- `retry_count`
- `error_message`
- `provider`
- `model`

### 11.7 run_steps

Run 中每个可观测执行步骤及其重试 attempt。RunStep 可以记录 Workflow 节点、Tool、LLM 或 Delegation 等执行片段，但不能替代 Child Run。

### 11.8 delegations

一个 Agent 通过 Runtime 把子任务交给另一个 Agent 的协作记录。每条 Delegation 必须连接一个 Parent Run 与唯一 Child Run，并关联请求 Message；完成后再关联结果 Message。

关键字段：
- `delegation_id`
- `thread_id`
- `parent_run_id`
- `child_run_id`
- `source_agent_id`
- `target_agent_id`
- `capability_code`
- `request_message_id`
- `result_message_id`
- `status`
- `input_json` / `output_json`
- `error_message`
- `started_at` / `finished_at`

### 11.9 实体关系

- `Agent 1:N Workflow / AgentEndpoint / AgentCapability / Run`
- `Thread 1:N Message / Run / Delegation`
- `Workflow 1:N Run`，Workflow 类型的 AgentCapability 可通过 `workflow_id` 指向其执行定义
- `Run 1:N RunStep`
- `Delegation N:1 Thread`，并通过 `parent_run_id -> child_run_id` 表达一次跨 Agent 协作
- `Delegation N:1 SourceAgent / TargetAgent`，请求和结果分别通过 Message 形成可回放通信记录
- `Message` 可关联 Run、Delegation 和父 Message，用于还原用户、Agent、Tool 与 Runtime 的通信顺序

首版使用稳定公开 ID、索引和唯一约束表达关联，不启用级联删除。Runtime/Service 层负责跨对象一致性与状态迁移，避免数据库迁移承担业务协调。

### 11.10 loop_records / loop_eval（后续）

用于 Trace、Replay、Eval、Prompt 快照和成本观测。

## 12. 关键运行链路

### 12.1 AG-UI 链路

`Client -> AG-UI Gateway -> Runtime StartRun -> Thread / Input Message / Run -> Kafka Worker -> Workflow / Tool / LLM -> RunStep / Result Message -> AG-UI SSE Events`

### 12.2 A2A 链路

`Source Agent -> A2A Client -> loopback HTTP / remote HTTPS -> A2A Gateway -> Delegation -> Child Run -> Target Agent Eino Graph -> A2A Result / Callback -> Result Message -> Parent Run Continue`

Workflow 中的 Agent/Delegation 节点只产出委派意图并交给 Runtime；Runtime 创建 Delegation 与 Child Run 后必须通过 A2A Client 调用目标 Agent 的 A2A Gateway。目标 Agent 无论本地还是远程，都执行自己的 Eino Graph，并遵守相同协议、鉴权、Message 与状态语义。

### 12.3 Replay 链路

`Thread / Run History -> Replay Request -> New Run -> Re-execution -> New Trace / Loop`

## 13. Infra 与 CozeLoop 风格能力

V1 不要求一步做到完整 CozeLoop，但要提前预留：

- `Trace`
  - 基于 OTel + TraceID / LoopID 串联
- `Prompt Snapshot`
  - 记录 prompt_version 与参数摘要
- `Evaluator`
  - 预留异步评估入口
- `Replay`
  - 基于 Thread / Run / Loop 重放
- `Cost / Performance`
  - 记录 token、latency、error、step cost

## 14. 当前仓库如何对齐这个设计

### 14.1 直接复用

- JWT / RBAC
- 统一响应与错误码
- trace_id
- Run / RunStep
- Kafka 异步执行
- Workflow DSL 校验与执行顺序解析
- Provider 抽象
- 幂等与去重
- HTTP / SSE / Kafka / Worker 优雅关闭

### 14.2 当前新增实现

- AG-UI Gateway 已使用官方 Go SDK 完成首版接入
- AG-UI 请求先映射到协议无关 Runtime，再原子创建或复用 Thread、持久化 Message 并触发 Run
- RunStep 与 Result Message 可通过官方 SSE 事件回传
- 当前仅支持普通文本消息；多模态、高级消息字段、非空 `state / tools / context / forwardedProps` 仍会显式拒绝，空对象/空数组仅作为 SDK 默认输入接受且不会写入内部 Run。Issue #61 已支持 `parentRunId` 的 Thread 分支 lineage、`waiting_input` interrupt 和用户主动 `resume`；AG-UI lineage 与 A2A Delegation Parent/Child Run 是两套独立语义
- 已引入平台自己的 Eino Graph executor，Runtime 可解析、校验并串行执行 Agent Workflow，将节点输入输出、重试和终态持久化为 RunStep
- Workflow 的 `agent` 节点通过官方 A2A Go SDK 完成 Agent Card discovery，并使用带 PushConfig 的 HTTP+JSON `message:send` 异步委派；同进程 Agent 也必须经过 loopback HTTP，远程 Agent 强制 HTTPS，不提供 Service 直调或 Kafka 旁路
- Supervisor 的 `agent` 节点支持 `routing_policy=registry`；Registry Router 只选择 active Agent、版本一致的 Workflow Capability 和健康 A2A Endpoint，并把选路结果写入 RunStep
- A2A Gateway 已将协议请求映射为内部 Thread、Message、Delegation 和 Child Run；目标返回 accepted 后 Parent Run/RunStep 进入 `waiting_external` 并释放 Worker，目标 Agent 执行自己的 Eino Graph 后通过认证 callback 回流，源 Runtime 再经 Kafka `run_resume` 从持久化游标继续执行并回传 AG-UI SSE
- `agent_group` 已实现显式多 Agent fan-out/fan-in：每个成员通过 A2A HTTP(S) 创建独立 Delegation、Child Run、A2A Task 和 Message，支持 `all`、`any`、`quorum`，group coordinator 负责聚合结果和恢复 Parent Run；late callback 仅更新审计，不改变已推进的 Parent checkpoint
- callback、Result Message 与 resume 消息具备幂等收敛和持久化恢复能力，重复投递或进程重启不会重复执行后继节点
- Agent Card 已发布 capability 版本及输入输出契约；V1 对 capability contract 使用稳定 JSON Schema 子集进行输入和终态输出校验
- Agent Registry 管理 API 已覆盖 Agent、Capability、Endpoint 的 owner-scoped CRUD、Agent Card 健康检查、发布校验和启停；V1 仅发布可执行的 Workflow Capability，Endpoint `config_json` 拒绝敏感字段；管理员通过独立 `agent:manage` 权限跨 owner 管理，真实凭据不进入 API 响应
- MCP Tool Runtime 已使用官方 MCP Go SDK 支持 Streamable HTTP 的 Server Registry、健康检查、Tool discovery snapshot 和 `tools/call`；Eino `tool` 节点只保存 `server_code + tool_name` 稳定引用，通过 `ToolInvoker` 将 JSON-safe Tool 结果纳入 RunStep、重试和 Replay。MCP 固定承担 Agent -> Tool，A2A 继续承担 Agent -> Agent

### 14.3 仍需扩展

- 多副本共享 nonce store、凭据轮换与 mTLS/OIDC 增强
- 多 Runtime 部署下 callback/resume 恢复扫描的分片与共享协调
- 基于负载、租户、成本或模型能力的高级 Supervisor / Router 策略
- 任意并行 DAG、主动取消剩余远程 Child Task 与更复杂的跨节点补偿策略
- AgentAsTool、多模态消息，以及完整前端管理控制台、批量运维和跨租户治理
- 完整 Eval、成本分析和管理控制台

## 15. 交付标准

当 V1 达到下面这些条件时，可以认为方向正确：

- 可以从 AG-UI 发起一轮对话并创建 Thread
- 可以触发一次完整 Run 并落库
- 可以由一个 Agent 把任务委派给另一个 Agent
- 可以记录 Message、Run、RunStep、Delegation
- 可以 Replay 一个 Thread 或 Run
- 可以通过 Trace / Loop 看到关键链路
- JWT / RBAC / Kafka / Redis / MySQL 这些底座仍然成立并可展示

## 16. 实施顺序

建议顺序：

1. 服务生命周期收口（已完成）
2. 清理 Task 残留与旧命名（已完成）
3. 基础设施显式依赖装配（已完成）
4. 内部统一领域模型落地
5. AG-UI Gateway
6. A2A Gateway
7. Delegation / Multi-Agent Runtime
8. Loop / Trace / Replay / Eval
9. 文档、CI、OpenAPI、压测、治理与审计补齐

## 17. 最后的产品判断

GoAI / FORGE 的核心价值，不在“又支持了几家模型”，而在：

- 统一协议
- 统一运行时
- 统一底座
- 统一观测

它要成为的是：

- 一个多 Agent 平台的后端内核
- 一个 Go AI 应用的基础框架
- 一个能够承载后续模板化、平台化和工程化能力的中台
