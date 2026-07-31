# GoAI / FORGE 产品设计提案

更新时间：2026-07-31

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

### 8.1 Protocol Layer

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
- 远程 Agent 使用 HTTPS transport；本地 Agent 可以使用进程内 transport adapter
- 不同 transport 必须共享同一套 A2A 请求、状态与结果模型
- 每次跨 Agent 调用必须持久化 Message、Delegation、Child Run，并关联 Trace
- 不允许用 service 直接调用或只投递一个 Kafka run_id 来替代 A2A 协作语义

### 8.2 Runtime Layer

负责 Thread / Run / Delegation 的业务运行时协调。

包含：

- Thread 管理
- Run 创建与推进
- 消息持久化
- 协作调度
- Replay
- 异步执行边界

这是平台中心。

### 8.3 Capability Layer

负责 Agent 真正会做什么。

包含：

- Eino Graph Runner
- Workflow DSL
- Tool 执行
- LLM Provider
- Retrieval / Memory（后续）

这里的能力可以扩展，但不应把能力层反客为主变成产品本体。

### 8.4 Infra Layer

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

表示 Agent 的协议入口或回调地址。

关键字段建议：
- `agent_id`
- `protocol` (`agui` / `a2a` / `mcp` / `internal`)
- `base_url`
- `path`
- `auth_type`
- `auth_config_json`
- `timeout_ms`
- `is_active`

### 11.3 agent_capabilities

表示 Agent 支持的能力，例如 graph、tool、llm、delegation。

关键字段建议：
- `agent_id`
- `capability_code`
- `capability_type`
- `config_json`
- `is_active`

### 11.4 threads

一次会话或协作上下文。

关键字段建议：
- `thread_id`
- `user_id`
- `source_protocol`
- `status`
- `metadata_json`
- `last_run_id`

### 11.5 messages

Thread 内的消息单元。

关键字段建议：
- `message_id`
- `thread_id`
- `run_id`
- `sender_type`
- `sender_agent_id`
- `receiver_agent_id`
- `protocol`
- `message_type`
- `content_json`
- `status`

### 11.6 runs

一次完整业务回合。

关键字段建议：
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

Run 中每个执行步骤的记录。

### 11.8 delegations

一个 Agent 把任务交给另一个 Agent 的协作记录。

关键字段建议：
- `delegation_id`
- `thread_id`
- `parent_run_id`
- `source_agent_id`
- `target_agent_id`
- `trigger_message_id`
- `status`
- `input_json`
- `output_json`
- `error_message`

### 11.9 loop_records / loop_eval（后续）

用于 Trace、Replay、Eval、Prompt 快照和成本观测。

## 12. 关键运行链路

### 12.1 AG-UI 链路

`Client -> AG-UI Gateway -> Thread / Message -> Create Run -> Runtime Coordination -> Eino Graph / Tool / LLM -> RunStep / Message -> Stream Back to Client`

### 12.2 A2A 链路

`Source Agent -> A2A Gateway / Local Transport -> Delegation -> Child Run -> Target Agent Execution -> A2A Result / Callback -> Result Message -> Parent Run Continue`

Workflow 中的 Agent/Delegation 节点必须调用 Runtime 创建 Delegation 与 Child Run；目标 Agent 无论本地还是远程，都遵守相同协议和状态语义。

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

### 14.2 需要重写/扩展

- 从“Run-only 主线”升级到“Thread / Message / Delegation / Run 主线”
- 从“chat 调试接口”升级到“AG-UI Gateway”
- 从“单执行链 worker”升级到“多 Agent 协作运行时”
- 增加协议层与 Agent 端点/能力建模

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
3. 基础设施显式依赖装配
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
