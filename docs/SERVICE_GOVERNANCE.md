# GoAI 服务治理

更新时间：2026-08-03

GoAI 的服务治理属于 Infra 层，不改变 AG-UI、A2A、Thread、Run、Delegation 或 Workflow 的业务语义。它只保护协议入口和下游 HTTP 依赖，避免局部流量或依赖故障扩散到整个 Runtime。

## 当前能力

- 关键 HTTP 入口使用 keyed token bucket 限流
- 限流支持 `api`、`a2a`、`agui` scope
- 限流响应为 HTTP `429`，返回统一响应 envelope 和 `Retry-After`
- 下游 Provider 与 A2A Client 共用受控 HTTP Client
- 下游请求带独立超时，不修改上游协议 payload
- 每个下游 target 使用独立 circuit breaker
- 网络错误、超时、HTTP `429` 和 HTTP `5xx` 会计入失败次数
- 熔断打开后快速失败，恢复窗口只允许一个 half-open 探针
- 探针成功后恢复 closed，失败后重新打开
- 限流、失败、打开、拒绝和恢复事件进入日志与 Prometheus 指标

## 限流边界

治理中间件在公开的 A2A 入口按传输层身份限流，在 API/AG-UI 路由中位于 JWT/RBAC 之后，因此已认证请求优先按用户限流。key 组成如下：

```text
scope | user-or-ip | agent | route
```

API/AG-UI 身份优先级：

1. 已存在的 `user_id`
2. 客户端 IP

Agent 优先读取 URL 路径参数 `agent_code`；`POST /api/runs` 没有路径参数时从 JSON body 提取 `agent_code`，无法解析时使用 `-`，不信任客户端自定义 Header；route 优先使用 Gin 注册路径。未配置 scope 时默认保护 `api`、`a2a` 和 `agui`，不会限制 `/ping` 和 `/metrics`。

当前 limiter 是进程内实现，`RATE_LIMIT_MAX_KEYS` 用于限制内存中的 key 数量；达到上限时淘汰最久未访问的 bucket，让新客户端可以进入，同时保持内存有界。它不是跨实例的分布式限流；多副本部署时应在后续版本增加 Redis 计数或网关级限流，但不需要改变现有中间件契约。

## 下游保护

受控 HTTP Client 只包裹出站依赖：

- OpenAI-compatible Provider
- A2A discovery、message send 和终态 callback 投递

A2A 仍然通过官方 A2A HTTP+JSON 协议通信。Kafka 只承载 `run_execute`、`run_resume` 等内部异步调度消息，不能替代 Agent-to-Agent 协议，也不参与 Delegation 业务决策。Parent Run 等待远端 Agent 时使用持久化 `waiting_external` 状态，不占用 Worker 进行 Task polling。

熔断 target 使用 scheme、host、port 作为 key，因此同一个下游服务的不同路径共享保护状态，而不同下游服务互不影响。治理层只控制 transport，不修改 A2A 或 Provider 的请求字段。

## 降级与错误契约

治理层的首版降级策略是快速失败，不在 Runtime 内伪造 Agent 结果：

- 限流：`429 RATE_LIMITED`
- 熔断拒绝：`503 SERVICE_UNAVAILABLE`
- 下游超时：`504 DOWNSTREAM_TIMEOUT`
- 其他未映射故障：沿用统一错误处理，避免暴露第三方原始错误

这样可以保证 Thread、Run、Delegation 的一致性：依赖不可用时，运行时记录真实失败并由上层重试/回放策略决定后续动作，不把错误伪装成成功结果。

## 观测

治理事件通过现有观测装配输出：

```text
goai_governance_events_total{type,scope,status}
```

HTTP 请求日志继续包含 `trace_id method path status latency_ms`；下游治理事件至少包含 target、status 和错误信息。治理事件类型包括 `rate_limited`、`downstream_failure`、`downstream_timeout`、`circuit_opened`、`circuit_rejected`、`circuit_half_open` 和 `circuit_recovered`。

## 配置

```env
SERVICE_GOVERNANCE_ENABLE=true
RATE_LIMIT_REQUESTS_PER_SECOND=20
RATE_LIMIT_BURST=40
RATE_LIMIT_MAX_KEYS=10000
RATE_LIMIT_SCOPES=api,a2a,agui
DOWNSTREAM_REQUEST_TIMEOUT_SECONDS=30
CIRCUIT_FAILURE_THRESHOLD=3
CIRCUIT_OPEN_TIMEOUT_SECONDS=10
CIRCUIT_MAX_TARGETS=1024
```

配置校验在启动阶段完成。治理关闭时跳过治理参数校验，便于本地排障，但生产环境应保持启用并使用显式配置。

## 当前不在范围内

- Redis 分布式限流
- 跨实例熔断状态共享
- 自动切换备用模型或备用 Agent
- 超出当前 callback/resume 发布恢复范围的业务级补偿与 step 级断点续跑
- 完整 OTel span 级治理策略

这些能力应在不改变 A2A/AG-UI 协议边界的前提下单独演进。
