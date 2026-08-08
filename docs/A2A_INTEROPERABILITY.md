# A2A 互操作契约

本文档定义 GoAI 与独立外部 Agent 之间的 A2A HTTP+JSON 互操作边界。

## 目标

GoAI 可以管理并调用不依赖 GoAI 内部代码的 Agent。外部 Agent 不需要使用 Go、Eino 或 GoAI 的 `services` 包，只需要实现当前声明的 A2A HTTP+JSON 契约。

Agent 间业务通信始终走 A2A HTTP+JSON：

- 本地开发使用 loopback HTTP。
- 远程 Endpoint 必须使用 HTTPS。
- Kafka 只负责 GoAI Runtime 内部的执行、恢复和重试，不替代 A2A。
- 未登记的 URL 不能被 Workflow 直接调用。

## Registry 接入

外部 Agent 必须先在 GoAI Agent Registry 中登记：

1. 创建 Agent，设置唯一 `agent_code`。
2. 创建 `remote` 类型 Capability。`remote` Capability 不引用本地 Workflow，表示能力由外部 A2A Agent 执行。
3. 创建 A2A Endpoint，配置地址、传输方式和 `credential_ref`。
4. 完成 Agent Card 健康检查并发布 Agent。

`remote` Capability 与本地 `workflow` Capability 都可以被 Registry Router 选择。Router 只返回 active Agent、active Capability 和 active A2A Endpoint，禁止任意 URL 旁路。

## Agent Card

GoAI 和外部 Agent 都应公开：

```text
GET /a2a/agents/{agent_code}/.well-known/agent-card.json
```

最低要求：

- 一个 HTTP+JSON `supportedInterface`。
- 至少一个可执行 `skill`。
- `pushNotifications=true`，当使用异步 callback 时必须声明。
- 使用 HMAC 机器身份时，声明 `GoAI-HMAC-SHA256` security scheme。

GoAI delegation extension：

```text
https://goai.dev/extensions/delegation/v1
```

是可选扩展，Card 中 `required` 必须为 `false`。GoAI Peer 可以使用它补充以下关联信息：

```json
{
  "sourceAgentCode": "planner",
  "capabilityCode": "write",
  "parentRunId": "run-parent",
  "traceId": "trace-parent",
  "delegationId": "delegation-parent"
}
```

外部 Agent 不声明该扩展时，GoAI 出站请求只发送标准 Message、Task 和 PushConfig，不发送 GoAI 扩展字段。

## 标准入站请求

外部 Agent 向 GoAI 发送标准 `message:send` 时，可以不携带 GoAI delegation extension：

```text
POST /a2a/agents/{target_agent_code}/message:send
```

来源身份从经过 HMAC 验证的 `X-GoAI-Agent-Code` 获取，不能信任请求正文中的来源字段。GoAI 将请求映射为：

- Thread
- request Message
- Delegation
- Child Run

标准 A2A Message 没有统一的 capability 选择字段，因此 GoAI 只在目标 Agent 恰好暴露一个 active workflow capability 时自动选择。若目标有多个候选能力，返回明确的 `invalid params` 错误，要求调用方使用 GoAI delegation extension 或其他已约定的能力选择方式；GoAI 不猜测能力。

标准请求没有 GoAI Parent Run 时，Runtime 使用由来源 Agent 和 Thread 派生的稳定协议关联 ID。这个 ID 只用于 Delegation 追踪，不伪造用户侧 Parent Run。

## Task 与异步回流

`message:send` 可以携带 `PushConfig`：

```json
{
  "configuration": {
    "returnImmediately": true,
    "taskPushNotificationConfig": {
      "taskId": "task-1",
      "id": "push-1",
      "url": "https://source.example.com/callback",
      "token": "opaque-notification-token"
    }
  }
}
```

任务生命周期映射如下：

| A2A Task | GoAI Run / Delegation |
| --- | --- |
| `submitted` | `queued` / `accepted` |
| `working` | `running` 或 `waiting_external` |
| `completed` | `success` / `succeeded` |
| `failed` | `failed` |
| `canceled` | `cancelled` |

终态 callback 使用 `StreamResponse` 携带 Task 或终态事件。GoAI 对 callback 做 token、来源、Task ID、状态和幂等校验，然后恢复 Parent Run；重复 callback 不会重复执行后继节点。

## 错误与安全

业务 A2A 请求使用 HMAC-SHA256 机器身份、时间窗和 nonce 防重放。Agent Card discovery 保持公开，但 `message:send`、Task 查询、CancelTask 和 callback 受业务认证保护。

标准错误必须保持以下语义：

- 未认证：HTTP `401`。
- 来源身份与委派不匹配：HTTP `401` 或 `403`，不得进入 Runtime。
- Task 不存在：标准 Task not found 错误。
- 能力不匹配或无法安全选择：标准 invalid params 错误。
- Task ID 与请求或 callback 不一致：invalid params / invalid request。

响应不得泄露数据库、Provider 或真实凭据错误。数据库只保存 `credential_ref`，真实密钥由部署配置解析。

## Conformance 范围

CI 中的独立 external Agent 必须：

- 不导入 `GoAI/services`。
- 提供标准 Agent Card。
- 接受标准 `message:send`。
- 支持 Task 查询与 CancelTask。
- 支持终态 Push callback。
- 验证 HMAC 身份和 notification token。

测试覆盖两个方向：

1. GoAI -> external Agent：Card discovery、能力校验、异步委派、callback、Parent Run resume。
2. external Agent -> GoAI：标准 Message、Task 查询、CancelTask、callback 和未认证请求拒绝。

## 当前范围

当前只覆盖 HTTP+JSON。多模态内容、其他 Transport、复杂 capability routing 和跨租户策略不属于本阶段互操作契约。
