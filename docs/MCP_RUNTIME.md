# GoAI MCP Tool Runtime

本文档描述 GoAI 当前 V1 的 MCP Tool Runtime。MCP 只负责 `Agent -> Tool`，不承担 Agent 间通信；Agent 间通信必须使用 A2A，单个 Agent 内部编排使用 Eino Graph，Kafka 只承载 GoAI 内部 Run 调度。

## Boundary

```text
AG-UI  User/Frontend -> Agent Runtime
A2A   Agent -> Agent
MCP   Agent -> Tool
Eino  one Agent's Graph/Workflow
Kafka GoAI internal run_execute/run_resume scheduling
```

一个 Workflow 的 `agent` 或 `agent_group` 节点进入 A2A Client，经 loopback HTTP 或 remote HTTPS 到目标 Agent 的 A2A Gateway；一个 `tool` 节点进入 `ToolInvoker`，经官方 MCP Go SDK 的 Streamable HTTP transport 调用独立 MCP Server。两条路径不会互相替代，也不会通过进程内 Service 直调绕过协议边界。

## Server Registry

MCP Server 由管理面注册，数据库只保存协议配置和 `credential_ref`，不保存解析后的 Token。Server 创建后默认 `inactive`，只有成功完成官方 SDK 的 `initialize/connect` 和 `tools/list`，并发现至少一个合法 Tool 后才会变成 `active`。

支持的配置：

| Field | Rule |
| --- | --- |
| `server_code` | owner 内唯一的稳定引用，只允许 Registry code 字符集 |
| `transport` | V1 固定为 `streamable_http` |
| `endpoint` | loopback HTTP，或远程 HTTPS；禁止任意远程 HTTP |
| `auth_type` | `none` 或 `bearer` |
| `credential_ref` | bearer 必填，只是 SecretResolver 的逻辑引用 |

健康检查成功后，`mcp_tools` 保存名称、描述、输入 Schema 和输出 Schema 快照。Server 的 endpoint、transport 或凭据引用发生变化时，配置版本递增、Server 回到 `inactive`、旧快照被删除，并要求再次健康检查。并发健康检查使用配置版本 fencing，旧结果不能覆盖更新或停用后的状态。

## Management API

所有接口都需要 JWT 和对应的 RBAC 权限，普通 JSON 返回 `{code,message,data,trace_id}`。

| Method | Path | Permission | Meaning |
| --- | --- | --- | --- |
| `POST` | `/api/mcp/servers` | `mcp:create` | create inactive Server |
| `GET` | `/api/mcp/servers` | `mcp:read` | list owned Servers; admin may list all |
| `GET` | `/api/mcp/servers/:server_code` | `mcp:read` | get Server |
| `PUT` | `/api/mcp/servers/:server_code` | `mcp:update` | update metadata or protocol config |
| `POST` | `/api/mcp/servers/:server_code/deactivate` | `mcp:update` | deactivate Server |
| `POST` | `/api/mcp/servers/:server_code/health-check` | `mcp:update` | initialize and discover Tools |
| `GET` | `/api/mcp/servers/:server_code/tools` | `mcp:read` | list discovery snapshot |

`member` 只能访问自己拥有的 Server；`admin` 通过 `mcp:manage` 跨 owner 管理。管理请求使用严格 JSON 解码，未知字段会被拒绝，因而 `token`、`secret` 和 `authorization` 等字段不会被接受或回显。

## Workflow Tool Node

`tool` 节点只保存稳定的 `server_code`、`tool_name` 和业务输入，不保存 endpoint、认证头或真实凭据。

```json
{
  "key": "search_docs",
  "type": "tool",
  "config": {
    "server_code": "docs",
    "tool_name": "search",
    "input": {"query": "eino"},
    "timeout_ms": 30000
  }
}
```

也可以从已经成功的 RunStep 聚合输入：

```json
{
  "key": "format_result",
  "type": "tool",
  "config": {
    "server_code": "docs",
    "tool_name": "format",
    "input_from": ["search_docs"]
  }
}
```

`input` 与 `input_from` 必须二选一；`timeout_ms` 范围为 `0..300000`；未知字段、空引用、自引用和不存在的 `input_from` 都会被拒绝。Agent 发布前会校验 active Workflow 中引用的 Server 属于 Agent owner、Server 为 active 且 Tool 存在于发现快照。

## Runtime and Reliability

执行链路为：

```text
Eino tool node
  -> RunService ToolInvoker boundary
  -> owner/status/snapshot/schema validation
  -> official MCP SDK initialize/connect + tools/call
  -> JSON-safe result
  -> RunStep output
  -> successor Eino node
```

每次尝试都创建并完成一个 `RunStep`，沿用 Run 的重试和终态状态机。Tool 调用继承 context cancellation、deadline 和 `trace_id`；MCP session 在调用结束后关闭，凭据解析结果只存在于调用生命周期内并被清零。重复 Kafka 消息首先受 Run 的原子 claim 保护，已经成功的 Tool Step 在 resume checkpoint 中直接复用，不会重复调用外部 Tool。

## Stable Errors

MCP 管理 API 和运行时错误映射为稳定码：

| Code | Meaning |
| --- | --- |
| `MCP_SERVER_NOT_FOUND` | Server 不存在或不属于当前 owner |
| `MCP_SERVER_ALREADY_EXISTS` | owner 下 `server_code` 冲突 |
| `MCP_SERVER_INVALID_STATE` | 配置版本、发布引用或状态迁移冲突 |
| `MCP_SERVER_UNHEALTHY` | 健康检查未通过 |
| `MCP_TOOL_NOT_FOUND` | Tool 不在发现快照中 |
| `MCP_TOOL_INVOCATION_FAILED` | Tool 调用边界失败 |
| `MCP_INVALID_CONFIG` | MCP 配置或 Tool 输入不合法 |
| `MCP_CREDENTIAL_NOT_FOUND` | credential_ref 无法解析 |
| `MCP_TRANSPORT_FAILED` | HTTP/MCP transport 中断 |
| `MCP_PROTOCOL_FAILED` | MCP initialize、tools/list 或协议响应异常 |
| `MCP_TOOL_REPORTED_ERROR` | Tool 返回 `isError` |

客户端只接收稳定 `code/message`；第三方错误、下游响应和凭据不会进入响应、数据库错误摘要或日志。

## V1 Limits

V1 不包含 stdio/command transport、OAuth 浏览器授权、mTLS、Resources、Prompts、Elicitation、Tool Marketplace 和完整前端管理控制台。它们可以在 MCP 协议边界稳定后作为增量能力加入。
