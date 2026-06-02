想自己模仿做一个和学长那样的多agent管理平台

很不好意思但是引用一下学长的文档

![文档](https://img.cdn1.vip/i/69b82ee3997a6_1773678307.webp)

主用golang语言开发，尽量自己弄明白不使用ai

想得倒是很美好说是，也能用，后续加OCR啥的

滑动窗口等等，现在刚好有另一个项目

类似opencode我看看两者有无共通之处

————等我下次更新

## Provider 环境变量（V1）

`/api/chat` 支持按 `provider` 字段路由到不同厂商配置。当前推荐优先使用 OpenAI-compatible 通道。

- `MODEL_PROVIDER_DEFAULT`：默认 provider（例如 `deepseek`）
- `MODEL_DRIVER_<PROVIDER>`：驱动类型，默认 `openai_compatible`
- `MODEL_BASE_URL_<PROVIDER>`：厂商网关地址
- `MODEL_API_KEY_<PROVIDER>`：厂商密钥
- `MODEL_NAME_DEFAULT_<PROVIDER>`：默认模型名
- `MODEL_ENDPOINT_PATH_<PROVIDER>`：聊天端点（可选，不填按代码默认值）

DeepSeek 推荐配置可参考 [`F:/GoAI/.env.example`](/F:/GoAI/.env.example)。

## RBAC 配置

- `RBAC_ENABLE`：是否启用 RBAC 授权，默认 `true`
- `RBAC_BOOTSTRAP_ADMIN_USERNAME`：启动补种时自动授予 `admin` 角色的用户名（推荐配置）

## RBAC 权限矩阵（Issue #1）

### 预置角色
- `admin`：拥有全部预置权限
- `member`：默认角色，拥有除 `user:manage` 以外的预置权限

### 预置权限
- `run:create` `run:read` `run:replay`
- `user:read_self` `user:update_self` `user:manage`
- `chat:use`

### 路由授权规则
- `POST /api/chat`：需要 `chat:use`
- `POST /api/runs`：需要 `run:create`
- `GET /api/runs/:run_id`、`GET /api/runs/:run_id/steps`：需要 `run:read`，且默认仅本人可读（`admin` 可绕过）
- `POST /api/runs/:run_id/replay`：需要 `run:replay`，且默认仅本人可重放（`admin` 可绕过）
- `GET /api/users/:id`：本人需要 `user:read_self`，非本人需要 `user:manage`
- `PUT /api/users/:id`：本人需要 `user:update_self`，非本人需要 `user:manage`
- `POST /api/users`、`GET /api/users`、`DELETE /api/users/:id`：需要 `user:manage`

### 403 语义
- 请求已通过 JWT 认证但权限不足，统一返回 `403` 与错误体 `{ "error": "forbidden" }`
- `RBAC_ENABLE=false` 时关闭 RBAC 权限校验（仅保留 JWT 认证）

## 当前后端架构（Run 编排主线）

### 分层
- `handlers/`：HTTP 接口层（鉴权后的请求解析与响应）
- `services/`：应用服务层（CreateRun/ReplayRun/执行编排用例）
- `domain/workflow`：工作流 DSL 校验与拓扑执行顺序
- `domain/runstate`：Run/Step 状态机迁移规则
- `kafka/`：消息生产消费基础设施
- `worker/`：异步执行入口（Kafka 消费后触发 Run 执行）
- `ai/`：模型适配层（编排中的 LLM 节点调用）

### 主链路
1. `POST /api/runs` 写入 `runs`（状态 `queued`）并发送 `run_execute` 消息
2. `worker` 消费消息后执行工作流节点，逐步写入 `run_steps`
3. 节点执行完成后更新 `runs` 终态（`success/failed`）
4. `GET /api/runs/:run_id` 与 `GET /api/runs/:run_id/steps` 提供查询与回放支撑
