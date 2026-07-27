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
- 请求已通过 JWT 认证但权限不足，统一返回 `403` 与错误体 `{ "code": "AUTH_FORBIDDEN", "message": "forbidden", "data": null, "trace_id": "..." }`
- `RBAC_ENABLE=false` 时关闭 RBAC 权限校验（仅保留 JWT 认证）

## 统一响应与错误码（Issue #2）

### JSON 响应结构
- 所有普通 JSON API 统一返回：`{ code, message, data, trace_id }`
- 成功响应固定 `code=OK`
- 错误响应返回稳定字符串错误码，并在响应头与响应体中同时返回 `X-Trace-ID` / `trace_id`

### 常用错误码
- 认证：`AUTH_MISSING_TOKEN` `AUTH_INVALID_TOKEN` `AUTH_INVALID_CREDENTIALS`
- 授权：`AUTH_FORBIDDEN`
- 参数：`VALIDATION_FAILED` `INVALID_ID`
- 资源：`USER_NOT_FOUND` `USER_ALREADY_EXISTS` `RUN_NOT_FOUND`
- Provider：`PROVIDER_NOT_FOUND` `PROVIDER_DRIVER_NOT_FOUND` `PROVIDER_INVALID_CONFIG` `MODEL_NOT_CONFIGURED` `STREAM_INTERRUPTED`
- 系统：`INTERNAL_ERROR` `RBAC_PERMISSION_LOAD_FAILED` `KAFKA_PUBLISH_FAILED`

### SSE 响应约定（`POST /api/chat`）
- 保持 `Content-Type: text/event-stream`
- `event: chunk`：`data.content` 为增量文本
- `event: done`：`data.done=true`
- `event: error`：返回统一 envelope 的错误事件

### 示例

```json
{
  "code": "OK",
  "message": "success",
  "data": {
    "run_id": "run_xxx",
    "status": "queued"
  },
  "trace_id": "8dca6d4a9f6f0c44f8a2e77d"
}
```

## 参数校验与启动期配置校验（Issue #3）

### 请求参数校验
- `POST /auth/register`：校验 `username/email/password` 的必填、格式和长度。
- `POST /auth/login`：校验 `username/password` 必填。
- `POST /api/chat`：校验 `messages` 非空、消息 `role` 合法、`content` 非空，以及可选 `provider/model` 长度。
- `POST /api/runs`：校验 `agent_code`、`workflow_version`、`trigger_type`、`thread_id`、`provider`、`model` 和 `input` JSON 合法性。
- `GET /api/runs/:run_id`、`GET /api/runs/:run_id/steps`、`POST /api/runs/:run_id/replay`：校验 `run_id` 非空、长度与字符集。
- `GET/PUT/DELETE /api/users/:id`：校验数值型 `id`，非法时统一返回 `INVALID_ID`。

### 启动期关键配置校验
服务启动时会在 `config.LoadConfig()` 阶段直接校验下面这些关键配置，缺失时拒绝启动：

- MySQL：`MYSQL_HOST` `MYSQL_PORT` `MYSQL_USER` `MYSQL_DATABASE`
- Redis：`REDIS_HOST` `REDIS_PORT`
- Server：`SERVER_PORT`
- Kafka：`KAFKA_BOOTSTRAP_SERVERS` `KAFKA_RUN_TOPIC` `KAFKA_RUN_GROUP_ID`
- JWT：`JWT_SECRET`
- Provider：当存在 provider profile 时，必须有 `MODEL_PROVIDER_DEFAULT`，且默认 provider 的 `MODEL_BASE_URL`、`MODEL_API_KEY`、`MODEL_NAME_DEFAULT` 完整可用

### 推荐最小本地配置
参考 [`F:/GoAI/.env.example`](/F:/GoAI/.env.example)。至少保证下面这些变量可用：

```env
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_DATABASE=goai_db
REDIS_HOST=localhost
REDIS_PORT=6379
SERVER_PORT=8080
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_RUN_TOPIC=run_execute
KAFKA_RUN_GROUP_ID=run-worker-group
JWT_SECRET=change_me
MODEL_PROVIDER_DEFAULT=deepseek
MODEL_BASE_URL_DEEPSEEK=https://api.deepseek.com
MODEL_API_KEY_DEEPSEEK=
MODEL_NAME_DEFAULT_DEEPSEEK=deepseek-chat
```

### 常见失败语义
- 参数不合法：返回 `400` + `VALIDATION_FAILED`
- 用户路径参数非法：返回 `400` + `INVALID_ID`
- 启动缺配置：进程直接报错退出，不延迟到运行期再暴露

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
