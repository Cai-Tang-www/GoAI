# GoAI Console

GoAI 的中文管理控制台和 AG-UI 对话工作台。前端使用 React、TypeScript、Vite、Ant Design、TanStack Query 和 React Flow。

## 本地启动

先启动 GoAI 后端：

```bash
cd /Users/apple/GoAI
docker-compose up -d
go run .
```

再启动前端：

```bash
cd /Users/apple/GoAI/web
npm install
npm run dev
```

打开 `http://127.0.0.1:5173`。Vite 会把 `/api`、`/auth`、`/a2a`、`/ping` 代理到 `http://localhost:8080`，因为 GoAI 后端不启用 CORS。

若后端地址不同，复制 `.env.example` 为 `.env.local` 并修改：

```dotenv
GOAI_API_TARGET=http://localhost:8080
```

## 功能

- JWT 登录、注册和个人资料
- AG-UI POST SSE 对话、Step 状态、interrupt/resume、停止观察
- 浏览器本地 Thread 和 Run 历史
- Run 详情、Step 时间线、Trace、Delegation 和 Replay
- Agent、Capability、Endpoint、Workflow 及发布门禁
- Workflow DAG 只读视图和 JSON 编辑
- MCP Server、健康检查和 Tool discovery 快照
- 用户管理和 RBAC 错误提示

Run 和 Thread 列表只保存在浏览器，因为后端当前没有对应的列表接口。A2A 业务路由使用机器身份认证，前端不会直接调用。

## 契约与检查

生成 OpenAPI TypeScript 声明并执行检查：

```bash
npm run generate:api
npm run typecheck
npm run lint
npm run test
npm run build
```

协议依据为 `../docs/openapi.yaml` 和 `../docs/FRONTEND_SPEC.md`。
