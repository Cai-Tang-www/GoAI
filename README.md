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
