package ai

import "context"

// Message 定义对话消息结构
type Message struct {
	Role    string
	Content string
}

// AIProvider 是所有 AI 模型的抽象接口
type AIProvider interface {
	// Chat 返回一个只读 channel 用于流式传输字符串
	Chat(ctx context.Context, messages []Message) (<-chan string, error)
	// GetModelName 返回模型名称
	GetModelName() string
}
