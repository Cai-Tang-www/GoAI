package ai

import (
	"context"
)

// OpenAIProvider 实现了 AIProvider 接口，用于与 OpenAI 模型交互
type OpenAIProvider struct {
	APIKey string
	Model  string
}

// GetModelName 返回模型名称
func (p *OpenAIProvider) GetModelName() string {
	return p.Model
}

// Chat 实现了 AIProvider 接口的 Chat 方法，用于与 OpenAI 模型交互
func (p *OpenAIProvider) Chat(ctx context.Context, messages []Message) (<-chan string, error) {
	out := make(chan string)

	go func() {
		defer close(out)
		// 这里模拟调用 SDK 的流式接口
		// 实际开发时，你会在这里调用 openai.CreateChatCompletionStream
		dummyResp := []string{"你好", "！", "我是", "Gopher", "AI", "助手", "。"}
		for _, word := range dummyResp {
			select {
			case <-ctx.Done():
				return
			case out <- word:
				// 模拟网络延迟
			}
		}
	}()

	return out, nil
}
