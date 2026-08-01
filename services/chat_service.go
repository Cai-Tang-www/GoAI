package services

import (
	"GoAI/ai"
	"GoAI/config"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// ChatService 持有单个应用实例的 LLM Provider 注册表。
// 它显式管理 Provider 构造，避免测试和运行时共享可变的包级 HTTP 客户端状态。
type ChatService struct {
	registry *ai.Registry
}

// NewChatService 根据应用配置和 HTTP 客户端创建 Provider 注册表。
// 传入的客户端可与其他受治理的下游依赖共享。
func NewChatService(appConfig *config.Config, httpClient *http.Client) (*ChatService, error) {
	if appConfig == nil {
		return nil, errors.New("creating chat service: app config is nil")
	}

	registry := ai.NewRegistry()
	if err := registry.RegisterDriver(ai.DriverOpenAICompatible, ai.NewOpenAICompatProvider); err != nil {
		return nil, err
	}
	for name, providerConfig := range appConfig.ModelProviders {
		profile := ai.ProviderProfile{
			Name:         strings.ToLower(strings.TrimSpace(name)),
			Driver:       providerConfig.Driver,
			BaseURL:      providerConfig.BaseURL,
			APIKey:       providerConfig.APIKey,
			DefaultModel: providerConfig.DefaultModel,
			EndpointPath: providerConfig.EndpointPath,
			HTTPClient:   httpClient,
		}
		if err := registry.RegisterProfile(profile); err != nil {
			return nil, fmt.Errorf("register provider %s: %w", name, err)
		}
	}
	registry.SetDefaultProvider(appConfig.ModelProviderDefault)
	return &ChatService{registry: registry}, nil
}

// Chat 通过当前服务持有的 Provider 注册表发起流式聊天请求。
func (s *ChatService) Chat(ctx context.Context, messages []ai.Message, providerName, model string) (*ai.ChatStream, error) {
	if s == nil || s.registry == nil {
		return nil, errors.New("chat service is not initialized")
	}
	provider, err := s.registry.ResolveProvider(providerName)
	if err != nil {
		return nil, err
	}
	return provider.Chat(ctx, ai.ChatRequest{
		Messages: messages,
		Model:    model,
		Stream:   true,
	})
}
