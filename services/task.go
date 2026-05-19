package services

import (
	"GoAI/ai"
	"GoAI/config"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	providerRegistry     *ai.Registry
	providerRegistryErr  error
	providerRegistryOnce sync.Once
)

func initProviderRegistry() (*ai.Registry, error) {
	providerRegistryOnce.Do(func() {
		if config.AppConfig == nil {
			providerRegistryErr = errors.New("app config is not initialized")
			return
		}

		reg := ai.NewRegistry()
		if err := reg.RegisterDriver(ai.DriverOpenAICompatible, ai.NewOpenAICompatProvider); err != nil {
			providerRegistryErr = err
			return
		}

		for name, pc := range config.AppConfig.ModelProviders {
			profile := ai.ProviderProfile{
				Name:         strings.ToLower(strings.TrimSpace(name)),
				Driver:       pc.Driver,
				BaseURL:      pc.BaseURL,
				APIKey:       pc.APIKey,
				DefaultModel: pc.DefaultModel,
				EndpointPath: pc.EndpointPath,
			}
			if err := reg.RegisterProfile(profile); err != nil {
				providerRegistryErr = fmt.Errorf("register provider %s: %w", name, err)
				return
			}
		}

		reg.SetDefaultProvider(config.AppConfig.ModelProviderDefault)
		providerRegistry = reg
	})
	return providerRegistry, providerRegistryErr
}

// Chat 调用统一 provider 接口
func Chat(ctx context.Context, messages []ai.Message, providerName, model string) (*ai.ChatStream, error) {
	reg, err := initProviderRegistry()
	if err != nil {
		return nil, err
	}

	provider, err := reg.ResolveProvider(providerName)
	if err != nil {
		return nil, err
	}

	req := ai.ChatRequest{
		Messages: messages,
		Model:    model,
		Stream:   true,
	}
	return provider.Chat(ctx, req)
}
