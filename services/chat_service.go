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

// ChatService owns the configured LLM provider registry for one application instance.
// It keeps provider construction explicit so tests and runtimes do not share mutable
// package-level HTTP client state.
type ChatService struct {
	registry *ai.Registry
}

// NewChatService builds a provider registry from the supplied application config and
// HTTP client. The client is shared with other governed outbound dependencies.
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

// Chat sends a streaming chat request through the service-owned provider registry.
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

// Chat is a compatibility helper for legacy callers. New code should inject a
// ChatService so provider dependencies remain explicit.
func Chat(ctx context.Context, messages []ai.Message, providerName, model string) (*ai.ChatStream, error) {
	service, err := NewChatService(config.AppConfig, nil)
	if err != nil {
		return nil, err
	}
	return service.Chat(ctx, messages, providerName, model)
}

// ResetProviderRegistryForTest is retained for source compatibility. Provider
// registries are now instance-owned, so there is no package state to reset.
func ResetProviderRegistryForTest() {}
