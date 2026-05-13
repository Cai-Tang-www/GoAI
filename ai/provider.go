package ai

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Message 定义对话消息结构
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatRequest struct {
	Messages []Message `json:"messages"`
	Model    string    `json:"model"`
	Stream   bool      `json:"stream"`
}

type ChatStream struct {
	Chunks <-chan string
	Errs   <-chan error
}

type ProviderProfile struct {
	Name         string
	Driver       string
	BaseURL      string
	APIKey       string
	DefaultModel string
	EndpointPath string
}

type AIProvider interface {
	Chat(ctx context.Context, req ChatRequest) (*ChatStream, error)
	Name() string
}

type DriverFactory func(profile ProviderProfile) (AIProvider, error)

const DriverOpenAICompatible = "openai_compatible"

var (
	ErrDriverNotFound       = errors.New("provider driver not found")
	ErrProviderNotFound     = errors.New("provider not found")
	ErrInvalidProviderInput = errors.New("invalid provider profile")
	ErrModelNotConfigured   = errors.New("model is not configured")
	ErrStreamInterrupted    = errors.New("stream interrupted before [DONE]")
)

type Registry struct {
	mu              sync.RWMutex
	drivers         map[string]DriverFactory
	profiles        map[string]ProviderProfile
	defaultProvider string
}

func NewRegistry() *Registry {
	return &Registry{
		drivers:  make(map[string]DriverFactory),
		profiles: make(map[string]ProviderProfile),
	}
}

func (r *Registry) RegisterDriver(name string, factory DriverFactory) error {
	if factory == nil {
		return fmt.Errorf("%w: driver factory is nil", ErrInvalidProviderInput)
	}
	key := normalizeKey(name)
	if key == "" {
		return fmt.Errorf("%w: driver name is empty", ErrInvalidProviderInput)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.drivers[key] = factory
	return nil
}

func (r *Registry) RegisterProfile(profile ProviderProfile) error {
	profile.Name = normalizeKey(profile.Name)
	profile.Driver = normalizeKey(profile.Driver)
	profile.BaseURL = strings.TrimSpace(profile.BaseURL)
	profile.APIKey = strings.TrimSpace(profile.APIKey)
	profile.DefaultModel = strings.TrimSpace(profile.DefaultModel)
	profile.EndpointPath = normalizeEndpointPath(profile.EndpointPath)

	if profile.Name == "" || profile.Driver == "" || profile.BaseURL == "" || profile.APIKey == "" {
		return fmt.Errorf("%w: profile(name/driver/base_url/api_key) is required", ErrInvalidProviderInput)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.profiles[profile.Name] = profile
	return nil
}

func (r *Registry) SetDefaultProvider(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.defaultProvider = normalizeKey(name)
}

func (r *Registry) ResolveProvider(name string) (AIProvider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	target := normalizeKey(name)
	if target == "" {
		target = r.defaultProvider
	}
	if target == "" && len(r.profiles) == 1 {
		for only := range r.profiles {
			target = only
		}
	}
	if target == "" {
		return nil, fmt.Errorf("%w: empty provider and no default configured", ErrProviderNotFound)
	}

	profile, ok := r.profiles[target]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrProviderNotFound, target)
	}
	factory, ok := r.drivers[profile.Driver]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrDriverNotFound, profile.Driver)
	}
	return factory(profile)
}

func normalizeKey(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func normalizeEndpointPath(path string) string {
	p := strings.TrimSpace(path)
	if p == "" {
		return "/chat/completions"
	}
	if strings.HasPrefix(p, "/") {
		return p
	}
	return "/" + p
}
