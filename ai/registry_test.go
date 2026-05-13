package ai

import (
	"context"
	"errors"
	"testing"
)

type stubProvider struct {
	name string
}

func (s *stubProvider) Name() string { return s.name }
func (s *stubProvider) Chat(context.Context, ChatRequest) (*ChatStream, error) {
	ch := make(chan string)
	er := make(chan error)
	close(ch)
	close(er)
	return &ChatStream{Chunks: ch, Errs: er}, nil
}

func TestRegistryResolveWithDefault(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterDriver(DriverOpenAICompatible, func(profile ProviderProfile) (AIProvider, error) {
		return &stubProvider{name: profile.Name}, nil
	}); err != nil {
		t.Fatalf("register driver: %v", err)
	}
	if err := reg.RegisterProfile(ProviderProfile{
		Name:         "deepseek",
		Driver:       DriverOpenAICompatible,
		BaseURL:      "https://example.com",
		APIKey:       "k",
		DefaultModel: "m",
	}); err != nil {
		t.Fatalf("register profile: %v", err)
	}
	reg.SetDefaultProvider("deepseek")

	p, err := reg.ResolveProvider("")
	if err != nil {
		t.Fatalf("resolve provider: %v", err)
	}
	if p.Name() != "deepseek" {
		t.Fatalf("unexpected provider name: %s", p.Name())
	}
}

func TestRegistryUnknownProvider(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.ResolveProvider("missing")
	if !errors.Is(err, ErrProviderNotFound) {
		t.Fatalf("expected ErrProviderNotFound, got %v", err)
	}
}

func TestRegisterProfileRequiresFields(t *testing.T) {
	reg := NewRegistry()
	err := reg.RegisterProfile(ProviderProfile{
		Name:    "mimo",
		Driver:  DriverOpenAICompatible,
		BaseURL: "https://example.com",
	})
	if !errors.Is(err, ErrInvalidProviderInput) {
		t.Fatalf("expected ErrInvalidProviderInput, got %v", err)
	}
}
