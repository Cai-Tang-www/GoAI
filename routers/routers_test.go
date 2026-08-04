package routers

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"GoAI/config"
	"GoAI/services"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNewRejectsMissingDependencies(t *testing.T) {
	gdb, err := gorm.Open(sqlite.Open("file:router_dependencies?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	runService, err := services.NewRunService(gdb, services.RunEventPublisherFunc(func(context.Context, string) error { return nil }))
	if err != nil {
		t.Fatalf("create run service failed: %v", err)
	}
	runtimeService, err := services.NewRuntimeService(gdb, runService)
	if err != nil {
		t.Fatalf("create runtime service failed: %v", err)
	}
	chatService, err := services.NewChatService(&config.Config{}, &http.Client{})
	if err != nil {
		t.Fatalf("create chat service failed: %v", err)
	}
	agentRegistry, err := services.NewAgentRegistryService(gdb, services.AgentCardHealthCheckerFunc(func(context.Context, services.AgentCardHealthCheckRequest) error { return nil }), nil, false)
	if err != nil {
		t.Fatalf("create agent registry service failed: %v", err)
	}
	valid := Dependencies{
		Database: gdb, RunService: runService, ChatService: chatService,
		AgentRegistry: agentRegistry, Runtime: runtimeService, A2AGateway: http.NotFoundHandler(),
	}
	tests := []struct {
		name   string
		want   string
		mutate func(*Dependencies)
	}{
		{name: "database", want: "database is nil", mutate: func(deps *Dependencies) { deps.Database = nil }},
		{name: "run service", want: "run service is nil", mutate: func(deps *Dependencies) { deps.RunService = nil }},
		{name: "chat service", want: "chat service is nil", mutate: func(deps *Dependencies) { deps.ChatService = nil }},
		{name: "agent registry", want: "agent registry service is nil", mutate: func(deps *Dependencies) { deps.AgentRegistry = nil }},
		{name: "runtime", want: "runtime is nil", mutate: func(deps *Dependencies) { deps.Runtime = nil }},
		{name: "A2A gateway", want: "A2A gateway is nil", mutate: func(deps *Dependencies) { deps.A2AGateway = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			deps := valid
			test.mutate(&deps)
			_, err := New(deps)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("New() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestNewBuildsRouterFromExplicitDependencies(t *testing.T) {
	gdb, err := gorm.Open(sqlite.Open("file:router_valid?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	service, err := services.NewRunService(gdb, services.RunEventPublisherFunc(func(context.Context, string) error { return nil }))
	if err != nil {
		t.Fatalf("create run service failed: %v", err)
	}
	runtimeService, err := services.NewRuntimeService(gdb, service)
	if err != nil {
		t.Fatalf("create runtime service failed: %v", err)
	}
	chatService, err := services.NewChatService(&config.Config{}, &http.Client{})
	if err != nil {
		t.Fatalf("create chat service failed: %v", err)
	}
	agentRegistry, err := services.NewAgentRegistryService(gdb, services.AgentCardHealthCheckerFunc(func(context.Context, services.AgentCardHealthCheckRequest) error { return nil }), nil, false)
	if err != nil {
		t.Fatalf("create agent registry service failed: %v", err)
	}
	router, err := New(Dependencies{
		Database:      gdb,
		RunService:    service,
		ChatService:   chatService,
		AgentRegistry: agentRegistry,
		Runtime:       runtimeService,
		A2AGateway:    http.NotFoundHandler(),
	})
	if err != nil {
		t.Fatalf("create router failed: %v", err)
	}
	if router == nil {
		t.Fatal("expected router instance")
	}
}
