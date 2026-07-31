package routers

import (
	"GoAI/services"
	"context"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestNewRejectsMissingDependencies(t *testing.T) {
	if _, err := New(Dependencies{}); err == nil {
		t.Fatal("expected nil database error")
	}
	gdb, err := gorm.Open(sqlite.Open("file:router_dependencies?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if _, err := New(Dependencies{Database: gdb}); err == nil {
		t.Fatal("expected nil run service error")
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
	router, err := New(Dependencies{Database: gdb, RunService: service})
	if err != nil {
		t.Fatalf("create router failed: %v", err)
	}
	if router == nil {
		t.Fatal("expected router instance")
	}
}
