package db

import (
	"context"
	"strings"
	"testing"

	"GoAI/a2aauth"
	"GoAI/models"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestValidateA2AEndpointAuthentication(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:a2a_auth?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := database.AutoMigrate(&models.AgentEndpoint{}); err != nil {
		t.Fatalf("migrate endpoint failed: %v", err)
	}
	endpoint := models.AgentEndpoint{AgentID: 1, EndpointCode: "local", Protocol: models.AgentEndpointProtocolA2A,
		Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1/a2a/agents/planner",
		AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "planner-key", Status: models.AgentEndpointStatusActive}
	if err := database.Create(&endpoint).Error; err != nil {
		t.Fatalf("create endpoint failed: %v", err)
	}
	resolver, err := a2aauth.NewStaticCredentialResolver(map[string]string{"planner-key": "test-only-a2a-secret-at-least-32-bytes-long"})
	if err != nil {
		t.Fatalf("create resolver failed: %v", err)
	}
	if err := ValidateA2AEndpointAuthentication(context.Background(), database, resolver, true); err != nil {
		t.Fatalf("valid endpoint rejected: %v", err)
	}
	if err := database.Model(&endpoint).Update("credential_ref", "missing-key").Error; err != nil {
		t.Fatalf("update endpoint failed: %v", err)
	}
	if err := ValidateA2AEndpointAuthentication(context.Background(), database, resolver, true); err == nil || strings.Contains(err.Error(), "test-only") {
		t.Fatalf("expected safe unresolved credential error, got %v", err)
	}
	if err := database.Model(&endpoint).Update("credential_ref", "planner-key").Error; err != nil {
		t.Fatalf("restore endpoint credential failed: %v", err)
	}
	second := models.AgentEndpoint{AgentID: 1, EndpointCode: "remote", Protocol: models.AgentEndpointProtocolA2A,
		Transport: models.AgentEndpointTransportHTTPS, Address: "https://agents.example.com/a2a/agents/planner",
		AuthType: models.AgentEndpointAuthTypeHMACSHA256, CredentialRef: "planner-key-next", Status: models.AgentEndpointStatusActive}
	if err := database.Create(&second).Error; err != nil {
		t.Fatalf("create second endpoint failed: %v", err)
	}
	rotatingResolver, err := a2aauth.NewStaticCredentialResolver(map[string]string{
		"planner-key":      "test-only-a2a-secret-at-least-32-bytes-long",
		"planner-key-next": "test-only-next-a2a-secret-at-least-32-bytes",
	})
	if err != nil {
		t.Fatalf("create rotating resolver failed: %v", err)
	}
	if err := ValidateA2AEndpointAuthentication(context.Background(), database, rotatingResolver, true); err == nil || !strings.Contains(err.Error(), "inconsistent credential references") {
		t.Fatalf("expected inconsistent source identity rejection, got %v", err)
	}
}
