package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"GoAI/a2aauth"
	"GoAI/models"

	"gorm.io/gorm"
)

// ValidateA2AEndpointAuthentication 校验活跃 A2A Endpoint 的认证类型和凭据引用，不暴露真实密钥。
func ValidateA2AEndpointAuthentication(ctx context.Context, database *gorm.DB, resolver a2aauth.CredentialResolver, required bool) error {
	if database == nil {
		return errors.New("validating A2A endpoint authentication: database is nil")
	}
	var endpoints []models.AgentEndpoint
	if err := database.WithContext(ctx).Where("protocol = ? AND status = ?", models.AgentEndpointProtocolA2A, models.AgentEndpointStatusActive).Find(&endpoints).Error; err != nil {
		return fmt.Errorf("loading active A2A endpoints: %w", err)
	}
	credentialByAgent := make(map[uint64]string)
	for _, endpoint := range endpoints {
		authType := strings.TrimSpace(endpoint.AuthType)
		credentialRef := strings.TrimSpace(endpoint.CredentialRef)
		if authType == "" {
			authType = models.AgentEndpointAuthTypeNone
		}
		if required && authType != models.AgentEndpointAuthTypeHMACSHA256 {
			return fmt.Errorf("active A2A endpoint %q must use %s authentication", endpoint.EndpointCode, models.AgentEndpointAuthTypeHMACSHA256)
		}
		switch authType {
		case models.AgentEndpointAuthTypeNone:
			if required {
				return fmt.Errorf("active A2A endpoint %q cannot disable authentication", endpoint.EndpointCode)
			}
		case models.AgentEndpointAuthTypeHMACSHA256:
			if credentialRef == "" || resolver == nil {
				return fmt.Errorf("active A2A endpoint %q has no resolvable credential reference", endpoint.EndpointCode)
			}
			if _, err := resolver.Resolve(ctx, credentialRef); err != nil {
				return fmt.Errorf("active A2A endpoint %q has no resolvable credential reference", endpoint.EndpointCode)
			}
			if existing, ok := credentialByAgent[endpoint.AgentID]; ok && existing != credentialRef {
				return fmt.Errorf("active A2A endpoints for agent %d use inconsistent credential references", endpoint.AgentID)
			}
			credentialByAgent[endpoint.AgentID] = credentialRef
		default:
			return fmt.Errorf("active A2A endpoint %q uses unsupported authentication type %q", endpoint.EndpointCode, authType)
		}
	}
	return nil
}
