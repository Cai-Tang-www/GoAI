package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"GoAI/mcpclient"
	"GoAI/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const mcpHealthCheckTimeout = 30 * time.Second

// CheckHealth 通过官方 MCP initialize 与 tools/list 刷新 Tool 快照并激活 Server。
func (s *MCPRegistryService) CheckHealth(ctx context.Context, actor MCPRegistryActor, serverCode string) (*MCPServerView, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	checkCtx, cancel := context.WithTimeout(ctx, mcpHealthCheckTimeout)
	defer cancel()
	server, err := s.loadOwnedServer(ctx, s.database, actor, serverCode, false)
	if err != nil {
		return nil, err
	}
	version := server.ConfigVersion
	tools, discoverErr := s.client.Discover(checkCtx, mcpclient.ServerConfig{
		Endpoint: server.Endpoint, AuthType: server.AuthType, CredentialRef: server.CredentialRef,
	})
	if discoverErr != nil {
		if persistErr := s.persistHealthFailure(ctx, server.ID, version, discoverErr); persistErr != nil {
			return nil, persistErr
		}
		return nil, fmt.Errorf("%w: %w", errMCPServerUnhealthy, sanitizeMCPHealthCause(discoverErr))
	}
	normalized, normalizeErr := normalizeDiscoveredMCPTools(tools)
	if normalizeErr != nil {
		if persistErr := s.persistHealthFailure(ctx, server.ID, version, normalizeErr); persistErr != nil {
			return nil, persistErr
		}
		return nil, fmt.Errorf("%w: %w", errMCPServerUnhealthy, sanitizeMCPHealthCause(normalizeErr))
	}

	var activated models.MCPServer
	err = s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current models.MCPServer
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND config_version = ?", server.ID, version).First(&current).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w: MCP server configuration changed during health check", errMCPServerInvalidState)
			}
			return fmt.Errorf("locking MCP server health result: %w", err)
		}
		if err := ensureDiscoveredToolsCoverActiveReferences(ctx, tx, current, normalized); err != nil {
			return err
		}
		if err := tx.Where("server_id = ?", current.ID).Delete(&models.MCPTool{}).Error; err != nil {
			return fmt.Errorf("replacing MCP tool snapshot: %w", err)
		}
		for _, tool := range normalized {
			model := models.MCPTool{
				ServerID: current.ID, ToolName: tool.Name, Description: tool.Description,
				InputSchemaJSON: tool.InputSchemaJSON, OutputSchemaJSON: tool.OutputSchemaJSON,
			}
			if err := tx.Create(&model).Error; err != nil {
				return fmt.Errorf("persisting MCP tool snapshot: %w", err)
			}
		}
		now := s.now()
		result := tx.Model(&models.MCPServer{}).Where("id = ? AND config_version = ?", current.ID, version).
			Updates(map[string]any{"status": models.MCPServerStatusActive, "last_error": "", "last_healthy_at": now})
		if result.Error != nil {
			return fmt.Errorf("activating MCP server: %w", result.Error)
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("%w: MCP server configuration changed during health check", errMCPServerInvalidState)
		}
		if err := tx.First(&activated, current.ID).Error; err != nil {
			return fmt.Errorf("reloading MCP server after health check: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	view := mcpServerView(activated)
	return &view, nil
}

func sanitizeMCPHealthCause(err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, mcpclient.ErrCredentialNotFound):
		return mcpclient.ErrCredentialNotFound
	case errors.Is(err, mcpclient.ErrInvalidConfig):
		return mcpclient.ErrInvalidConfig
	case errors.Is(err, mcpclient.ErrTransportFailed):
		return mcpclient.ErrTransportFailed
	case errors.Is(err, mcpclient.ErrProtocolFailed):
		return mcpclient.ErrProtocolFailed
	default:
		return mcpclient.ErrProtocolFailed
	}
}

func (s *MCPRegistryService) persistHealthFailure(ctx context.Context, serverID, configVersion uint64, healthErr error) error {
	result := s.database.WithContext(ctx).Model(&models.MCPServer{}).
		Where("id = ? AND config_version = ?", serverID, configVersion).
		Updates(map[string]any{"status": models.MCPServerStatusUnhealthy, "last_error": mcpHealthErrorSummary(healthErr), "last_healthy_at": nil})
	if result.Error != nil {
		return fmt.Errorf("persisting MCP health failure: %w", result.Error)
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: MCP server configuration changed during health check", errMCPServerInvalidState)
	}
	return nil
}

func normalizeDiscoveredMCPTools(tools []mcpclient.Tool) ([]mcpclient.Tool, error) {
	if len(tools) == 0 {
		return nil, fmt.Errorf("%w: server returned no tools", mcpclient.ErrProtocolFailed)
	}
	normalized := make([]mcpclient.Tool, 0, len(tools))
	seen := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		tool.Name = strings.TrimSpace(tool.Name)
		tool.Description = strings.TrimSpace(tool.Description)
		if tool.Name == "" || len(tool.Name) > 128 {
			return nil, fmt.Errorf("%w: invalid tool name", mcpclient.ErrProtocolFailed)
		}
		if _, exists := seen[tool.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate tool name", mcpclient.ErrProtocolFailed)
		}
		seen[tool.Name] = struct{}{}
		if !validJSONObject(tool.InputSchemaJSON) {
			return nil, fmt.Errorf("%w: invalid input schema", mcpclient.ErrProtocolFailed)
		}
		if strings.TrimSpace(tool.OutputSchemaJSON) != "" && !validJSONObject(tool.OutputSchemaJSON) {
			return nil, fmt.Errorf("%w: invalid output schema", mcpclient.ErrProtocolFailed)
		}
		normalized = append(normalized, tool)
	}
	return normalized, nil
}

func validJSONObject(raw string) bool {
	var object map[string]any
	return json.Unmarshal([]byte(raw), &object) == nil && object != nil
}

func ensureDiscoveredToolsCoverActiveReferences(ctx context.Context, database *gorm.DB, server models.MCPServer, tools []mcpclient.Tool) error {
	refs, err := activeMCPToolReferences(ctx, database, server.OwnerUserID, server.ServerCode)
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		return nil
	}
	discovered := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		discovered[tool.Name] = struct{}{}
	}
	for _, ref := range refs {
		if _, exists := discovered[ref.ToolName]; !exists {
			return fmt.Errorf("%w: active workflow tool %s is missing from discovery", errMCPServerInvalidState, ref.ToolName)
		}
	}
	return nil
}

func mcpHealthErrorSummary(err error) string {
	switch {
	case errors.Is(err, mcpclient.ErrCredentialNotFound):
		return "credential unavailable"
	case errors.Is(err, mcpclient.ErrInvalidConfig):
		return "invalid MCP configuration"
	case errors.Is(err, mcpclient.ErrTransportFailed):
		return "MCP transport failed"
	case errors.Is(err, mcpclient.ErrToolReportedError):
		return "MCP tool reported an error"
	default:
		return "MCP protocol health check failed"
	}
}
