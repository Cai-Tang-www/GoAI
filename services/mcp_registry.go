package services

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"GoAI/mcpclient"
	"GoAI/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	errMCPServerNotFound      = errors.New("MCP server not found")
	errMCPServerAlreadyExists = errors.New("MCP server already exists")
	errMCPServerInvalidState  = errors.New("MCP server state is invalid")
	errMCPServerUnhealthy     = errors.New("MCP server health check failed")
	errMCPToolNotFound        = errors.New("MCP tool not found")
	errMCPRegistryValidation  = errors.New("MCP registry validation failed")
	errMCPForbidden           = errors.New("MCP server access forbidden")
)

// ErrMCPServerNotFound 返回 MCP Server 不存在的统一 sentinel error。
func ErrMCPServerNotFound() error { return errMCPServerNotFound }

// ErrMCPServerAlreadyExists 返回 owner 下 server_code 重复的统一 sentinel error。
func ErrMCPServerAlreadyExists() error { return errMCPServerAlreadyExists }

// ErrMCPServerInvalidState 返回 MCP Server 状态不允许当前操作的统一 sentinel error。
func ErrMCPServerInvalidState() error { return errMCPServerInvalidState }

// ErrMCPServerUnhealthy 返回 MCP 协议健康检查失败的统一 sentinel error。
func ErrMCPServerUnhealthy() error { return errMCPServerUnhealthy }

// ErrMCPToolNotFound 返回发现快照中不存在目标 Tool 的统一 sentinel error。
func ErrMCPToolNotFound() error { return errMCPToolNotFound }

// ErrMCPRegistryValidation 返回 MCP 管理输入不合法的统一 sentinel error。
func ErrMCPRegistryValidation() error { return errMCPRegistryValidation }

// ErrMCPForbidden 返回当前主体无权管理目标 MCP Server 的统一 sentinel error。
func ErrMCPForbidden() error { return errMCPForbidden }

// MCPRegistryActor 表示调用 MCP Registry 的认证主体及其 ownership bypass 权限。
type MCPRegistryActor struct {
	UserID    uint64
	CanManage bool
}

// UpsertMCPServerCommand 描述创建或替换 MCP Server 配置的管理面命令。
type UpsertMCPServerCommand struct {
	ServerCode    string `json:"server_code"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	Transport     string `json:"transport"`
	Endpoint      string `json:"endpoint"`
	AuthType      string `json:"auth_type"`
	CredentialRef string `json:"credential_ref"`
}

// MCPServerView 是不包含真实凭据的 MCP Server 管理面视图。
type MCPServerView struct {
	ServerCode    string     `json:"server_code"`
	Name          string     `json:"name"`
	Description   string     `json:"description"`
	OwnerUserID   uint64     `json:"owner_user_id"`
	Transport     string     `json:"transport"`
	Endpoint      string     `json:"endpoint"`
	AuthType      string     `json:"auth_type"`
	CredentialRef string     `json:"credential_ref,omitempty"`
	Status        string     `json:"status"`
	ConfigVersion uint64     `json:"config_version"`
	LastError     string     `json:"last_error,omitempty"`
	LastHealthyAt *time.Time `json:"last_healthy_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// MCPToolView 是最近一次成功 tools/list 的稳定发现快照。
type MCPToolView struct {
	ToolName         string    `json:"tool_name"`
	Description      string    `json:"description"`
	InputSchemaJSON  string    `json:"input_schema_json"`
	OutputSchemaJSON string    `json:"output_schema_json,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// MCPProtocolClient 定义 Registry 健康检查依赖的官方 MCP 协议边界。
type MCPProtocolClient interface {
	Discover(context.Context, mcpclient.ServerConfig) ([]mcpclient.Tool, error)
}

// MCPRegistryService 实现 MCP Server 管理、工具发现、ownership 与发布不变量约束。
type MCPRegistryService struct {
	database *gorm.DB
	client   MCPProtocolClient
	now      func() time.Time
}

// NewMCPRegistryService 创建显式依赖的 MCP Registry 管理服务。
func NewMCPRegistryService(database *gorm.DB, client MCPProtocolClient) (*MCPRegistryService, error) {
	if database == nil {
		return nil, errors.New("creating MCP registry service: database is nil")
	}
	if client == nil {
		return nil, errors.New("creating MCP registry service: protocol client is nil")
	}
	return &MCPRegistryService{database: database, client: client, now: time.Now}, nil
}

// Create 创建归属于当前主体的 inactive MCP Server。
func (s *MCPRegistryService) Create(ctx context.Context, actor MCPRegistryActor, command UpsertMCPServerCommand) (*MCPServerView, error) {
	if err := validateMCPActor(actor); err != nil {
		return nil, err
	}
	command = normalizeMCPServerCommand(command)
	if err := validateMCPServerCommand(command); err != nil {
		return nil, err
	}
	server := models.MCPServer{
		OwnerUserID: actor.UserID, ServerCode: command.ServerCode, Name: command.Name,
		Description: command.Description, Transport: command.Transport, Endpoint: command.Endpoint,
		AuthType: command.AuthType, CredentialRef: command.CredentialRef,
		Status: models.MCPServerStatusInactive, ConfigVersion: 1,
	}
	if err := s.database.WithContext(ctx).Create(&server).Error; err != nil {
		var count int64
		if lookupErr := s.database.WithContext(ctx).Model(&models.MCPServer{}).
			Where("owner_user_id = ? AND server_code = ?", actor.UserID, command.ServerCode).Count(&count).Error; lookupErr == nil && count > 0 {
			return nil, errMCPServerAlreadyExists
		}
		return nil, fmt.Errorf("creating MCP server: %w", err)
	}
	view := mcpServerView(server)
	return &view, nil
}

// List 按 ownership 返回当前主体可见的 MCP Server。
func (s *MCPRegistryService) List(ctx context.Context, actor MCPRegistryActor) ([]MCPServerView, error) {
	if err := validateMCPActor(actor); err != nil {
		return nil, err
	}
	query := s.database.WithContext(ctx).Order("owner_user_id ASC, server_code ASC")
	if !actor.CanManage {
		query = query.Where("owner_user_id = ?", actor.UserID)
	}
	var servers []models.MCPServer
	if err := query.Find(&servers).Error; err != nil {
		return nil, fmt.Errorf("listing MCP servers: %w", err)
	}
	views := make([]MCPServerView, 0, len(servers))
	for _, server := range servers {
		views = append(views, mcpServerView(server))
	}
	return views, nil
}

// Get 返回当前主体可管理的 MCP Server。
func (s *MCPRegistryService) Get(ctx context.Context, actor MCPRegistryActor, serverCode string) (*MCPServerView, error) {
	server, err := s.loadOwnedServer(ctx, s.database, actor, serverCode, false)
	if err != nil {
		return nil, err
	}
	view := mcpServerView(*server)
	return &view, nil
}

// Update 替换 MCP Server 配置；协议配置变化后清空发现快照并要求重新健康检查。
func (s *MCPRegistryService) Update(ctx context.Context, actor MCPRegistryActor, serverCode string, command UpsertMCPServerCommand) (*MCPServerView, error) {
	command.ServerCode = strings.TrimSpace(serverCode)
	command = normalizeMCPServerCommand(command)
	if err := validateMCPServerCommand(command); err != nil {
		return nil, err
	}
	var updated models.MCPServer
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		server, err := s.loadOwnedServer(ctx, tx, actor, command.ServerCode, true)
		if err != nil {
			return err
		}
		configurationChanged := server.Transport != command.Transport || server.Endpoint != command.Endpoint ||
			server.AuthType != command.AuthType || server.CredentialRef != command.CredentialRef
		if configurationChanged {
			if err := s.ensureServerNotReferencedByActiveWorkflow(ctx, tx, server.OwnerUserID, server.ServerCode); err != nil {
				return err
			}
		}
		updates := map[string]any{"name": command.Name, "description": command.Description}
		if configurationChanged {
			updates["transport"] = command.Transport
			updates["endpoint"] = command.Endpoint
			updates["auth_type"] = command.AuthType
			updates["credential_ref"] = command.CredentialRef
			updates["status"] = models.MCPServerStatusInactive
			updates["config_version"] = server.ConfigVersion + 1
			updates["last_error"] = ""
			updates["last_healthy_at"] = nil
			if err := tx.Where("server_id = ?", server.ID).Delete(&models.MCPTool{}).Error; err != nil {
				return fmt.Errorf("clearing MCP tool snapshot: %w", err)
			}
		}
		if err := tx.Model(server).Updates(updates).Error; err != nil {
			return fmt.Errorf("updating MCP server: %w", err)
		}
		if err := tx.First(&updated, server.ID).Error; err != nil {
			return fmt.Errorf("reloading MCP server: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	view := mcpServerView(updated)
	return &view, nil
}

// Deactivate 停用未被 active Workflow 引用的 MCP Server，并使并发健康检查结果失效。
func (s *MCPRegistryService) Deactivate(ctx context.Context, actor MCPRegistryActor, serverCode string) (*MCPServerView, error) {
	var updated models.MCPServer
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		server, err := s.loadOwnedServer(ctx, tx, actor, serverCode, true)
		if err != nil {
			return err
		}
		if err := s.ensureServerNotReferencedByActiveWorkflow(ctx, tx, server.OwnerUserID, server.ServerCode); err != nil {
			return err
		}
		updates := map[string]any{
			"status": models.MCPServerStatusInactive, "config_version": server.ConfigVersion + 1,
			"last_error": "", "last_healthy_at": nil,
		}
		if err := tx.Model(server).Updates(updates).Error; err != nil {
			return fmt.Errorf("deactivating MCP server: %w", err)
		}
		if err := tx.First(&updated, server.ID).Error; err != nil {
			return fmt.Errorf("reloading MCP server: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	view := mcpServerView(updated)
	return &view, nil
}

// ListTools 返回当前主体可见 Server 的最近一次成功发现快照。
func (s *MCPRegistryService) ListTools(ctx context.Context, actor MCPRegistryActor, serverCode string) ([]MCPToolView, error) {
	server, err := s.loadOwnedServer(ctx, s.database, actor, serverCode, false)
	if err != nil {
		return nil, err
	}
	var tools []models.MCPTool
	if err := s.database.WithContext(ctx).Where("server_id = ?", server.ID).Order("tool_name ASC").Find(&tools).Error; err != nil {
		return nil, fmt.Errorf("listing MCP tools: %w", err)
	}
	views := make([]MCPToolView, 0, len(tools))
	for _, tool := range tools {
		views = append(views, mcpToolView(tool))
	}
	return views, nil
}

func (s *MCPRegistryService) loadOwnedServer(ctx context.Context, database *gorm.DB, actor MCPRegistryActor, serverCode string, lock bool) (*models.MCPServer, error) {
	if err := validateMCPActor(actor); err != nil {
		return nil, err
	}
	serverCode = strings.TrimSpace(serverCode)
	if !registryCodePattern.MatchString(serverCode) {
		return nil, fmt.Errorf("%w: invalid server_code", errMCPRegistryValidation)
	}
	query := database.WithContext(ctx).Where("server_code = ?", serverCode)
	if !actor.CanManage {
		query = query.Where("owner_user_id = ?", actor.UserID)
	}
	if lock {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var server models.MCPServer
	if err := query.First(&server).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errMCPServerNotFound
		}
		return nil, fmt.Errorf("loading MCP server: %w", err)
	}
	return &server, nil
}

func validateMCPActor(actor MCPRegistryActor) error {
	if actor.UserID == 0 {
		return errMCPForbidden
	}
	return nil
}

func normalizeMCPServerCommand(command UpsertMCPServerCommand) UpsertMCPServerCommand {
	command.ServerCode = strings.TrimSpace(command.ServerCode)
	command.Name = strings.TrimSpace(command.Name)
	command.Description = strings.TrimSpace(command.Description)
	command.Transport = strings.ToLower(strings.TrimSpace(command.Transport))
	command.Endpoint = strings.TrimSpace(command.Endpoint)
	command.AuthType = strings.ToLower(strings.TrimSpace(command.AuthType))
	command.CredentialRef = strings.TrimSpace(command.CredentialRef)
	if command.Transport == "" {
		command.Transport = models.MCPServerTransportStreamableHTTP
	}
	if command.AuthType == "" {
		command.AuthType = models.MCPServerAuthTypeNone
	}
	return command
}

func validateMCPServerCommand(command UpsertMCPServerCommand) error {
	if !registryCodePattern.MatchString(command.ServerCode) {
		return fmt.Errorf("%w: invalid server_code", errMCPRegistryValidation)
	}
	if command.Name == "" || len(command.Name) > 128 || len(command.Description) > 4000 {
		return fmt.Errorf("%w: invalid MCP server metadata", errMCPRegistryValidation)
	}
	if command.Transport != models.MCPServerTransportStreamableHTTP {
		return fmt.Errorf("%w: transport must be streamable_http", errMCPRegistryValidation)
	}
	if len(command.Endpoint) > 512 {
		return fmt.Errorf("%w: invalid endpoint", errMCPRegistryValidation)
	}
	parsed, err := url.Parse(command.Endpoint)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return fmt.Errorf("%w: invalid endpoint", errMCPRegistryValidation)
	}
	switch parsed.Scheme {
	case "http":
		if !isRegistryLoopbackHost(parsed.Hostname()) {
			return fmt.Errorf("%w: HTTP endpoint must use a loopback host", errMCPRegistryValidation)
		}
	case "https":
	default:
		return fmt.Errorf("%w: remote endpoint must use HTTPS", errMCPRegistryValidation)
	}
	switch command.AuthType {
	case models.MCPServerAuthTypeNone:
		if command.CredentialRef != "" {
			return fmt.Errorf("%w: anonymous server cannot set credential_ref", errMCPRegistryValidation)
		}
	case models.MCPServerAuthTypeBearer:
		if command.CredentialRef == "" || len(command.CredentialRef) > 255 {
			return fmt.Errorf("%w: bearer server requires credential_ref", errMCPRegistryValidation)
		}
	default:
		return fmt.Errorf("%w: invalid auth_type", errMCPRegistryValidation)
	}
	return nil
}

func mcpServerView(server models.MCPServer) MCPServerView {
	return MCPServerView{
		ServerCode: server.ServerCode, Name: server.Name, Description: server.Description,
		OwnerUserID: server.OwnerUserID, Transport: server.Transport, Endpoint: server.Endpoint,
		AuthType: server.AuthType, CredentialRef: server.CredentialRef, Status: server.Status,
		ConfigVersion: server.ConfigVersion, LastError: server.LastError, LastHealthyAt: server.LastHealthyAt,
		CreatedAt: server.CreatedAt, UpdatedAt: server.UpdatedAt,
	}
}

func mcpToolView(tool models.MCPTool) MCPToolView {
	return MCPToolView{
		ToolName: tool.ToolName, Description: tool.Description, InputSchemaJSON: tool.InputSchemaJSON,
		OutputSchemaJSON: tool.OutputSchemaJSON, CreatedAt: tool.CreatedAt, UpdatedAt: tool.UpdatedAt,
	}
}

func (s *MCPRegistryService) ensureServerNotReferencedByActiveWorkflow(ctx context.Context, database *gorm.DB, ownerUserID uint64, serverCode string) error {
	refs, err := activeMCPToolReferences(ctx, database, ownerUserID, serverCode)
	if err != nil {
		return err
	}
	if len(refs) > 0 {
		return fmt.Errorf("%w: server is referenced by an active workflow", errMCPServerInvalidState)
	}
	return nil
}

type activeMCPToolReference struct {
	AgentCode string
	NodeKey   string
	ToolName  string
}

func activeMCPToolReferences(ctx context.Context, database *gorm.DB, ownerUserID uint64, serverCode string) ([]activeMCPToolReference, error) {
	var workflows []models.Workflow
	if err := database.WithContext(ctx).
		Joins("JOIN agents ON agents.id = workflows.agent_id").
		Where("agents.owner_user_id = ? AND agents.status = ? AND workflows.is_active = ?", ownerUserID, models.AgentStatusActive, true).
		Order("workflows.id ASC").Find(&workflows).Error; err != nil {
		return nil, fmt.Errorf("loading active workflows for MCP reference validation: %w", err)
	}
	refs := make([]activeMCPToolReference, 0)
	for _, persisted := range workflows {
		definition, err := ParseAndValidateWorkflowDefinition(persisted.DefinitionJSON)
		if err != nil {
			return nil, fmt.Errorf("%w: active workflow %d is invalid", errMCPServerInvalidState, persisted.ID)
		}
		var agent models.Agent
		if err := database.WithContext(ctx).Select("agent_code").First(&agent, persisted.AgentID).Error; err != nil {
			return nil, fmt.Errorf("loading active workflow agent: %w", err)
		}
		for _, node := range definition.Nodes {
			if strings.TrimSpace(node.Type) != "tool" {
				continue
			}
			config, err := ParseToolNodeConfig(node)
			if err != nil {
				return nil, fmt.Errorf("%w: active workflow %d has invalid tool node", errMCPServerInvalidState, persisted.ID)
			}
			if config.ServerCode == serverCode {
				refs = append(refs, activeMCPToolReference{AgentCode: agent.AgentCode, NodeKey: node.Key, ToolName: config.ToolName})
			}
		}
	}
	return refs, nil
}

func validatePublishedAgentMCPReferences(ctx context.Context, database *gorm.DB, agent models.Agent, capabilities []models.AgentCapability) error {
	seenWorkflows := make(map[uint64]struct{})
	for _, capability := range capabilities {
		if capability.CapabilityType != models.AgentCapabilityTypeWorkflow || capability.WorkflowID == nil {
			continue
		}
		if _, exists := seenWorkflows[*capability.WorkflowID]; exists {
			continue
		}
		seenWorkflows[*capability.WorkflowID] = struct{}{}
		var persisted models.Workflow
		if err := database.WithContext(ctx).
			Where("id = ? AND agent_id = ? AND is_active = ?", *capability.WorkflowID, agent.ID, true).First(&persisted).Error; err != nil {
			return fmt.Errorf("workflow %d is unavailable", *capability.WorkflowID)
		}
		definition, err := ParseAndValidateWorkflowDefinition(persisted.DefinitionJSON)
		if err != nil {
			return fmt.Errorf("workflow %d is invalid", persisted.ID)
		}
		for _, node := range definition.Nodes {
			if !strings.EqualFold(strings.TrimSpace(node.Type), "tool") {
				continue
			}
			config, err := ParseToolNodeConfig(node)
			if err != nil {
				return fmt.Errorf("workflow %d tool node %s is invalid", persisted.ID, node.Key)
			}
			var server models.MCPServer
			if err := database.WithContext(ctx).Where(
				"owner_user_id = ? AND server_code = ? AND status = ?",
				agent.OwnerUserID, config.ServerCode, models.MCPServerStatusActive,
			).First(&server).Error; err != nil {
				return fmt.Errorf("workflow %d tool node %s references an inactive MCP server", persisted.ID, node.Key)
			}
			var count int64
			if err := database.WithContext(ctx).Model(&models.MCPTool{}).
				Where("server_id = ? AND tool_name = ?", server.ID, config.ToolName).Count(&count).Error; err != nil {
				return fmt.Errorf("validating workflow %d MCP tool: %w", persisted.ID, err)
			}
			if count == 0 {
				return fmt.Errorf("workflow %d tool node %s references an undiscovered MCP tool", persisted.ID, node.Key)
			}
		}
	}
	return nil
}
