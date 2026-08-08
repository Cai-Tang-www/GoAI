package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"GoAI/mcpclient"
	"GoAI/models"

	"gorm.io/gorm"
)

var (
	errMCPToolInvocationFailed   = errors.New("MCP tool invocation failed")
	errMCPToolInvokerUnavailable = errors.New("MCP tool invoker is unavailable")
)

// ErrMCPToolInvocationFailed 返回 MCP Tool 调用失败的统一 sentinel error。
func ErrMCPToolInvocationFailed() error { return errMCPToolInvocationFailed }

// ToolInvocationRequest 描述 Workflow tool 节点发起的一次受治理 MCP 调用。
type ToolInvocationRequest struct {
	OwnerUserID uint64
	ServerCode  string
	ToolName    string
	Arguments   map[string]any
}

// ToolInvoker 是 RunService 调用外部 Tool 的协议无关边界。
type ToolInvoker interface {
	Invoke(context.Context, ToolInvocationRequest) (any, error)
}

// MCPToolClient 定义运行时使用的官方 MCP tools/call 能力。
type MCPToolClient interface {
	CallTool(context.Context, mcpclient.ServerConfig, string, map[string]any) (*mcpclient.CallResult, error)
}

// MCPToolInvoker 按 Agent owner、Server 状态和发现快照执行 MCP tools/call。
type MCPToolInvoker struct {
	database *gorm.DB
	client   MCPToolClient
}

// NewMCPToolInvoker 创建显式依赖的 MCP Tool 调用器。
func NewMCPToolInvoker(database *gorm.DB, client MCPToolClient) (*MCPToolInvoker, error) {
	if database == nil {
		return nil, errors.New("creating MCP tool invoker: database is nil")
	}
	if client == nil {
		return nil, errors.New("creating MCP tool invoker: client is nil")
	}
	return &MCPToolInvoker{database: database, client: client}, nil
}

// Invoke 验证稳定引用、输入 Schema 和 Server 状态后通过官方 MCP Client 调用 Tool。
func (i *MCPToolInvoker) Invoke(ctx context.Context, request ToolInvocationRequest) (any, error) {
	request.ServerCode = strings.TrimSpace(request.ServerCode)
	request.ToolName = strings.TrimSpace(request.ToolName)
	if request.OwnerUserID == 0 || !registryCodePattern.MatchString(request.ServerCode) || request.ToolName == "" {
		return nil, fmt.Errorf("%w: invalid invocation reference", errMCPRegistryValidation)
	}
	var server models.MCPServer
	if err := i.database.WithContext(ctx).
		Where("owner_user_id = ? AND server_code = ?", request.OwnerUserID, request.ServerCode).First(&server).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errMCPServerNotFound
		}
		return nil, fmt.Errorf("loading MCP server for invocation: %w", err)
	}
	if server.Status != models.MCPServerStatusActive {
		return nil, errMCPServerUnhealthy
	}
	var tool models.MCPTool
	if err := i.database.WithContext(ctx).
		Where("server_id = ? AND tool_name = ?", server.ID, request.ToolName).First(&tool).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errMCPToolNotFound
		}
		return nil, fmt.Errorf("loading MCP tool snapshot: %w", err)
	}
	if request.Arguments == nil {
		request.Arguments = map[string]any{}
	}
	argumentsJSON, err := json.Marshal(request.Arguments)
	if err != nil {
		return nil, fmt.Errorf("%w: encoding MCP tool arguments", errMCPRegistryValidation)
	}
	if err := validateJSONSchema(tool.InputSchemaJSON, string(argumentsJSON)); err != nil {
		return nil, fmt.Errorf("%w: tool input does not match discovered schema", errMCPRegistryValidation)
	}
	result, err := i.client.CallTool(ctx, mcpclient.ServerConfig{
		Endpoint: server.Endpoint, AuthType: server.AuthType, CredentialRef: server.CredentialRef,
	}, tool.ToolName, request.Arguments)
	if err != nil {
		return nil, sanitizeMCPToolError(err)
	}
	if result == nil {
		return nil, fmt.Errorf("%w: empty MCP tool result", errMCPToolInvocationFailed)
	}
	return result, nil
}

func sanitizeMCPToolError(err error) error {
	if err == nil {
		return nil
	}
	var cause error
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		cause = context.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		cause = context.Canceled
	case errors.Is(err, mcpclient.ErrCredentialNotFound):
		cause = mcpclient.ErrCredentialNotFound
	case errors.Is(err, mcpclient.ErrInvalidConfig):
		cause = mcpclient.ErrInvalidConfig
	case errors.Is(err, mcpclient.ErrToolReportedError):
		cause = mcpclient.ErrToolReportedError
	case errors.Is(err, mcpclient.ErrTransportFailed):
		cause = mcpclient.ErrTransportFailed
	case errors.Is(err, mcpclient.ErrProtocolFailed):
		cause = mcpclient.ErrProtocolFailed
	default:
		cause = mcpclient.ErrProtocolFailed
	}
	return fmt.Errorf("%w: %w", errMCPToolInvocationFailed, cause)
}

func isPermanentMCPInvocationError(err error) bool {
	return errors.Is(err, errMCPToolInvokerUnavailable) ||
		errors.Is(err, errMCPServerNotFound) ||
		errors.Is(err, errMCPServerUnhealthy) ||
		errors.Is(err, errMCPToolNotFound) ||
		errors.Is(err, errMCPRegistryValidation) ||
		errors.Is(err, mcpclient.ErrInvalidConfig) ||
		errors.Is(err, mcpclient.ErrCredentialNotFound) ||
		errors.Is(err, mcpclient.ErrToolReportedError)
}
