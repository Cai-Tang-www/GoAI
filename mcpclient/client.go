// Package mcpclient 封装官方 MCP Go SDK 的 Streamable HTTP 客户端能力。
package mcpclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"GoAI/requestctx"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var (
	ErrInvalidConfig      = errors.New("MCP client config is invalid")
	ErrCredentialNotFound = errors.New("MCP credential is unavailable")
	ErrTransportFailed    = errors.New("MCP transport failed")
	ErrProtocolFailed     = errors.New("MCP protocol operation failed")
	ErrToolReportedError  = errors.New("MCP tool reported an error")
)

const (
	AuthTypeNone   = "none"
	AuthTypeBearer = "bearer"
)

// CredentialResolver 根据逻辑引用解析真实凭据，返回值不得被持久化或写入日志。
type CredentialResolver interface {
	Resolve(context.Context, string) ([]byte, error)
}

// ServerConfig 是建立 MCP 会话所需的非持久化配置。
type ServerConfig struct {
	Endpoint      string
	AuthType      string
	CredentialRef string
}

// Tool 描述 MCP tools/list 返回的稳定工具元数据。
type Tool struct {
	Name             string
	Description      string
	InputSchemaJSON  string
	OutputSchemaJSON string
}

// CallResult 是可安全 JSON 序列化并交给后续 Workflow 节点的工具结果。
type CallResult struct {
	Content           []any `json:"content"`
	StructuredContent any   `json:"structured_content,omitempty"`
}

// Client 使用官方 MCP SDK 完成初始化、工具发现和工具调用。
type Client struct {
	httpClient *http.Client
	resolver   CredentialResolver
	protocol   *mcp.Client
}

// New 创建 MCP 客户端。httpClient 的 Transport 会按会话克隆，不会被修改。
func New(httpClient *http.Client, resolver CredentialResolver) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		httpClient: httpClient,
		resolver:   resolver,
		protocol: mcp.NewClient(
			&mcp.Implementation{Name: "goai-runtime", Version: "v1"},
			nil,
		),
	}
}

// Discover 通过真实 MCP initialize 与 tools/list 返回全部 Tool。
func (c *Client) Discover(ctx context.Context, config ServerConfig) ([]Tool, error) {
	handle, err := c.connect(ctx, config)
	if err != nil {
		return nil, err
	}
	defer handle.Close()

	tools := make([]Tool, 0)
	for tool, listErr := range handle.session.Tools(ctx, nil) {
		if listErr != nil {
			return nil, mapOperationError(ctx, listErr)
		}
		if tool == nil || strings.TrimSpace(tool.Name) == "" {
			return nil, fmt.Errorf("%w: server returned a tool without a name", ErrProtocolFailed)
		}
		inputSchema, marshalErr := marshalSchema(tool.InputSchema)
		if marshalErr != nil {
			return nil, fmt.Errorf("%w: invalid input schema for tool %q", ErrProtocolFailed, tool.Name)
		}
		outputSchema, marshalErr := marshalOptionalSchema(tool.OutputSchema)
		if marshalErr != nil {
			return nil, fmt.Errorf("%w: invalid output schema for tool %q", ErrProtocolFailed, tool.Name)
		}
		tools = append(tools, Tool{
			Name:             strings.TrimSpace(tool.Name),
			Description:      strings.TrimSpace(tool.Description),
			InputSchemaJSON:  inputSchema,
			OutputSchemaJSON: outputSchema,
		})
	}
	return tools, nil
}

// CallTool 通过真实 MCP tools/call 调用工具并返回 JSON-safe 结果。
func (c *Client) CallTool(ctx context.Context, config ServerConfig, toolName string, arguments map[string]any) (*CallResult, error) {
	name := strings.TrimSpace(toolName)
	if name == "" {
		return nil, fmt.Errorf("%w: tool name is required", ErrInvalidConfig)
	}
	if arguments == nil {
		arguments = map[string]any{}
	}

	handle, err := c.connect(ctx, config)
	if err != nil {
		return nil, err
	}
	defer handle.Close()

	result, err := handle.session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: arguments})
	if err != nil {
		return nil, mapOperationError(ctx, err)
	}
	if result == nil {
		return nil, fmt.Errorf("%w: empty tools/call result", ErrProtocolFailed)
	}
	if result.IsError {
		return nil, ErrToolReportedError
	}
	return normalizeCallResult(result)
}

func (c *Client) connect(ctx context.Context, config ServerConfig) (*sessionHandle, error) {
	if c == nil || c.protocol == nil || c.httpClient == nil {
		return nil, fmt.Errorf("%w: client is not initialized", ErrInvalidConfig)
	}
	config.Endpoint = strings.TrimSpace(config.Endpoint)
	config.AuthType = strings.TrimSpace(config.AuthType)
	config.CredentialRef = strings.TrimSpace(config.CredentialRef)
	if config.Endpoint == "" {
		return nil, fmt.Errorf("%w: endpoint is required", ErrInvalidConfig)
	}
	if config.AuthType == "" {
		config.AuthType = AuthTypeNone
	}

	var bearer []byte
	switch config.AuthType {
	case AuthTypeNone:
		if config.CredentialRef != "" {
			return nil, fmt.Errorf("%w: credential_ref is not allowed for auth type none", ErrInvalidConfig)
		}
	case AuthTypeBearer:
		if config.CredentialRef == "" || c.resolver == nil {
			return nil, ErrCredentialNotFound
		}
		resolved, err := c.resolver.Resolve(ctx, config.CredentialRef)
		if err != nil || len(resolved) == 0 {
			return nil, ErrCredentialNotFound
		}
		bearer = append([]byte(nil), resolved...)
	default:
		return nil, fmt.Errorf("%w: unsupported auth type", ErrInvalidConfig)
	}

	httpClient := *c.httpClient
	base := httpClient.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	httpClient.Transport = &headerTransport{base: base, bearer: bearer, traceID: requestctx.TraceIDFromContext(ctx)}
	transport := &mcp.StreamableClientTransport{
		Endpoint:             config.Endpoint,
		HTTPClient:           &httpClient,
		MaxRetries:           -1,
		DisableStandaloneSSE: true,
	}
	session, err := c.protocol.Connect(ctx, transport, nil)
	if err != nil {
		return nil, mapOperationError(ctx, err)
	}
	return &sessionHandle{session: session, secret: bearer}, nil
}

type sessionHandle struct {
	session *mcp.ClientSession
	secret  []byte
}

func (h *sessionHandle) Close() {
	if h == nil {
		return
	}
	if h.session != nil {
		_ = h.session.Close()
	}
	clear(h.secret)
}

func mapOperationError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrTransportFailed, ctx.Err())
	}
	return fmt.Errorf("%w: %v", ErrProtocolFailed, err)
}

func marshalSchema(schema any) (string, error) {
	if schema == nil {
		return "{}", nil
	}
	raw, err := json.Marshal(schema)
	if err != nil {
		return "", err
	}
	if !json.Valid(raw) {
		return "", errors.New("schema is not valid JSON")
	}
	return string(raw), nil
}

func marshalOptionalSchema(schema any) (string, error) {
	if schema == nil {
		return "", nil
	}
	return marshalSchema(schema)
}

func normalizeCallResult(result *mcp.CallToolResult) (*CallResult, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("%w: encoding tools/call result", ErrProtocolFailed)
	}
	var wire struct {
		Content           []any `json:"content"`
		StructuredContent any   `json:"structuredContent"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, fmt.Errorf("%w: decoding tools/call result", ErrProtocolFailed)
	}
	if wire.Content == nil {
		wire.Content = []any{}
	}
	return &CallResult{Content: wire.Content, StructuredContent: wire.StructuredContent}, nil
}

type headerTransport struct {
	base    http.RoundTripper
	bearer  []byte
	traceID string
}

func (t *headerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	cloned := request.Clone(request.Context())
	cloned.Header = request.Header.Clone()
	if len(t.bearer) > 0 {
		cloned.Header.Set("Authorization", "Bearer "+string(t.bearer))
	}
	if t.traceID != "" {
		cloned.Header.Set(requestctx.TraceIDHeader, t.traceID)
	}
	return t.base.RoundTrip(cloned)
}
