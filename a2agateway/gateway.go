package a2agateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"

	"GoAI/a2aauth"
	"GoAI/a2aprotocol"
	"GoAI/models"
	"GoAI/observability"
	"GoAI/requestctx"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
)

const (
	// DelegationExtensionURI 保留 Gateway 原有公开常量，并复用共享协议定义。
	DelegationExtensionURI = a2aprotocol.DelegationExtensionURI
	gatewayPrefix          = "/a2a/agents/"
)

type targetAgentContextKey struct{}

// Gateway 将按 Agent Code 分租户的 HTTP 请求交给官方 A2A HTTP+JSON transport。
type Gateway struct {
	runtime      services.DelegationRuntime
	rest         http.Handler
	verifier     *a2aauth.Verifier
	authRequired bool
	logger       *slog.Logger
}

// Option 配置 A2A Gateway 的协议安全能力。
type Option func(*Gateway) error

// WithAuthentication 开启或关闭 A2A 业务请求的机器身份验签。
func WithAuthentication(verifier *a2aauth.Verifier, required bool) Option {
	return func(gateway *Gateway) error {
		if required && verifier == nil {
			return errors.New("configuring A2A gateway: verifier is nil")
		}
		gateway.verifier = verifier
		gateway.authRequired = required
		return nil
	}
}

// WithObservability 注入 A2A Gateway 的安全审计日志能力。
func WithObservability(bundle *observability.Bundle) Option {
	return func(gateway *Gateway) error {
		if bundle == nil || bundle.Logger == nil {
			return errors.New("configuring A2A gateway: observability logger is nil")
		}
		gateway.logger = bundle.Logger
		return nil
	}
}

// New 使用协议无关 Runtime 创建 A2A Gateway。
func New(runtime services.DelegationRuntime, options ...Option) (*Gateway, error) {
	if runtime == nil {
		return nil, errors.New("creating A2A gateway: runtime is nil")
	}
	gateway := &Gateway{runtime: runtime}
	for _, option := range options {
		if option != nil {
			if err := option(gateway); err != nil {
				return nil, err
			}
		}
	}
	handler := &requestHandler{runtime: runtime, authRequired: gateway.authRequired, logger: gateway.logger}
	gateway.rest = a2asrv.NewRESTHandler(handler)
	return gateway, nil
}

// ServeHTTP 解析目标 Agent，并保持官方 A2A REST 路径不变地执行协议请求。
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	targetAgent, protocolPath, ok := splitGatewayPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	ctx := context.WithValue(r.Context(), targetAgentContextKey{}, targetAgent)
	if protocolPath == a2asrv.WellKnownAgentCardPath {
		g.serveAgentCard(ctx, targetAgent, w)
		return
	}

	if g.authRequired {
		authenticatedAgent, err := g.authenticate(r)
		if err != nil {
			g.logSecurityRejection(r.Context(), targetAgent, r.Header.Get(a2aauth.HeaderAgentCode), authenticationFailureReason(err))
			writeAuthenticationError(w, http.StatusUnauthorized, "A2A authentication failed")
			return
		}
		ctx = a2aauth.WithAuthenticatedAgent(ctx, authenticatedAgent)
	}

	cloned := r.Clone(ctx)
	cloned.URL = cloneURL(r.URL)
	cloned.URL.Path = protocolPath
	cloned.URL.RawPath = ""
	g.rest.ServeHTTP(w, cloned)
}

func (g *Gateway) authenticate(request *http.Request) (string, error) {
	sourceCode := strings.TrimSpace(request.Header.Get(a2aauth.HeaderAgentCode))
	if sourceCode == "" {
		return "", a2aauth.ErrMissingAuthentication
	}
	descriptor, err := g.runtime.DescribeAgent(request.Context(), sourceCode)
	if err != nil || descriptor == nil || strings.TrimSpace(descriptor.Code) != sourceCode {
		return "", a2aauth.ErrInvalidAuthentication
	}
	credentialRef := ""
	for _, endpoint := range descriptor.Endpoints {
		if endpoint.AuthType != models.AgentEndpointAuthTypeHMACSHA256 || strings.TrimSpace(endpoint.CredentialRef) == "" {
			continue
		}
		if credentialRef != "" && credentialRef != strings.TrimSpace(endpoint.CredentialRef) {
			return "", a2aauth.ErrInvalidAuthentication
		}
		credentialRef = strings.TrimSpace(endpoint.CredentialRef)
	}
	if credentialRef == "" {
		return "", a2aauth.ErrInvalidAuthentication
	}
	return g.verifier.Verify(request, credentialRef)
}

func (g *Gateway) logSecurityRejection(ctx context.Context, targetAgent, sourceAgent, reason string) {
	if g == nil || g.logger == nil {
		return
	}
	g.logger.WarnContext(ctx, "A2A security request rejected",
		slog.String("trace_id", requestctx.TraceIDFromContext(ctx)),
		slog.String("target_agent", safeAuditValue(targetAgent)),
		slog.String("source_agent", safeAuditValue(sourceAgent)),
		slog.String("reason", reason),
	)
}

func authenticationFailureReason(err error) string {
	switch {
	case errors.Is(err, a2aauth.ErrMissingAuthentication):
		return "missing_authentication"
	case errors.Is(err, a2aauth.ErrExpiredRequest):
		return "expired_timestamp"
	case errors.Is(err, a2aauth.ErrReplayDetected):
		return "nonce_replay"
	case errors.Is(err, a2aauth.ErrRequestBodyTooLarge):
		return "body_too_large"
	case errors.Is(err, a2aauth.ErrInvalidAuthentication):
		return "invalid_authentication"
	default:
		return "verification_failed"
	}
}

func safeAuditValue(value string) string {
	value = strings.TrimSpace(value)
	const maxAuditValueRunes = 128
	runes := []rune(value)
	if len(runes) > maxAuditValueRunes {
		return string(runes[:maxAuditValueRunes])
	}
	return value
}
func writeAuthenticationError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func splitGatewayPath(path string) (string, string, bool) {
	if !strings.HasPrefix(path, gatewayPrefix) {
		return "", "", false
	}
	remainder := strings.TrimPrefix(path, gatewayPrefix)
	separator := strings.IndexByte(remainder, '/')
	if separator <= 0 {
		return "", "", false
	}
	target := strings.TrimSpace(remainder[:separator])
	protocolPath := remainder[separator:]
	if target == "" || protocolPath == "" {
		return "", "", false
	}
	return target, protocolPath, true
}

func cloneURL(source *url.URL) *url.URL {
	if source == nil {
		return &url.URL{}
	}
	cloned := *source
	return &cloned
}

func targetAgentFromContext(ctx context.Context) (string, error) {
	target, _ := ctx.Value(targetAgentContextKey{}).(string)
	target = strings.TrimSpace(target)
	if target == "" {
		return "", a2a.NewError(a2a.ErrInvalidRequest, "target agent is missing")
	}
	return target, nil
}

func (g *Gateway) serveAgentCard(ctx context.Context, targetAgent string, w http.ResponseWriter) {
	descriptor, err := g.runtime.DescribeAgent(ctx, targetAgent)
	if err != nil {
		status := http.StatusInternalServerError
		message := "agent card is unavailable"
		if errors.Is(err, services.ErrAgentNotFound()) {
			status = http.StatusNotFound
			message = "agent not found"
		}
		writeCardError(w, status, message)
		return
	}
	card, err := buildAgentCardWithAuthentication(descriptor, g.authRequired)
	if err != nil {
		writeCardError(w, http.StatusInternalServerError, "agent card is invalid")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(card)
}

func writeCardError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func buildAgentCard(descriptor *services.AgentDescriptor) (*a2a.AgentCard, error) {
	return buildAgentCardWithAuthentication(descriptor, false)
}

func buildAgentCardWithAuthentication(descriptor *services.AgentDescriptor, authRequired bool) (*a2a.AgentCard, error) {
	if descriptor == nil || strings.TrimSpace(descriptor.Code) == "" {
		return nil, errors.New("agent descriptor is empty")
	}
	interfaces := make([]*a2a.AgentInterface, 0, len(descriptor.Endpoints))
	for _, endpoint := range descriptor.Endpoints {
		if err := validateEndpoint(endpoint); err != nil {
			return nil, err
		}
		interfaces = append(interfaces, a2a.NewAgentInterface(strings.TrimRight(endpoint.Address, "/"), a2a.TransportProtocolHTTPJSON))
	}
	if len(interfaces) == 0 {
		return nil, errors.New("agent has no active A2A endpoint")
	}
	skills := make([]a2a.AgentSkill, 0, len(descriptor.Capabilities))
	contracts := make(map[string]any, len(descriptor.Capabilities))
	for _, capability := range descriptor.Capabilities {
		skills = append(skills, a2a.AgentSkill{
			ID:          capability.Code,
			Name:        capability.Name,
			Description: capability.Description,
			Tags:        []string{capability.Type, capability.Version},
			InputModes:  []string{"text/plain", "application/json"},
			OutputModes: []string{"application/json"},
		})
		contract := map[string]any{"version": capability.Version}
		if strings.TrimSpace(capability.InputSchemaJSON) != "" {
			inputSchema, err := decodeSchema(capability.InputSchemaJSON)
			if err != nil {
				return nil, fmt.Errorf("capability %s input schema: %w", capability.Code, err)
			}
			contract["inputSchema"] = inputSchema
		}
		if strings.TrimSpace(capability.OutputSchemaJSON) != "" {
			outputSchema, err := decodeSchema(capability.OutputSchemaJSON)
			if err != nil {
				return nil, fmt.Errorf("capability %s output schema: %w", capability.Code, err)
			}
			contract["outputSchema"] = outputSchema
		}
		contracts[capability.Code] = contract
	}
	card := &a2a.AgentCard{
		Name:                descriptor.Name,
		Description:         descriptor.Description,
		Version:             "1.0",
		SupportedInterfaces: interfaces,
		Capabilities: a2a.AgentCapabilities{Extensions: []a2a.AgentExtension{{
			URI:         DelegationExtensionURI,
			Description: "GoAI multi-agent delegation metadata and capability contracts",
			Required:    true,
			Params:      map[string]any{"capabilities": contracts},
		}}},
		DefaultInputModes:  []string{"text/plain", "application/json"},
		DefaultOutputModes: []string{"application/json"},
		Skills:             skills,
	}
	if authRequired {
		const schemeName a2a.SecuritySchemeName = "goaiHMACSHA256"
		card.SecuritySchemes = a2a.NamedSecuritySchemes{
			schemeName: a2a.HTTPAuthSecurityScheme{Scheme: a2aauth.AuthorizationScheme},
		}
		card.SecurityRequirements = a2a.SecurityRequirementsOptions{
			a2a.SecurityRequirements{schemeName: a2a.SecuritySchemeScopes{}},
		}
	}
	return card, nil
}

func decodeSchema(raw string) (any, error) {
	var schema any
	if err := json.Unmarshal([]byte(raw), &schema); err != nil {
		return nil, fmt.Errorf("schema must be valid JSON: %w", err)
	}
	if _, ok := schema.(map[string]any); !ok {
		return nil, errors.New("schema must be a JSON object")
	}
	return schema, nil
}

func validateEndpoint(endpoint services.AgentEndpointDescriptor) error {
	parsed, err := url.Parse(strings.TrimSpace(endpoint.Address))
	if err != nil || parsed.Host == "" {
		return fmt.Errorf("invalid A2A endpoint %q", endpoint.Code)
	}
	switch endpoint.Transport {
	case models.AgentEndpointTransportHTTP:
		if parsed.Scheme != "http" || !isLoopbackHost(parsed.Hostname()) {
			return fmt.Errorf("HTTP A2A endpoint %q must use a loopback host", endpoint.Code)
		}
	case models.AgentEndpointTransportHTTPS:
		if parsed.Scheme != "https" {
			return fmt.Errorf("HTTPS A2A endpoint %q must use https", endpoint.Code)
		}
	default:
		return fmt.Errorf("unsupported A2A endpoint transport %q", endpoint.Transport)
	}
	return nil
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
