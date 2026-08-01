package a2agateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"GoAI/a2aprotocol"
	"GoAI/models"
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
	runtime services.DelegationRuntime
	rest    http.Handler
}

// New 使用协议无关 Runtime 创建 A2A Gateway。
func New(runtime services.DelegationRuntime) (*Gateway, error) {
	if runtime == nil {
		return nil, errors.New("creating A2A gateway: runtime is nil")
	}
	handler := &requestHandler{runtime: runtime}
	return &Gateway{runtime: runtime, rest: a2asrv.NewRESTHandler(handler)}, nil
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

	cloned := r.Clone(ctx)
	cloned.URL = cloneURL(r.URL)
	cloned.URL.Path = protocolPath
	cloned.URL.RawPath = ""
	g.rest.ServeHTTP(w, cloned)
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
	card, err := buildAgentCard(descriptor)
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
	for _, capability := range descriptor.Capabilities {
		skills = append(skills, a2a.AgentSkill{
			ID:          capability.Code,
			Name:        capability.Name,
			Description: capability.Description,
			Tags:        []string{capability.Type, capability.Version},
			InputModes:  []string{"text/plain", "application/json"},
			OutputModes: []string{"application/json"},
		})
	}
	return &a2a.AgentCard{
		Name:                descriptor.Name,
		Description:         descriptor.Description,
		Version:             "1.0",
		SupportedInterfaces: interfaces,
		Capabilities: a2a.AgentCapabilities{Extensions: []a2a.AgentExtension{{
			URI:         DelegationExtensionURI,
			Description: "GoAI multi-agent delegation metadata",
			Required:    true,
		}}},
		DefaultInputModes:  []string{"text/plain", "application/json"},
		DefaultOutputModes: []string{"application/json"},
		Skills:             skills,
	}, nil
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
