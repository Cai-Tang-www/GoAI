package a2aclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"GoAI/a2aprotocol"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

func newHealthCheckClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()
	client, err := New(server.Client(), time.Second, WithCallbackBaseURL("http://127.0.0.1"))
	if err != nil {
		t.Fatalf("new A2A client: %v", err)
	}
	return client
}

func TestCheckAgentCardAcceptsMatchingDiscoveryContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/a2a/agents/writer/.well-known/agent-card.json" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(testCard(serverBaseURL(r), "write"))
	}))
	defer server.Close()

	err := newHealthCheckClient(t, server).CheckAgentCard(context.Background(), services.AgentCardHealthCheckRequest{
		AgentCode: "writer",
		Address:   server.URL + "/a2a/agents/writer",
	})
	if err != nil {
		t.Fatalf("check matching Agent Card: %v", err)
	}
}

func TestCheckAgentCardRejectsIdentityAndInterfaceMismatch(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*a2a.AgentCard)
		wantError string
	}{
		{
			name: "agent code mismatch",
			mutate: func(card *a2a.AgentCard) {
				card.Capabilities.Extensions[0].Params["agentCode"] = "reviewer"
			},
			wantError: "declares agent_code",
		},
		{
			name: "different origin interface",
			mutate: func(card *a2a.AgentCard) {
				card.SupportedInterfaces = []*a2a.AgentInterface{
					a2a.NewAgentInterface("http://127.0.0.1:1/a2a/agents/writer", a2a.TransportProtocolHTTPJSON),
				}
			},
			wantError: "safe HTTP+JSON interface",
		},
		{
			name: "unsupported interface transport",
			mutate: func(card *a2a.AgentCard) {
				card.SupportedInterfaces[0].ProtocolBinding = a2a.TransportProtocolJSONRPC
			},
			wantError: "safe HTTP+JSON interface",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				card := testCard(serverBaseURL(r), "write")
				test.mutate(card)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(card)
			}))
			defer server.Close()

			err := newHealthCheckClient(t, server).CheckAgentCard(context.Background(), services.AgentCardHealthCheckRequest{
				AgentCode: "writer",
				Address:   server.URL + "/a2a/agents/writer",
			})
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("got %v, want error containing %q", err, test.wantError)
			}
		})
	}
}

func TestCheckAgentCardPropagatesDiscoveryFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	err := newHealthCheckClient(t, server).CheckAgentCard(context.Background(), services.AgentCardHealthCheckRequest{
		AgentCode: "writer",
		Address:   server.URL + "/a2a/agents/writer",
	})
	if err == nil || !strings.Contains(err.Error(), "resolving Agent Card") {
		t.Fatalf("got %v, want discovery failure", err)
	}
}

func TestCheckAgentCardAcceptsOptionalDelegationAgentCode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		card := testCard(serverBaseURL(r), "write")
		card.Capabilities.Extensions = []a2a.AgentExtension{{URI: a2aprotocol.DelegationExtensionURI}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(card)
	}))
	defer server.Close()

	err := newHealthCheckClient(t, server).CheckAgentCard(context.Background(), services.AgentCardHealthCheckRequest{
		AgentCode: "writer",
		Address:   server.URL + "/a2a/agents/writer",
	})
	if err != nil {
		t.Fatalf("optional delegation agent_code must not block discovery: %v", err)
	}
}
