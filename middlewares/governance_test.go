package middlewares

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"GoAI/governance"

	"github.com/gin-gonic/gin"
)

func TestGovernanceMiddlewareRateLimitsAndKeepsTraceID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	service, err := governance.New(governance.Config{
		Enabled:                    true,
		RateLimitRequestsPerSecond: 1,
		RateLimitBurst:             1,
		RateLimitMaxKeys:           10,
		DownstreamRequestTimeout:   time.Second,
		CircuitFailureThreshold:    1,
		CircuitOpenTimeout:         time.Second,
		CircuitMaxTargets:          10,
	})
	if err != nil {
		t.Fatalf("create governance service: %v", err)
	}
	router := gin.New()
	router.Use(TraceMiddleware(), ErrorHandlingMiddleware(), GovernanceMiddleware(service, []string{"api"}))
	called := 0
	router.GET("/api/runs", func(c *gin.Context) {
		called++
		Success(c, http.StatusOK, map[string]bool{"ok": true}, "success")
	})

	first := httptest.NewRecorder()
	firstReq := httptest.NewRequest(http.MethodGet, "/api/runs", nil)
	firstReq.Header.Set("X-Trace-ID", "trace-governance-001")
	router.ServeHTTP(first, firstReq)
	if first.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want 200", first.Code)
	}
	if first.Header().Get("X-Trace-ID") != "trace-governance-001" {
		t.Fatalf("first response trace id = %q", first.Header().Get("X-Trace-ID"))
	}

	second := httptest.NewRecorder()
	secondReq := httptest.NewRequest(http.MethodGet, "/api/runs", nil)
	secondReq.Header.Set("X-Trace-ID", "trace-governance-002")
	router.ServeHTTP(second, secondReq)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want 429", second.Code)
	}
	if second.Header().Get("Retry-After") == "" {
		t.Fatal("rate limited response should include Retry-After")
	}
	var envelope ResponseEnvelope
	if err := json.Unmarshal(second.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response envelope: %v", err)
	}
	if envelope.Code != CodeRateLimited || envelope.TraceID != "trace-governance-002" {
		t.Fatalf("unexpected rate limited envelope: %+v", envelope)
	}
	if called != 1 {
		t.Fatalf("handler called %d times, want 1", called)
	}
}

func TestGovernanceMiddlewareHonorsConfiguredScopes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	service, err := governance.New(governance.Config{
		Enabled:                    true,
		RateLimitRequestsPerSecond: 1,
		RateLimitBurst:             1,
		RateLimitMaxKeys:           10,
		DownstreamRequestTimeout:   time.Second,
		CircuitFailureThreshold:    1,
		CircuitOpenTimeout:         time.Second,
		CircuitMaxTargets:          10,
	})
	if err != nil {
		t.Fatalf("create governance service: %v", err)
	}
	router := gin.New()
	router.Use(GovernanceMiddleware(service, []string{"a2a"}))
	router.GET("/api/runs", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	request := httptest.NewRequest(http.MethodGet, "/api/runs", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("disabled scope status = %d, want 204", response.Code)
	}
}

func TestRateLimitKeyUsesAgentRouteParamBeforeHeader(t *testing.T) {
	gin.SetMode(gin.TestMode)
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Request = httptest.NewRequest(http.MethodGet, "/a2a/agents/writer/.well-known/agent-card.json", nil)
	context.Params = gin.Params{{Key: "agent_code", Value: "writer"}}
	context.Request.Header.Set("X-Agent-Code", "header-agent")

	key := rateLimitKey(context, "a2a")
	if !strings.Contains(key, "agent:writer") || strings.Contains(key, "header-agent") {
		t.Fatalf("unexpected agent rate limit key: %s", key)
	}
}

func TestRateLimitKeyUsesAuthenticatedUserIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/runs", nil)
	ctx.Set("user_id", uint(42))
	key := rateLimitKey(ctx, "api")
	if !strings.Contains(key, "user:42") {
		t.Fatalf("authenticated key = %q, want user identity", key)
	}
	if strings.Contains(key, "ip:") {
		t.Fatalf("authenticated key should not fall back to ip: %q", key)
	}
}

func TestRateLimitKeyDoesNotTrustAgentHeaderWithoutRouteParam(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest(http.MethodGet, "/a2a/agents/writer/tasks", nil)
	ctx.Request.Header.Set("X-Agent-Code", "spoofed")
	key := rateLimitKey(ctx, "a2a")
	if strings.Contains(key, "spoofed") || !strings.Contains(key, "agent:-") {
		t.Fatalf("header should not influence key: %q", key)
	}
}
