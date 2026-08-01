package middlewares

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"

	"GoAI/governance"

	"github.com/gin-gonic/gin"
)

// GovernanceMiddleware 使用进程内限流器保护协议入口。
// 它只限制传输入口，协议载荷处理和运行时状态迁移仍由既有 Gateway 与 Service 层负责。
func GovernanceMiddleware(service *governance.Service, configuredScopes []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if service == nil || !service.Enabled || service.Limiter == nil {
			c.Next()
			return
		}

		scope := requestScope(c)
		if !scopeEnabled(configuredScopes, scope) {
			c.Next()
			return
		}

		allowed, retryAfter := service.Limiter.Allow(rateLimitKey(c, scope))
		if allowed {
			c.Next()
			return
		}

		seconds := int(math.Ceil(retryAfter.Seconds()))
		if seconds < 1 {
			seconds = 1
		}
		c.Header("Retry-After", strconv.Itoa(seconds))
		service.Emit(governance.Event{Type: "rate_limited", Target: scope, Status: "429"})
		AbortWithError(c, RateLimitedError())
	}
}

func requestScope(c *gin.Context) string {
	if c == nil || c.Request == nil {
		return "unknown"
	}
	path := c.Request.URL.Path
	if strings.HasPrefix(path, "/a2a/") {
		return "a2a"
	}
	if strings.Contains(path, "/agui") {
		return "agui"
	}
	if strings.HasPrefix(path, "/api/") {
		return "api"
	}
	return "other"
}

func scopeEnabled(configuredScopes []string, scope string) bool {
	if len(configuredScopes) == 0 {
		return scope == "api" || scope == "agui" || scope == "a2a"
	}
	for _, configured := range configuredScopes {
		if strings.EqualFold(strings.TrimSpace(configured), scope) {
			return true
		}
	}
	return false
}

func rateLimitKey(c *gin.Context, scope string) string {
	if c == nil {
		return scope + "|unknown"
	}
	identity := "ip:" + c.ClientIP()
	if userID, ok := c.Get("user_id"); ok {
		identity = "user:" + stringifyUserID(userID)
	}
	agent := agentCodeForRateLimit(c)
	if agent == "" {
		agent = "-"
	}
	route := c.FullPath()
	if route == "" && c.Request != nil {
		route = c.Request.URL.Path
	}
	return strings.Join([]string{scope, identity, "agent:" + agent, "route:" + route}, "|")
}

// agentCodeForRateLimit 从路由参数或 Run 创建请求体提取目标 Agent 标识，
// 并恢复请求体供后续 handler 继续读取。
func agentCodeForRateLimit(c *gin.Context) string {
	if c == nil {
		return ""
	}
	if agent := strings.TrimSpace(c.Param("agent_code")); agent != "" {
		return agent
	}
	if c.Request == nil || c.Request.URL == nil ||
		c.Request.Method != http.MethodPost || c.Request.URL.Path != "/api/runs" ||
		c.Request.Body == nil {
		return ""
	}

	const maxInspectionBody = 1 << 20
	originalBody := c.Request.Body
	body, err := io.ReadAll(io.LimitReader(originalBody, maxInspectionBody+1))
	_ = originalBody.Close()
	c.Request.Body = io.NopCloser(bytes.NewReader(body))
	if err != nil || len(body) > maxInspectionBody {
		return ""
	}

	var payload struct {
		AgentCode string `json:"agent_code"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(payload.AgentCode)
}

func stringifyUserID(value any) string {
	switch id := value.(type) {
	case uint:
		return strconv.FormatUint(uint64(id), 10)
	case uint64:
		return strconv.FormatUint(id, 10)
	case uint32:
		return strconv.FormatUint(uint64(id), 10)
	case int:
		return strconv.Itoa(id)
	default:
		return "unknown"
	}
}
