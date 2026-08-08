package middlewares

import (
	"context"
	"errors"
	"log"
	"net/http"

	"GoAI/ai"
	"GoAI/governance"
	"GoAI/mcpclient"
	"GoAI/requestctx"
	"GoAI/services"

	"github.com/gin-gonic/gin"
)

const (
	contextTraceIDKey = "trace_id"

	CodeOK                        = "OK"
	CodeAuthMissingToken          = "AUTH_MISSING_TOKEN"
	CodeAuthInvalidToken          = "AUTH_INVALID_TOKEN"
	CodeAuthInvalidCredentials    = "AUTH_INVALID_CREDENTIALS"
	CodeAuthForbidden             = "AUTH_FORBIDDEN"
	CodeValidationFailed          = "VALIDATION_FAILED"
	CodeInvalidID                 = "INVALID_ID"
	CodeUserNotFound              = "USER_NOT_FOUND"
	CodeUserAlreadyExists         = "USER_ALREADY_EXISTS"
	CodeRunNotFound               = "RUN_NOT_FOUND"
	CodeRunAlreadyExists          = "RUN_ALREADY_EXISTS"
	CodeRunNotWaitingInput        = "RUN_NOT_WAITING_INPUT"
	CodeRunNotReplayable          = "RUN_NOT_REPLAYABLE"
	CodeInterruptNotFound         = "INTERRUPT_NOT_FOUND"
	CodeInterruptAlreadyResolved  = "INTERRUPT_ALREADY_RESOLVED"
	CodeParentRunThreadMismatch   = "PARENT_RUN_THREAD_MISMATCH"
	CodeThreadUnavailable         = "THREAD_UNAVAILABLE"
	CodeThreadNotFound            = "THREAD_NOT_FOUND"
	CodeMessageConflict           = "MESSAGE_CONFLICT"
	CodeAgentNotFound             = "AGENT_NOT_FOUND"
	CodeAgentAlreadyExists        = "AGENT_ALREADY_EXISTS"
	CodeAgentInvalidState         = "AGENT_INVALID_STATE"
	CodeAgentPublishValidation    = "AGENT_PUBLISH_VALIDATION_FAILED"
	CodeCapabilityNotFound        = "CAPABILITY_NOT_FOUND"
	CodeCapabilityAlreadyExists   = "CAPABILITY_ALREADY_EXISTS"
	CodeEndpointNotFound          = "ENDPOINT_NOT_FOUND"
	CodeEndpointAlreadyExists     = "ENDPOINT_ALREADY_EXISTS"
	CodeEndpointHealthCheckFailed = "ENDPOINT_HEALTH_CHECK_FAILED"
	CodeAgentRouteNotFound        = "AGENT_ROUTE_NOT_FOUND"
	CodeAgentRouteUnavailable     = "AGENT_ROUTE_UNAVAILABLE"
	CodeAgentRouteInvalid         = "AGENT_ROUTE_INVALID"
	CodeWorkflowNotFound          = "WORKFLOW_NOT_FOUND"
	CodeMCPServerNotFound         = "MCP_SERVER_NOT_FOUND"
	CodeMCPServerAlreadyExists    = "MCP_SERVER_ALREADY_EXISTS"
	CodeMCPServerInvalidState     = "MCP_SERVER_INVALID_STATE"
	CodeMCPServerUnhealthy        = "MCP_SERVER_UNHEALTHY"
	CodeMCPToolNotFound           = "MCP_TOOL_NOT_FOUND"
	CodeMCPToolInvocationFailed   = "MCP_TOOL_INVOCATION_FAILED"
	CodeMCPInvalidConfig          = "MCP_INVALID_CONFIG"
	CodeMCPCredentialNotFound     = "MCP_CREDENTIAL_NOT_FOUND"
	CodeMCPTransportFailed        = "MCP_TRANSPORT_FAILED"
	CodeMCPProtocolFailed         = "MCP_PROTOCOL_FAILED"
	CodeMCPToolReportedError      = "MCP_TOOL_REPORTED_ERROR"
	CodeProviderNotFound          = "PROVIDER_NOT_FOUND"
	CodeProviderDriverNotFound    = "PROVIDER_DRIVER_NOT_FOUND"
	CodeProviderInvalidConfig     = "PROVIDER_INVALID_CONFIG"
	CodeModelNotConfigured        = "MODEL_NOT_CONFIGURED"
	CodeStreamInterrupted         = "STREAM_INTERRUPTED"
	CodeRateLimited               = "RATE_LIMITED"
	CodeServiceUnavailable        = "SERVICE_UNAVAILABLE"
	CodeDownstreamTimeout         = "DOWNSTREAM_TIMEOUT"
	CodeInternalError             = "INTERNAL_ERROR"
	CodeRBACPermissionLoad        = "RBAC_PERMISSION_LOAD_FAILED"
	CodeKafkaPublishFailed        = "KAFKA_PUBLISH_FAILED"
	CodeIdempotencyKeyReused      = "IDEMPOTENCY_KEY_REUSED"
	CodeLoopNotFound              = "LOOP_NOT_FOUND"
)

// ResponseEnvelope 定义所有 HTTP JSON 与 SSE payload 共用的响应结构。
type ResponseEnvelope struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
	TraceID string `json:"trace_id"`
}

// AppError 描述统一错误码、HTTP 状态和对外文案。
type AppError struct {
	HTTPStatus int
	Code       string
	Message    string
	Data       any
	Err        error
}

// Error 返回底层错误文本，便于继续包装与日志记录。
func (e *AppError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Message
}

// TraceID 从 Gin 上下文读取当前请求链路标识。
func TraceID(c *gin.Context) string {
	if c == nil {
		return ""
	}
	if traceID, ok := c.Get(contextTraceIDKey); ok {
		if value, ok := traceID.(string); ok {
			return value
		}
	}
	return requestctx.TraceIDFromContext(c.Request.Context())
}

// Success 输出统一成功响应。
func Success(c *gin.Context, status int, data any, message string) {
	if message == "" {
		message = "success"
	}
	c.JSON(status, ResponseEnvelope{
		Code:    CodeOK,
		Message: message,
		Data:    data,
		TraceID: TraceID(c),
	})
}

// Fail 输出统一错误响应。
func Fail(c *gin.Context, appErr *AppError) {
	if c.Writer.Written() {
		return
	}
	if appErr == nil {
		appErr = InternalError("internal error", nil)
	}
	c.JSON(appErr.HTTPStatus, ResponseEnvelope{
		Code:    appErr.Code,
		Message: appErr.Message,
		Data:    appErr.Data,
		TraceID: TraceID(c),
	})
}

// AbortWithError 将错误挂到 Gin 上下文，由统一错误处理中间件输出响应。
func AbortWithError(c *gin.Context, appErr *AppError) {
	_ = c.Error(appErr)
	c.Abort()
}

// UnauthorizedMissingToken 构造缺少 Authorization 的认证错误。
func UnauthorizedMissingToken() *AppError {
	return &AppError{HTTPStatus: http.StatusUnauthorized, Code: CodeAuthMissingToken, Message: "authorization header is required"}
}

// UnauthorizedInvalidToken 构造无效 token 的认证错误。
func UnauthorizedInvalidToken() *AppError {
	return &AppError{HTTPStatus: http.StatusUnauthorized, Code: CodeAuthInvalidToken, Message: "invalid token"}
}

// UnauthorizedInvalidCredentials 构造账号密码错误的认证错误。
func UnauthorizedInvalidCredentials() *AppError {
	return &AppError{HTTPStatus: http.StatusUnauthorized, Code: CodeAuthInvalidCredentials, Message: "invalid username or password"}
}

// ForbiddenError 构造统一 403 错误。
func ForbiddenError() *AppError {
	return &AppError{HTTPStatus: http.StatusForbidden, Code: CodeAuthForbidden, Message: "forbidden"}
}

// RateLimitedError 构造统一限流错误。`Retry-After` 由治理中间件写入响应头。
func RateLimitedError() *AppError {
	return &AppError{HTTPStatus: http.StatusTooManyRequests, Code: CodeRateLimited, Message: "rate limit exceeded"}
}

// ServiceUnavailableError 构造下游服务暂时不可用错误。
func ServiceUnavailableError(err error) *AppError {
	return &AppError{HTTPStatus: http.StatusServiceUnavailable, Code: CodeServiceUnavailable, Message: "service temporarily unavailable", Err: err}
}

// DownstreamTimeoutError 构造下游超时错误。
func DownstreamTimeoutError(err error) *AppError {
	return &AppError{HTTPStatus: http.StatusGatewayTimeout, Code: CodeDownstreamTimeout, Message: "downstream request timed out", Err: err}
}

// ValidationFailed 构造请求参数错误。
func ValidationFailed(message string, data any) *AppError {
	if message == "" {
		message = "validation failed"
	}
	return &AppError{HTTPStatus: http.StatusBadRequest, Code: CodeValidationFailed, Message: message, Data: data}
}

// InvalidIDError 构造非法 ID 错误。
func InvalidIDError() *AppError {
	return &AppError{HTTPStatus: http.StatusBadRequest, Code: CodeInvalidID, Message: "invalid id"}
}

// UserNotFoundError 构造用户不存在错误。
func UserNotFoundError() *AppError {
	return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeUserNotFound, Message: "user not found"}
}

// UserAlreadyExistsError 构造用户名或邮箱冲突错误。
func UserAlreadyExistsError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeUserAlreadyExists, Message: "user already exists"}
}

// RunNotFoundError 构造 Run 不存在错误。
func RunNotFoundError() *AppError {
	return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeRunNotFound, Message: "run not found"}
}

// LoopNotFoundError 构造 Loop 不存在错误。
func LoopNotFoundError() *AppError {
	return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeLoopNotFound, Message: "loop not found"}
}

// AgentNotFoundError 构造 Agent 不存在或未启用错误。
func AgentNotFoundError() *AppError {
	return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeAgentNotFound, Message: "agent not found"}
}

// AgentAlreadyExistsError 构造 AgentCode 冲突错误。
func AgentAlreadyExistsError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeAgentAlreadyExists, Message: "agent already exists"}
}

// AgentInvalidStateError 构造 Agent 状态冲突错误。
func AgentInvalidStateError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeAgentInvalidState, Message: "agent state is invalid"}
}

// AgentPublishValidationError 构造 Agent 发布不变量校验失败错误。
func AgentPublishValidationError(err error) *AppError {
	return &AppError{HTTPStatus: http.StatusUnprocessableEntity, Code: CodeAgentPublishValidation, Message: "agent publish validation failed", Err: err}
}

// AgentRegistryValidationError 构造 Agent Registry 参数校验错误。
func AgentRegistryValidationError(err error) *AppError {
	return &AppError{HTTPStatus: http.StatusBadRequest, Code: CodeValidationFailed, Message: "validation failed", Err: err}
}

// CapabilityNotFoundError 构造 Capability 不存在错误。
func CapabilityNotFoundError() *AppError {
	return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeCapabilityNotFound, Message: "capability not found"}
}

// CapabilityAlreadyExistsError 构造 CapabilityCode 冲突错误。
func CapabilityAlreadyExistsError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeCapabilityAlreadyExists, Message: "capability already exists"}
}

// EndpointNotFoundError 构造 Endpoint 不存在错误。
func EndpointNotFoundError() *AppError {
	return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeEndpointNotFound, Message: "endpoint not found"}
}

// EndpointAlreadyExistsError 构造 EndpointCode 冲突错误。
func EndpointAlreadyExistsError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeEndpointAlreadyExists, Message: "endpoint already exists"}
}

// EndpointHealthCheckFailedError 构造 A2A Endpoint 健康检查失败错误。
func EndpointHealthCheckFailedError(err error) *AppError {
	return &AppError{HTTPStatus: http.StatusBadGateway, Code: CodeEndpointHealthCheckFailed, Message: "endpoint health check failed", Err: err}
}

// WorkflowNotFoundError 构造 Workflow 不存在错误。
func WorkflowNotFoundError() *AppError {
	return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeWorkflowNotFound, Message: "workflow not found"}
}

// MCPServerNotFoundError 构造 MCP Server 不存在错误。
func MCPServerNotFoundError() *AppError {
	return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeMCPServerNotFound, Message: "MCP server not found"}
}

// MCPServerAlreadyExistsError 构造 MCP Server 唯一键冲突错误。
func MCPServerAlreadyExistsError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeMCPServerAlreadyExists, Message: "MCP server already exists"}
}

// MCPServerInvalidStateError 构造 MCP Server 状态冲突错误。
func MCPServerInvalidStateError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeMCPServerInvalidState, Message: "MCP server state is invalid"}
}

// MCPServerUnhealthyError 构造 MCP Server 健康检查失败错误。
func MCPServerUnhealthyError(_ error) *AppError {
	return &AppError{HTTPStatus: http.StatusBadGateway, Code: CodeMCPServerUnhealthy, Message: "MCP server health check failed", Err: services.ErrMCPServerUnhealthy()}
}

// MCPToolNotFoundError 构造发现快照中 Tool 不存在错误。
func MCPToolNotFoundError() *AppError {
	return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeMCPToolNotFound, Message: "MCP tool not found"}
}

// MCPToolInvocationFailedError 构造 MCP Tool 调用失败错误。
func MCPToolInvocationFailedError(_ error) *AppError {
	return &AppError{HTTPStatus: http.StatusBadGateway, Code: CodeMCPToolInvocationFailed, Message: "MCP tool invocation failed", Err: services.ErrMCPToolInvocationFailed()}
}

// MCPInvalidConfigError 构造 MCP 配置或 Tool 输入校验错误。
func MCPInvalidConfigError(_ error) *AppError {
	return &AppError{HTTPStatus: http.StatusBadRequest, Code: CodeMCPInvalidConfig, Message: "MCP configuration is invalid", Err: mcpclient.ErrInvalidConfig}
}

// MCPCredentialNotFoundError 构造 MCP 凭据引用无法解析错误。
func MCPCredentialNotFoundError(_ error) *AppError {
	return &AppError{HTTPStatus: http.StatusServiceUnavailable, Code: CodeMCPCredentialNotFound, Message: "MCP credential is unavailable", Err: mcpclient.ErrCredentialNotFound}
}

// MCPTransportFailedError 构造 MCP 传输失败错误。
func MCPTransportFailedError(_ error) *AppError {
	return &AppError{HTTPStatus: http.StatusBadGateway, Code: CodeMCPTransportFailed, Message: "MCP transport failed", Err: mcpclient.ErrTransportFailed}
}

// MCPProtocolFailedError 构造 MCP 协议操作失败错误。
func MCPProtocolFailedError(_ error) *AppError {
	return &AppError{HTTPStatus: http.StatusBadGateway, Code: CodeMCPProtocolFailed, Message: "MCP protocol operation failed", Err: mcpclient.ErrProtocolFailed}
}

// MCPToolReportedError 构造 MCP Tool 主动返回 isError 的错误。
func MCPToolReportedError(_ error) *AppError {
	return &AppError{HTTPStatus: http.StatusBadGateway, Code: CodeMCPToolReportedError, Message: "MCP tool reported an error", Err: mcpclient.ErrToolReportedError}
}

// IdempotencyKeyReusedError 构造幂等键冲突错误。
func IdempotencyKeyReusedError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeIdempotencyKeyReused, Message: "idempotency key reused with different request"}
}

// RunAlreadyExistsError 构造协议 RunID 冲突错误。
func RunAlreadyExistsError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeRunAlreadyExists, Message: "run id already exists with different request"}
}

// RunNotWaitingInputError 构造 resume 目标状态冲突错误。
func RunNotWaitingInputError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeRunNotWaitingInput, Message: "run is not waiting for input"}
}

// RunNotReplayableError 构造来源 Run 尚未进入可回放终态的错误。
func RunNotReplayableError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeRunNotReplayable, Message: "run is not replayable"}
}

// InterruptNotFoundError 构造不存在的 Interrupt 错误。
func InterruptNotFoundError() *AppError {
	return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeInterruptNotFound, Message: "interrupt not found"}
}

// InterruptAlreadyResolvedError 构造重复处理 Interrupt 错误。
func InterruptAlreadyResolvedError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeInterruptAlreadyResolved, Message: "interrupt is already resolved"}
}

// ParentRunThreadMismatchError 构造父子 Run Thread 不一致错误。
func ParentRunThreadMismatchError() *AppError {
	return &AppError{HTTPStatus: http.StatusBadRequest, Code: CodeParentRunThreadMismatch, Message: "parent run and child run must use the same thread"}
}

// ThreadUnavailableError 构造 Thread 已关闭或归档错误。
func ThreadUnavailableError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeThreadUnavailable, Message: "thread is not active"}
}

// ThreadNotFoundError 构造 Thread 不存在错误。
func ThreadNotFoundError() *AppError {
	return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeThreadNotFound, Message: "thread not found"}
}

// MessageConflictError 构造 MessageID 内容冲突错误。
func MessageConflictError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeMessageConflict, Message: "message id already exists with different content"}
}

// RbacPermissionLoadFailed 构造 RBAC 查询失败错误。
func RbacPermissionLoadFailed(err error) *AppError {
	return &AppError{HTTPStatus: http.StatusInternalServerError, Code: CodeRBACPermissionLoad, Message: "rbac permission load failed", Err: err}
}

// KafkaPublishFailed 构造 Kafka 投递失败错误。
func KafkaPublishFailed(err error) *AppError {
	return &AppError{HTTPStatus: http.StatusInternalServerError, Code: CodeKafkaPublishFailed, Message: "kafka publish failed", Err: err}
}

// InternalError 构造统一内部错误。
func InternalError(message string, err error) *AppError {
	if message == "" {
		message = "internal error"
	}
	return &AppError{HTTPStatus: http.StatusInternalServerError, Code: CodeInternalError, Message: message, Err: err}
}

// WrapError 将业务层 sentinel error 映射为统一错误码。
func WrapError(err error) *AppError {
	if err == nil {
		return nil
	}

	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr
	}

	switch {
	case errors.Is(err, services.ErrRunNotFound()):
		return RunNotFoundError()
	case errors.Is(err, services.ErrLoopNotFound()):
		return LoopNotFoundError()
	case errors.Is(err, services.ErrRunForbidden()):
		return ForbiddenError()
	case errors.Is(err, services.ErrRunDispatchFailed()):
		return KafkaPublishFailed(err)
	case errors.Is(err, services.ErrIdempotencyKeyReused()):
		return IdempotencyKeyReusedError()
	case errors.Is(err, services.ErrRunAlreadyExists()):
		return RunAlreadyExistsError()
	case errors.Is(err, services.ErrRunNotWaitingInput()):
		return RunNotWaitingInputError()
	case errors.Is(err, services.ErrRunNotReplayable()):
		return RunNotReplayableError()
	case errors.Is(err, services.ErrInterruptNotFound()):
		return InterruptNotFoundError()
	case errors.Is(err, services.ErrInterruptAlreadyResolved()):
		return InterruptAlreadyResolvedError()
	case errors.Is(err, services.ErrParentRunThreadMismatch()):
		return ParentRunThreadMismatchError()
	case errors.Is(err, services.ErrThreadUnavailable()):
		return ThreadUnavailableError()
	case errors.Is(err, services.ErrThreadNotFound()):
		return ThreadNotFoundError()
	case errors.Is(err, services.ErrMessageConflict()):
		return MessageConflictError()
	case errors.Is(err, services.ErrAgentNotFound()):
		return AgentNotFoundError()
	case errors.Is(err, services.ErrAgentForbidden()):
		return ForbiddenError()
	case errors.Is(err, services.ErrAgentAlreadyExists()):
		return AgentAlreadyExistsError()
	case errors.Is(err, services.ErrAgentInvalidState()):
		return AgentInvalidStateError()
	case errors.Is(err, services.ErrAgentPublishValidation()):
		return AgentPublishValidationError(err)
	case errors.Is(err, services.ErrAgentRegistryValidation()):
		return AgentRegistryValidationError(err)
	case errors.Is(err, services.ErrCapabilityNotFound()):
		return CapabilityNotFoundError()
	case errors.Is(err, services.ErrCapabilityAlreadyExists()):
		return CapabilityAlreadyExistsError()
	case errors.Is(err, services.ErrEndpointNotFound()):
		return EndpointNotFoundError()
	case errors.Is(err, services.ErrEndpointAlreadyExists()):
		return EndpointAlreadyExistsError()
	case errors.Is(err, services.ErrEndpointHealthCheckFailed()):
		return EndpointHealthCheckFailedError(err)
	case errors.Is(err, services.ErrAgentRouteNotFound()):
		return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeAgentRouteNotFound, Message: "agent route not found", Err: err}
	case errors.Is(err, services.ErrAgentRouteUnavailable()):
		return &AppError{HTTPStatus: http.StatusServiceUnavailable, Code: CodeAgentRouteUnavailable, Message: "agent route is unavailable", Err: err}
	case errors.Is(err, services.ErrAgentRouteInvalid()):
		return &AppError{HTTPStatus: http.StatusBadRequest, Code: CodeAgentRouteInvalid, Message: "agent route is invalid", Err: err}
	case errors.Is(err, services.ErrWorkflowNotFound()):
		return WorkflowNotFoundError()
	case errors.Is(err, context.DeadlineExceeded):
		return DownstreamTimeoutError(err)
	case errors.Is(err, services.ErrMCPServerNotFound()):
		return MCPServerNotFoundError()
	case errors.Is(err, services.ErrMCPServerAlreadyExists()):
		return MCPServerAlreadyExistsError()
	case errors.Is(err, services.ErrMCPServerInvalidState()):
		return MCPServerInvalidStateError()
	case errors.Is(err, services.ErrMCPForbidden()):
		return ForbiddenError()
	case errors.Is(err, services.ErrMCPToolNotFound()):
		return MCPToolNotFoundError()
	case errors.Is(err, services.ErrMCPRegistryValidation()):
		return MCPInvalidConfigError(err)
	case errors.Is(err, mcpclient.ErrCredentialNotFound):
		return MCPCredentialNotFoundError(err)
	case errors.Is(err, mcpclient.ErrInvalidConfig):
		return MCPInvalidConfigError(err)
	case errors.Is(err, mcpclient.ErrToolReportedError):
		return MCPToolReportedError(err)
	case errors.Is(err, mcpclient.ErrTransportFailed):
		return MCPTransportFailedError(err)
	case errors.Is(err, mcpclient.ErrProtocolFailed):
		return MCPProtocolFailedError(err)
	case errors.Is(err, services.ErrMCPServerUnhealthy()):
		return MCPServerUnhealthyError(err)
	case errors.Is(err, services.ErrMCPToolInvocationFailed()):
		return MCPToolInvocationFailedError(err)
	case errors.Is(err, ai.ErrProviderNotFound):
		return &AppError{HTTPStatus: http.StatusBadRequest, Code: CodeProviderNotFound, Message: "provider not found", Err: err}
	case errors.Is(err, ai.ErrDriverNotFound):
		return &AppError{HTTPStatus: http.StatusBadRequest, Code: CodeProviderDriverNotFound, Message: "provider driver not found", Err: err}
	case errors.Is(err, ai.ErrInvalidProviderInput):
		return &AppError{HTTPStatus: http.StatusBadRequest, Code: CodeProviderInvalidConfig, Message: "provider config is invalid", Err: err}
	case errors.Is(err, ai.ErrModelNotConfigured):
		return &AppError{HTTPStatus: http.StatusBadRequest, Code: CodeModelNotConfigured, Message: "model is not configured", Err: err}
	case errors.Is(err, ai.ErrStreamInterrupted):
		return &AppError{HTTPStatus: http.StatusInternalServerError, Code: CodeStreamInterrupted, Message: "stream interrupted", Err: err}
	case errors.Is(err, governance.ErrCircuitOpen):
		return ServiceUnavailableError(err)
	case errors.Is(err, governance.ErrDownstreamTimeout):
		return DownstreamTimeoutError(err)
	default:
		return InternalError("internal error", err)
	}
}

// ErrorHandlingMiddleware 统一处理 panic 和通过 c.Error 上抛的业务错误。
func ErrorHandlingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("request panic trace_id=%s method=%s path=%s err=%v", TraceID(c), c.Request.Method, c.Request.URL.Path, recovered)
				Fail(c, InternalError("internal error", nil))
				c.Abort()
			}
		}()

		c.Next()
		if c.Writer.Written() || len(c.Errors) == 0 {
			return
		}
		appErr := WrapError(c.Errors.Last().Err)
		if appErr != nil && appErr.Err != nil {
			log.Printf("request error trace_id=%s method=%s path=%s code=%s status=%d err=%v", TraceID(c), c.Request.Method, c.Request.URL.Path, appErr.Code, appErr.HTTPStatus, appErr.Err)
		}
		Fail(c, appErr)
	}
}
