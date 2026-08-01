package middlewares

import (
	"errors"
	"log"
	"net/http"

	"GoAI/ai"
	"GoAI/governance"
	"GoAI/requestctx"
	"GoAI/services"

	"github.com/gin-gonic/gin"
)

const (
	contextTraceIDKey = "trace_id"

	CodeOK                     = "OK"
	CodeAuthMissingToken       = "AUTH_MISSING_TOKEN"
	CodeAuthInvalidToken       = "AUTH_INVALID_TOKEN"
	CodeAuthInvalidCredentials = "AUTH_INVALID_CREDENTIALS"
	CodeAuthForbidden          = "AUTH_FORBIDDEN"
	CodeValidationFailed       = "VALIDATION_FAILED"
	CodeInvalidID              = "INVALID_ID"
	CodeUserNotFound           = "USER_NOT_FOUND"
	CodeUserAlreadyExists      = "USER_ALREADY_EXISTS"
	CodeRunNotFound            = "RUN_NOT_FOUND"
	CodeRunAlreadyExists       = "RUN_ALREADY_EXISTS"
	CodeThreadUnavailable      = "THREAD_UNAVAILABLE"
	CodeMessageConflict        = "MESSAGE_CONFLICT"
	CodeAgentNotFound          = "AGENT_NOT_FOUND"
	CodeWorkflowNotFound       = "WORKFLOW_NOT_FOUND"
	CodeProviderNotFound       = "PROVIDER_NOT_FOUND"
	CodeProviderDriverNotFound = "PROVIDER_DRIVER_NOT_FOUND"
	CodeProviderInvalidConfig  = "PROVIDER_INVALID_CONFIG"
	CodeModelNotConfigured     = "MODEL_NOT_CONFIGURED"
	CodeStreamInterrupted      = "STREAM_INTERRUPTED"
	CodeRateLimited            = "RATE_LIMITED"
	CodeServiceUnavailable     = "SERVICE_UNAVAILABLE"
	CodeDownstreamTimeout      = "DOWNSTREAM_TIMEOUT"
	CodeInternalError          = "INTERNAL_ERROR"
	CodeRBACPermissionLoad     = "RBAC_PERMISSION_LOAD_FAILED"
	CodeKafkaPublishFailed     = "KAFKA_PUBLISH_FAILED"
	CodeIdempotencyKeyReused   = "IDEMPOTENCY_KEY_REUSED"
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

// AgentNotFoundError 构造 Agent 不存在或未启用错误。
func AgentNotFoundError() *AppError {
	return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeAgentNotFound, Message: "agent not found"}
}

// WorkflowNotFoundError 构造 Workflow 不存在错误。
func WorkflowNotFoundError() *AppError {
	return &AppError{HTTPStatus: http.StatusNotFound, Code: CodeWorkflowNotFound, Message: "workflow not found"}
}

// IdempotencyKeyReusedError 构造幂等键冲突错误。
func IdempotencyKeyReusedError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeIdempotencyKeyReused, Message: "idempotency key reused with different request"}
}

// RunAlreadyExistsError 构造协议 RunID 冲突错误。
func RunAlreadyExistsError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeRunAlreadyExists, Message: "run id already exists with different request"}
}

// ThreadUnavailableError 构造 Thread 已关闭或归档错误。
func ThreadUnavailableError() *AppError {
	return &AppError{HTTPStatus: http.StatusConflict, Code: CodeThreadUnavailable, Message: "thread is not active"}
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
	case errors.Is(err, services.ErrRunForbidden()):
		return ForbiddenError()
	case errors.Is(err, services.ErrRunDispatchFailed()):
		return KafkaPublishFailed(err)
	case errors.Is(err, services.ErrIdempotencyKeyReused()):
		return IdempotencyKeyReusedError()
	case errors.Is(err, services.ErrRunAlreadyExists()):
		return RunAlreadyExistsError()
	case errors.Is(err, services.ErrThreadUnavailable()):
		return ThreadUnavailableError()
	case errors.Is(err, services.ErrMessageConflict()):
		return MessageConflictError()
	case errors.Is(err, services.ErrAgentNotFound()):
		return AgentNotFoundError()
	case errors.Is(err, services.ErrWorkflowNotFound()):
		return WorkflowNotFoundError()
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
