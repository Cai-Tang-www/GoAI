package handlers

import (
	"GoAI/ai"
	"GoAI/middlewares"
	"GoAI/services"
	"encoding/json"
	"net/mail"
	"regexp"
	"strconv"
	"strings"
)

type userWriteRequest struct {
	Username string `json:"username"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type chatRequest struct {
	Messages []ai.Message `json:"messages"`
	Model    string       `json:"model"`
	Provider string       `json:"provider"`
}

type validationFieldError struct {
	Field  string `json:"field"`
	Reason string `json:"reason"`
}

var usernamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{3,32}$`)
var runIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
var allowedMessageRoles = map[string]struct{}{
	"system":    {},
	"user":      {},
	"assistant": {},
	"tool":      {},
}
var allowedRunTriggerTypes = map[string]struct{}{
	"api":      {},
	"manual":   {},
	"replay":   {},
	"schedule": {},
	"webhook":  {},
}

// normalizeUserWriteRequest 对用户写接口的公共字段做 trim 规范化。
func normalizeUserWriteRequest(req *userWriteRequest) {
	if req == nil {
		return
	}
	req.Username = strings.TrimSpace(req.Username)
	req.Email = strings.TrimSpace(req.Email)
}

// validateUserWriteRequest 校验用户创建与更新请求的字段完整性和格式。
func validateUserWriteRequest(req userWriteRequest, passwordRequired bool) *middlewares.AppError {
	var fieldErrors []validationFieldError
	if req.Username == "" {
		fieldErrors = append(fieldErrors, newValidationFieldError("username", "is required"))
	} else if !usernamePattern.MatchString(req.Username) {
		fieldErrors = append(fieldErrors, newValidationFieldError("username", "must be 3-32 chars of letters, digits, underscore or hyphen"))
	}
	if req.Email == "" {
		fieldErrors = append(fieldErrors, newValidationFieldError("email", "is required"))
	} else if _, err := mail.ParseAddress(req.Email); err != nil {
		fieldErrors = append(fieldErrors, newValidationFieldError("email", "must be a valid email address"))
	}
	if passwordRequired && req.Password == "" {
		fieldErrors = append(fieldErrors, newValidationFieldError("password", "is required"))
	}
	if req.Password != "" {
		if len(req.Password) < 6 {
			fieldErrors = append(fieldErrors, newValidationFieldError("password", "must be at least 6 characters"))
		}
		if len(req.Password) > 72 {
			fieldErrors = append(fieldErrors, newValidationFieldError("password", "must be at most 72 characters"))
		}
	}
	if len(fieldErrors) > 0 {
		return validationError("invalid user payload", fieldErrors)
	}
	return nil
}

// normalizeLoginRequest 对登录请求中的账号字段做 trim 规范化。
func normalizeLoginRequest(req *loginRequest) {
	if req == nil {
		return
	}
	req.Username = strings.TrimSpace(req.Username)
}

// validateLoginRequest 校验登录请求至少包含用户名和密码。
func validateLoginRequest(req loginRequest) *middlewares.AppError {
	var fieldErrors []validationFieldError
	if req.Username == "" {
		fieldErrors = append(fieldErrors, newValidationFieldError("username", "is required"))
	}
	if req.Password == "" {
		fieldErrors = append(fieldErrors, newValidationFieldError("password", "is required"))
	}
	if len(fieldErrors) > 0 {
		return validationError("invalid login payload", fieldErrors)
	}
	return nil
}

// normalizeChatRequest 对聊天请求中的 provider 与 model 字段做 trim 规范化。
func normalizeChatRequest(req *chatRequest) {
	if req == nil {
		return
	}
	req.Provider = strings.TrimSpace(req.Provider)
	req.Model = strings.TrimSpace(req.Model)
}

// validateChatRequest 校验聊天请求中的消息、角色和可选模型参数是否合法。
func validateChatRequest(req chatRequest) *middlewares.AppError {
	var fieldErrors []validationFieldError
	if len(req.Messages) == 0 {
		fieldErrors = append(fieldErrors, newValidationFieldError("messages", "must contain at least one message"))
	}
	for idx, message := range req.Messages {
		role := strings.TrimSpace(message.Role)
		content := strings.TrimSpace(message.Content)
		if role == "" {
			fieldErrors = append(fieldErrors, newValidationFieldError("messages["+strconv.Itoa(idx)+"].role", "is required"))
			continue
		}
		if _, ok := allowedMessageRoles[role]; !ok {
			fieldErrors = append(fieldErrors, newValidationFieldError("messages["+strconv.Itoa(idx)+"].role", "must be one of system,user,assistant,tool"))
		}
		if content == "" {
			fieldErrors = append(fieldErrors, newValidationFieldError("messages["+strconv.Itoa(idx)+"].content", "is required"))
		}
	}
	if len(req.Provider) > 64 {
		fieldErrors = append(fieldErrors, newValidationFieldError("provider", "must be at most 64 characters"))
	}
	if len(req.Model) > 128 {
		fieldErrors = append(fieldErrors, newValidationFieldError("model", "must be at most 128 characters"))
	}
	if len(fieldErrors) > 0 {
		return validationError("invalid chat payload", fieldErrors)
	}
	return nil
}

// validateCreateRunRequest 校验创建 Run 请求中的关键业务字段和 JSON 输入合法性。
func validateCreateRunRequest(req *services.CreateRunRequest) *middlewares.AppError {
	if req == nil {
		return validationError("invalid run payload", []validationFieldError{newValidationFieldError("body", "is required")})
	}
	var fieldErrors []validationFieldError
	if strings.TrimSpace(req.AgentCode) == "" {
		fieldErrors = append(fieldErrors, newValidationFieldError("agent_code", "is required"))
	}
	if len(strings.TrimSpace(req.AgentCode)) > 64 {
		fieldErrors = append(fieldErrors, newValidationFieldError("agent_code", "must be at most 64 characters"))
	}
	if req.WorkflowVersion < 0 {
		fieldErrors = append(fieldErrors, newValidationFieldError("workflow_version", "must be greater than or equal to 0"))
	}
	if len(strings.TrimSpace(req.ThreadID)) > 128 {
		fieldErrors = append(fieldErrors, newValidationFieldError("thread_id", "must be at most 128 characters"))
	}
	triggerType := strings.TrimSpace(req.TriggerType)
	if triggerType != "" {
		if _, ok := allowedRunTriggerTypes[triggerType]; !ok {
			fieldErrors = append(fieldErrors, newValidationFieldError("trigger_type", "must be one of api,manual,replay,schedule,webhook"))
		}
	}
	if len(strings.TrimSpace(req.Provider)) > 64 {
		fieldErrors = append(fieldErrors, newValidationFieldError("provider", "must be at most 64 characters"))
	}
	if len(strings.TrimSpace(req.Model)) > 128 {
		fieldErrors = append(fieldErrors, newValidationFieldError("model", "must be at most 128 characters"))
	}
	inputJSON := strings.TrimSpace(string(req.Input))
	if inputJSON != "" && !json.Valid(req.Input) {
		fieldErrors = append(fieldErrors, newValidationFieldError("input", "must be valid JSON"))
	}
	if len(fieldErrors) > 0 {
		return validationError("invalid run payload", fieldErrors)
	}
	return nil
}

// validateRunIDParam 校验 run_id 路径参数，避免无效路径值进入查询与回放逻辑。
func validateRunIDParam(runID string) *middlewares.AppError {
	return validateResourceIDParam("run_id", runID)
}

// validateResourceIDParam 校验资源 ID 路径参数，并在错误中保留实际字段名。
func validateResourceIDParam(field, value string) *middlewares.AppError {
	field = strings.TrimSpace(field)
	if field == "" {
		field = "id"
	}
	normalized := strings.TrimSpace(value)
	var fieldErrors []validationFieldError
	if normalized == "" {
		fieldErrors = append(fieldErrors, newValidationFieldError(field, "is required"))
	}
	if len(normalized) > 64 {
		fieldErrors = append(fieldErrors, newValidationFieldError(field, "must be at most 64 characters"))
	}
	if normalized != "" && !runIDPattern.MatchString(normalized) {
		fieldErrors = append(fieldErrors, newValidationFieldError(field, "must contain only letters, digits, underscore or hyphen"))
	}
	if len(fieldErrors) > 0 {
		return validationError("invalid run id", fieldErrors)
	}
	return nil
}

// validateIdempotencyKey 校验可选幂等键的长度，避免异常 header 进入服务层。
func validateIdempotencyKey(key string) *middlewares.AppError {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}
	if len(key) > 128 {
		return validationError("invalid idempotency key", []validationFieldError{newValidationFieldError("idempotency_key", "must be at most 128 characters")})
	}
	return nil
}

// validationError 将字段级错误聚合成统一的 ValidationFailed 响应。
func validationError(message string, fieldErrors []validationFieldError) *middlewares.AppError {
	if len(fieldErrors) == 0 {
		return middlewares.ValidationFailed(message, nil)
	}
	return middlewares.ValidationFailed(message, fieldErrors)
}

// newValidationFieldError 生成单个字段的校验失败描述。
func newValidationFieldError(field string, reason string) validationFieldError {
	return validationFieldError{Field: field, Reason: reason}
}
