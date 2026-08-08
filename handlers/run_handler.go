package handlers

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"GoAI/middlewares"
	"GoAI/services"

	"github.com/gin-gonic/gin"
)

const idempotencyKeyHeader = "Idempotency-Key"

// RunHandler 负责把 Run HTTP 请求映射到显式注入的 RunService。
type RunHandler struct {
	service *services.RunService
	runtime services.Runtime
}

// NewRunHandler 创建 Run 接口处理器。
func NewRunHandler(service *services.RunService, runtime services.Runtime) *RunHandler {
	return &RunHandler{service: service, runtime: runtime}
}

// CreateRun 处理创建 Run 请求，并在进入 service 前完成基础参数校验。
func (h *RunHandler) CreateRun(c *gin.Context) {
	var req services.CreateRunRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid run payload", nil))
		return
	}
	req.IdempotencyKey = c.GetHeader(idempotencyKeyHeader)
	if appErr := validateIdempotencyKey(req.IdempotencyKey); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}
	if appErr := validateCreateRunRequest(&req); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}

	userID, _, ok := authPrincipal(c)
	if !ok {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidToken())
		return
	}

	result, err := h.service.CreateRun(c.Request.Context(), userID, req)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}

	status := http.StatusAccepted
	if result.IdempotentHit {
		status = http.StatusOK
	}
	middlewares.Success(c, status, services.CreateRunResponse{
		RunID:  result.Run.RunID,
		Status: result.Run.Status,
	}, "success")
}

// GetRun 处理 Run 详情查询并映射 service 层返回的统一错误。
func (h *RunHandler) GetRun(c *gin.Context) {
	userID, isAdmin, ok := authPrincipal(c)
	if !ok {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidToken())
		return
	}
	runID := c.Param("run_id")
	if appErr := validateRunIDParam(runID); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}
	run, err := h.service.GetRunDetailByRunID(c.Request.Context(), userID, isAdmin, runID)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}
	middlewares.Success(c, http.StatusOK, run, "success")
}

// ListRunSteps 处理 Run 步骤查询并保证 owner 与 admin 的访问语义一致。
func (h *RunHandler) ListRunSteps(c *gin.Context) {
	userID, isAdmin, ok := authPrincipal(c)
	if !ok {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidToken())
		return
	}
	runID := c.Param("run_id")
	if appErr := validateRunIDParam(runID); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}
	steps, err := h.service.GetRunStepsByRunID(c.Request.Context(), userID, isAdmin, runID)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}
	middlewares.Success(c, http.StatusOK, steps, "success")
}

// GetRunTrace 返回 Run 及其可访问协作后代的 Trace/Loop 只读快照。
func (h *RunHandler) GetRunTrace(c *gin.Context) {
	userID, isAdmin, ok := authPrincipal(c)
	if !ok {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidToken())
		return
	}
	runID := c.Param("run_id")
	if appErr := validateRunIDParam(runID); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}
	trace, err := h.service.GetRunTrace(c.Request.Context(), userID, isAdmin, runID)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}
	middlewares.Success(c, http.StatusOK, trace, "success")
}

// ListRunLoops 返回当前 Run 的 Loop 列表。
func (h *RunHandler) ListRunLoops(c *gin.Context) {
	userID, isAdmin, ok := authPrincipal(c)
	if !ok {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidToken())
		return
	}
	runID := c.Param("run_id")
	if appErr := validateRunIDParam(runID); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}
	loops, err := h.service.GetRunLoops(c.Request.Context(), userID, isAdmin, runID)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}
	middlewares.Success(c, http.StatusOK, loops, "success")
}

// GetLoop 返回单个 Loop 及其评估结果。
func (h *RunHandler) GetLoop(c *gin.Context) {
	userID, isAdmin, ok := authPrincipal(c)
	if !ok {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidToken())
		return
	}
	loopID := c.Param("loop_id")
	if appErr := validateResourceIDParam("loop_id", loopID); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}
	loop, err := h.service.GetLoopDetail(c.Request.Context(), userID, isAdmin, loopID)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}
	middlewares.Success(c, http.StatusOK, loop, "success")
}

// ListLoopEvaluations 返回单个 Loop 的异步评估记录。
func (h *RunHandler) ListLoopEvaluations(c *gin.Context) {
	userID, isAdmin, ok := authPrincipal(c)
	if !ok {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidToken())
		return
	}
	loopID := c.Param("loop_id")
	if appErr := validateResourceIDParam("loop_id", loopID); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}
	evaluations, err := h.service.GetLoopEvaluations(c.Request.Context(), userID, isAdmin, loopID)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}
	middlewares.Success(c, http.StatusOK, evaluations, "success")
}

// ReplayRun 处理 Run 回放请求，并复用 service 层的稳定回放逻辑。
func (h *RunHandler) ReplayRun(c *gin.Context) {
	userID, isAdmin, ok := authPrincipal(c)
	if !ok {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidToken())
		return
	}
	runID := c.Param("run_id")
	if appErr := validateRunIDParam(runID); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}
	idempotencyKey := c.GetHeader(idempotencyKeyHeader)
	if appErr := validateIdempotencyKey(idempotencyKey); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}
	result, err := h.service.ReplayRun(c.Request.Context(), userID, isAdmin, runID, idempotencyKey)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}
	status := http.StatusAccepted
	if result.IdempotentHit {
		status = http.StatusOK
	}
	middlewares.Success(c, status, services.CreateRunResponse{
		RunID:  result.Run.RunID,
		Status: result.Run.Status,
	}, "success")
}

// ReplayThread 基于 Thread 的持久化消息历史创建新的 replay Run。
func (h *RunHandler) ReplayThread(c *gin.Context) {
	userID, isAdmin, ok := authPrincipal(c)
	if !ok {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidToken())
		return
	}
	threadID := c.Param("thread_id")
	if appErr := validateResourceIDParam("thread_id", threadID); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}

	var payload struct {
		SourceRunID string `json:"source_run_id"`
	}
	if err := c.ShouldBindJSON(&payload); err != nil && !errors.Is(err, io.EOF) {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid thread replay payload", nil))
		return
	}
	payload.SourceRunID = strings.TrimSpace(payload.SourceRunID)
	if payload.SourceRunID != "" {
		if appErr := validateResourceIDParam("source_run_id", payload.SourceRunID); appErr != nil {
			middlewares.AbortWithError(c, appErr)
			return
		}
	}
	idempotencyKey := c.GetHeader(idempotencyKeyHeader)
	if appErr := validateIdempotencyKey(idempotencyKey); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}
	if h.runtime == nil {
		middlewares.AbortWithError(c, middlewares.InternalError("thread replay runtime is unavailable", nil))
		return
	}

	result, err := h.runtime.ReplayThread(c.Request.Context(), services.ThreadReplayCommand{
		OwnerUserID:    userID,
		IsAdmin:        isAdmin,
		ThreadID:       threadID,
		SourceRunID:    payload.SourceRunID,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}
	status := http.StatusAccepted
	if result.IdempotentHit {
		status = http.StatusOK
	}
	middlewares.Success(c, status, services.CreateRunResponse{
		RunID:  result.Run.RunID,
		Status: result.Run.Status,
	}, "success")
}

// authPrincipal 统一提取当前请求用户 ID 与 admin 标记，供 Run 权限判断复用。
func authPrincipal(c *gin.Context) (uint64, bool, bool) {
	rawUserID, exists := c.Get("user_id")
	if !exists {
		return 0, false, false
	}
	userID, ok := rawUserID.(uint)
	if !ok {
		return 0, false, false
	}
	return uint64(userID), middlewares.IsAdmin(c), true
}
