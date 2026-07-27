package handlers

import (
	"errors"
	"net/http"

	"GoAI/middlewares"
	"GoAI/services"

	"github.com/gin-gonic/gin"
)

// CreateRun 处理创建 Run 请求，并在进入 service 前完成基础参数校验。
func CreateRun(c *gin.Context) {
	var req services.CreateRunRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid run payload", nil))
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

	run, err := services.CreateRun(c.Request.Context(), userID, req)
	if err != nil {
		if errors.Is(err, services.ErrRunDispatchFailed()) {
			middlewares.AbortWithError(c, middlewares.WrapError(err))
			return
		}
		middlewares.AbortWithError(c, middlewares.ValidationFailed(err.Error(), nil))
		return
	}

	middlewares.Success(c, http.StatusAccepted, services.CreateRunResponse{
		RunID:  run.RunID,
		Status: run.Status,
	}, "success")
}

// GetRun 处理 Run 详情查询并映射 service 层返回的统一错误。
func GetRun(c *gin.Context) {
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
	run, err := services.GetRunByRunID(c.Request.Context(), userID, isAdmin, runID)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}
	middlewares.Success(c, http.StatusOK, run, "success")
}

// ListRunSteps 处理 Run 步骤查询并保证 owner 与 admin 的访问语义一致。
func ListRunSteps(c *gin.Context) {
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
	steps, err := services.GetRunStepsByRunID(c.Request.Context(), userID, isAdmin, runID)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}
	middlewares.Success(c, http.StatusOK, steps, "success")
}

// ReplayRun 处理 Run 回放请求，并复用 service 层的稳定回放逻辑。
func ReplayRun(c *gin.Context) {
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
	newRun, err := services.ReplayRun(c.Request.Context(), userID, isAdmin, runID)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}
	middlewares.Success(c, http.StatusAccepted, services.CreateRunResponse{
		RunID:  newRun.RunID,
		Status: newRun.Status,
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
