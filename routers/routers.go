package routers

import (
	"GoAI/models"
	"net/http"

	"GoAI/handlers"
	"GoAI/middlewares"

	"github.com/gin-gonic/gin"
)

// InitRouter 初始化所有 API 路由
func InitRouter() *gin.Engine { //Engine rounter group
	router := gin.New()
	router.Use(
		middlewares.TraceMiddleware(),
		middlewares.RequestLogMiddleware(),
		middlewares.ErrorHandlingMiddleware(),
	)

	// 健康检查路由
	router.GET("/ping", func(c *gin.Context) {
		middlewares.Success(c, http.StatusOK, gin.H{"message": "ping success"}, "success")
	})

	// 用户认证相关的路由 (例如注册、登录，这些通常不需要 JWT 认证)
	userAuthGroup := router.Group("/auth")
	{
		userAuthGroup.POST("/register", handlers.RegisterUser)

		userAuthGroup.POST("/login", handlers.LoginUser)
	}

	// 需要 JWT 认证的 API 路由组
	apiGroup := router.Group("/api")
	apiGroup.Use(middlewares.JWTAuthMiddleware(), middlewares.RBACContextMiddleware()) // 认证 + RBAC 上下文
	{
		// 示例受保护路由
		apiGroup.GET("/protected", func(c *gin.Context) {
			userID, exists := c.Get("user_id")
			if !exists {
				middlewares.AbortWithError(c, middlewares.InternalError("internal error", nil))
				return
			}
			middlewares.Success(c, http.StatusOK, gin.H{
				"message": "你已通过认证",
				"user_id": userID,
			}, "success")
		})
		apiGroup.POST("/chat", middlewares.RequirePermission(models.PermissionChatUse), handlers.Chat)
		apiGroup.POST("/runs", middlewares.RequirePermission(models.PermissionRunCreate), handlers.CreateRun)
		apiGroup.GET("/runs/:run_id", middlewares.RequirePermission(models.PermissionRunRead), handlers.GetRun)
		apiGroup.GET("/runs/:run_id/steps", middlewares.RequirePermission(models.PermissionRunRead), handlers.ListRunSteps)
		apiGroup.POST("/runs/:run_id/replay", middlewares.RequirePermission(models.PermissionRunReplay), handlers.ReplayRun)

		apiGroup.POST("/users", middlewares.RequirePermission(models.PermissionUserManage), handlers.CreateUser)
		apiGroup.GET("/users", middlewares.RequirePermission(models.PermissionUserManage), handlers.ListUsers)
		apiGroup.GET("/users/:id",
			middlewares.RequireSelfOrPermission("id", models.PermissionUserReadSelf, models.PermissionUserManage),
			handlers.GetUserByID,
		)
		apiGroup.PUT("/users/:id",
			middlewares.RequireSelfOrPermission("id", models.PermissionUserUpdateSelf, models.PermissionUserManage),
			handlers.UpdateUser,
		)
		apiGroup.DELETE("/users/:id", middlewares.RequirePermission(models.PermissionUserManage), handlers.DeleteUser)
	}

	return router
}
