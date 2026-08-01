package routers

import (
	"fmt"
	"net/http"

	"GoAI/handlers"
	"GoAI/middlewares"
	"GoAI/models"
	"GoAI/observability"
	"GoAI/services"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// Dependencies 是 HTTP 路由装配所需的显式应用依赖。
type Dependencies struct {
	Database      *gorm.DB
	RunService    *services.RunService
	Runtime       services.Runtime
	A2AGateway    http.Handler
	Observability *observability.Bundle
}

// New 使用显式依赖创建完整 API 路由。
func New(deps Dependencies) (*gin.Engine, error) {
	if deps.Database == nil {
		return nil, fmt.Errorf("creating router: database is nil")
	}
	if deps.RunService == nil {
		return nil, fmt.Errorf("creating router: run service is nil")
	}
	if deps.Runtime == nil {
		return nil, fmt.Errorf("creating router: runtime is nil")
	}
	if deps.A2AGateway == nil {
		return nil, fmt.Errorf("creating router: A2A gateway is nil")
	}
	if deps.Observability == nil {
		deps.Observability = observability.NewNoop()
	}

	userHandler := handlers.NewUserHandler(deps.Database)
	runHandler := handlers.NewRunHandler(deps.RunService)
	aguiHandler, err := handlers.NewAGUIHandler(deps.Runtime)
	if err != nil {
		return nil, fmt.Errorf("creating AG-UI handler: %w", err)
	}

	router := gin.New()
	router.Use(
		middlewares.TraceMiddleware(),
		middlewares.ErrorHandlingMiddleware(),
		observability.HTTPMiddleware(deps.Observability),
	)

	router.GET("/metrics", gin.WrapH(deps.Observability.Metrics.Handler()))
	router.Any("/a2a/agents/:agent_code/*a2a_path", gin.WrapH(deps.A2AGateway))

	router.GET("/ping", func(c *gin.Context) {
		middlewares.Success(c, http.StatusOK, gin.H{"message": "ping success"}, "success")
	})

	userAuthGroup := router.Group("/auth")
	{
		userAuthGroup.POST("/register", userHandler.RegisterUser)
		userAuthGroup.POST("/login", userHandler.LoginUser)
	}

	apiGroup := router.Group("/api")
	apiGroup.Use(middlewares.JWTAuthMiddleware(), middlewares.RBACContextMiddleware(deps.Database))
	{
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
		apiGroup.POST("/agents/:agent_code/agui", middlewares.RequirePermission(models.PermissionRunCreate), aguiHandler.RunAgent)
		apiGroup.POST("/runs", middlewares.RequirePermission(models.PermissionRunCreate), runHandler.CreateRun)
		apiGroup.GET("/runs/:run_id", middlewares.RequirePermission(models.PermissionRunRead), runHandler.GetRun)
		apiGroup.GET("/runs/:run_id/steps", middlewares.RequirePermission(models.PermissionRunRead), runHandler.ListRunSteps)
		apiGroup.POST("/runs/:run_id/replay", middlewares.RequirePermission(models.PermissionRunReplay), runHandler.ReplayRun)

		apiGroup.POST("/users", middlewares.RequirePermission(models.PermissionUserManage), userHandler.CreateUser)
		apiGroup.GET("/users", middlewares.RequirePermission(models.PermissionUserManage), userHandler.ListUsers)
		apiGroup.GET("/users/:id",
			middlewares.RequireSelfOrPermission("id", models.PermissionUserReadSelf, models.PermissionUserManage),
			userHandler.GetUserByID,
		)
		apiGroup.PUT("/users/:id",
			middlewares.RequireSelfOrPermission("id", models.PermissionUserUpdateSelf, models.PermissionUserManage),
			userHandler.UpdateUser,
		)
		apiGroup.DELETE("/users/:id", middlewares.RequirePermission(models.PermissionUserManage), userHandler.DeleteUser)
	}

	return router, nil
}
