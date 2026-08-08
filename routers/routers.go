package routers

import (
	"context"
	"fmt"
	"net/http"

	"GoAI/governance"
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
	Database         *gorm.DB
	RunService       *services.RunService
	ChatService      *services.ChatService
	AgentRegistry    *services.AgentRegistryService
	MCPRegistry      *services.MCPRegistryService
	Runtime          services.Runtime
	A2AGateway       http.Handler
	Observability    *observability.Bundle
	Governance       *governance.Service
	GovernanceScopes []string
	StreamShutdown   context.Context
}

// New 使用显式依赖创建完整 API 路由。
func New(deps Dependencies) (*gin.Engine, error) {
	if deps.Database == nil {
		return nil, fmt.Errorf("creating router: database is nil")
	}
	if deps.RunService == nil {
		return nil, fmt.Errorf("creating router: run service is nil")
	}
	if deps.ChatService == nil {
		return nil, fmt.Errorf("creating router: chat service is nil")
	}
	if deps.AgentRegistry == nil {
		return nil, fmt.Errorf("creating router: agent registry service is nil")
	}
	if deps.MCPRegistry == nil {
		return nil, fmt.Errorf("creating router: MCP registry service is nil")
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
	chatHandler := handlers.NewChatHandler(deps.ChatService)
	agentRegistryHandler := handlers.NewAgentRegistryHandler(deps.AgentRegistry)
	mcpRegistryHandler := handlers.NewMCPRegistryHandler(deps.MCPRegistry)
	aguiHandler, err := handlers.NewAGUIHandler(deps.Runtime)
	if err != nil {
		return nil, fmt.Errorf("creating AG-UI handler: %w", err)
	}

	router := gin.New()
	if err := router.SetTrustedProxies(nil); err != nil {
		return nil, fmt.Errorf("configuring trusted proxies: %w", err)
	}
	router.Use(
		middlewares.TraceMiddleware(),
		middlewares.ErrorHandlingMiddleware(),
		observability.HTTPMiddleware(deps.Observability),
		middlewares.GovernanceMiddleware(deps.Governance, []string{"a2a"}),
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
	apiGroup.Use(
		middlewares.JWTAuthMiddleware(),
		middlewares.RBACContextMiddleware(deps.Database),
		middlewares.GovernanceMiddleware(deps.Governance, deps.GovernanceScopes),
	)
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
		apiGroup.POST("/chat",
			middlewares.RequirePermission(models.PermissionChatUse),
			middlewares.StreamShutdownMiddleware(deps.StreamShutdown),
			chatHandler.Serve,
		)
		apiGroup.POST("/agents/:agent_code/agui",
			middlewares.RequirePermission(models.PermissionRunCreate),
			middlewares.StreamShutdownMiddleware(deps.StreamShutdown),
			aguiHandler.RunAgent,
		)
		apiGroup.POST("/agents", middlewares.RequirePermission(models.PermissionAgentCreate), agentRegistryHandler.CreateAgent)
		apiGroup.GET("/agents", middlewares.RequirePermission(models.PermissionAgentRead), agentRegistryHandler.ListAgents)
		apiGroup.GET("/agents/:agent_code", middlewares.RequirePermission(models.PermissionAgentRead), agentRegistryHandler.GetAgent)
		apiGroup.PUT("/agents/:agent_code", middlewares.RequirePermission(models.PermissionAgentUpdate), agentRegistryHandler.UpdateAgent)
		apiGroup.POST("/agents/:agent_code/activate", middlewares.RequirePermission(models.PermissionAgentActivate), agentRegistryHandler.ActivateAgent)
		apiGroup.POST("/agents/:agent_code/deactivate", middlewares.RequirePermission(models.PermissionAgentActivate), agentRegistryHandler.DeactivateAgent)

		apiGroup.POST("/agents/:agent_code/capabilities", middlewares.RequirePermission(models.PermissionAgentUpdate), agentRegistryHandler.CreateCapability)
		apiGroup.GET("/agents/:agent_code/capabilities", middlewares.RequirePermission(models.PermissionAgentRead), agentRegistryHandler.ListCapabilities)
		apiGroup.PUT("/agents/:agent_code/capabilities/:capability_code", middlewares.RequirePermission(models.PermissionAgentUpdate), agentRegistryHandler.UpdateCapability)
		apiGroup.POST("/agents/:agent_code/capabilities/:capability_code/deactivate", middlewares.RequirePermission(models.PermissionAgentUpdate), agentRegistryHandler.DeactivateCapability)

		apiGroup.POST("/agents/:agent_code/endpoints", middlewares.RequirePermission(models.PermissionAgentUpdate), agentRegistryHandler.CreateEndpoint)
		apiGroup.GET("/agents/:agent_code/endpoints", middlewares.RequirePermission(models.PermissionAgentRead), agentRegistryHandler.ListEndpoints)
		apiGroup.PUT("/agents/:agent_code/endpoints/:endpoint_code", middlewares.RequirePermission(models.PermissionAgentUpdate), agentRegistryHandler.UpdateEndpoint)
		apiGroup.POST("/agents/:agent_code/endpoints/:endpoint_code/deactivate", middlewares.RequirePermission(models.PermissionAgentUpdate), agentRegistryHandler.DeactivateEndpoint)
		apiGroup.POST("/agents/:agent_code/endpoints/:endpoint_code/health-check", middlewares.RequirePermission(models.PermissionAgentUpdate), agentRegistryHandler.CheckEndpointHealth)

		apiGroup.POST("/mcp/servers", middlewares.RequirePermission(models.PermissionMCPCreate), mcpRegistryHandler.CreateServer)
		apiGroup.GET("/mcp/servers", middlewares.RequirePermission(models.PermissionMCPRead), mcpRegistryHandler.ListServers)
		apiGroup.GET("/mcp/servers/:server_code", middlewares.RequirePermission(models.PermissionMCPRead), mcpRegistryHandler.GetServer)
		apiGroup.PUT("/mcp/servers/:server_code", middlewares.RequirePermission(models.PermissionMCPUpdate), mcpRegistryHandler.UpdateServer)
		apiGroup.POST("/mcp/servers/:server_code/deactivate", middlewares.RequirePermission(models.PermissionMCPUpdate), mcpRegistryHandler.DeactivateServer)
		apiGroup.POST("/mcp/servers/:server_code/health-check", middlewares.RequirePermission(models.PermissionMCPUpdate), mcpRegistryHandler.CheckServerHealth)
		apiGroup.GET("/mcp/servers/:server_code/tools", middlewares.RequirePermission(models.PermissionMCPRead), mcpRegistryHandler.ListTools)

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
