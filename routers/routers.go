package routers

import (
	"net/http"

	"GoAI/handlers"
	"GoAI/middlewares"

	"github.com/gin-gonic/gin"
)

// InitRouter 初始化所有 API 路由
func InitRouter() *gin.Engine {
	router := gin.Default()

	// 健康检查路由
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	// 用户认证相关的路由 (例如注册、登录，这些通常不需要 JWT 认证)
	userAuthGroup := router.Group("/auth")
	{
		userAuthGroup.POST("/register", handlers.RegisterUser)

		userAuthGroup.POST("/login", handlers.LoginUser)
	}

	// 需要 JWT 认证的 API 路由组
	apiGroup := router.Group("/api")
	apiGroup.Use(middlewares.JWTAuthMiddleware()) // 应用 JWT 认证中间件
	{
		// 示例受保护路由
		apiGroup.GET("/protected", func(c *gin.Context) {
			userID, exists := c.Get("user_id")
			if !exists {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "无法获取用户ID"})
				return
			}
			c.JSON(http.StatusOK, gin.H{
				"message": "你已通过认证",
				"user_id": userID,
			})
		})

		apiGroup.POST("/users", handlers.CreateUser)
		apiGroup.GET("/users", handlers.ListUsers)
		apiGroup.GET("/users/:id", handlers.GetUserByID)
		apiGroup.PUT("/users/:id", handlers.UpdateUser)
		apiGroup.DELETE("/users/:id", handlers.DeleteUser)
	}

	return router
}
