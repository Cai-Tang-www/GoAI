package middlewares

import (
	"context"

	"github.com/gin-gonic/gin"
)

// StreamShutdownMiddleware 将流式请求同时绑定到客户端连接和服务关闭上下文。
// 普通 HTTP 请求仍由 http.Server.Shutdown 负责 drain，不会被提前取消。
func StreamShutdownMiddleware(shutdownCtx context.Context) gin.HandlerFunc {
	return func(c *gin.Context) {
		if shutdownCtx == nil {
			c.Next()
			return
		}

		requestCtx, cancel := context.WithCancel(c.Request.Context())
		stop := context.AfterFunc(shutdownCtx, cancel)
		c.Request = c.Request.WithContext(requestCtx)
		defer func() {
			stop()
			cancel()
		}()
		c.Next()
	}
}
