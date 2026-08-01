package handlers

import (
	"log"
	"net/http"

	"GoAI/middlewares"
	"GoAI/services"

	"github.com/gin-gonic/gin"
)

// ChatHandler 负责处理注入 ChatService 的 Provider 调试流接口。
type ChatHandler struct {
	service *services.ChatService
}

// NewChatHandler 创建绑定到单个应用 ChatService 的调试流处理器。
func NewChatHandler(service *services.ChatService) *ChatHandler {
	return &ChatHandler{service: service}
}

// Chat 保留旧入口以兼容已有路由，但不再通过全局配置隐式创建 Provider。
func Chat(c *gin.Context) {
	NewChatHandler(nil).Serve(c)
}

// Serve 处理聊天调试接口的流式响应。
func (h *ChatHandler) Serve(c *gin.Context) {
	var req chatRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid chat payload", nil))
		return
	}
	normalizeChatRequest(&req)
	if appErr := validateChatRequest(req); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}

	if h == nil || h.service == nil {
		middlewares.AbortWithError(c, middlewares.InternalError("chat service is not initialized", nil))
		return
	}
	stream, streamErr := h.service.Chat(c.Request.Context(), req.Messages, req.Provider, req.Model)
	if streamErr != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(streamErr))
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Status(http.StatusOK)

	chunks := stream.Chunks
	errs := stream.Errs
	for chunks != nil || errs != nil {
		select {
		case content, ok := <-chunks:
			if !ok {
				chunks = nil
				continue
			}
			if err := writeSSEEnvelope(c, "chunk", middlewares.CodeOK, "success", map[string]any{"content": content}); err != nil {
				log.Printf("sse chunk write failed trace_id=%s err=%v", middlewares.TraceID(c), err)
				return
			}
		case <-c.Request.Context().Done():
			log.Printf("sse stream stopped trace_id=%s reason=%v", middlewares.TraceID(c), c.Request.Context().Err())
			return
		case streamErr, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if streamErr != nil {
				appErr := middlewares.WrapError(streamErr)
				if err := writeSSEEnvelope(c, "error", appErr.Code, appErr.Message, nil); err != nil {
					log.Printf("sse error write failed trace_id=%s err=%v", middlewares.TraceID(c), err)
				}
				return
			}
		}
	}

	if err := writeSSEEnvelope(c, "done", middlewares.CodeOK, "success", map[string]any{"done": true}); err != nil {
		log.Printf("sse done write failed trace_id=%s err=%v", middlewares.TraceID(c), err)
	}
}
