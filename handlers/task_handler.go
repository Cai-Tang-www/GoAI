package handlers

import (
	"log"
	"net/http"

	"GoAI/middlewares"
	"GoAI/services"

	"github.com/gin-gonic/gin"
)

// Chat 处理聊天请求
func Chat(c *gin.Context) {
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

	stream, err := services.Chat(c.Request.Context(), req.Messages, req.Provider, req.Model)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
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
