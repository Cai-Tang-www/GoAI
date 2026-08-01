package handlers

import (
	"log"
	"net/http"

	"GoAI/ai"
	"GoAI/middlewares"
	"GoAI/services"

	"github.com/gin-gonic/gin"
)

// ChatHandler handles the provider debugging endpoint with an injected chat service.
type ChatHandler struct {
	service *services.ChatService
}

// NewChatHandler creates a chat handler bound to one application chat service.
func NewChatHandler(service *services.ChatService) *ChatHandler {
	return &ChatHandler{service: service}
}

// Chat keeps the legacy package-level entrypoint for existing integrations.
func Chat(c *gin.Context) {
	NewChatHandler(nil).Serve(c)
}

// Serve handles the streaming chat debugging endpoint.
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

	var streamErr error
	var stream *ai.ChatStream
	if h != nil && h.service != nil {
		stream, streamErr = h.service.Chat(c.Request.Context(), req.Messages, req.Provider, req.Model)
	} else {
		stream, streamErr = services.Chat(c.Request.Context(), req.Messages, req.Provider, req.Model)
	}
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
