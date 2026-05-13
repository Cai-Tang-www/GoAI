package handlers

import (
	"errors"
	"net/http"

	"GoAI/ai"
	"GoAI/models"
	"GoAI/services"

	"github.com/gin-gonic/gin"
)

// CreateTask 创建任务
func CreateTask(c *gin.Context) {
	var task models.Task
	if err := c.ShouldBindJSON(&task); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// 调用服务层创建任务
	if err := services.CreateTask(c, &task); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "任务创建成功"})
}

// Chat 处理聊天请求
func Chat(c *gin.Context) {
	// 1. 解析请求体（前端传的消息列表）
	var req struct {
		Messages []ai.Message `json:"messages"`
		Model    string       `json:"model"`
		Provider string       `json:"provider"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误"})
		return
	}

	// 2. 调用业务层
	stream, err := services.Chat(c.Request.Context(), req.Messages, req.Provider, req.Model)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, ai.ErrProviderNotFound) || errors.Is(err, ai.ErrDriverNotFound) ||
			errors.Is(err, ai.ErrInvalidProviderInput) || errors.Is(err, ai.ErrModelNotConfigured) {
			status = http.StatusBadRequest
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}

	// 流式响应给前端（SSE 格式）
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")

	// 逐字流式输出
	chunks := stream.Chunks
	errs := stream.Errs
	for chunks != nil || errs != nil {
		select {
		case content, ok := <-chunks:
			if !ok {
				chunks = nil
				continue
			}
			_, _ = c.Writer.WriteString("data: " + content + "\n\n")
			c.Writer.Flush() // 强制刷新到客户端
		case streamErr, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if streamErr != nil {
				_, _ = c.Writer.WriteString("event: error\ndata: " + streamErr.Error() + "\n\n")
				c.Writer.Flush()
				return
			}
		}
	}
}
