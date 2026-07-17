package handlers

import (
	"GoAI/middlewares"
	"encoding/json"
	"time"

	"github.com/gin-gonic/gin"
)

type userPayload struct {
	ID        uint      `json:"id"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// buildUserPayload 生成单个用户的安全响应对象。
func buildUserPayload(id uint, username, email string, createdAt, updatedAt time.Time) userPayload {
	return userPayload{
		ID:        id,
		Username:  username,
		Email:     email,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
}

// writeSSEEnvelope 将统一 envelope 以 SSE 事件格式写回客户端。
func writeSSEEnvelope(c *gin.Context, event string, code string, message string, data any) error {
	payload, err := json.Marshal(middlewares.ResponseEnvelope{
		Code:    code,
		Message: message,
		Data:    data,
		TraceID: middlewares.TraceID(c),
	})
	if err != nil {
		return err
	}
	if _, err := c.Writer.WriteString("event: " + event + "\n"); err != nil {
		return err
	}
	if _, err := c.Writer.WriteString("data: " + string(payload) + "\n\n"); err != nil {
		return err
	}
	c.Writer.Flush()
	return nil
}
