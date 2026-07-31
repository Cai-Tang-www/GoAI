package models

import "time"

const (
	MessageStatusPending   = "pending"
	MessageStatusDelivered = "delivered"
	MessageStatusProcessed = "processed"
	MessageStatusFailed    = "failed"
)

const (
	MessageSenderUser    = "user"
	MessageSenderAgent   = "agent"
	MessageSenderTool    = "tool"
	MessageSenderRuntime = "runtime"
	MessageSenderSystem  = "system"
)

const (
	MessageTypeInput        = "input"
	MessageTypeDelegation   = "delegation"
	MessageTypeResult       = "result"
	MessageTypeToolResult   = "tool_result"
	MessageTypeStatusUpdate = "status_update"
	MessageTypeSystemEvent  = "system_event"
)

// Message 表示 Thread 内部的通信单元，协议网关需要先把外部消息映射为该稳定模型。
type Message struct {
	ID              uint64    `gorm:"primaryKey;autoIncrement"`
	MessageID       string    `gorm:"size:64;uniqueIndex;not null"`
	ThreadID        string    `gorm:"size:64;not null;index"`
	RunID           string    `gorm:"size:64;index"`
	DelegationID    string    `gorm:"size:64;index"`
	ParentMessageID string    `gorm:"size:64;index"`
	SenderType      string    `gorm:"size:20;not null;index"`
	SenderID        string    `gorm:"size:64;index"`
	ReceiverType    string    `gorm:"size:20;index"`
	ReceiverID      string    `gorm:"size:64;index"`
	MessageType     string    `gorm:"size:32;not null;index"`
	ContentType     string    `gorm:"size:32;not null"`
	ContentJSON     string    `gorm:"type:json;not null"`
	MetadataJSON    string    `gorm:"type:json"`
	Status          string    `gorm:"size:20;not null;index"`
	CreatedAt       time.Time `gorm:"autoCreateTime"`
	UpdatedAt       time.Time `gorm:"autoUpdateTime"`
}
