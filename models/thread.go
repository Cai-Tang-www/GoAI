package models

import "time"

const (
	ThreadStatusActive   = "active"
	ThreadStatusClosed   = "closed"
	ThreadStatusArchived = "archived"
)

// Thread 表示一次用户会话或多 Agent 协作上下文，是 Message 与 Run 的顶层容器。
type Thread struct {
	ID           uint64    `gorm:"primaryKey;autoIncrement"`
	ThreadID     string    `gorm:"size:64;uniqueIndex;not null"`
	OwnerUserID  uint64    `gorm:"not null;index"`
	Title        string    `gorm:"size:255"`
	Status       string    `gorm:"size:20;not null;index"`
	MetadataJSON string    `gorm:"type:json"`
	CreatedAt    time.Time `gorm:"autoCreateTime"`
	UpdatedAt    time.Time `gorm:"autoUpdateTime"`
}
