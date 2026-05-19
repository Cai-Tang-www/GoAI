package models

import "time"

const (
	RunStatusPending   = "pending"
	RunStatusQueued    = "queued"
	RunStatusRunning   = "running"
	RunStatusSuccess   = "success"
	RunStatusFailed    = "failed"
	RunStatusCancelled = "cancelled"
)

// Run 代表一次工作流执行实例。
type Run struct {
	ID           uint64 `gorm:"primaryKey;autoIncrement"`
	RunID        string `gorm:"size:64;uniqueIndex;not null"`
	ThreadID     string `gorm:"size:64;index"`
	AgentID      uint64 `gorm:"not null;index"`
	WorkflowID   uint64 `gorm:"not null;index"`
	UserID       uint64 `gorm:"not null;index"`
	TriggerType  string `gorm:"size:32;not null;index"`
	InputJSON    string `gorm:"type:json;not null"`
	Status       string `gorm:"size:20;not null;index"`
	CurrentStep  string `gorm:"size:128"`
	RetryCount   int    `gorm:"not null;default:0"`
	ErrorMessage string `gorm:"type:text"`
	Provider     string `gorm:"size:64"`
	Model        string `gorm:"size:128"`
	StartedAt    *time.Time
	FinishedAt   *time.Time
	CreatedAt    time.Time `gorm:"autoCreateTime"`
	UpdatedAt    time.Time `gorm:"autoUpdateTime"`
}
