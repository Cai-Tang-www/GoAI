package models

import "time"

const (
	A2APushStatusPending = "pending"
	A2APushStatusFailed  = "failed"
	A2APushStatusSent    = "sent"
)

// A2APushConfig 持久化 A2A Task 的 Push Notification 配置和投递恢复状态。
type A2APushConfig struct {
	ID            uint64     `gorm:"primaryKey;autoIncrement"`
	ConfigID      string     `gorm:"size:64;not null;uniqueIndex:idx_a2a_push_task_config"`
	TaskID        string     `gorm:"size:64;not null;index;uniqueIndex:idx_a2a_push_task_config"`
	DelegationID  string     `gorm:"size:64;not null;index"`
	SourceAgentID uint64     `gorm:"not null;index"`
	TargetAgentID uint64     `gorm:"not null;index"`
	CallbackURL   string     `gorm:"size:2048;not null"`
	Token         string     `gorm:"size:256"`
	Status        string     `gorm:"size:20;not null;index"`
	AttemptCount  int        `gorm:"not null;default:0"`
	LastError     string     `gorm:"type:text"`
	NextAttemptAt *time.Time `gorm:"index"`
	SentAt        *time.Time
	CreatedAt     time.Time `gorm:"autoCreateTime"`
	UpdatedAt     time.Time `gorm:"autoUpdateTime"`
}
