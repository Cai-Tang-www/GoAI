package models

import "time"

const (
	DelegationStatusPending   = "pending"
	DelegationStatusAccepted  = "accepted"
	DelegationStatusRunning   = "running"
	DelegationStatusSucceeded = "succeeded"
	DelegationStatusFailed    = "failed"
	DelegationStatusCancelled = "cancelled"
)

// Delegation 表示源 Agent 通过 Runtime 将子任务交给目标 Agent 的可追踪协作记录。
type Delegation struct {
	ID               uint64 `gorm:"primaryKey;autoIncrement"`
	DelegationID     string `gorm:"size:64;uniqueIndex;not null"`
	ThreadID         string `gorm:"size:64;not null;index"`
	ParentRunID      string `gorm:"size:64;not null;index"`
	ChildRunID       string `gorm:"size:64;uniqueIndex;not null"`
	SourceAgentID    uint64 `gorm:"not null;index"`
	TargetAgentID    uint64 `gorm:"not null;index"`
	CapabilityCode   string `gorm:"size:64;not null;index"`
	RequestMessageID string `gorm:"size:64;not null;index"`
	ResultMessageID  string `gorm:"size:64;index"`
	InputJSON        string `gorm:"type:json;not null"`
	OutputJSON       string `gorm:"type:json"`
	Status           string `gorm:"size:20;not null;index"`
	ErrorMessage     string `gorm:"type:text"`
	StartedAt        *time.Time
	FinishedAt       *time.Time
	CreatedAt        time.Time `gorm:"autoCreateTime"`
	UpdatedAt        time.Time `gorm:"autoUpdateTime"`
}
