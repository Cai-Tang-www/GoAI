package models

import "time"

const (
	DelegationGroupStrategyAll    = "all"
	DelegationGroupStrategyAny    = "any"
	DelegationGroupStrategyQuorum = "quorum"
)

const (
	DelegationGroupStatusWaiting   = "waiting"
	DelegationGroupStatusSucceeded = "succeeded"
	DelegationGroupStatusFailed    = "failed"
	DelegationGroupStatusCancelled = "cancelled"
)

// DelegationGroup 记录一个 Workflow agent_group 节点的 fan-out/fan-in 协调状态。
type DelegationGroup struct {
	ID                      uint64 `gorm:"primaryKey;autoIncrement"`
	GroupID                 string `gorm:"size:64;uniqueIndex;not null"`
	ThreadID                string `gorm:"size:64;not null;index"`
	ParentRunID             string `gorm:"size:64;not null;index:idx_delegation_group_parent_step,priority:1;uniqueIndex:uidx_delegation_group_parent_step,priority:1"`
	ParentStepKey           string `gorm:"size:128;not null;index:idx_delegation_group_parent_step,priority:2;uniqueIndex:uidx_delegation_group_parent_step,priority:2"`
	CoordinatorDelegationID string `gorm:"size:64;not null;uniqueIndex:uidx_delegation_group_coordinator"`
	TraceID                 string `gorm:"size:128;index"`
	LoopID                  string `gorm:"size:64;index"`
	ResumeNodeKey           string `gorm:"size:128"`
	Strategy                string `gorm:"size:16;not null;index"`
	RequiredSuccesses       int    `gorm:"not null"`
	TotalMembers            int    `gorm:"not null"`
	SucceededMembers        int    `gorm:"not null;default:0"`
	FailedMembers           int    `gorm:"not null;default:0"`
	CancelledMembers        int    `gorm:"not null;default:0"`
	Status                  string `gorm:"size:20;not null;index"`
	ResultJSON              string `gorm:"type:json;not null"`
	ErrorMessage            string `gorm:"type:text"`
	StartedAt               *time.Time
	FinishedAt              *time.Time
	CreatedAt               time.Time `gorm:"autoCreateTime"`
	UpdatedAt               time.Time `gorm:"autoUpdateTime"`
}
