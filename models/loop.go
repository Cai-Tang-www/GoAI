package models

import "time"

const (
	LoopTypeRun        = "run"
	LoopTypeStep       = "step"
	LoopTypeDelegation = "delegation"
)

const (
	LoopStatusRunning   = "running"
	LoopStatusSuccess   = "success"
	LoopStatusFailed    = "failed"
	LoopStatusCancelled = "cancelled"
)

const (
	EvaluationStatusPending = "pending"
	EvaluationStatusRunning = "running"
	EvaluationStatusSuccess = "success"
	EvaluationStatusFailed  = "failed"
)

// LoopRecord 记录一次可观测执行片段，并通过 TraceID、RunID 和父 Loop 关联完整链路。
type LoopRecord struct {
	ID                 uint64  `gorm:"primaryKey;autoIncrement"`
	LoopID             string  `gorm:"size:64;uniqueIndex;not null"`
	TraceID            string  `gorm:"size:128;index"`
	ThreadID           string  `gorm:"size:64;index"`
	RunID              string  `gorm:"size:64;not null;index"`
	ParentLoopID       string  `gorm:"size:64;index"`
	DelegationID       string  `gorm:"size:64;index"`
	AgentID            uint64  `gorm:"not null;index"`
	WorkflowID         *uint64 `gorm:"index"`
	RunStepID          *uint64 `gorm:"index"`
	LoopType           string  `gorm:"size:32;not null;index"`
	Status             string  `gorm:"size:20;not null;index"`
	InputSnapshotJSON  string  `gorm:"type:json;not null"`
	OutputSnapshotJSON string  `gorm:"type:json"`
	PromptVersion      string  `gorm:"size:128"`
	Provider           string  `gorm:"size:64"`
	Model              string  `gorm:"size:128"`
	InputTokens        *int64  `gorm:"default:null"`
	OutputTokens       *int64  `gorm:"default:null"`
	TotalTokens        *int64  `gorm:"default:null"`
	LatencyMS          int64   `gorm:"not null;default:0"`
	ErrorCode          string  `gorm:"size:64"`
	ErrorMessage       string  `gorm:"type:text"`
	StartedAt          *time.Time
	FinishedAt         *time.Time
	CreatedAt          time.Time `gorm:"autoCreateTime"`
	UpdatedAt          time.Time `gorm:"autoUpdateTime"`
}

// LoopEvaluation 预留 Loop 的异步评估结果，评估失败不改变原始 Loop 终态。
type LoopEvaluation struct {
	ID            uint64    `gorm:"primaryKey;autoIncrement"`
	LoopID        string    `gorm:"size:64;not null;uniqueIndex:idx_loop_evaluation_unique,priority:1"`
	EvaluatorCode string    `gorm:"size:128;not null;uniqueIndex:idx_loop_evaluation_unique,priority:2"`
	Status        string    `gorm:"size:20;not null;index"`
	Score         *float64  `gorm:"default:null"`
	ResultJSON    string    `gorm:"type:json"`
	ErrorMessage  string    `gorm:"type:text"`
	CreatedAt     time.Time `gorm:"autoCreateTime"`
	UpdatedAt     time.Time `gorm:"autoUpdateTime"`
}
