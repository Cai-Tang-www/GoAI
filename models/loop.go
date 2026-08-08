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
	ID                 uint64     `gorm:"primaryKey;autoIncrement" json:"id"`
	LoopID             string     `gorm:"size:64;uniqueIndex;not null" json:"loop_id"`
	TraceID            string     `gorm:"size:128;index" json:"trace_id"`
	ThreadID           string     `gorm:"size:64;index" json:"thread_id"`
	RunID              string     `gorm:"size:64;not null;index" json:"run_id"`
	ParentLoopID       string     `gorm:"size:64;index" json:"parent_loop_id"`
	DelegationID       string     `gorm:"size:64;index" json:"delegation_id"`
	AgentID            uint64     `gorm:"not null;index" json:"agent_id"`
	WorkflowID         *uint64    `gorm:"index" json:"workflow_id"`
	RunStepID          *uint64    `gorm:"index" json:"run_step_id"`
	LoopType           string     `gorm:"size:32;not null;index" json:"loop_type"`
	Status             string     `gorm:"size:20;not null;index" json:"status"`
	InputSnapshotJSON  string     `gorm:"type:json;not null" json:"input_snapshot_json"`
	OutputSnapshotJSON string     `gorm:"type:json" json:"output_snapshot_json"`
	PromptVersion      string     `gorm:"size:128" json:"prompt_version"`
	Provider           string     `gorm:"size:64" json:"provider"`
	Model              string     `gorm:"size:128" json:"model"`
	InputTokens        *int64     `gorm:"default:null" json:"input_tokens"`
	OutputTokens       *int64     `gorm:"default:null" json:"output_tokens"`
	TotalTokens        *int64     `gorm:"default:null" json:"total_tokens"`
	LatencyMS          int64      `gorm:"not null;default:0" json:"latency_ms"`
	ErrorCode          string     `gorm:"size:64" json:"error_code"`
	ErrorMessage       string     `gorm:"type:text" json:"error_message"`
	StartedAt          *time.Time `json:"started_at"`
	FinishedAt         *time.Time `json:"finished_at"`
	CreatedAt          time.Time  `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt          time.Time  `gorm:"autoUpdateTime" json:"updated_at"`
}

// LoopEvaluation 预留 Loop 的异步评估结果，评估失败不改变原始 Loop 终态。
type LoopEvaluation struct {
	ID            uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	LoopID        string    `gorm:"size:64;not null;uniqueIndex:idx_loop_evaluation_unique,priority:1" json:"loop_id"`
	EvaluatorCode string    `gorm:"size:128;not null;uniqueIndex:idx_loop_evaluation_unique,priority:2" json:"evaluator_code"`
	Status        string    `gorm:"size:20;not null;index" json:"status"`
	Score         *float64  `gorm:"default:null" json:"score"`
	ResultJSON    string    `gorm:"type:json" json:"result_json"`
	ErrorMessage  string    `gorm:"type:text" json:"error_message"`
	CreatedAt     time.Time `gorm:"autoCreateTime" json:"created_at"`
	UpdatedAt     time.Time `gorm:"autoUpdateTime" json:"updated_at"`
}
