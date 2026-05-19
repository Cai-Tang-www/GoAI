package models

import "time"

const (
	RunStepStatusPending = "pending"
	RunStepStatusRunning = "running"
	RunStepStatusSuccess = "success"
	RunStepStatusFailed  = "failed"
	RunStepStatusSkipped = "skipped"
)

// RunStep 记录 Run 中每个节点（含重试 attempt）的执行结果。
type RunStep struct {
	ID           uint64 `gorm:"primaryKey;autoIncrement"`
	RunID        string `gorm:"size:64;not null;index:idx_run_steps_run_key_attempt,priority:1"`
	StepKey      string `gorm:"size:128;not null;index:idx_run_steps_run_key_attempt,priority:2"`
	StepType     string `gorm:"size:64;not null"`
	Attempt      int    `gorm:"not null;index:idx_run_steps_run_key_attempt,priority:3"`
	Status       string `gorm:"size:20;not null;index"`
	InputJSON    string `gorm:"type:json"`
	OutputJSON   string `gorm:"type:json"`
	Provider     string `gorm:"size:64"`
	Model        string `gorm:"size:128"`
	LatencyMS    int64  `gorm:"not null;default:0"`
	ErrorMessage string `gorm:"type:text"`
	StartedAt    *time.Time
	FinishedAt   *time.Time
	CreatedAt    time.Time `gorm:"autoCreateTime"`
}
