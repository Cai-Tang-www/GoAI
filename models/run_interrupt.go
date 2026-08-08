package models

import "time"

const (
	RunInterruptStatusPending   = "pending"
	RunInterruptStatusResolved  = "resolved"
	RunInterruptStatusCancelled = "cancelled"
)

// RunInterrupt 持久化一个需要外部输入后才能继续的 Workflow 暂停点。
//
// Interrupt 属于 Run，而不是 HTTP 请求；因此客户端断开 SSE 后，暂停点仍然
// 可被后续 AG-UI resume 请求安全地处理。
type RunInterrupt struct {
	ID                 uint64 `gorm:"primaryKey;autoIncrement"`
	RunID              string `gorm:"size:64;not null;index:idx_run_interrupts_run_status,priority:1;uniqueIndex:uidx_run_interrupts_run_interrupt,priority:1"`
	InterruptID        string `gorm:"size:128;not null;index:idx_run_interrupts_interrupt_id;uniqueIndex:uidx_run_interrupts_run_interrupt,priority:2"`
	StepKey            string `gorm:"size:128;not null;index"`
	Reason             string `gorm:"size:128;not null"`
	Message            string `gorm:"type:text"`
	ResponseSchemaJSON string `gorm:"type:json"`
	MetadataJSON       string `gorm:"type:json"`
	ResumeNodeKey      string `gorm:"size:128"`
	Status             string `gorm:"size:20;not null;index:idx_run_interrupts_run_status,priority:2"`
	PayloadJSON        string `gorm:"type:json"`
	ResolvedAt         *time.Time
	CreatedAt          time.Time `gorm:"autoCreateTime"`
	UpdatedAt          time.Time `gorm:"autoUpdateTime"`
}
