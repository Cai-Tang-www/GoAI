package models

import "time"

const (
	RunIdempotencyOperationCreate = "create"
	RunIdempotencyOperationReplay = "replay"
)

// RunIdempotency 记录 Run 创建与 replay 的幂等映射关系。
type RunIdempotency struct {
	ID             uint64    `gorm:"primaryKey;autoIncrement"`
	OwnerUserID    uint64    `gorm:"not null;uniqueIndex:idx_run_idempotency_owner_op_key,priority:1;index"`
	Operation      string    `gorm:"size:16;not null;uniqueIndex:idx_run_idempotency_owner_op_key,priority:2"`
	IdempotencyKey string    `gorm:"size:128;not null;uniqueIndex:idx_run_idempotency_owner_op_key,priority:3"`
	RequestHash    string    `gorm:"size:64;not null"`
	RunID          string    `gorm:"size:64;index"`
	SourceRunID    string    `gorm:"size:64;index"`
	CreatedAt      time.Time `gorm:"autoCreateTime"`
	UpdatedAt      time.Time `gorm:"autoUpdateTime"`
}
