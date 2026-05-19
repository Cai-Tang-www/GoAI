package models

import "time"

const (
	AgentStatusActive   = "active"
	AgentStatusInactive = "inactive"
)

// Agent 代表一个可执行编排实体。
type Agent struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement"`
	AgentCode   string    `gorm:"size:64;uniqueIndex;not null"`
	Name        string    `gorm:"size:128;not null"`
	Description string    `gorm:"type:text"`
	OwnerUserID uint64    `gorm:"not null;index"`
	Status      string    `gorm:"size:20;not null;index"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
	UpdatedAt   time.Time `gorm:"autoUpdateTime"`
}
