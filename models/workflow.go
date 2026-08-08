package models

import "time"

// Workflow 代表 Agent 的某个版本编排定义。
type Workflow struct {
	ID             uint64    `gorm:"primaryKey;autoIncrement"`
	AgentID        uint64    `gorm:"not null;uniqueIndex:uidx_workflows_agent_version,priority:1;index"`
	Version        int       `gorm:"not null;uniqueIndex:uidx_workflows_agent_version,priority:2"`
	DefinitionJSON string    `gorm:"type:json;not null"`
	Checksum       string    `gorm:"size:128;not null"`
	IsActive       bool      `gorm:"not null;index"`
	CreatedBy      uint64    `gorm:"not null;index"`
	CreatedAt      time.Time `gorm:"autoCreateTime"`
	UpdatedAt      time.Time `gorm:"autoUpdateTime"`
}
