package models

import "time"

const (
	AgentCapabilityTypeWorkflow = "workflow"
	// AgentCapabilityTypeRemote 表示由外部 A2A Agent 执行、由 Registry 受控发现的能力。
	AgentCapabilityTypeRemote = "remote"
	AgentCapabilityTypeTool   = "tool"
	AgentCapabilityTypeCustom = "custom"
)

const (
	AgentCapabilityStatusActive   = "active"
	AgentCapabilityStatusInactive = "inactive"
)

// AgentCapability 描述 Agent 对 Runtime 和其他 Agent 暴露的可发现业务能力。
type AgentCapability struct {
	ID               uint64    `gorm:"primaryKey;autoIncrement"`
	AgentID          uint64    `gorm:"not null;uniqueIndex:idx_agent_capability_unique,priority:1;index"`
	CapabilityCode   string    `gorm:"size:64;not null;uniqueIndex:idx_agent_capability_unique,priority:2"`
	Name             string    `gorm:"size:128;not null"`
	Description      string    `gorm:"type:text"`
	CapabilityType   string    `gorm:"size:32;not null;index"`
	WorkflowID       *uint64   `gorm:"index"`
	Version          string    `gorm:"size:32;not null"`
	InputSchemaJSON  string    `gorm:"type:json"`
	OutputSchemaJSON string    `gorm:"type:json"`
	ConfigJSON       string    `gorm:"type:json"`
	Status           string    `gorm:"size:20;not null;index"`
	CreatedAt        time.Time `gorm:"autoCreateTime"`
	UpdatedAt        time.Time `gorm:"autoUpdateTime"`
}
