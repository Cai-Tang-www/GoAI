package models

import "time"

// MCPTool 是一次成功 tools/list 后持久化的可执行 Tool 快照。
type MCPTool struct {
	ID               uint64    `gorm:"primaryKey;autoIncrement"`
	ServerID         uint64    `gorm:"not null;uniqueIndex:idx_mcp_tool_server_name,priority:1;index"`
	ToolName         string    `gorm:"size:128;not null;uniqueIndex:idx_mcp_tool_server_name,priority:2"`
	Description      string    `gorm:"type:text"`
	InputSchemaJSON  string    `gorm:"type:json;not null"`
	OutputSchemaJSON string    `gorm:"type:json"`
	CreatedAt        time.Time `gorm:"autoCreateTime"`
	UpdatedAt        time.Time `gorm:"autoUpdateTime"`
}
