package models

import "time"

const (
	MCPServerTransportStreamableHTTP = "streamable_http"
)

const (
	MCPServerAuthTypeNone   = "none"
	MCPServerAuthTypeBearer = "bearer"
)

const (
	MCPServerStatusInactive  = "inactive"
	MCPServerStatusActive    = "active"
	MCPServerStatusUnhealthy = "unhealthy"
)

// MCPServer 描述由平台治理、通过 MCP 协议向 Agent 暴露 Tool 的服务端配置。
// 凭据只保存引用，真实 secret 必须在调用时通过 CredentialResolver 解析。
type MCPServer struct {
	ID            uint64 `gorm:"primaryKey;autoIncrement"`
	OwnerUserID   uint64 `gorm:"not null;uniqueIndex:idx_mcp_server_owner_code,priority:1;index"`
	ServerCode    string `gorm:"size:64;not null;uniqueIndex:idx_mcp_server_owner_code,priority:2"`
	Name          string `gorm:"size:128;not null"`
	Description   string `gorm:"type:text"`
	Transport     string `gorm:"size:32;not null;index"`
	Endpoint      string `gorm:"size:512;not null"`
	AuthType      string `gorm:"size:32;not null"`
	CredentialRef string `gorm:"size:255"`
	Status        string `gorm:"size:20;not null;index"`
	ConfigVersion uint64 `gorm:"not null;default:1"`
	LastError     string `gorm:"type:text"`
	LastHealthyAt *time.Time
	CreatedAt     time.Time `gorm:"autoCreateTime"`
	UpdatedAt     time.Time `gorm:"autoUpdateTime"`
}
