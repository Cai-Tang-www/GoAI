package models

import "time"

const (
	AgentEndpointProtocolA2A = "a2a"
)

const (
	AgentEndpointTransportLocal = "local"
	AgentEndpointTransportHTTPS = "https"
)

const (
	AgentEndpointStatusActive    = "active"
	AgentEndpointStatusInactive  = "inactive"
	AgentEndpointStatusUnhealthy = "unhealthy"
)

// AgentEndpoint 描述 Agent 的协议入口；Protocol 定义协作语义，Transport 仅定义本地或远程传输方式。
type AgentEndpoint struct {
	ID            uint64 `gorm:"primaryKey;autoIncrement"`
	AgentID       uint64 `gorm:"not null;uniqueIndex:idx_agent_endpoint_unique,priority:1;index"`
	EndpointCode  string `gorm:"size:64;not null;uniqueIndex:idx_agent_endpoint_unique,priority:2"`
	Protocol      string `gorm:"size:32;not null;index"`
	Transport     string `gorm:"size:32;not null;index"`
	Address       string `gorm:"size:512;not null"`
	AuthType      string `gorm:"size:32"`
	CredentialRef string `gorm:"size:255"`
	ConfigJSON    string `gorm:"type:json"`
	Status        string `gorm:"size:20;not null;index"`
	LastHealthyAt *time.Time
	CreatedAt     time.Time `gorm:"autoCreateTime"`
	UpdatedAt     time.Time `gorm:"autoUpdateTime"`
}
