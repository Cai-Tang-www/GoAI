package models

import "time"

const (
	AgentEndpointProtocolA2A = "a2a"
)

const (
	// AgentEndpointTransportHTTP 表示本地开发通过 loopback HTTP 访问统一 A2A Gateway。
	AgentEndpointTransportHTTP = "http"
	// AgentEndpointTransportHTTPS 表示远程 Agent 通过 HTTPS 访问统一 A2A Gateway。
	AgentEndpointTransportHTTPS = "https"
)

const (
	// AgentEndpointAuthTypeNone 表示仅在显式关闭 A2A 认证时允许匿名访问。
	AgentEndpointAuthTypeNone = "none"
	// AgentEndpointAuthTypeHMACSHA256 表示使用 GoAI HMAC-SHA256 机器身份签名。
	AgentEndpointAuthTypeHMACSHA256 = "goai_hmac_sha256"
)

const (
	AgentEndpointStatusActive    = "active"
	AgentEndpointStatusInactive  = "inactive"
	AgentEndpointStatusUnhealthy = "unhealthy"
)

// AgentEndpoint 描述 Agent 的 A2A 协议入口；本地开发使用 loopback HTTP，远程使用 HTTPS，二者都必须经过统一 A2A Gateway。
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
