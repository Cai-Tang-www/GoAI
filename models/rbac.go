package models

import "time"

const (
	RoleAdmin  = "admin"
	RoleMember = "member"
)

const (
	PermissionRunCreate      = "run:create"
	PermissionRunRead        = "run:read"
	PermissionRunReplay      = "run:replay"
	PermissionUserReadSelf   = "user:read_self"
	PermissionUserUpdateSelf = "user:update_self"
	PermissionUserManage     = "user:manage"
	PermissionChatUse        = "chat:use"
)

type Role struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement"`
	Name        string    `gorm:"size:64;uniqueIndex;not null"`
	Description string    `gorm:"type:text"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
	UpdatedAt   time.Time `gorm:"autoUpdateTime"`
}

type Permission struct {
	ID          uint64    `gorm:"primaryKey;autoIncrement"`
	Code        string    `gorm:"size:128;uniqueIndex;not null"`
	Description string    `gorm:"type:text"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
	UpdatedAt   time.Time `gorm:"autoUpdateTime"`
}

type UserRole struct {
	ID        uint64    `gorm:"primaryKey;autoIncrement"`
	UserID    uint64    `gorm:"not null;uniqueIndex:idx_user_role_unique,priority:1"`
	RoleID    uint64    `gorm:"not null;uniqueIndex:idx_user_role_unique,priority:2"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

type RolePermission struct {
	ID           uint64    `gorm:"primaryKey;autoIncrement"`
	RoleID       uint64    `gorm:"not null;uniqueIndex:idx_role_perm_unique,priority:1"`
	PermissionID uint64    `gorm:"not null;uniqueIndex:idx_role_perm_unique,priority:2"`
	CreatedAt    time.Time `gorm:"autoCreateTime"`
}
