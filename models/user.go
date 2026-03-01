package models

import "gorm.io/gorm"

// User 用户模型
type User struct {
	gorm.Model
	Username string `gorm:"unique;not null" json:"username"`
	Email    string `gorm:"unique;not null" json:"email"`
	Password string `gorm:"not null" json:"-"` // 密码字段不应该被 JSON 序列化
}
