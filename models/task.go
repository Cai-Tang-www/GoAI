package models

import (
	"time"
)

// Task 定义任务模型
type Task struct {
	ID        uint64 `gorm:"primaryKey;autoIncrement"`
	TaskID    string `gorm:"size:64;uniqueIndex;not null"`
	UserID    uint64 `gorm:"not null;index"`
	SessionID string `gorm:"size:64;index"` // 可关联对话
	Status    string `gorm:"size:20;not null;index"`
	Result    string `gorm:"type:text"`
	Error     string `gorm:"type:text"`
	CreatedAt int64  `gorm:"autoCreateTime"`
	UpdatedAt int64  `gorm:"autoUpdateTime"`
}

type DialogueMessage struct {
	ID        int64     `json:"id"`
	TaskID    int64     `json:"task_id"`
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

// 任务状态常量
const (
	TaskPending    = "Pending"
	TaskProcessing = "Processing"
	TaskCompleted  = "Completed"
	TaskFailed     = "Failed"
)
