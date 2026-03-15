package services

import (
	"GoAI/ai"
	"GoAI/config"
	"GoAI/db"
	"GoAI/kafka"
	"GoAI/models"
	"context"
	"errors"
)

// CreateTask 创建任务
func CreateTask(ctx context.Context, task *models.Task) error {
	// 验证任务参数
	if task.TaskID == "" {
		return errors.New("任务ID不能为空")
	}

	// 创建任务
	if err := db.DB.Create(task).Error; err != nil {
		return err
	}

	// 发送任务创建事件
	if err := kafka.SendTaskEvent(ctx, task.TaskID, "created"); err != nil {
		return err
	}

	return nil
}

// Chat 调用模型scope的聊天接口
func Chat(ctx context.Context, messages []ai.Message, model string) (<-chan string, error) {
	provider := &ai.ModelScopeProvider{
		APIKey: config.AppConfig.ModelScopeKey,
		Model:  model,
	}
	return provider.Chat(ctx, messages)
}
