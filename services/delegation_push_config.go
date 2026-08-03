package services

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"GoAI/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CreateDelegationPushConfig 创建或更新指定 A2A Child Task 的回调配置。
func (s *RuntimeService) CreateDelegationPushConfig(ctx context.Context, targetAgentCode, sourceAgentCode string, config DelegationPushConfig) (*DelegationPushConfig, error) {
	normalized, err := normalizeDelegationPushConfig(config)
	if err != nil {
		return nil, err
	}
	if normalized.ConfigID == "" {
		normalized.ConfigID = newPrefixedID("push")
	}
	var stored models.A2APushConfig
	err = s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		delegation, err := loadDelegationForPushConfig(tx, targetAgentCode, sourceAgentCode, normalized.TaskID)
		if err != nil {
			return err
		}
		stored = models.A2APushConfig{
			ConfigID:      normalized.ConfigID,
			TaskID:        normalized.TaskID,
			DelegationID:  delegation.DelegationID,
			SourceAgentID: delegation.SourceAgentID,
			TargetAgentID: delegation.TargetAgentID,
			CallbackURL:   normalized.CallbackURL,
			Token:         normalized.Token,
			Status:        models.A2APushStatusPending,
		}
		return tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "task_id"}, {Name: "config_id"}},
			DoUpdates: clause.Assignments(map[string]any{
				"callback_url":    stored.CallbackURL,
				"token":           stored.Token,
				"status":          models.A2APushStatusPending,
				"attempt_count":   0,
				"last_error":      "",
				"next_attempt_at": nil,
				"sent_at":         nil,
			}),
		}).Create(&stored).Error
	})
	if err != nil {
		return nil, err
	}
	return pushConfigFromModel(&stored), nil
}

// GetDelegationPushConfig 返回认证源 Agent 为指定 Child Task 注册的单个回调配置。
func (s *RuntimeService) GetDelegationPushConfig(ctx context.Context, targetAgentCode, sourceAgentCode, taskID, configID string) (*DelegationPushConfig, error) {
	if _, err := loadDelegationForPushConfig(s.database.WithContext(ctx), targetAgentCode, sourceAgentCode, taskID); err != nil {
		return nil, err
	}
	var stored models.A2APushConfig
	if err := s.database.WithContext(ctx).Where("task_id = ? AND config_id = ?", strings.TrimSpace(taskID), strings.TrimSpace(configID)).First(&stored).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errPushConfigNotFound
		}
		return nil, fmt.Errorf("loading A2A push config: %w", err)
	}
	return pushConfigFromModel(&stored), nil
}

// ListDelegationPushConfigs 返回认证源 Agent 为指定 Child Task 注册的全部回调配置。
func (s *RuntimeService) ListDelegationPushConfigs(ctx context.Context, targetAgentCode, sourceAgentCode, taskID string) ([]DelegationPushConfig, error) {
	if _, err := loadDelegationForPushConfig(s.database.WithContext(ctx), targetAgentCode, sourceAgentCode, taskID); err != nil {
		return nil, err
	}
	var stored []models.A2APushConfig
	if err := s.database.WithContext(ctx).Where("task_id = ?", strings.TrimSpace(taskID)).Order("config_id ASC").Find(&stored).Error; err != nil {
		return nil, fmt.Errorf("listing A2A push configs: %w", err)
	}
	result := make([]DelegationPushConfig, 0, len(stored))
	for i := range stored {
		result = append(result, *pushConfigFromModel(&stored[i]))
	}
	return result, nil
}

// DeleteDelegationPushConfig 删除认证源 Agent 为指定 Child Task 注册的回调配置。
func (s *RuntimeService) DeleteDelegationPushConfig(ctx context.Context, targetAgentCode, sourceAgentCode, taskID, configID string) error {
	if _, err := loadDelegationForPushConfig(s.database.WithContext(ctx), targetAgentCode, sourceAgentCode, taskID); err != nil {
		return err
	}
	result := s.database.WithContext(ctx).Where("task_id = ? AND config_id = ?", strings.TrimSpace(taskID), strings.TrimSpace(configID)).Delete(&models.A2APushConfig{})
	if result.Error != nil {
		return fmt.Errorf("deleting A2A push config: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return errPushConfigNotFound
	}
	return nil
}

func createDelegationPushConfig(tx *gorm.DB, delegation *models.Delegation, config DelegationPushConfig) error {
	if tx == nil || delegation == nil {
		return errors.New("creating A2A push config: missing transaction or delegation")
	}
	normalized, err := normalizeDelegationPushConfig(config)
	if err != nil {
		return err
	}
	if normalized.TaskID != delegation.ChildRunID {
		return fmt.Errorf("%w: push config task_id does not match child run", errInvalidDelegation)
	}
	if normalized.ConfigID == "" {
		normalized.ConfigID = delegation.DelegationID
	}
	stored := &models.A2APushConfig{
		ConfigID:      normalized.ConfigID,
		TaskID:        normalized.TaskID,
		DelegationID:  delegation.DelegationID,
		SourceAgentID: delegation.SourceAgentID,
		TargetAgentID: delegation.TargetAgentID,
		CallbackURL:   normalized.CallbackURL,
		Token:         normalized.Token,
		Status:        models.A2APushStatusPending,
	}
	if err := tx.Create(stored).Error; err != nil {
		return fmt.Errorf("creating A2A push config: %w", err)
	}
	return nil
}

func loadDelegationForPushConfig(database *gorm.DB, targetAgentCode, sourceAgentCode, taskID string) (*models.Delegation, error) {
	targetCode := strings.TrimSpace(targetAgentCode)
	sourceCode := strings.TrimSpace(sourceAgentCode)
	taskID = strings.TrimSpace(taskID)
	if targetCode == "" || taskID == "" {
		return nil, fmt.Errorf("%w: target_agent_code and task_id are required", errInvalidDelegation)
	}
	var delegation models.Delegation
	query := database.Table("delegations AS d").
		Select("d.*").
		Joins("JOIN agents AS target ON target.id = d.target_agent_id AND target.agent_code = ?", targetCode).
		Where("d.child_run_id = ?", taskID)
	if sourceCode != "" {
		query = query.Joins("JOIN agents AS source ON source.id = d.source_agent_id AND source.agent_code = ?", sourceCode)
	}
	if err := query.First(&delegation).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			var task models.Delegation
			if lookupErr := database.Where("child_run_id = ?", taskID).First(&task).Error; lookupErr == nil {
				return nil, errDelegationForbidden
			}
			return nil, errDelegationNotFound
		}
		return nil, fmt.Errorf("loading delegation for A2A push config: %w", err)
	}
	return &delegation, nil
}

func normalizeDelegationPushConfig(config DelegationPushConfig) (DelegationPushConfig, error) {
	config.ConfigID = strings.TrimSpace(config.ConfigID)
	config.TaskID = strings.TrimSpace(config.TaskID)
	config.CallbackURL = strings.TrimSpace(config.CallbackURL)
	config.Token = strings.TrimSpace(config.Token)
	if config.TaskID == "" || config.CallbackURL == "" {
		return DelegationPushConfig{}, fmt.Errorf("%w: push config task_id and callback_url are required", errInvalidDelegation)
	}
	if len(config.ConfigID) > 64 || len(config.TaskID) > 64 || len(config.CallbackURL) > 2048 || len(config.Token) > 256 {
		return DelegationPushConfig{}, fmt.Errorf("%w: push config field exceeds maximum length", errInvalidDelegation)
	}
	parsed, err := url.Parse(config.CallbackURL)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" || parsed.User != nil {
		return DelegationPushConfig{}, fmt.Errorf("%w: callback_url must be an absolute URL without user info", errInvalidDelegation)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "https":
	case "http":
		if !isLoopbackCallbackHost(parsed.Hostname()) {
			return DelegationPushConfig{}, fmt.Errorf("%w: remote callback_url must use HTTPS", errInvalidDelegation)
		}
	default:
		return DelegationPushConfig{}, fmt.Errorf("%w: unsupported callback_url scheme", errInvalidDelegation)
	}
	return config, nil
}

func isLoopbackCallbackHost(host string) bool {
	if strings.EqualFold(strings.TrimSpace(host), "localhost") {
		return true
	}
	ip := net.ParseIP(strings.TrimSpace(host))
	return ip != nil && ip.IsLoopback()
}

func pushConfigFromModel(config *models.A2APushConfig) *DelegationPushConfig {
	if config == nil {
		return nil
	}
	return &DelegationPushConfig{
		ConfigID:    config.ConfigID,
		TaskID:      config.TaskID,
		CallbackURL: config.CallbackURL,
		Token:       config.Token,
	}
}
