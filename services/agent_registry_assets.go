package services

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"GoAI/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CreateCapability 在目标 Agent 下创建默认 active 的能力资产。
func (s *AgentRegistryService) CreateCapability(ctx context.Context, actor RegistryActor, agentCode string, command UpsertCapabilityCommand) (*AgentCapabilityView, error) {
	var created models.AgentCapability
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		agent, err := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if err != nil {
			return err
		}
		command, err = normalizeCapabilityCommand(command)
		if err != nil {
			return err
		}
		if err := validateCapabilityCommand(ctx, tx, *agent, command); err != nil {
			return err
		}
		created = models.AgentCapability{
			AgentID: agent.ID, CapabilityCode: command.CapabilityCode, Name: command.Name,
			Description: command.Description, CapabilityType: command.CapabilityType,
			WorkflowID: command.WorkflowID, Version: command.Version,
			InputSchemaJSON: command.InputSchemaJSON, OutputSchemaJSON: command.OutputSchemaJSON,
			ConfigJSON: command.ConfigJSON, Status: command.Status,
		}
		if err := tx.Create(&created).Error; err != nil {
			var existing models.AgentCapability
			if lookupErr := tx.Where("agent_id = ? AND capability_code = ?", agent.ID, command.CapabilityCode).First(&existing).Error; lookupErr == nil {
				return errCapabilityAlreadyExists
			}
			return fmt.Errorf("creating agent capability: %w", err)
		}
		if agent.Status == models.AgentStatusActive {
			return s.validatePublishState(ctx, tx, *agent)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	view := capabilityView(created)
	return &view, nil
}

// ListCapabilities 返回目标 Agent 的全部能力资产，包括 inactive 资产。
func (s *AgentRegistryService) ListCapabilities(ctx context.Context, actor RegistryActor, agentCode string) ([]AgentCapabilityView, error) {
	agent, err := s.loadOwnedAgent(ctx, s.database, actor, agentCode, false)
	if err != nil {
		return nil, err
	}
	var capabilities []models.AgentCapability
	if err := s.database.WithContext(ctx).Where("agent_id = ?", agent.ID).Order("capability_code ASC").Find(&capabilities).Error; err != nil {
		return nil, fmt.Errorf("listing agent capabilities: %w", err)
	}
	views := make([]AgentCapabilityView, 0, len(capabilities))
	for _, capability := range capabilities {
		views = append(views, capabilityView(capability))
	}
	return views, nil
}

// UpdateCapability 全量替换 Capability 配置；active Agent 的发布不变量在同一事务中重新校验。
func (s *AgentRegistryService) UpdateCapability(ctx context.Context, actor RegistryActor, agentCode, capabilityCode string, command UpsertCapabilityCommand) (*AgentCapabilityView, error) {
	command.CapabilityCode = strings.TrimSpace(capabilityCode)
	var updated models.AgentCapability
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		agent, err := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if err != nil {
			return err
		}
		capability, err := loadAgentCapability(ctx, tx, agent.ID, command.CapabilityCode, true)
		if err != nil {
			return err
		}
		command, err = normalizeCapabilityCommand(command)
		if err != nil {
			return err
		}
		if err := validateCapabilityCommand(ctx, tx, *agent, command); err != nil {
			return err
		}
		updates := map[string]any{
			"name": command.Name, "description": command.Description,
			"capability_type": command.CapabilityType, "workflow_id": command.WorkflowID,
			"version": command.Version, "input_schema_json": command.InputSchemaJSON,
			"output_schema_json": command.OutputSchemaJSON, "config_json": command.ConfigJSON,
			"status": command.Status,
		}
		if err := tx.Model(capability).Updates(updates).Error; err != nil {
			return fmt.Errorf("updating agent capability: %w", err)
		}
		capability.Name = command.Name
		capability.Description = command.Description
		capability.CapabilityType = command.CapabilityType
		capability.WorkflowID = command.WorkflowID
		capability.Version = command.Version
		capability.InputSchemaJSON = command.InputSchemaJSON
		capability.OutputSchemaJSON = command.OutputSchemaJSON
		capability.ConfigJSON = command.ConfigJSON
		capability.Status = command.Status
		if agent.Status == models.AgentStatusActive {
			if err := s.validatePublishState(ctx, tx, *agent); err != nil {
				return err
			}
		}
		updated = *capability
		return nil
	})
	if err != nil {
		return nil, err
	}
	view := capabilityView(updated)
	return &view, nil
}

// DeactivateCapability 停用能力；若会破坏 active Agent 的发布不变量则回滚。
func (s *AgentRegistryService) DeactivateCapability(ctx context.Context, actor RegistryActor, agentCode, capabilityCode string) (*AgentCapabilityView, error) {
	var updated models.AgentCapability
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		agent, err := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if err != nil {
			return err
		}
		capability, err := loadAgentCapability(ctx, tx, agent.ID, strings.TrimSpace(capabilityCode), true)
		if err != nil {
			return err
		}
		if capability.Status != models.AgentCapabilityStatusInactive {
			if err := tx.Model(capability).Update("status", models.AgentCapabilityStatusInactive).Error; err != nil {
				return fmt.Errorf("deactivating agent capability: %w", err)
			}
			capability.Status = models.AgentCapabilityStatusInactive
		}
		if agent.Status == models.AgentStatusActive {
			if err := s.validatePublishState(ctx, tx, *agent); err != nil {
				return err
			}
		}
		updated = *capability
		return nil
	})
	if err != nil {
		return nil, err
	}
	view := capabilityView(updated)
	return &view, nil
}

// CreateEndpoint 创建待健康检查的 inactive A2A Endpoint。
func (s *AgentRegistryService) CreateEndpoint(ctx context.Context, actor RegistryActor, agentCode string, command UpsertEndpointCommand) (*AgentEndpointView, error) {
	var created models.AgentEndpoint
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		agent, err := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if err != nil {
			return err
		}
		command, err = normalizeEndpointCommand(command, s.authRequired)
		if err != nil {
			return err
		}
		if err := validateEndpointCommand(command, s.authRequired); err != nil {
			return err
		}
		created = models.AgentEndpoint{
			AgentID: agent.ID, EndpointCode: command.EndpointCode, Protocol: command.Protocol,
			Transport: command.Transport, Address: command.Address, AuthType: command.AuthType,
			CredentialRef: command.CredentialRef, ConfigJSON: command.ConfigJSON,
			Status: models.AgentEndpointStatusInactive,
		}
		if err := tx.Create(&created).Error; err != nil {
			var existing models.AgentEndpoint
			if lookupErr := tx.Where("agent_id = ? AND endpoint_code = ?", agent.ID, command.EndpointCode).First(&existing).Error; lookupErr == nil {
				return errEndpointAlreadyExists
			}
			return fmt.Errorf("creating agent endpoint: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	view := endpointView(created)
	return &view, nil
}

// ListEndpoints 返回目标 Agent 的全部 A2A Endpoint 资产。
func (s *AgentRegistryService) ListEndpoints(ctx context.Context, actor RegistryActor, agentCode string) ([]AgentEndpointView, error) {
	agent, err := s.loadOwnedAgent(ctx, s.database, actor, agentCode, false)
	if err != nil {
		return nil, err
	}
	var endpoints []models.AgentEndpoint
	if err := s.database.WithContext(ctx).Where("agent_id = ?", agent.ID).Order("endpoint_code ASC").Find(&endpoints).Error; err != nil {
		return nil, fmt.Errorf("listing agent endpoints: %w", err)
	}
	views := make([]AgentEndpointView, 0, len(endpoints))
	for _, endpoint := range endpoints {
		views = append(views, endpointView(endpoint))
	}
	return views, nil
}

// UpdateEndpoint 替换 Endpoint 配置并重置为 inactive，要求重新完成 Agent Card 健康检查。
func (s *AgentRegistryService) UpdateEndpoint(ctx context.Context, actor RegistryActor, agentCode, endpointCode string, command UpsertEndpointCommand) (*AgentEndpointView, error) {
	command.EndpointCode = strings.TrimSpace(endpointCode)
	var updated models.AgentEndpoint
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		agent, err := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if err != nil {
			return err
		}
		endpoint, err := loadAgentEndpoint(ctx, tx, agent.ID, command.EndpointCode, true)
		if err != nil {
			return err
		}
		command, err = normalizeEndpointCommand(command, s.authRequired)
		if err != nil {
			return err
		}
		if err := validateEndpointCommand(command, s.authRequired); err != nil {
			return err
		}
		updates := map[string]any{
			"protocol": command.Protocol, "transport": command.Transport, "address": command.Address,
			"auth_type": command.AuthType, "credential_ref": command.CredentialRef,
			"config_json": command.ConfigJSON, "status": models.AgentEndpointStatusInactive,
			"last_healthy_at": nil,
		}
		if err := tx.Model(endpoint).Updates(updates).Error; err != nil {
			return fmt.Errorf("updating agent endpoint: %w", err)
		}
		endpoint.Protocol = command.Protocol
		endpoint.Transport = command.Transport
		endpoint.Address = command.Address
		endpoint.AuthType = command.AuthType
		endpoint.CredentialRef = command.CredentialRef
		endpoint.ConfigJSON = command.ConfigJSON
		endpoint.Status = models.AgentEndpointStatusInactive
		endpoint.LastHealthyAt = nil
		if agent.Status == models.AgentStatusActive {
			if err := s.validatePublishState(ctx, tx, *agent); err != nil {
				return err
			}
		}
		updated = *endpoint
		return nil
	})
	if err != nil {
		return nil, err
	}
	view := endpointView(updated)
	return &view, nil
}

// DeactivateEndpoint 停用 Endpoint；active Agent 必须仍保留至少一个合法 active Endpoint。
func (s *AgentRegistryService) DeactivateEndpoint(ctx context.Context, actor RegistryActor, agentCode, endpointCode string) (*AgentEndpointView, error) {
	var updated models.AgentEndpoint
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		agent, err := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if err != nil {
			return err
		}
		endpoint, err := loadAgentEndpoint(ctx, tx, agent.ID, strings.TrimSpace(endpointCode), true)
		if err != nil {
			return err
		}
		if endpoint.Status != models.AgentEndpointStatusInactive {
			if err := tx.Model(endpoint).Update("status", models.AgentEndpointStatusInactive).Error; err != nil {
				return fmt.Errorf("deactivating agent endpoint: %w", err)
			}
			endpoint.Status = models.AgentEndpointStatusInactive
		}
		if agent.Status == models.AgentStatusActive {
			if err := s.validatePublishState(ctx, tx, *agent); err != nil {
				return err
			}
		}
		updated = *endpoint
		return nil
	})
	if err != nil {
		return nil, err
	}
	view := endpointView(updated)
	return &view, nil
}

func loadAgentCapability(ctx context.Context, database *gorm.DB, agentID uint64, capabilityCode string, lock bool) (*models.AgentCapability, error) {
	if !registryCodePattern.MatchString(capabilityCode) {
		return nil, fmt.Errorf("%w: invalid capability_code", errAgentRegistryValidation)
	}
	query := database.WithContext(ctx).Where("agent_id = ? AND capability_code = ?", agentID, capabilityCode)
	if lock {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var capability models.AgentCapability
	if err := query.First(&capability).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errCapabilityNotFound
		}
		return nil, fmt.Errorf("loading agent capability: %w", err)
	}
	return &capability, nil
}

func loadAgentEndpoint(ctx context.Context, database *gorm.DB, agentID uint64, endpointCode string, lock bool) (*models.AgentEndpoint, error) {
	if !registryCodePattern.MatchString(endpointCode) {
		return nil, fmt.Errorf("%w: invalid endpoint_code", errAgentRegistryValidation)
	}
	query := database.WithContext(ctx).Where("agent_id = ? AND endpoint_code = ?", agentID, endpointCode)
	if lock {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var endpoint models.AgentEndpoint
	if err := query.First(&endpoint).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errEndpointNotFound
		}
		return nil, fmt.Errorf("loading agent endpoint: %w", err)
	}
	return &endpoint, nil
}
