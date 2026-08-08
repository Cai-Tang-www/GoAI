package services

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"GoAI/models"

	"gorm.io/gorm"
)

// CheckEndpointHealth 使用官方 A2A Agent Card discovery 校验 Endpoint，并持久化健康状态。
func (s *AgentRegistryService) CheckEndpointHealth(ctx context.Context, actor RegistryActor, agentCode, endpointCode string) (*AgentEndpointView, error) {
	agent, err := s.loadOwnedAgent(ctx, s.database, actor, agentCode, false)
	if err != nil {
		return nil, err
	}
	endpoint, err := loadAgentEndpoint(ctx, s.database, agent.ID, strings.TrimSpace(endpointCode), false)
	if err != nil {
		return nil, err
	}
	snapshot := *endpoint
	checkErr := s.cardChecker.CheckAgentCard(ctx, AgentCardHealthCheckRequest{
		AgentCode: agent.AgentCode,
		Address:   endpoint.Address,
	})

	var updated models.AgentEndpoint
	persistErr := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		lockedAgent, err := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if err != nil {
			return err
		}
		lockedEndpoint, err := loadAgentEndpoint(ctx, tx, lockedAgent.ID, endpoint.EndpointCode, true)
		if err != nil {
			return err
		}
		if endpointConfigurationChanged(snapshot, *lockedEndpoint) {
			return fmt.Errorf("%w: endpoint changed during health check", errAgentInvalidState)
		}
		if checkErr != nil {
			if err := tx.Model(lockedEndpoint).Update("status", models.AgentEndpointStatusUnhealthy).Error; err != nil {
				return fmt.Errorf("marking endpoint unhealthy: %w", err)
			}
			lockedEndpoint.Status = models.AgentEndpointStatusUnhealthy
			if lockedAgent.Status == models.AgentStatusActive {
				var activeCount int64
				if err := tx.Model(&models.AgentEndpoint{}).
					Where("agent_id = ? AND id <> ? AND status = ?", lockedAgent.ID, lockedEndpoint.ID, models.AgentEndpointStatusActive).
					Count(&activeCount).Error; err != nil {
					return fmt.Errorf("counting healthy endpoints: %w", err)
				}
				if activeCount == 0 {
					if err := tx.Model(lockedAgent).Update("status", models.AgentStatusInactive).Error; err != nil {
						return fmt.Errorf("deactivating unreachable agent: %w", err)
					}
				}
			}
			updated = *lockedEndpoint
			return nil
		}
		now := s.now().UTC()
		if err := tx.Model(lockedEndpoint).Updates(map[string]any{
			"status":          models.AgentEndpointStatusActive,
			"last_healthy_at": now,
		}).Error; err != nil {
			return fmt.Errorf("marking endpoint healthy: %w", err)
		}
		lockedEndpoint.Status = models.AgentEndpointStatusActive
		lockedEndpoint.LastHealthyAt = &now
		updated = *lockedEndpoint
		return nil
	})
	if persistErr != nil {
		return nil, persistErr
	}
	if checkErr != nil {
		return nil, fmt.Errorf("%w: %v", errEndpointHealthCheckFailed, checkErr)
	}
	view := endpointView(updated)
	return &view, nil
}

func (s *AgentRegistryService) validatePublishState(ctx context.Context, database *gorm.DB, agent models.Agent) error {
	var capabilities []models.AgentCapability
	if err := database.WithContext(ctx).
		Where("agent_id = ? AND status = ?", agent.ID, models.AgentCapabilityStatusActive).
		Order("capability_code ASC").Find(&capabilities).Error; err != nil {
		return fmt.Errorf("validating publish capabilities: %w", err)
	}
	executableCapabilities := 0
	for _, capability := range capabilities {
		command := UpsertCapabilityCommand{
			CapabilityCode: capability.CapabilityCode, Name: capability.Name,
			Description: capability.Description, CapabilityType: capability.CapabilityType,
			WorkflowID: capability.WorkflowID, Version: capability.Version,
			InputSchemaJSON: capability.InputSchemaJSON, OutputSchemaJSON: capability.OutputSchemaJSON,
			ConfigJSON: capability.ConfigJSON, Status: capability.Status,
		}
		command, err := normalizeCapabilityCommand(command)
		if err != nil {
			return fmt.Errorf("%w: capability %s is invalid", errAgentPublishValidation, capability.CapabilityCode)
		}
		if err := validateCapabilityCommand(ctx, database, agent, command); err != nil {
			return fmt.Errorf("%w: capability %s is invalid: %v", errAgentPublishValidation, capability.CapabilityCode, err)
		}
		if capability.CapabilityType == models.AgentCapabilityTypeWorkflow {
			executableCapabilities++
		}
	}
	if executableCapabilities == 0 {
		return fmt.Errorf("%w: agent requires at least one active workflow-backed capability", errAgentPublishValidation)
	}
	if err := validatePublishedAgentMCPReferences(ctx, database, agent, capabilities); err != nil {
		return fmt.Errorf("%w: %v", errAgentPublishValidation, err)
	}

	var endpoints []models.AgentEndpoint
	if err := database.WithContext(ctx).
		Where("agent_id = ? AND protocol = ? AND status = ?", agent.ID, models.AgentEndpointProtocolA2A, models.AgentEndpointStatusActive).
		Order("endpoint_code ASC").Find(&endpoints).Error; err != nil {
		return fmt.Errorf("validating publish endpoints: %w", err)
	}
	if len(endpoints) == 0 {
		return fmt.Errorf("%w: agent requires at least one healthy active A2A endpoint", errAgentPublishValidation)
	}
	credentialRef := ""
	for _, endpoint := range endpoints {
		command := UpsertEndpointCommand{
			EndpointCode: endpoint.EndpointCode, Protocol: endpoint.Protocol,
			Transport: endpoint.Transport, Address: endpoint.Address, AuthType: endpoint.AuthType,
			CredentialRef: endpoint.CredentialRef, ConfigJSON: endpoint.ConfigJSON,
		}
		command, err := normalizeEndpointCommand(command, s.authRequired)
		if err != nil || validateEndpointCommand(command, s.authRequired) != nil {
			return fmt.Errorf("%w: endpoint %s is invalid", errAgentPublishValidation, endpoint.EndpointCode)
		}
		if command.AuthType != models.AgentEndpointAuthTypeHMACSHA256 {
			continue
		}
		if credentialRef == "" {
			credentialRef = command.CredentialRef
		} else if credentialRef != command.CredentialRef {
			return fmt.Errorf("%w: active HMAC endpoints must use one credential_ref", errAgentPublishValidation)
		}
		if s.credentials == nil {
			return fmt.Errorf("%w: credential resolver is unavailable", errAgentPublishValidation)
		}
		if _, err := s.credentials.Resolve(ctx, command.CredentialRef); err != nil {
			return fmt.Errorf("%w: endpoint credential_ref cannot be resolved", errAgentPublishValidation)
		}
	}
	return nil
}

func (s *AgentRegistryService) agentDetail(ctx context.Context, database *gorm.DB, agent models.Agent) (*AgentDetailView, error) {
	var capabilities []models.AgentCapability
	if err := database.WithContext(ctx).Where("agent_id = ?", agent.ID).Order("capability_code ASC").Find(&capabilities).Error; err != nil {
		return nil, fmt.Errorf("loading agent detail capabilities: %w", err)
	}
	var endpoints []models.AgentEndpoint
	if err := database.WithContext(ctx).Where("agent_id = ?", agent.ID).Order("endpoint_code ASC").Find(&endpoints).Error; err != nil {
		return nil, fmt.Errorf("loading agent detail endpoints: %w", err)
	}
	view := &AgentDetailView{
		AgentSummaryView: agentSummaryView(agent),
		Capabilities:     make([]AgentCapabilityView, 0, len(capabilities)),
		Endpoints:        make([]AgentEndpointView, 0, len(endpoints)),
	}
	for _, capability := range capabilities {
		view.Capabilities = append(view.Capabilities, capabilityView(capability))
	}
	for _, endpoint := range endpoints {
		view.Endpoints = append(view.Endpoints, endpointView(endpoint))
	}
	var workflow models.Workflow
	if err := database.WithContext(ctx).
		Where("agent_id = ? AND is_active = ?", agent.ID, true).
		Order("version DESC").First(&workflow).Error; err == nil {
		view.ActiveWorkflow = &ActiveWorkflowView{ID: workflow.ID, Version: workflow.Version, Checksum: workflow.Checksum}
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("loading agent active workflow: %w", err)
	}
	return view, nil
}

func agentSummaryView(agent models.Agent) AgentSummaryView {
	return AgentSummaryView{
		AgentCode: agent.AgentCode, Name: agent.Name, Description: agent.Description,
		OwnerUserID: agent.OwnerUserID, Status: agent.Status,
		CreatedAt: agent.CreatedAt, UpdatedAt: agent.UpdatedAt,
	}
}

func capabilityView(capability models.AgentCapability) AgentCapabilityView {
	return AgentCapabilityView{
		CapabilityCode: capability.CapabilityCode, Name: capability.Name,
		Description: capability.Description, CapabilityType: capability.CapabilityType,
		WorkflowID: capability.WorkflowID, Version: capability.Version,
		InputSchemaJSON: capability.InputSchemaJSON, OutputSchemaJSON: capability.OutputSchemaJSON,
		ConfigJSON: capability.ConfigJSON, Status: capability.Status,
		CreatedAt: capability.CreatedAt, UpdatedAt: capability.UpdatedAt,
	}
}

func endpointView(endpoint models.AgentEndpoint) AgentEndpointView {
	return AgentEndpointView{
		EndpointCode: endpoint.EndpointCode, Protocol: endpoint.Protocol,
		Transport: endpoint.Transport, Address: endpoint.Address, AuthType: endpoint.AuthType,
		CredentialRef: endpoint.CredentialRef, ConfigJSON: safeEndpointConfigJSON(endpoint.ConfigJSON),
		Status: endpoint.Status, LastHealthyAt: endpoint.LastHealthyAt,
		CreatedAt: endpoint.CreatedAt, UpdatedAt: endpoint.UpdatedAt,
	}
}

func endpointConfigurationChanged(before, after models.AgentEndpoint) bool {
	return before.ID != after.ID || before.AgentID != after.AgentID ||
		before.Protocol != after.Protocol || before.Transport != after.Transport ||
		before.Address != after.Address || before.AuthType != after.AuthType ||
		before.CredentialRef != after.CredentialRef || before.ConfigJSON != after.ConfigJSON ||
		before.Status != after.Status || !before.UpdatedAt.Equal(after.UpdatedAt)
}
