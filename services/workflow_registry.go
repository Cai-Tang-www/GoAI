package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"GoAI/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CreateWorkflowCommand 描述为 Agent 创建一个新的 Workflow 版本。
type CreateWorkflowCommand struct {
	Version    int             `json:"version"`
	Definition json.RawMessage `json:"definition"`
}

// UpdateWorkflowCommand 描述替换一个 inactive Workflow 版本的定义。
type UpdateWorkflowCommand struct {
	Definition json.RawMessage `json:"definition"`
}

// WorkflowCapabilityReferenceView 描述引用当前 Workflow 的 Capability。
type WorkflowCapabilityReferenceView struct {
	CapabilityCode string `json:"capability_code"`
	Version        string `json:"version"`
	Status         string `json:"status"`
}

// WorkflowView 是 Workflow 管理 API 的稳定返回视图。
type WorkflowView struct {
	ID           uint64                            `json:"id"`
	AgentCode    string                            `json:"agent_code"`
	Version      int                               `json:"version"`
	Definition   json.RawMessage                   `json:"definition"`
	Checksum     string                            `json:"checksum"`
	IsActive     bool                              `json:"is_active"`
	CreatedBy    uint64                            `json:"created_by"`
	Capabilities []WorkflowCapabilityReferenceView `json:"capabilities"`
	CreatedAt    time.Time                         `json:"created_at"`
	UpdatedAt    time.Time                         `json:"updated_at"`
}

// CreateWorkflow 创建一个 inactive Workflow 版本；跨 Agent 调用仍由运行时通过 A2A 完成。
func (s *AgentRegistryService) CreateWorkflow(ctx context.Context, actor RegistryActor, agentCode string, command CreateWorkflowCommand) (*WorkflowView, error) {
	normalizedDefinition, checksum, err := normalizeWorkflowDefinition(command.Definition)
	if err != nil {
		return nil, err
	}
	if command.Version <= 0 {
		return nil, fmt.Errorf("%w: workflow version must be greater than zero", errAgentRegistryValidation)
	}

	var created models.Workflow
	err = s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		agent, loadErr := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if loadErr != nil {
			return loadErr
		}
		created = models.Workflow{
			AgentID: agent.ID, Version: command.Version, DefinitionJSON: normalizedDefinition,
			Checksum: checksum, IsActive: false, CreatedBy: actor.UserID,
		}
		if createErr := tx.Create(&created).Error; createErr != nil {
			var existing models.Workflow
			lookupErr := tx.Where("agent_id = ? AND version = ?", agent.ID, command.Version).First(&existing).Error
			if lookupErr == nil {
				return errWorkflowAlreadyExists
			}
			return fmt.Errorf("creating workflow: %w", createErr)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	views, err := s.workflowViews(ctx, s.database, models.Agent{ID: created.AgentID, AgentCode: strings.TrimSpace(agentCode)}, []models.Workflow{created})
	if err != nil {
		return nil, err
	}
	return &views[0], nil
}

// ListWorkflows 返回目标 Agent 的 Workflow 版本，按版本号从高到低排序。
func (s *AgentRegistryService) ListWorkflows(ctx context.Context, actor RegistryActor, agentCode string) ([]WorkflowView, error) {
	agent, err := s.loadOwnedAgent(ctx, s.database, actor, agentCode, false)
	if err != nil {
		return nil, err
	}
	var workflows []models.Workflow
	if err := s.database.WithContext(ctx).Where("agent_id = ?", agent.ID).Order("version DESC").Find(&workflows).Error; err != nil {
		return nil, fmt.Errorf("listing workflows: %w", err)
	}
	return s.workflowViews(ctx, s.database, *agent, workflows)
}

// GetWorkflow 返回目标 Agent 的指定 Workflow 版本及其 Capability 引用。
func (s *AgentRegistryService) GetWorkflow(ctx context.Context, actor RegistryActor, agentCode string, version int) (*WorkflowView, error) {
	agent, err := s.loadOwnedAgent(ctx, s.database, actor, agentCode, false)
	if err != nil {
		return nil, err
	}
	workflow, err := loadAgentWorkflow(ctx, s.database, agent.ID, version, false)
	if err != nil {
		return nil, err
	}
	views, err := s.workflowViews(ctx, s.database, *agent, []models.Workflow{*workflow})
	if err != nil {
		return nil, err
	}
	return &views[0], nil
}

// UpdateWorkflow 替换 inactive Workflow 的定义；active 版本必须通过新版本演进。
func (s *AgentRegistryService) UpdateWorkflow(ctx context.Context, actor RegistryActor, agentCode string, version int, command UpdateWorkflowCommand) (*WorkflowView, error) {
	normalizedDefinition, checksum, err := normalizeWorkflowDefinition(command.Definition)
	if err != nil {
		return nil, err
	}

	var updated models.Workflow
	err = s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		agent, loadErr := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if loadErr != nil {
			return loadErr
		}
		workflow, loadErr := loadAgentWorkflow(ctx, tx, agent.ID, version, true)
		if loadErr != nil {
			return loadErr
		}
		if workflow.IsActive {
			return errWorkflowInvalidState
		}
		if err := rejectActiveWorkflowCapabilityReferences(ctx, tx, workflow.ID); err != nil {
			return err
		}
		if err := tx.Model(workflow).Updates(map[string]any{
			"definition_json": normalizedDefinition,
			"checksum":        checksum,
		}).Error; err != nil {
			return fmt.Errorf("updating workflow: %w", err)
		}
		workflow.DefinitionJSON = normalizedDefinition
		workflow.Checksum = checksum
		updated = *workflow
		return nil
	})
	if err != nil {
		return nil, err
	}
	views, err := s.workflowViews(ctx, s.database, models.Agent{ID: updated.AgentID, AgentCode: strings.TrimSpace(agentCode)}, []models.Workflow{updated})
	if err != nil {
		return nil, err
	}
	return &views[0], nil
}

// ActivateWorkflow 将 Workflow 版本设为可执行，并校验已有 Capability 引用的一致性。
func (s *AgentRegistryService) ActivateWorkflow(ctx context.Context, actor RegistryActor, agentCode string, version int) (*WorkflowView, error) {
	var activated models.Workflow
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		agent, loadErr := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if loadErr != nil {
			return loadErr
		}
		workflow, loadErr := loadAgentWorkflow(ctx, tx, agent.ID, version, true)
		if loadErr != nil {
			return loadErr
		}
		if err := validateWorkflowCapabilityReferences(ctx, tx, *workflow); err != nil {
			return err
		}
		if !workflow.IsActive {
			if err := tx.Model(workflow).Update("is_active", true).Error; err != nil {
				return fmt.Errorf("activating workflow: %w", err)
			}
			workflow.IsActive = true
		}
		if agent.Status == models.AgentStatusActive {
			if err := s.validatePublishState(ctx, tx, *agent); err != nil {
				return err
			}
		}
		activated = *workflow
		return nil
	})
	if err != nil {
		return nil, err
	}
	views, err := s.workflowViews(ctx, s.database, models.Agent{ID: activated.AgentID, AgentCode: strings.TrimSpace(agentCode)}, []models.Workflow{activated})
	if err != nil {
		return nil, err
	}
	return &views[0], nil
}

// DeactivateWorkflow 停用 Workflow；仍被 active Workflow Capability 引用时拒绝变更。
func (s *AgentRegistryService) DeactivateWorkflow(ctx context.Context, actor RegistryActor, agentCode string, version int) (*WorkflowView, error) {
	var deactivated models.Workflow
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		agent, loadErr := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if loadErr != nil {
			return loadErr
		}
		workflow, loadErr := loadAgentWorkflow(ctx, tx, agent.ID, version, true)
		if loadErr != nil {
			return loadErr
		}
		if !workflow.IsActive {
			deactivated = *workflow
			return nil
		}
		if err := rejectActiveWorkflowCapabilityReferences(ctx, tx, workflow.ID); err != nil {
			return err
		}
		if err := tx.Model(workflow).Update("is_active", false).Error; err != nil {
			return fmt.Errorf("deactivating workflow: %w", err)
		}
		workflow.IsActive = false
		if agent.Status == models.AgentStatusActive {
			if err := s.validatePublishState(ctx, tx, *agent); err != nil {
				return err
			}
		}
		deactivated = *workflow
		return nil
	})
	if err != nil {
		return nil, err
	}
	views, err := s.workflowViews(ctx, s.database, models.Agent{ID: deactivated.AgentID, AgentCode: strings.TrimSpace(agentCode)}, []models.Workflow{deactivated})
	if err != nil {
		return nil, err
	}
	return &views[0], nil
}

func loadAgentWorkflow(ctx context.Context, database *gorm.DB, agentID uint64, version int, lock bool) (*models.Workflow, error) {
	if version <= 0 {
		return nil, fmt.Errorf("%w: workflow version must be greater than zero", errAgentRegistryValidation)
	}
	query := database.WithContext(ctx).Where("agent_id = ? AND version = ?", agentID, version)
	if lock {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var workflow models.Workflow
	if err := query.First(&workflow).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errWorkflowNotFound
		}
		return nil, fmt.Errorf("loading workflow: %w", err)
	}
	return &workflow, nil
}

func validateWorkflowCapabilityReferences(ctx context.Context, database *gorm.DB, workflow models.Workflow) error {
	var capabilities []models.AgentCapability
	if err := database.WithContext(ctx).Where("workflow_id = ?", workflow.ID).Find(&capabilities).Error; err != nil {
		return fmt.Errorf("validating workflow capabilities: %w", err)
	}
	wantVersion := strconv.Itoa(workflow.Version)
	for _, capability := range capabilities {
		if capability.AgentID != workflow.AgentID || capability.CapabilityType != models.AgentCapabilityTypeWorkflow || capability.Version != wantVersion {
			return fmt.Errorf("%w: capability %s does not match workflow version", errWorkflowInvalidState, capability.CapabilityCode)
		}
	}
	return nil
}

func rejectActiveWorkflowCapabilityReferences(ctx context.Context, database *gorm.DB, workflowID uint64) error {
	var count int64
	if err := database.WithContext(ctx).Model(&models.AgentCapability{}).
		Where("workflow_id = ? AND status = ?", workflowID, models.AgentCapabilityStatusActive).
		Count(&count).Error; err != nil {
		return fmt.Errorf("checking workflow capability references: %w", err)
	}
	if count > 0 {
		return fmt.Errorf("%w: active capability references workflow", errWorkflowInvalidState)
	}
	return nil
}

func (s *AgentRegistryService) workflowViews(ctx context.Context, database *gorm.DB, agent models.Agent, workflows []models.Workflow) ([]WorkflowView, error) {
	views := make([]WorkflowView, len(workflows))
	if len(workflows) == 0 {
		return views, nil
	}
	workflowIDs := make([]uint64, 0, len(workflows))
	for _, workflow := range workflows {
		workflowIDs = append(workflowIDs, workflow.ID)
	}
	var capabilities []models.AgentCapability
	if err := database.WithContext(ctx).
		Where("agent_id = ? AND workflow_id IN ?", agent.ID, workflowIDs).
		Order("capability_code ASC").Find(&capabilities).Error; err != nil {
		return nil, fmt.Errorf("loading workflow capability references: %w", err)
	}
	byWorkflow := make(map[uint64][]WorkflowCapabilityReferenceView, len(workflows))
	for _, capability := range capabilities {
		if capability.WorkflowID == nil {
			continue
		}
		byWorkflow[*capability.WorkflowID] = append(byWorkflow[*capability.WorkflowID], WorkflowCapabilityReferenceView{
			CapabilityCode: capability.CapabilityCode, Version: capability.Version, Status: capability.Status,
		})
	}
	for index, workflow := range workflows {
		views[index] = WorkflowView{
			ID: workflow.ID, AgentCode: agent.AgentCode, Version: workflow.Version,
			Definition: json.RawMessage(append([]byte(nil), []byte(workflow.DefinitionJSON)...)),
			Checksum:   workflow.Checksum, IsActive: workflow.IsActive, CreatedBy: workflow.CreatedBy,
			Capabilities: byWorkflow[workflow.ID], CreatedAt: workflow.CreatedAt, UpdatedAt: workflow.UpdatedAt,
		}
		if views[index].Capabilities == nil {
			views[index].Capabilities = []WorkflowCapabilityReferenceView{}
		}
	}
	return views, nil
}

func normalizeWorkflowDefinition(raw json.RawMessage) (string, string, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return "", "", fmt.Errorf("%w: workflow definition is required", errAgentRegistryValidation)
	}
	definition, err := ParseAndValidateWorkflowDefinition(string(raw))
	if err != nil {
		return "", "", fmt.Errorf("%w: %v", errAgentRegistryValidation, err)
	}
	normalized := *definition
	normalized.EntryNode = strings.TrimSpace(normalized.EntryNode)
	for index := range normalized.Nodes {
		node := &normalized.Nodes[index]
		node.Key = strings.TrimSpace(node.Key)
		node.Type = strings.TrimSpace(node.Type)
		config, configErr := normalizeWorkflowNodeConfig(*node)
		if configErr != nil {
			return "", "", fmt.Errorf("%w: %v", errAgentRegistryValidation, configErr)
		}
		node.Config = config
	}
	for index := range normalized.Edges {
		normalized.Edges[index].From = strings.TrimSpace(normalized.Edges[index].From)
		normalized.Edges[index].To = strings.TrimSpace(normalized.Edges[index].To)
	}
	if err := ValidateWorkflowDefinition(&normalized); err != nil {
		return "", "", fmt.Errorf("%w: %v", errAgentRegistryValidation, err)
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return "", "", fmt.Errorf("%w: encoding workflow definition: %v", errAgentRegistryValidation, err)
	}
	canonical, err := canonicalizeJSON(encoded)
	if err != nil {
		return "", "", fmt.Errorf("%w: normalizing workflow definition: %v", errAgentRegistryValidation, err)
	}
	sum := sha256.Sum256([]byte(canonical))
	return canonical, hex.EncodeToString(sum[:]), nil
}

func normalizeWorkflowNodeConfig(node WorkflowNode) (json.RawMessage, error) {
	if len(node.Config) == 0 {
		return nil, nil
	}
	var value any
	if err := json.Unmarshal(node.Config, &value); err != nil {
		return nil, err
	}
	var normalized any = value
	var err error
	switch node.Type {
	case "agent":
		var config *AgentNodeConfig
		config, err = ParseAgentNodeConfig(node)
		normalized = config
	case "agent_tool":
		var config *AgentToolNodeConfig
		config, err = ParseAgentToolNodeConfig(node)
		normalized = config
	case "agent_group":
		var config *AgentGroupNodeConfig
		config, err = ParseAgentGroupNodeConfig(node)
		normalized = config
	case "tool":
		var config *ToolNodeConfig
		config, err = ParseToolNodeConfig(node)
		normalized = config
	case "interrupt":
		var config *InterruptNodeConfig
		config, err = ParseInterruptNodeConfig(node)
		normalized = config
	}
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	canonical, err := canonicalizeJSON(encoded)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(canonical), nil
}
