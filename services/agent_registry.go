package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"GoAI/a2aauth"
	"GoAI/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	errAgentAlreadyExists        = errors.New("agent already exists")
	errAgentInvalidState         = errors.New("agent state is invalid")
	errAgentPublishValidation    = errors.New("agent publish validation failed")
	errAgentRegistryValidation   = errors.New("agent registry validation failed")
	errAgentForbidden            = errors.New("agent access forbidden")
	errCapabilityAlreadyExists   = errors.New("agent capability already exists")
	errEndpointNotFound          = errors.New("agent endpoint not found")
	errEndpointAlreadyExists     = errors.New("agent endpoint already exists")
	errEndpointHealthCheckFailed = errors.New("agent endpoint health check failed")
	errWorkflowAlreadyExists     = errors.New("agent workflow already exists")
	errWorkflowInvalidState      = errors.New("agent workflow state is invalid")
)

// ErrAgentAlreadyExists 返回 AgentCode 已存在的统一 sentinel error。
func ErrAgentAlreadyExists() error { return errAgentAlreadyExists }

// ErrAgentInvalidState 返回 Agent 状态不允许当前操作的统一 sentinel error。
func ErrAgentInvalidState() error { return errAgentInvalidState }

// ErrAgentPublishValidation 返回 Agent 发布资产不完整的统一 sentinel error。
func ErrAgentPublishValidation() error { return errAgentPublishValidation }

// ErrAgentRegistryValidation 返回 Agent Registry 输入不合法的统一 sentinel error。
func ErrAgentRegistryValidation() error { return errAgentRegistryValidation }

// ErrAgentForbidden 返回当前主体无权管理目标 Agent 的统一 sentinel error。
func ErrAgentForbidden() error { return errAgentForbidden }

// ErrCapabilityAlreadyExists 返回同一 Agent 下 CapabilityCode 重复的统一 sentinel error。
func ErrCapabilityAlreadyExists() error { return errCapabilityAlreadyExists }

// ErrEndpointNotFound 返回 Agent Endpoint 不存在的统一 sentinel error。
func ErrEndpointNotFound() error { return errEndpointNotFound }

// ErrEndpointAlreadyExists 返回同一 Agent 下 EndpointCode 重复的统一 sentinel error。
func ErrEndpointAlreadyExists() error { return errEndpointAlreadyExists }

// ErrEndpointHealthCheckFailed 返回 A2A Agent Card 健康检查失败的统一 sentinel error。
func ErrEndpointHealthCheckFailed() error { return errEndpointHealthCheckFailed }

// ErrWorkflowAlreadyExists 返回同一 Agent 下 Workflow 版本重复的统一 sentinel error。
func ErrWorkflowAlreadyExists() error { return errWorkflowAlreadyExists }

// ErrWorkflowInvalidState 返回 Workflow 不允许当前变更的统一 sentinel error。
func ErrWorkflowInvalidState() error { return errWorkflowInvalidState }

// RegistryActor 表示调用 Agent Registry 的认证主体及其 ownership bypass 权限。
type RegistryActor struct {
	UserID    uint64
	CanManage bool
}

// CreateAgentCommand 描述创建 Agent 的管理面命令。
type CreateAgentCommand struct {
	AgentCode   string `json:"agent_code"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

// UpdateAgentCommand 描述替换 Agent 可变元数据的管理面命令。
type UpdateAgentCommand struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// UpsertCapabilityCommand 描述创建或替换 Agent Capability 的管理面命令。
type UpsertCapabilityCommand struct {
	CapabilityCode   string  `json:"capability_code"`
	Name             string  `json:"name"`
	Description      string  `json:"description"`
	CapabilityType   string  `json:"capability_type"`
	WorkflowID       *uint64 `json:"workflow_id"`
	Version          string  `json:"version"`
	InputSchemaJSON  string  `json:"input_schema_json"`
	OutputSchemaJSON string  `json:"output_schema_json"`
	ConfigJSON       string  `json:"config_json"`
	Status           string  `json:"status"`
}

// UpsertEndpointCommand 描述创建或替换 Agent A2A Endpoint 的管理面命令。
type UpsertEndpointCommand struct {
	EndpointCode  string `json:"endpoint_code"`
	Protocol      string `json:"protocol"`
	Transport     string `json:"transport"`
	Address       string `json:"address"`
	AuthType      string `json:"auth_type"`
	CredentialRef string `json:"credential_ref"`
	ConfigJSON    string `json:"config_json"`
}

// AgentCardHealthCheckRequest 是 Registry 与 A2A Client 之间的协议健康检查请求。
type AgentCardHealthCheckRequest struct {
	AgentCode string
	Address   string
}

// AgentCardHealthChecker 通过官方 A2A Agent Card HTTP 契约验证 Endpoint 身份和可达性。
type AgentCardHealthChecker interface {
	CheckAgentCard(context.Context, AgentCardHealthCheckRequest) error
}

// AgentCardHealthCheckerFunc 将函数适配为 AgentCardHealthChecker，便于测试和装配轻量实现。
type AgentCardHealthCheckerFunc func(context.Context, AgentCardHealthCheckRequest) error

// CheckAgentCard 调用底层健康检查函数。
func (f AgentCardHealthCheckerFunc) CheckAgentCard(ctx context.Context, request AgentCardHealthCheckRequest) error {
	return f(ctx, request)
}

// AgentSummaryView 是 Agent 列表接口使用的稳定管理面视图。
type AgentSummaryView struct {
	AgentCode   string    `json:"agent_code"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	OwnerUserID uint64    `json:"owner_user_id"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// AgentCapabilityView 是管理 API 返回的 Capability 视图。
type AgentCapabilityView struct {
	CapabilityCode   string    `json:"capability_code"`
	Name             string    `json:"name"`
	Description      string    `json:"description"`
	CapabilityType   string    `json:"capability_type"`
	WorkflowID       *uint64   `json:"workflow_id"`
	Version          string    `json:"version"`
	InputSchemaJSON  string    `json:"input_schema_json"`
	OutputSchemaJSON string    `json:"output_schema_json"`
	ConfigJSON       string    `json:"config_json"`
	Status           string    `json:"status"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// AgentEndpointView 是管理 API 返回的 Endpoint 视图，仅暴露 credential_ref，不包含真实凭据。
type AgentEndpointView struct {
	EndpointCode  string     `json:"endpoint_code"`
	Protocol      string     `json:"protocol"`
	Transport     string     `json:"transport"`
	Address       string     `json:"address"`
	AuthType      string     `json:"auth_type"`
	CredentialRef string     `json:"credential_ref"`
	ConfigJSON    string     `json:"config_json"`
	Status        string     `json:"status"`
	LastHealthyAt *time.Time `json:"last_healthy_at"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// ActiveWorkflowView 是 Agent 详情中当前最高版本 active Workflow 的摘要。
type ActiveWorkflowView struct {
	ID       uint64 `json:"id"`
	Version  int    `json:"version"`
	Checksum string `json:"checksum"`
}

// AgentDetailView 聚合 Agent、Capability、Endpoint 与 active Workflow 摘要。
type AgentDetailView struct {
	AgentSummaryView
	Capabilities   []AgentCapabilityView `json:"capabilities"`
	Endpoints      []AgentEndpointView   `json:"endpoints"`
	ActiveWorkflow *ActiveWorkflowView   `json:"active_workflow"`
}

// AgentRegistryService 实现 Agent 管理、发布校验、发现资产维护和 ownership 约束。
type AgentRegistryService struct {
	database     *gorm.DB
	cardChecker  AgentCardHealthChecker
	credentials  a2aauth.CredentialResolver
	authRequired bool
	now          func() time.Time
}

// NewAgentRegistryService 创建显式依赖的 Agent Registry 管理服务。
func NewAgentRegistryService(database *gorm.DB, checker AgentCardHealthChecker, credentials a2aauth.CredentialResolver, authRequired bool) (*AgentRegistryService, error) {
	if database == nil {
		return nil, errors.New("creating agent registry service: database is nil")
	}
	if checker == nil {
		return nil, errors.New("creating agent registry service: card checker is nil")
	}
	if authRequired && credentials == nil {
		return nil, errors.New("creating agent registry service: credential resolver is nil")
	}
	return &AgentRegistryService{
		database:     database,
		cardChecker:  checker,
		credentials:  credentials,
		authRequired: authRequired,
		now:          time.Now,
	}, nil
}

// CreateAgent 创建归属于当前主体的 inactive Agent。
func (s *AgentRegistryService) CreateAgent(ctx context.Context, actor RegistryActor, command CreateAgentCommand) (*AgentDetailView, error) {
	if err := validateActor(actor); err != nil {
		return nil, err
	}
	command = normalizeCreateAgentCommand(command)
	if err := validateCreateAgentCommand(command); err != nil {
		return nil, err
	}
	agent := models.Agent{
		AgentCode: command.AgentCode, Name: command.Name, Description: command.Description,
		OwnerUserID: actor.UserID, Status: models.AgentStatusInactive,
	}
	if err := s.database.WithContext(ctx).Create(&agent).Error; err != nil {
		var existing models.Agent
		if lookupErr := s.database.WithContext(ctx).Where("agent_code = ?", command.AgentCode).First(&existing).Error; lookupErr == nil {
			return nil, errAgentAlreadyExists
		}
		return nil, fmt.Errorf("creating agent: %w", err)
	}
	return s.agentDetail(ctx, s.database, agent)
}

// ListAgents 按 ownership 返回当前主体可见的 Agent 列表。
func (s *AgentRegistryService) ListAgents(ctx context.Context, actor RegistryActor) ([]AgentSummaryView, error) {
	if err := validateActor(actor); err != nil {
		return nil, err
	}
	query := s.database.WithContext(ctx).Order("agent_code ASC")
	if !actor.CanManage {
		query = query.Where("owner_user_id = ?", actor.UserID)
	}
	var agents []models.Agent
	if err := query.Find(&agents).Error; err != nil {
		return nil, fmt.Errorf("listing agents: %w", err)
	}
	views := make([]AgentSummaryView, 0, len(agents))
	for _, agent := range agents {
		views = append(views, agentSummaryView(agent))
	}
	return views, nil
}

// GetAgent 返回当前主体可管理的 Agent 完整详情。
func (s *AgentRegistryService) GetAgent(ctx context.Context, actor RegistryActor, agentCode string) (*AgentDetailView, error) {
	agent, err := s.loadOwnedAgent(ctx, s.database, actor, agentCode, false)
	if err != nil {
		return nil, err
	}
	return s.agentDetail(ctx, s.database, *agent)
}

// UpdateAgent 更新 Agent 名称和描述，不改变协议身份和 owner。
func (s *AgentRegistryService) UpdateAgent(ctx context.Context, actor RegistryActor, agentCode string, command UpdateAgentCommand) (*AgentDetailView, error) {
	command.Name = strings.TrimSpace(command.Name)
	command.Description = strings.TrimSpace(command.Description)
	if command.Name == "" || len(command.Name) > 128 || len(command.Description) > 4000 {
		return nil, fmt.Errorf("%w: invalid agent metadata", errAgentRegistryValidation)
	}
	var updated models.Agent
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		agent, err := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if err != nil {
			return err
		}
		if err := tx.Model(agent).Updates(map[string]any{"name": command.Name, "description": command.Description}).Error; err != nil {
			return fmt.Errorf("updating agent: %w", err)
		}
		agent.Name, agent.Description = command.Name, command.Description
		updated = *agent
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.agentDetail(ctx, s.database, updated)
}

// ActivateAgent 在同一事务中锁定 Agent、验证发布资产并原子切换为 active。
func (s *AgentRegistryService) ActivateAgent(ctx context.Context, actor RegistryActor, agentCode string) (*AgentDetailView, error) {
	var activated models.Agent
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		agent, err := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if err != nil {
			return err
		}
		if agent.Status != models.AgentStatusInactive && agent.Status != models.AgentStatusActive {
			return errAgentInvalidState
		}
		if err := s.validatePublishState(ctx, tx, *agent); err != nil {
			return err
		}
		if agent.Status == models.AgentStatusInactive {
			result := tx.Model(&models.Agent{}).
				Where("id = ? AND status = ?", agent.ID, models.AgentStatusInactive).
				Update("status", models.AgentStatusActive)
			if result.Error != nil {
				return fmt.Errorf("activating agent: %w", result.Error)
			}
			if result.RowsAffected != 1 {
				return errAgentInvalidState
			}
			agent.Status = models.AgentStatusActive
		}
		activated = *agent
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.agentDetail(ctx, s.database, activated)
}

// DeactivateAgent 幂等停用 Agent，并保留全部历史引用和审计数据。
func (s *AgentRegistryService) DeactivateAgent(ctx context.Context, actor RegistryActor, agentCode string) (*AgentDetailView, error) {
	var deactivated models.Agent
	err := s.database.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		agent, err := s.loadOwnedAgent(ctx, tx, actor, agentCode, true)
		if err != nil {
			return err
		}
		if agent.Status != models.AgentStatusInactive && agent.Status != models.AgentStatusActive {
			return errAgentInvalidState
		}
		if agent.Status == models.AgentStatusActive {
			if err := tx.Model(agent).Update("status", models.AgentStatusInactive).Error; err != nil {
				return fmt.Errorf("deactivating agent: %w", err)
			}
			agent.Status = models.AgentStatusInactive
		}
		deactivated = *agent
		return nil
	})
	if err != nil {
		return nil, err
	}
	return s.agentDetail(ctx, s.database, deactivated)
}

func (s *AgentRegistryService) loadOwnedAgent(ctx context.Context, database *gorm.DB, actor RegistryActor, agentCode string, lock bool) (*models.Agent, error) {
	if err := validateActor(actor); err != nil {
		return nil, err
	}
	code := strings.TrimSpace(agentCode)
	if !registryCodePattern.MatchString(code) {
		return nil, fmt.Errorf("%w: invalid agent_code", errAgentRegistryValidation)
	}
	query := database.WithContext(ctx).Where("agent_code = ?", code)
	if lock {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var agent models.Agent
	if err := query.First(&agent).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errAgentNotFound
		}
		return nil, fmt.Errorf("loading agent: %w", err)
	}
	if !actor.CanManage && agent.OwnerUserID != actor.UserID {
		return nil, errAgentForbidden
	}
	return &agent, nil
}

func validateActor(actor RegistryActor) error {
	if actor.UserID == 0 {
		return errAgentForbidden
	}
	return nil
}
