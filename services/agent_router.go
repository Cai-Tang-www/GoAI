package services

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"GoAI/models"

	"gorm.io/gorm"
)

var (
	errAgentRouteNotFound    = errors.New("agent route not found")
	errAgentRouteUnavailable = errors.New("agent route unavailable")
	errAgentRouteInvalid     = errors.New("agent route is invalid")
)

// ErrAgentRouteNotFound 表示 Registry 中没有满足能力和发布条件的目标 Agent。
func ErrAgentRouteNotFound() error { return errAgentRouteNotFound }

// ErrAgentRouteUnavailable 表示目标 Agent 有能力资产，但没有健康可用的 A2A Endpoint。
func ErrAgentRouteUnavailable() error { return errAgentRouteUnavailable }

// ErrAgentRouteInvalid 表示协作策略提供了不满足运行时约束的路由请求。
func ErrAgentRouteInvalid() error { return errAgentRouteInvalid }

// AgentRouteRequest 描述 Supervisor 交给 Router 的协议无关选路请求。
type AgentRouteRequest struct {
	SourceAgentID      uint64
	CapabilityCode     string
	PreferredAgentCode string
}

// AgentRoute 是 Router 选出的可执行 Agent 资产快照。
type AgentRoute struct {
	Agent           models.Agent
	Capability      models.AgentCapability
	Workflow        models.Workflow
	Endpoint        models.AgentEndpoint
	SelectionReason string
}

// AgentRouter 定义 Supervisor 选择 Worker 的最小运行时边界。
type AgentRouter interface {
	Route(context.Context, AgentRouteRequest) (*AgentRoute, error)
}

// RegistryAgentRouter 只从已发布的 Registry 资产中选择可通过 A2A 调用的 Worker。
type RegistryAgentRouter struct {
	database *gorm.DB
}

// NewRegistryAgentRouter 创建基于 Agent Registry 的确定性 Router。
func NewRegistryAgentRouter(database *gorm.DB) (*RegistryAgentRouter, error) {
	if database == nil {
		return nil, errors.New("creating agent router: database is nil")
	}
	return &RegistryAgentRouter{database: database}, nil
}

// Route 按 agent_code、endpoint_code 的稳定顺序选择 active Worker。
func (r *RegistryAgentRouter) Route(ctx context.Context, request AgentRouteRequest) (*AgentRoute, error) {
	if r == nil || r.database == nil {
		return nil, fmt.Errorf("%w: database is unavailable", errAgentRouteInvalid)
	}
	capabilityCode := strings.TrimSpace(request.CapabilityCode)
	preferredCode := strings.TrimSpace(request.PreferredAgentCode)
	if capabilityCode == "" {
		return nil, fmt.Errorf("%w: capability_code is required", errAgentRouteInvalid)
	}

	query := r.database.WithContext(ctx).
		Where("status = ?", models.AgentStatusActive).
		Order("agent_code ASC")
	if request.SourceAgentID != 0 {
		query = query.Where("id <> ?", request.SourceAgentID)
	}
	if preferredCode != "" {
		query = query.Where("agent_code = ?", preferredCode)
	}
	var agents []models.Agent
	if err := query.Find(&agents).Error; err != nil {
		return nil, fmt.Errorf("listing route candidates: %w", err)
	}

	matchingCapability := false
	matchingWorkflow := false
	for _, agent := range agents {
		var capability models.AgentCapability
		if err := r.database.WithContext(ctx).
			Where("agent_id = ? AND capability_code = ? AND status = ?", agent.ID, capabilityCode, models.AgentCapabilityStatusActive).
			First(&capability).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			return nil, fmt.Errorf("loading route capability: %w", err)
		}
		matchingCapability = true
		if capability.CapabilityType != models.AgentCapabilityTypeWorkflow || capability.WorkflowID == nil || *capability.WorkflowID == 0 {
			continue
		}

		var workflow models.Workflow
		if err := r.database.WithContext(ctx).
			Where("id = ? AND agent_id = ? AND is_active = ?", *capability.WorkflowID, agent.ID, true).
			First(&workflow).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			return nil, fmt.Errorf("loading route workflow: %w", err)
		}
		if capability.Version != "" && capability.Version != strconv.Itoa(workflow.Version) {
			continue
		}
		matchingWorkflow = true

		var endpoint models.AgentEndpoint
		if err := r.database.WithContext(ctx).
			Where("agent_id = ? AND protocol = ? AND status = ?", agent.ID, models.AgentEndpointProtocolA2A, models.AgentEndpointStatusActive).
			Order("endpoint_code ASC").First(&endpoint).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			return nil, fmt.Errorf("loading route endpoint: %w", err)
		}
		return &AgentRoute{
			Agent:           agent,
			Capability:      capability,
			Workflow:        workflow,
			Endpoint:        endpoint,
			SelectionReason: fmt.Sprintf("registry:agent_code=%s;endpoint_code=%s", agent.AgentCode, endpoint.EndpointCode),
		}, nil
	}

	if matchingWorkflow && preferredCode != "" {
		return nil, fmt.Errorf("%w: preferred agent %s has no healthy A2A endpoint", errAgentRouteUnavailable, preferredCode)
	}
	if matchingWorkflow {
		return nil, fmt.Errorf("%w: capability %s has no healthy A2A endpoint", errAgentRouteUnavailable, capabilityCode)
	}
	if matchingCapability {
		return nil, fmt.Errorf("%w: capability %s has no active workflow", errAgentRouteNotFound, capabilityCode)
	}
	return nil, fmt.Errorf("%w: capability %s has no active candidate", errAgentRouteNotFound, capabilityCode)
}

var _ AgentRouter = (*RegistryAgentRouter)(nil)
