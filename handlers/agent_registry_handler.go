package handlers

import (
	"net/http"

	"GoAI/middlewares"
	"GoAI/models"
	"GoAI/services"

	"github.com/gin-gonic/gin"
)

// AgentRegistryHandler 将 Agent 管理面 HTTP 请求映射到 AgentRegistryService。
type AgentRegistryHandler struct {
	service *services.AgentRegistryService
}

// NewAgentRegistryHandler 创建 Agent Registry 管理接口处理器。
func NewAgentRegistryHandler(service *services.AgentRegistryService) *AgentRegistryHandler {
	return &AgentRegistryHandler{service: service}
}

// CreateAgent 创建当前用户拥有的 inactive Agent。
func (h *AgentRegistryHandler) CreateAgent(c *gin.Context) {
	var command services.CreateAgentCommand
	if err := c.ShouldBindJSON(&command); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid agent payload", nil))
		return
	}
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	view, err := h.service.CreateAgent(c.Request.Context(), actor, command)
	respondRegistry(c, http.StatusCreated, view, err)
}

// ListAgents 列出当前用户拥有的 Agent；拥有 manage 权限时返回全部 Agent。
func (h *AgentRegistryHandler) ListAgents(c *gin.Context) {
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	views, err := h.service.ListAgents(c.Request.Context(), actor)
	respondRegistry(c, http.StatusOK, views, err)
}

// GetAgent 返回目标 Agent 的管理面详情。
func (h *AgentRegistryHandler) GetAgent(c *gin.Context) {
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	view, err := h.service.GetAgent(c.Request.Context(), actor, c.Param("agent_code"))
	respondRegistry(c, http.StatusOK, view, err)
}

// UpdateAgent 替换目标 Agent 的可变元数据。
func (h *AgentRegistryHandler) UpdateAgent(c *gin.Context) {
	var command services.UpdateAgentCommand
	if err := c.ShouldBindJSON(&command); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid agent payload", nil))
		return
	}
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	view, err := h.service.UpdateAgent(c.Request.Context(), actor, c.Param("agent_code"), command)
	respondRegistry(c, http.StatusOK, view, err)
}

// ActivateAgent 校验发布资产并启用目标 Agent。
func (h *AgentRegistryHandler) ActivateAgent(c *gin.Context) {
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	view, err := h.service.ActivateAgent(c.Request.Context(), actor, c.Param("agent_code"))
	respondRegistry(c, http.StatusOK, view, err)
}

// DeactivateAgent 幂等停用目标 Agent。
func (h *AgentRegistryHandler) DeactivateAgent(c *gin.Context) {
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	view, err := h.service.DeactivateAgent(c.Request.Context(), actor, c.Param("agent_code"))
	respondRegistry(c, http.StatusOK, view, err)
}

// CreateCapability 为目标 Agent 创建可发现能力。
func (h *AgentRegistryHandler) CreateCapability(c *gin.Context) {
	var command services.UpsertCapabilityCommand
	if err := c.ShouldBindJSON(&command); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid capability payload", nil))
		return
	}
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	view, err := h.service.CreateCapability(c.Request.Context(), actor, c.Param("agent_code"), command)
	respondRegistry(c, http.StatusCreated, view, err)
}

// ListCapabilities 返回目标 Agent 的全部能力资产。
func (h *AgentRegistryHandler) ListCapabilities(c *gin.Context) {
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	views, err := h.service.ListCapabilities(c.Request.Context(), actor, c.Param("agent_code"))
	respondRegistry(c, http.StatusOK, views, err)
}

// UpdateCapability 替换路径指定的 Capability 配置。
func (h *AgentRegistryHandler) UpdateCapability(c *gin.Context) {
	var command services.UpsertCapabilityCommand
	if err := c.ShouldBindJSON(&command); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid capability payload", nil))
		return
	}
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	view, err := h.service.UpdateCapability(c.Request.Context(), actor, c.Param("agent_code"), c.Param("capability_code"), command)
	respondRegistry(c, http.StatusOK, view, err)
}

// DeactivateCapability 幂等停用路径指定的 Capability。
func (h *AgentRegistryHandler) DeactivateCapability(c *gin.Context) {
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	view, err := h.service.DeactivateCapability(c.Request.Context(), actor, c.Param("agent_code"), c.Param("capability_code"))
	respondRegistry(c, http.StatusOK, view, err)
}

// CreateEndpoint 为目标 Agent 创建默认 inactive 的 A2A Endpoint。
func (h *AgentRegistryHandler) CreateEndpoint(c *gin.Context) {
	var command services.UpsertEndpointCommand
	if err := c.ShouldBindJSON(&command); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid endpoint payload", nil))
		return
	}
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	view, err := h.service.CreateEndpoint(c.Request.Context(), actor, c.Param("agent_code"), command)
	respondRegistry(c, http.StatusCreated, view, err)
}

// ListEndpoints 返回目标 Agent 的全部 A2A Endpoint 资产。
func (h *AgentRegistryHandler) ListEndpoints(c *gin.Context) {
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	views, err := h.service.ListEndpoints(c.Request.Context(), actor, c.Param("agent_code"))
	respondRegistry(c, http.StatusOK, views, err)
}

// UpdateEndpoint 替换路径指定的 Endpoint，并重置其健康状态。
func (h *AgentRegistryHandler) UpdateEndpoint(c *gin.Context) {
	var command services.UpsertEndpointCommand
	if err := c.ShouldBindJSON(&command); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid endpoint payload", nil))
		return
	}
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	view, err := h.service.UpdateEndpoint(c.Request.Context(), actor, c.Param("agent_code"), c.Param("endpoint_code"), command)
	respondRegistry(c, http.StatusOK, view, err)
}

// DeactivateEndpoint 幂等停用路径指定的 Endpoint。
func (h *AgentRegistryHandler) DeactivateEndpoint(c *gin.Context) {
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	view, err := h.service.DeactivateEndpoint(c.Request.Context(), actor, c.Param("agent_code"), c.Param("endpoint_code"))
	respondRegistry(c, http.StatusOK, view, err)
}

// CheckEndpointHealth 通过 A2A Agent Card 契约校验 Endpoint 身份和可达性。
func (h *AgentRegistryHandler) CheckEndpointHealth(c *gin.Context) {
	actor, ok := registryActor(c)
	if !ok {
		return
	}
	view, err := h.service.CheckEndpointHealth(c.Request.Context(), actor, c.Param("agent_code"), c.Param("endpoint_code"))
	respondRegistry(c, http.StatusOK, view, err)
}

func registryActor(c *gin.Context) (services.RegistryActor, bool) {
	userID, _, ok := authPrincipal(c)
	if !ok {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidToken())
		return services.RegistryActor{}, false
	}
	return services.RegistryActor{
		UserID:    userID,
		CanManage: middlewares.HasPermission(c, models.PermissionAgentManage),
	}, true
}

func respondRegistry(c *gin.Context, status int, data any, err error) {
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}
	middlewares.Success(c, status, data, "success")
}
