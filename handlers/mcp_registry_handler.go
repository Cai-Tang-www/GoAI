package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"GoAI/middlewares"
	"GoAI/models"
	"GoAI/services"

	"github.com/gin-gonic/gin"
)

const maxMCPRegistryPayloadBytes = 1 << 20

// MCPRegistryHandler 将 MCP Server 管理请求映射到 MCPRegistryService。
type MCPRegistryHandler struct {
	service *services.MCPRegistryService
}

// NewMCPRegistryHandler 创建 MCP Registry 管理接口处理器。
func NewMCPRegistryHandler(service *services.MCPRegistryService) *MCPRegistryHandler {
	return &MCPRegistryHandler{service: service}
}

// CreateServer 创建当前用户拥有的 inactive MCP Server。
func (h *MCPRegistryHandler) CreateServer(c *gin.Context) {
	var command services.UpsertMCPServerCommand
	if err := bindStrictMCPJSON(c, &command); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid MCP server payload", nil))
		return
	}
	actor, ok := mcpRegistryActor(c)
	if !ok {
		return
	}
	view, err := h.service.Create(c.Request.Context(), actor, command)
	respondMCPRegistry(c, http.StatusCreated, view, err)
}

// ListServers 列出当前用户拥有的 MCP Server；管理员可查看全部。
func (h *MCPRegistryHandler) ListServers(c *gin.Context) {
	actor, ok := mcpRegistryActor(c)
	if !ok {
		return
	}
	views, err := h.service.List(c.Request.Context(), actor)
	respondMCPRegistry(c, http.StatusOK, views, err)
}

// GetServer 返回目标 MCP Server 的管理面详情。
func (h *MCPRegistryHandler) GetServer(c *gin.Context) {
	actor, ok := mcpRegistryActor(c)
	if !ok {
		return
	}
	view, err := h.service.Get(c.Request.Context(), actor, c.Param("server_code"))
	respondMCPRegistry(c, http.StatusOK, view, err)
}

// UpdateServer 替换目标 MCP Server 配置，协议配置变化后要求重新健康检查。
func (h *MCPRegistryHandler) UpdateServer(c *gin.Context) {
	var command services.UpsertMCPServerCommand
	if err := bindStrictMCPJSON(c, &command); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid MCP server payload", nil))
		return
	}
	actor, ok := mcpRegistryActor(c)
	if !ok {
		return
	}
	view, err := h.service.Update(c.Request.Context(), actor, c.Param("server_code"), command)
	respondMCPRegistry(c, http.StatusOK, view, err)
}

// DeactivateServer 停用未被 active Workflow 引用的 MCP Server。
func (h *MCPRegistryHandler) DeactivateServer(c *gin.Context) {
	actor, ok := mcpRegistryActor(c)
	if !ok {
		return
	}
	view, err := h.service.Deactivate(c.Request.Context(), actor, c.Param("server_code"))
	respondMCPRegistry(c, http.StatusOK, view, err)
}

// CheckServerHealth 通过官方 MCP 协议执行 initialize 和 tools/list。
func (h *MCPRegistryHandler) CheckServerHealth(c *gin.Context) {
	actor, ok := mcpRegistryActor(c)
	if !ok {
		return
	}
	view, err := h.service.CheckHealth(c.Request.Context(), actor, c.Param("server_code"))
	respondMCPRegistry(c, http.StatusOK, view, err)
}

// ListTools 返回目标 MCP Server 最近一次成功发现的 Tool 快照。
func (h *MCPRegistryHandler) ListTools(c *gin.Context) {
	actor, ok := mcpRegistryActor(c)
	if !ok {
		return
	}
	views, err := h.service.ListTools(c.Request.Context(), actor, c.Param("server_code"))
	respondMCPRegistry(c, http.StatusOK, views, err)
}

func mcpRegistryActor(c *gin.Context) (services.MCPRegistryActor, bool) {
	userID, _, ok := authPrincipal(c)
	if !ok {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidToken())
		return services.MCPRegistryActor{}, false
	}
	return services.MCPRegistryActor{
		UserID:    userID,
		CanManage: middlewares.HasPermission(c, models.PermissionMCPManage),
	}, true
}

func respondMCPRegistry(c *gin.Context, status int, data any, err error) {
	if err != nil {
		middlewares.AbortWithError(c, middlewares.WrapError(err))
		return
	}
	middlewares.Success(c, status, data, "success")
}

func bindStrictMCPJSON(c *gin.Context, target any) error {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxMCPRegistryPayloadBytes)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain one JSON value")
		}
		return err
	}
	return nil
}
