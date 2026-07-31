package middlewares

import (
	"GoAI/config"
	"GoAI/models"
	"errors"
	"strconv"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const (
	contextPermissionSetKey = "permission_set"
	contextIsAdminKey       = "is_admin"
)

// RBACContextMiddleware 在 JWT 之后加载用户权限集并写入请求上下文。
func RBACContextMiddleware(database *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		if config.AppConfig == nil || !config.AppConfig.RBACEnable {
			c.Set(contextPermissionSetKey, map[string]bool{})
			c.Set(contextIsAdminKey, false)
			c.Next()
			return
		}

		userID, ok := getUserIDFromContext(c)
		if !ok {
			AbortWithError(c, UnauthorizedInvalidToken())
			return
		}

		permissionSet, isAdmin, err := loadPermissionSet(database, userID)
		if err != nil {
			AbortWithError(c, RbacPermissionLoadFailed(err))
			return
		}
		c.Set(contextPermissionSetKey, permissionSet)
		c.Set(contextIsAdminKey, isAdmin)
		c.Next()
	}
}

// RequirePermission 校验当前请求是否具备指定权限，不满足时返回 403。
func RequirePermission(permission string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if config.AppConfig == nil || !config.AppConfig.RBACEnable {
			c.Next()
			return
		}
		if HasPermission(c, permission) {
			c.Next()
			return
		}
		AbortWithError(c, ForbiddenError())
	}
}

// RequireSelfOrPermission 要求本人具备自有权限，非本人则必须具备管理权限。
func RequireSelfOrPermission(idParam string, selfPermission string, managePermission string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if config.AppConfig == nil || !config.AppConfig.RBACEnable {
			c.Next()
			return
		}
		userID, ok := getUserIDFromContext(c)
		if !ok {
			AbortWithError(c, UnauthorizedInvalidToken())
			return
		}

		targetIDRaw := c.Param(idParam)
		targetID, err := strconv.ParseUint(targetIDRaw, 10, 64)
		if err != nil {
			AbortWithError(c, InvalidIDError())
			return
		}

		if uint64(userID) == targetID {
			if HasPermission(c, selfPermission) {
				c.Next()
				return
			}
			AbortWithError(c, ForbiddenError())
			return
		}

		if HasPermission(c, managePermission) {
			c.Next()
			return
		}
		AbortWithError(c, ForbiddenError())
	}
}

// IsAdmin 返回当前上下文主体是否拥有 admin 角色。
func IsAdmin(c *gin.Context) bool {
	v, ok := c.Get(contextIsAdminKey)
	if !ok {
		return false
	}
	isAdmin, ok := v.(bool)
	return ok && isAdmin
}

// HasPermission 判断上下文中的权限集合是否包含目标权限码。
func HasPermission(c *gin.Context, permission string) bool {
	v, ok := c.Get(contextPermissionSetKey)
	if !ok {
		return false
	}
	set, ok := v.(map[string]bool)
	if !ok {
		return false
	}
	return set[permission]
}

// getUserIDFromContext 从 JWT 中间件写入的上下文读取用户 ID。
func getUserIDFromContext(c *gin.Context) (uint, bool) {
	raw, exists := c.Get("user_id")
	if !exists {
		return 0, false
	}
	id, ok := raw.(uint)
	return id, ok
}

// loadPermissionSet 实时查询用户角色与权限，并返回权限集合及 admin 标记。
func loadPermissionSet(database *gorm.DB, userID uint) (map[string]bool, bool, error) {
	type permissionRow struct {
		RoleName       string `gorm:"column:role_name"`
		PermissionCode string `gorm:"column:permission_code"`
	}
	var rows []permissionRow
	if database == nil {
		return nil, false, errors.New("RBAC database is nil")
	}
	err := database.Table("user_roles AS ur").
		Select("r.name AS role_name, p.code AS permission_code").
		Joins("JOIN roles r ON r.id = ur.role_id").
		Joins("LEFT JOIN role_permissions rp ON rp.role_id = r.id").
		Joins("LEFT JOIN permissions p ON p.id = rp.permission_id").
		Where("ur.user_id = ?", userID).
		Scan(&rows).Error
	if err != nil {
		return nil, false, err
	}

	set := make(map[string]bool, len(rows))
	isAdmin := false
	for _, row := range rows {
		if row.RoleName == models.RoleAdmin {
			isAdmin = true
		}
		if row.PermissionCode != "" {
			set[row.PermissionCode] = true
		}
	}
	return set, isAdmin, nil
}
