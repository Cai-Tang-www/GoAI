package db

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"GoAI/config"
	"GoAI/models"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SeedRBAC 在启动阶段执行 RBAC 角色、权限和映射的幂等补种。
func SeedRBAC(database *gorm.DB, cfg *config.Config) error {
	if cfg == nil {
		return fmt.Errorf("seeding RBAC: config is nil")
	}
	if !cfg.RBACEnable {
		return nil
	}
	if database == nil {
		return fmt.Errorf("seeding RBAC: database is nil")
	}

	roleIDs, err := ensureRoles(database)
	if err != nil {
		return err
	}
	permissionIDs, err := ensurePermissions(database)
	if err != nil {
		return err
	}
	if err := ensureRolePermissions(database, roleIDs, permissionIDs); err != nil {
		return err
	}
	if err := ensureAllUsersHaveMemberRole(database, roleIDs[models.RoleMember]); err != nil {
		return err
	}
	if err := ensureBootstrapAdmin(database, cfg, roleIDs[models.RoleAdmin]); err != nil {
		return err
	}
	return nil
}

// AssignMemberRole 为新用户补齐 member 角色；调用方可以在用户创建事务中使用它。
func AssignMemberRole(database *gorm.DB, userID uint64) error {
	if database == nil {
		return fmt.Errorf("assigning member role: database is nil")
	}
	if userID == 0 {
		return fmt.Errorf("assigning member role: user id is required")
	}

	var role models.Role
	if err := database.Where("name = ?", models.RoleMember).First(&role).Error; err != nil {
		return fmt.Errorf("loading member role: %w", err)
	}

	binding := models.UserRole{UserID: userID, RoleID: role.ID}
	if err := database.Clauses(clause.OnConflict{DoNothing: true}).Create(&binding).Error; err != nil {
		return fmt.Errorf("creating member role binding: %w", err)
	}
	return nil
}

// ensureRoles 确保基础角色存在，并返回角色名到角色 ID 的映射。
func ensureRoles(database *gorm.DB) (map[string]uint64, error) {
	type roleSeed struct {
		Name        string
		Description string
	}
	seeds := []roleSeed{
		{Name: models.RoleAdmin, Description: "System administrator"},
		{Name: models.RoleMember, Description: "Regular member"},
	}
	result := make(map[string]uint64, len(seeds))
	for _, seed := range seeds {
		role := models.Role{Name: seed.Name}
		if err := database.Where("name = ?", seed.Name).First(&role).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				role.Description = seed.Description
				if createErr := database.Create(&role).Error; createErr != nil {
					return nil, createErr
				}
			} else {
				return nil, err
			}
		}
		result[seed.Name] = role.ID
	}
	return result, nil
}

// ensurePermissions 确保预置权限存在，并返回权限码到权限 ID 的映射。
func ensurePermissions(database *gorm.DB) (map[string]uint64, error) {
	type permissionSeed struct {
		Code        string
		Description string
	}
	seeds := []permissionSeed{
		{Code: models.PermissionRunCreate, Description: "Create run"},
		{Code: models.PermissionRunRead, Description: "Read run"},
		{Code: models.PermissionRunReplay, Description: "Replay run"},
		{Code: models.PermissionUserReadSelf, Description: "Read self profile"},
		{Code: models.PermissionUserUpdateSelf, Description: "Update self profile"},
		{Code: models.PermissionUserManage, Description: "Manage users"},
		{Code: models.PermissionChatUse, Description: "Use chat api"},
		{Code: models.PermissionAgentCreate, Description: "Create agents"},
		{Code: models.PermissionAgentRead, Description: "Read agents"},
		{Code: models.PermissionAgentUpdate, Description: "Update owned agents"},
		{Code: models.PermissionAgentActivate, Description: "Activate owned agents"},
		{Code: models.PermissionAgentManage, Description: "Manage all agents"},
		{Code: models.PermissionMCPCreate, Description: "Create MCP servers"},
		{Code: models.PermissionMCPRead, Description: "Read owned MCP servers"},
		{Code: models.PermissionMCPUpdate, Description: "Update owned MCP servers"},
		{Code: models.PermissionMCPInvoke, Description: "Invoke tools on owned MCP servers"},
		{Code: models.PermissionMCPManage, Description: "Manage all MCP servers"},
		{Code: models.PermissionLoopRead, Description: "Read execution loops and traces"},
	}
	result := make(map[string]uint64, len(seeds))
	for _, seed := range seeds {
		perm := models.Permission{Code: seed.Code}
		if err := database.Where("code = ?", seed.Code).First(&perm).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				perm.Description = seed.Description
				if createErr := database.Create(&perm).Error; createErr != nil {
					return nil, createErr
				}
			} else {
				return nil, err
			}
		}
		result[seed.Code] = perm.ID
	}
	return result, nil
}

// ensureRolePermissions 绑定角色与权限关系，保证 admin/member 的权限集完整。
func ensureRolePermissions(database *gorm.DB, roleIDs map[string]uint64, permissionIDs map[string]uint64) error {
	adminPerms := []string{
		models.PermissionRunCreate,
		models.PermissionRunRead,
		models.PermissionRunReplay,
		models.PermissionUserReadSelf,
		models.PermissionUserUpdateSelf,
		models.PermissionUserManage,
		models.PermissionChatUse,
		models.PermissionAgentCreate,
		models.PermissionAgentRead,
		models.PermissionAgentUpdate,
		models.PermissionAgentActivate,
		models.PermissionAgentManage,
		models.PermissionMCPCreate,
		models.PermissionMCPRead,
		models.PermissionMCPUpdate,
		models.PermissionMCPInvoke,
		models.PermissionMCPManage,
		models.PermissionLoopRead,
	}
	memberPerms := []string{
		models.PermissionRunCreate,
		models.PermissionRunRead,
		models.PermissionRunReplay,
		models.PermissionUserReadSelf,
		models.PermissionUserUpdateSelf,
		models.PermissionChatUse,
		models.PermissionAgentCreate,
		models.PermissionAgentRead,
		models.PermissionAgentUpdate,
		models.PermissionAgentActivate,
		models.PermissionMCPCreate,
		models.PermissionMCPRead,
		models.PermissionMCPUpdate,
		models.PermissionMCPInvoke,
		models.PermissionLoopRead,
	}

	if err := bindRolePermissions(database, roleIDs[models.RoleAdmin], adminPerms, permissionIDs); err != nil {
		return err
	}
	if err := bindRolePermissions(database, roleIDs[models.RoleMember], memberPerms, permissionIDs); err != nil {
		return err
	}
	return nil
}

// bindRolePermissions 按权限码列表为指定角色补齐缺失的权限绑定。
func bindRolePermissions(database *gorm.DB, roleID uint64, permCodes []string, permissionIDs map[string]uint64) error {
	for _, code := range permCodes {
		permID, ok := permissionIDs[code]
		if !ok {
			continue
		}
		rp := models.RolePermission{RoleID: roleID, PermissionID: permID}
		if err := database.Where("role_id = ? AND permission_id = ?", roleID, permID).First(&rp).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				if createErr := database.Create(&rp).Error; createErr != nil {
					return createErr
				}
			} else {
				return err
			}
		}
	}
	return nil
}

// ensureAllUsersHaveMemberRole 为存量用户补齐 member 角色，保证默认可用权限。
func ensureAllUsersHaveMemberRole(database *gorm.DB, memberRoleID uint64) error {
	var userIDs []uint64
	if err := database.Model(&models.User{}).Pluck("id", &userIDs).Error; err != nil {
		return err
	}
	for _, uid := range userIDs {
		ur := models.UserRole{UserID: uid, RoleID: memberRoleID}
		if err := database.Where("user_id = ? AND role_id = ?", uid, memberRoleID).First(&ur).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				if createErr := database.Create(&ur).Error; createErr != nil {
					return createErr
				}
			} else {
				return err
			}
		}
	}
	return nil
}

// ensureBootstrapAdmin 按配置用户名补 admin 角色，不存在用户时仅告警不中断启动。
func ensureBootstrapAdmin(database *gorm.DB, cfg *config.Config, adminRoleID uint64) error {
	username := strings.TrimSpace(cfg.RBACBootstrapAdminUsername)
	if username == "" {
		return nil
	}

	var user models.User
	if err := database.Where("username = ?", username).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("RBAC bootstrap admin user %q not found; skip admin role assign", username)
			return nil
		}
		return err
	}
	ur := models.UserRole{UserID: uint64(user.ID), RoleID: adminRoleID}
	if err := database.Where("user_id = ? AND role_id = ?", ur.UserID, ur.RoleID).First(&ur).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if createErr := database.Create(&ur).Error; createErr != nil {
				return createErr
			}
		} else {
			return err
		}
	}
	return nil
}
