package db

import (
	"GoAI/config"
	"GoAI/models"
	"errors"
	"log"
	"strings"

	"gorm.io/gorm"
)

// SeedRBAC 在启动阶段执行 RBAC 角色、权限和映射的幂等补种。
func SeedRBAC() error {
	if DB == nil || config.AppConfig == nil || !config.AppConfig.RBACEnable {
		return nil
	}

	roleIDs, err := ensureRoles()
	if err != nil {
		return err
	}
	permissionIDs, err := ensurePermissions()
	if err != nil {
		return err
	}
	if err := ensureRolePermissions(roleIDs, permissionIDs); err != nil {
		return err
	}
	if err := ensureAllUsersHaveMemberRole(roleIDs[models.RoleMember]); err != nil {
		return err
	}
	if err := ensureBootstrapAdmin(roleIDs[models.RoleAdmin]); err != nil {
		return err
	}
	return nil
}

// ensureRoles 确保基础角色存在，并返回角色名到角色 ID 的映射。
func ensureRoles() (map[string]uint64, error) {
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
		if err := DB.Where("name = ?", seed.Name).First(&role).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				role.Description = seed.Description
				if createErr := DB.Create(&role).Error; createErr != nil {
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
func ensurePermissions() (map[string]uint64, error) {
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
	}
	result := make(map[string]uint64, len(seeds))
	for _, seed := range seeds {
		perm := models.Permission{Code: seed.Code}
		if err := DB.Where("code = ?", seed.Code).First(&perm).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				perm.Description = seed.Description
				if createErr := DB.Create(&perm).Error; createErr != nil {
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
func ensureRolePermissions(roleIDs map[string]uint64, permissionIDs map[string]uint64) error {
	adminPerms := []string{
		models.PermissionRunCreate,
		models.PermissionRunRead,
		models.PermissionRunReplay,
		models.PermissionUserReadSelf,
		models.PermissionUserUpdateSelf,
		models.PermissionUserManage,
		models.PermissionChatUse,
	}
	memberPerms := []string{
		models.PermissionRunCreate,
		models.PermissionRunRead,
		models.PermissionRunReplay,
		models.PermissionUserReadSelf,
		models.PermissionUserUpdateSelf,
		models.PermissionChatUse,
	}

	if err := bindRolePermissions(roleIDs[models.RoleAdmin], adminPerms, permissionIDs); err != nil {
		return err
	}
	if err := bindRolePermissions(roleIDs[models.RoleMember], memberPerms, permissionIDs); err != nil {
		return err
	}
	return nil
}

// bindRolePermissions 按权限码列表为指定角色补齐缺失的权限绑定。
func bindRolePermissions(roleID uint64, permCodes []string, permissionIDs map[string]uint64) error {
	for _, code := range permCodes {
		permID, ok := permissionIDs[code]
		if !ok {
			continue
		}
		rp := models.RolePermission{RoleID: roleID, PermissionID: permID}
		if err := DB.Where("role_id = ? AND permission_id = ?", roleID, permID).First(&rp).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				if createErr := DB.Create(&rp).Error; createErr != nil {
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
func ensureAllUsersHaveMemberRole(memberRoleID uint64) error {
	var userIDs []uint64
	if err := DB.Model(&models.User{}).Pluck("id", &userIDs).Error; err != nil {
		return err
	}
	for _, uid := range userIDs {
		ur := models.UserRole{UserID: uid, RoleID: memberRoleID}
		if err := DB.Where("user_id = ? AND role_id = ?", uid, memberRoleID).First(&ur).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				if createErr := DB.Create(&ur).Error; createErr != nil {
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
func ensureBootstrapAdmin(adminRoleID uint64) error {
	username := strings.TrimSpace(config.AppConfig.RBACBootstrapAdminUsername)
	if username == "" {
		return nil
	}

	var user models.User
	if err := DB.Where("username = ?", username).First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("RBAC bootstrap admin user %q not found; skip admin role assign", username)
			return nil
		}
		return err
	}
	ur := models.UserRole{UserID: uint64(user.ID), RoleID: adminRoleID}
	if err := DB.Where("user_id = ? AND role_id = ?", ur.UserID, ur.RoleID).First(&ur).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if createErr := DB.Create(&ur).Error; createErr != nil {
				return createErr
			}
		} else {
			return err
		}
	}
	return nil
}
