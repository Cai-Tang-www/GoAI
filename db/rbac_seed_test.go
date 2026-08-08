package db

import (
	"testing"

	"GoAI/config"
	"GoAI/models"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestSeedRBACValidatesExplicitDependencies(t *testing.T) {
	if err := SeedRBAC(nil, nil); err == nil {
		t.Fatal("expected nil config error")
	}
	if err := SeedRBAC(nil, &config.Config{RBACEnable: true}); err == nil {
		t.Fatal("expected nil database error when RBAC is enabled")
	}
	if err := SeedRBAC(nil, &config.Config{RBACEnable: false}); err != nil {
		t.Fatalf("disabled RBAC should not require a database: %v", err)
	}

}

func TestSeedRBACIdempotent(t *testing.T) {
	gdb, err := gorm.Open(sqlite.Open("file:rbac_seed_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := gdb.AutoMigrate(
		&models.User{},
		&models.Role{},
		&models.Permission{},
		&models.UserRole{},
		&models.RolePermission{},
	); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	cfg := &config.Config{
		RBACEnable:                 true,
		RBACBootstrapAdminUsername: "admin",
	}

	admin := models.User{Username: "admin", Email: "admin@test.com", Password: "x"}
	member := models.User{Username: "member", Email: "member@test.com", Password: "x"}
	if err := gdb.Create(&admin).Error; err != nil {
		t.Fatalf("create admin failed: %v", err)
	}
	if err := gdb.Create(&member).Error; err != nil {
		t.Fatalf("create member failed: %v", err)
	}

	if err := SeedRBAC(gdb, cfg); err != nil {
		t.Fatalf("seed rbac first time failed: %v", err)
	}
	if err := SeedRBAC(gdb, cfg); err != nil {
		t.Fatalf("seed rbac second time failed: %v", err)
	}

	var roleCount int64
	if err := gdb.Model(&models.Role{}).Count(&roleCount).Error; err != nil {
		t.Fatalf("count roles failed: %v", err)
	}
	if roleCount != 2 {
		t.Fatalf("expected 2 roles, got %d", roleCount)
	}

	var permCount int64
	if err := gdb.Model(&models.Permission{}).Count(&permCount).Error; err != nil {
		t.Fatalf("count permissions failed: %v", err)
	}
	if permCount != 17 {
		t.Fatalf("expected 17 permissions, got %d", permCount)
	}

	var adminRole models.Role
	if err := gdb.Where("name = ?", models.RoleAdmin).First(&adminRole).Error; err != nil {
		t.Fatalf("query admin role failed: %v", err)
	}
	var memberRole models.Role
	if err := gdb.Where("name = ?", models.RoleMember).First(&memberRole).Error; err != nil {
		t.Fatalf("query member role failed: %v", err)
	}

	var allUserRoles []models.UserRole
	if err := gdb.Find(&allUserRoles).Error; err != nil {
		t.Fatalf("query user roles failed: %v", err)
	}
	if len(allUserRoles) < 3 {
		t.Fatalf("expected at least 3 user roles (2 member + 1 admin), got %d", len(allUserRoles))
	}

	var adminBindings int64
	if err := gdb.Model(&models.UserRole{}).Where("user_id = ? AND role_id = ?", uint64(admin.ID), adminRole.ID).Count(&adminBindings).Error; err != nil {
		t.Fatalf("count admin bindings failed: %v", err)
	}
	if adminBindings != 1 {
		t.Fatalf("expected admin user have one admin role binding, got %d", adminBindings)
	}
	var memberBindings int64
	if err := gdb.Model(&models.UserRole{}).Where("role_id = ?", memberRole.ID).Count(&memberBindings).Error; err != nil {
		t.Fatalf("count member bindings failed: %v", err)
	}
	if memberBindings != 2 {
		t.Fatalf("expected exactly 2 member role bindings, got %d", memberBindings)
	}

	var memberManageBindings int64
	if err := gdb.Table("role_permissions AS rp").
		Joins("JOIN permissions p ON p.id = rp.permission_id").
		Where("rp.role_id = ? AND p.code = ?", memberRole.ID, models.PermissionAgentManage).
		Count(&memberManageBindings).Error; err != nil {
		t.Fatalf("count member agent manage permission failed: %v", err)
	}
	if memberManageBindings != 0 {
		t.Fatalf("member role must not have agent:manage, got %d bindings", memberManageBindings)
	}

	for _, code := range []string{
		models.PermissionAgentCreate,
		models.PermissionAgentRead,
		models.PermissionAgentUpdate,
		models.PermissionAgentActivate,
		models.PermissionMCPCreate,
		models.PermissionMCPRead,
		models.PermissionMCPUpdate,
		models.PermissionMCPInvoke,
	} {
		var count int64
		if err := gdb.Table("role_permissions AS rp").
			Joins("JOIN permissions p ON p.id = rp.permission_id").
			Where("rp.role_id = ? AND p.code = ?", memberRole.ID, code).
			Count(&count).Error; err != nil {
			t.Fatalf("count member permission %s failed: %v", code, err)
		}
		if count != 1 {
			t.Fatalf("member role must have %s exactly once, got %d", code, count)
		}
	}

	var memberMCPManageBindings int64
	if err := gdb.Table("role_permissions AS rp").
		Joins("JOIN permissions p ON p.id = rp.permission_id").
		Where("rp.role_id = ? AND p.code = ?", memberRole.ID, models.PermissionMCPManage).
		Count(&memberMCPManageBindings).Error; err != nil {
		t.Fatalf("count member mcp manage permission failed: %v", err)
	}
	if memberMCPManageBindings != 0 {
		t.Fatalf("member role must not have mcp:manage, got %d bindings", memberMCPManageBindings)
	}
}

func TestAssignMemberRoleIsIdempotent(t *testing.T) {
	database, err := gorm.Open(sqlite.Open("file:assign_member_role_test?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := database.AutoMigrate(&models.User{}, &models.Role{}, &models.UserRole{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	user := models.User{Username: "member", Email: "member@role.test", Password: "x"}
	if err := database.Create(&user).Error; err != nil {
		t.Fatalf("create user failed: %v", err)
	}
	role := models.Role{Name: models.RoleMember}
	if err := database.Create(&role).Error; err != nil {
		t.Fatalf("create member role failed: %v", err)
	}

	if err := AssignMemberRole(database, uint64(user.ID)); err != nil {
		t.Fatalf("assign member role first time failed: %v", err)
	}
	if err := AssignMemberRole(database, uint64(user.ID)); err != nil {
		t.Fatalf("assign member role second time failed: %v", err)
	}
	var count int64
	if err := database.Model(&models.UserRole{}).Where("user_id = ? AND role_id = ?", user.ID, role.ID).Count(&count).Error; err != nil {
		t.Fatalf("count member role bindings failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("member role binding count = %d, want 1", count)
	}
}
