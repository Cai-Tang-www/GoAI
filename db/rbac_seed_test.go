package db

import (
	"GoAI/config"
	"GoAI/models"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

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
	DB = gdb
	config.AppConfig = &config.Config{
		RBACEnable:                 true,
		RBACBootstrapAdminUsername: "admin",
	}

	admin := models.User{Username: "admin", Email: "admin@test.com", Password: "x"}
	member := models.User{Username: "member", Email: "member@test.com", Password: "x"}
	if err := DB.Create(&admin).Error; err != nil {
		t.Fatalf("create admin failed: %v", err)
	}
	if err := DB.Create(&member).Error; err != nil {
		t.Fatalf("create member failed: %v", err)
	}

	if err := SeedRBAC(); err != nil {
		t.Fatalf("seed rbac first time failed: %v", err)
	}
	if err := SeedRBAC(); err != nil {
		t.Fatalf("seed rbac second time failed: %v", err)
	}

	var roleCount int64
	if err := DB.Model(&models.Role{}).Count(&roleCount).Error; err != nil {
		t.Fatalf("count roles failed: %v", err)
	}
	if roleCount != 2 {
		t.Fatalf("expected 2 roles, got %d", roleCount)
	}

	var permCount int64
	if err := DB.Model(&models.Permission{}).Count(&permCount).Error; err != nil {
		t.Fatalf("count permissions failed: %v", err)
	}
	if permCount != 7 {
		t.Fatalf("expected 7 permissions, got %d", permCount)
	}

	var adminRole models.Role
	if err := DB.Where("name = ?", models.RoleAdmin).First(&adminRole).Error; err != nil {
		t.Fatalf("query admin role failed: %v", err)
	}
	var memberRole models.Role
	if err := DB.Where("name = ?", models.RoleMember).First(&memberRole).Error; err != nil {
		t.Fatalf("query member role failed: %v", err)
	}

	var allUserRoles []models.UserRole
	if err := DB.Find(&allUserRoles).Error; err != nil {
		t.Fatalf("query user roles failed: %v", err)
	}
	if len(allUserRoles) < 3 {
		t.Fatalf("expected at least 3 user roles (2 member + 1 admin), got %d", len(allUserRoles))
	}

	var adminBindings int64
	if err := DB.Model(&models.UserRole{}).Where("user_id = ? AND role_id = ?", uint64(admin.ID), adminRole.ID).Count(&adminBindings).Error; err != nil {
		t.Fatalf("count admin bindings failed: %v", err)
	}
	if adminBindings != 1 {
		t.Fatalf("expected admin user have one admin role binding, got %d", adminBindings)
	}
	var memberBindings int64
	if err := DB.Model(&models.UserRole{}).Where("role_id = ?", memberRole.ID).Count(&memberBindings).Error; err != nil {
		t.Fatalf("count member bindings failed: %v", err)
	}
	if memberBindings != 2 {
		t.Fatalf("expected exactly 2 member role bindings, got %d", memberBindings)
	}
}
