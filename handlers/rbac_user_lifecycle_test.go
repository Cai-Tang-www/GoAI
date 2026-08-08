package handlers_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"GoAI/config"
	"GoAI/db"
	"GoAI/middlewares"
	"GoAI/models"
)

func TestRegisterUserAssignsMemberRoleAndCanUseSelfRoute(t *testing.T) {
	database := openSQLiteTestDB(t)
	if err := database.AutoMigrate(&models.User{}, &models.Role{}, &models.Permission{}, &models.UserRole{}, &models.RolePermission{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	config.AppConfig = &config.Config{JWTSecret: "rbac-lifecycle-register-secret", RBACEnable: true, ModelProviders: map[string]config.ModelProviderConfig{}}
	if err := db.SeedRBAC(database, config.AppConfig); err != nil {
		t.Fatalf("seed RBAC failed: %v", err)
	}
	router := newTestRouter(t, database, nil)

	request := httptest.NewRequest(http.MethodPost, "/auth/register", bytes.NewBufferString(`{"username":"new-member","email":"new-member@example.com","password":"pass123"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("register status = %d, want %d: %s", response.Code, http.StatusCreated, response.Body.String())
	}
	var user models.User
	if err := database.Where("username = ?", "new-member").First(&user).Error; err != nil {
		t.Fatalf("load registered user failed: %v", err)
	}
	var memberRole models.Role
	if err := database.Where("name = ?", models.RoleMember).First(&memberRole).Error; err != nil {
		t.Fatalf("load member role failed: %v", err)
	}
	var binding models.UserRole
	if err := database.Where("user_id = ? AND role_id = ?", user.ID, memberRole.ID).First(&binding).Error; err != nil {
		t.Fatalf("registered user member role binding missing: %v", err)
	}

	token, err := middlewares.GenerateToken(user.ID)
	if err != nil {
		t.Fatalf("generate user token failed: %v", err)
	}
	selfRequest := httptest.NewRequest(http.MethodGet, "/api/users/"+itoa(user.ID), nil)
	selfRequest.Header.Set("Authorization", "Bearer "+token)
	selfResponse := httptest.NewRecorder()
	router.ServeHTTP(selfResponse, selfRequest)
	if selfResponse.Code != http.StatusOK {
		t.Fatalf("new member self route status = %d, want %d: %s", selfResponse.Code, http.StatusOK, selfResponse.Body.String())
	}
}

func TestAdminCreateUserAssignsMemberRole(t *testing.T) {
	database := openSQLiteTestDB(t)
	if err := database.AutoMigrate(&models.User{}, &models.Role{}, &models.Permission{}, &models.UserRole{}, &models.RolePermission{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	admin := models.User{Username: "lifecycle-admin", Email: "lifecycle-admin@example.com", Password: "x"}
	if err := database.Create(&admin).Error; err != nil {
		t.Fatalf("create admin failed: %v", err)
	}
	config.AppConfig = &config.Config{
		JWTSecret: "rbac-lifecycle-admin-secret", RBACEnable: true,
		RBACBootstrapAdminUsername: admin.Username, ModelProviders: map[string]config.ModelProviderConfig{},
	}
	if err := db.SeedRBAC(database, config.AppConfig); err != nil {
		t.Fatalf("seed RBAC failed: %v", err)
	}
	router := newTestRouter(t, database, nil)
	token, err := middlewares.GenerateToken(admin.ID)
	if err != nil {
		t.Fatalf("generate admin token failed: %v", err)
	}
	body, err := json.Marshal(map[string]string{"username": "created-member", "email": "created-member@example.com", "password": "pass123"})
	if err != nil {
		t.Fatalf("marshal user payload failed: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/users", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("admin create user status = %d, want %d: %s", response.Code, http.StatusCreated, response.Body.String())
	}
	var user models.User
	if err := database.Where("username = ?", "created-member").First(&user).Error; err != nil {
		t.Fatalf("load created user failed: %v", err)
	}
	var memberRole models.Role
	if err := database.Where("name = ?", models.RoleMember).First(&memberRole).Error; err != nil {
		t.Fatalf("load member role failed: %v", err)
	}
	var bindingCount int64
	if err := database.Model(&models.UserRole{}).Where("user_id = ? AND role_id = ?", user.ID, memberRole.ID).Count(&bindingCount).Error; err != nil {
		t.Fatalf("count member role binding failed: %v", err)
	}
	if bindingCount != 1 {
		t.Fatalf("created user member role binding count = %d, want 1", bindingCount)
	}
}

func TestRBACUserCreationRollsBackWhenMemberRoleCannotBeAssigned(t *testing.T) {
	database := openSQLiteTestDB(t)
	if err := database.AutoMigrate(&models.User{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	config.AppConfig = &config.Config{JWTSecret: "rbac-lifecycle-rollback-secret", RBACEnable: true, ModelProviders: map[string]config.ModelProviderConfig{}}
	router := newTestRouter(t, database, nil)
	request := httptest.NewRequest(http.MethodPost, "/auth/register", bytes.NewBufferString(`{"username":"rolled-back","email":"rolled-back@example.com","password":"pass123"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("rollback register status = %d, want %d: %s", response.Code, http.StatusInternalServerError, response.Body.String())
	}
	var count int64
	if err := database.Model(&models.User{}).Where("username = ?", "rolled-back").Count(&count).Error; err != nil {
		t.Fatalf("count rolled-back user failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("user persisted after member role assignment failure: %d", count)
	}
}

func itoa(value uint) string {
	return strconv.FormatUint(uint64(value), 10)
}
