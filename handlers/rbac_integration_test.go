package handlers_test

import (
	"GoAI/config"
	"GoAI/db"
	"GoAI/middlewares"
	"GoAI/models"
	routers "GoAI/routers"
	"GoAI/services"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupRBACIntegrationDB(t *testing.T) {
	t.Helper()
	gdb, err := gorm.Open(sqlite.Open("file:rbac_integration?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := gdb.AutoMigrate(
		&models.User{},
		&models.Role{},
		&models.Permission{},
		&models.UserRole{},
		&models.RolePermission{},
		&models.Agent{},
		&models.Workflow{},
		&models.Run{},
		&models.RunStep{},
	); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	db.DB = gdb
}

func TestRBACMemberAndAdminAccess(t *testing.T) {
	setupRBACIntegrationDB(t)

	admin := models.User{Username: "admin", Email: "admin@t.com", Password: "x"}
	member := models.User{Username: "member", Email: "member@t.com", Password: "x"}
	if err := db.DB.Create(&admin).Error; err != nil {
		t.Fatalf("create admin failed: %v", err)
	}
	if err := db.DB.Create(&member).Error; err != nil {
		t.Fatalf("create member failed: %v", err)
	}

	config.AppConfig = &config.Config{
		JWTSecret:                  "rbac-test-secret",
		RBACEnable:                 true,
		RBACBootstrapAdminUsername: "admin",
		ModelProviderDefault:       "",
		ModelProviders:             map[string]config.ModelProviderConfig{},
	}
	if err := db.SeedRBAC(); err != nil {
		t.Fatalf("seed rbac failed: %v", err)
	}

	services.SetPublishRunExecuteEventForTest(func(ctx context.Context, runID string) error { return nil })
	defer services.SetPublishRunExecuteEventForTest(nil)

	agent := models.Agent{
		AgentCode:   "agent_rbac",
		Name:        "RBAC Agent",
		Description: "rbac test agent",
		OwnerUserID: uint64(member.ID),
		Status:      models.AgentStatusActive,
	}
	if err := db.DB.Create(&agent).Error; err != nil {
		t.Fatalf("create agent failed: %v", err)
	}
	workflow := models.Workflow{
		AgentID:        agent.ID,
		Version:        1,
		DefinitionJSON: `{"entry_node":"planner","nodes":[{"key":"planner","type":"planner"}],"edges":[]}`,
		Checksum:       "rbac-sum",
		IsActive:       true,
		CreatedBy:      uint64(member.ID),
	}
	if err := db.DB.Create(&workflow).Error; err != nil {
		t.Fatalf("create workflow failed: %v", err)
	}

	memberToken, err := middlewares.GenerateToken(member.ID)
	if err != nil {
		t.Fatalf("generate member token failed: %v", err)
	}
	adminToken, err := middlewares.GenerateToken(admin.ID)
	if err != nil {
		t.Fatalf("generate admin token failed: %v", err)
	}

	router := routers.InitRouter()

	// member 不能访问 users 列表（需要 user:manage）
	reqUsers := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	reqUsers.Header.Set("Authorization", "Bearer "+memberToken)
	wUsers := httptest.NewRecorder()
	router.ServeHTTP(wUsers, reqUsers)
	if wUsers.Code != http.StatusForbidden {
		t.Fatalf("member list users expected 403, got %d body=%s", wUsers.Code, wUsers.Body.String())
	}
	usersEnv := decodeEnvelope(t, wUsers)
	if usersEnv.Code != "AUTH_FORBIDDEN" {
		t.Fatalf("unexpected users error code: %s body=%s", usersEnv.Code, wUsers.Body.String())
	}

	// member 可创建 run（需要 run:create）
	createBody := map[string]any{
		"agent_code": "agent_rbac",
		"input": map[string]any{
			"prompt": "hello",
		},
	}
	raw, _ := json.Marshal(createBody)
	reqCreate := httptest.NewRequest(http.MethodPost, "/api/runs", bytes.NewReader(raw))
	reqCreate.Header.Set("Authorization", "Bearer "+memberToken)
	reqCreate.Header.Set("Content-Type", "application/json")
	wCreate := httptest.NewRecorder()
	router.ServeHTTP(wCreate, reqCreate)
	if wCreate.Code != http.StatusAccepted {
		t.Fatalf("member create run expected 202, got %d body=%s", wCreate.Code, wCreate.Body.String())
	}
	createEnv := decodeEnvelope(t, wCreate)
	if createEnv.Code != "OK" {
		t.Fatalf("unexpected create code: %s body=%s", createEnv.Code, wCreate.Body.String())
	}
	var createResp map[string]any
	_ = json.Unmarshal(createEnv.Data, &createResp)
	memberRunID, _ := createResp["run_id"].(string)
	if memberRunID == "" {
		t.Fatalf("member run_id empty: %v", createResp)
	}

	// admin 直接创建一条 run，验证 member 无法读取他人 run
	adminRun := models.Run{
		RunID:       "run_admin_owned",
		ThreadID:    "t1",
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      uint64(admin.ID),
		TriggerType: "api",
		InputJSON:   `{"prompt":"a"}`,
		Status:      models.RunStatusQueued,
	}
	if err := db.DB.Create(&adminRun).Error; err != nil {
		t.Fatalf("create admin run failed: %v", err)
	}

	reqMemberReadAdminRun := httptest.NewRequest(http.MethodGet, "/api/runs/"+adminRun.RunID, nil)
	reqMemberReadAdminRun.Header.Set("Authorization", "Bearer "+memberToken)
	wMemberReadAdminRun := httptest.NewRecorder()
	router.ServeHTTP(wMemberReadAdminRun, reqMemberReadAdminRun)
	if wMemberReadAdminRun.Code != http.StatusForbidden {
		t.Fatalf("member read admin run expected 403, got %d body=%s", wMemberReadAdminRun.Code, wMemberReadAdminRun.Body.String())
	}
	memberReadEnv := decodeEnvelope(t, wMemberReadAdminRun)
	if memberReadEnv.Code != "AUTH_FORBIDDEN" {
		t.Fatalf("unexpected member read code: %s body=%s", memberReadEnv.Code, wMemberReadAdminRun.Body.String())
	}

	// admin 可读取 member 的 run（admin 绕过 ownership）
	reqAdminReadMemberRun := httptest.NewRequest(http.MethodGet, "/api/runs/"+memberRunID, nil)
	reqAdminReadMemberRun.Header.Set("Authorization", "Bearer "+adminToken)
	wAdminReadMemberRun := httptest.NewRecorder()
	router.ServeHTTP(wAdminReadMemberRun, reqAdminReadMemberRun)
	if wAdminReadMemberRun.Code != http.StatusOK {
		t.Fatalf("admin read member run expected 200, got %d body=%s", wAdminReadMemberRun.Code, wAdminReadMemberRun.Body.String())
	}
	adminReadEnv := decodeEnvelope(t, wAdminReadMemberRun)
	if adminReadEnv.Code != "OK" {
		t.Fatalf("unexpected admin read code: %s body=%s", adminReadEnv.Code, wAdminReadMemberRun.Body.String())
	}
}

func TestRBACRoleChangeTakesEffectWithoutNewToken(t *testing.T) {
	setupRBACIntegrationDB(t)

	user := models.User{Username: "promote", Email: "promote@t.com", Password: "x"}
	if err := db.DB.Create(&user).Error; err != nil {
		t.Fatalf("create user failed: %v", err)
	}
	config.AppConfig = &config.Config{
		JWTSecret:                  "rbac-test-secret",
		RBACEnable:                 true,
		RBACBootstrapAdminUsername: "not-exist",
		ModelProviders:             map[string]config.ModelProviderConfig{},
	}
	if err := db.SeedRBAC(); err != nil {
		t.Fatalf("seed rbac failed: %v", err)
	}

	token, err := middlewares.GenerateToken(user.ID)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}

	router := routers.InitRouter()
	reqBefore := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	reqBefore.Header.Set("Authorization", "Bearer "+token)
	wBefore := httptest.NewRecorder()
	router.ServeHTTP(wBefore, reqBefore)
	if wBefore.Code != http.StatusForbidden {
		t.Fatalf("before promote expected 403, got %d body=%s", wBefore.Code, wBefore.Body.String())
	}
	beforeEnv := decodeEnvelope(t, wBefore)
	if beforeEnv.Code != "AUTH_FORBIDDEN" {
		t.Fatalf("unexpected before promote code: %s body=%s", beforeEnv.Code, wBefore.Body.String())
	}

	var adminRole models.Role
	if err := db.DB.Where("name = ?", models.RoleAdmin).First(&adminRole).Error; err != nil {
		t.Fatalf("query admin role failed: %v", err)
	}
	if err := db.DB.Create(&models.UserRole{UserID: uint64(user.ID), RoleID: adminRole.ID}).Error; err != nil {
		t.Fatalf("bind admin role failed: %v", err)
	}

	// 使用同一 token 再请求，应该立即生效（DB 实时查询授权）
	reqAfter := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	reqAfter.Header.Set("Authorization", "Bearer "+token)
	wAfter := httptest.NewRecorder()
	router.ServeHTTP(wAfter, reqAfter)
	if wAfter.Code != http.StatusOK {
		t.Fatalf("after promote expected 200, got %d body=%s", wAfter.Code, wAfter.Body.String())
	}
	afterEnv := decodeEnvelope(t, wAfter)
	if afterEnv.Code != "OK" {
		t.Fatalf("unexpected after promote code: %s body=%s", afterEnv.Code, wAfter.Body.String())
	}
}

func TestRBACUsersAndChatPermissionMatrix(t *testing.T) {
	setupRBACIntegrationDB(t)

	admin := models.User{Username: "admin2", Email: "admin2@t.com", Password: "x"}
	member := models.User{Username: "member2", Email: "member2@t.com", Password: "x"}
	outsider := models.User{Username: "outsider", Email: "outsider@t.com", Password: "x"}
	if err := db.DB.Create(&admin).Error; err != nil {
		t.Fatalf("create admin failed: %v", err)
	}
	if err := db.DB.Create(&member).Error; err != nil {
		t.Fatalf("create member failed: %v", err)
	}

	config.AppConfig = &config.Config{
		JWTSecret:                  "rbac-test-secret",
		RBACEnable:                 true,
		RBACBootstrapAdminUsername: "admin2",
		ModelProviderDefault:       "",
		ModelProviders:             map[string]config.ModelProviderConfig{},
	}
	if err := db.SeedRBAC(); err != nil {
		t.Fatalf("seed rbac failed: %v", err)
	}

	// seed 后再创建用户，确保该用户没有任何角色，用于验证缺权限 403。
	if err := db.DB.Create(&outsider).Error; err != nil {
		t.Fatalf("create outsider failed: %v", err)
	}

	memberToken, err := middlewares.GenerateToken(member.ID)
	if err != nil {
		t.Fatalf("generate member token failed: %v", err)
	}
	outsiderToken, err := middlewares.GenerateToken(outsider.ID)
	if err != nil {
		t.Fatalf("generate outsider token failed: %v", err)
	}

	router := routers.InitRouter()

	// member 可读取自己的用户信息（self: user:read_self）。
	reqGetSelf := httptest.NewRequest(http.MethodGet, "/api/users/"+strconv.FormatUint(uint64(member.ID), 10), nil)
	reqGetSelf.Header.Set("Authorization", "Bearer "+memberToken)
	wGetSelf := httptest.NewRecorder()
	router.ServeHTTP(wGetSelf, reqGetSelf)
	if wGetSelf.Code != http.StatusOK {
		t.Fatalf("member get self expected 200, got %d body=%s", wGetSelf.Code, wGetSelf.Body.String())
	}
	getSelfEnv := decodeEnvelope(t, wGetSelf)
	if getSelfEnv.Code != "OK" {
		t.Fatalf("unexpected get self code: %s body=%s", getSelfEnv.Code, wGetSelf.Body.String())
	}

	// member 读取他人资料应为 403（需要 user:manage）。
	reqGetOther := httptest.NewRequest(http.MethodGet, "/api/users/"+strconv.FormatUint(uint64(admin.ID), 10), nil)
	reqGetOther.Header.Set("Authorization", "Bearer "+memberToken)
	wGetOther := httptest.NewRecorder()
	router.ServeHTTP(wGetOther, reqGetOther)
	if wGetOther.Code != http.StatusForbidden {
		t.Fatalf("member get other expected 403, got %d body=%s", wGetOther.Code, wGetOther.Body.String())
	}
	getOtherEnv := decodeEnvelope(t, wGetOther)
	if getOtherEnv.Code != "AUTH_FORBIDDEN" {
		t.Fatalf("unexpected get other code: %s body=%s", getOtherEnv.Code, wGetOther.Body.String())
	}

	// member 更新自己资料可通过授权（具体更新成功由 handler 逻辑决定，这里只验证非 403）。
	updateSelfBody := map[string]any{
		"username": "member2",
		"email":    "member2+updated@t.com",
		"password": "x",
	}
	updateRaw, _ := json.Marshal(updateSelfBody)
	reqUpdateSelf := httptest.NewRequest(http.MethodPut, "/api/users/"+strconv.FormatUint(uint64(member.ID), 10), bytes.NewReader(updateRaw))
	reqUpdateSelf.Header.Set("Authorization", "Bearer "+memberToken)
	reqUpdateSelf.Header.Set("Content-Type", "application/json")
	wUpdateSelf := httptest.NewRecorder()
	router.ServeHTTP(wUpdateSelf, reqUpdateSelf)
	if wUpdateSelf.Code == http.StatusForbidden {
		t.Fatalf("member update self expected non-403, got %d body=%s", wUpdateSelf.Code, wUpdateSelf.Body.String())
	}

	// member 创建用户应为 403（需要 user:manage）。
	createUserBody := map[string]any{
		"username": "u_forbidden",
		"email":    "u_forbidden@t.com",
		"password": "x",
	}
	createUserRaw, _ := json.Marshal(createUserBody)
	reqCreateUser := httptest.NewRequest(http.MethodPost, "/api/users", bytes.NewReader(createUserRaw))
	reqCreateUser.Header.Set("Authorization", "Bearer "+memberToken)
	reqCreateUser.Header.Set("Content-Type", "application/json")
	wCreateUser := httptest.NewRecorder()
	router.ServeHTTP(wCreateUser, reqCreateUser)
	if wCreateUser.Code != http.StatusForbidden {
		t.Fatalf("member create user expected 403, got %d body=%s", wCreateUser.Code, wCreateUser.Body.String())
	}
	createUserEnv := decodeEnvelope(t, wCreateUser)
	if createUserEnv.Code != "AUTH_FORBIDDEN" {
		t.Fatalf("unexpected create user code: %s body=%s", createUserEnv.Code, wCreateUser.Body.String())
	}

	// member 删除用户应为 403（需要 user:manage）。
	reqDeleteUser := httptest.NewRequest(http.MethodDelete, "/api/users/"+strconv.FormatUint(uint64(admin.ID), 10), nil)
	reqDeleteUser.Header.Set("Authorization", "Bearer "+memberToken)
	wDeleteUser := httptest.NewRecorder()
	router.ServeHTTP(wDeleteUser, reqDeleteUser)
	if wDeleteUser.Code != http.StatusForbidden {
		t.Fatalf("member delete user expected 403, got %d body=%s", wDeleteUser.Code, wDeleteUser.Body.String())
	}
	deleteUserEnv := decodeEnvelope(t, wDeleteUser)
	if deleteUserEnv.Code != "AUTH_FORBIDDEN" {
		t.Fatalf("unexpected delete user code: %s body=%s", deleteUserEnv.Code, wDeleteUser.Body.String())
	}

	// member 可访问 chat（具备 chat:use），这里验证授权通过（非 403）。
	chatBody := map[string]any{
		"messages": []map[string]any{
			{"role": "user", "content": "hello"},
		},
	}
	chatRaw, _ := json.Marshal(chatBody)
	reqChatMember := httptest.NewRequest(http.MethodPost, "/api/chat", bytes.NewReader(chatRaw))
	reqChatMember.Header.Set("Authorization", "Bearer "+memberToken)
	reqChatMember.Header.Set("Content-Type", "application/json")
	wChatMember := httptest.NewRecorder()
	router.ServeHTTP(wChatMember, reqChatMember)
	if wChatMember.Code == http.StatusForbidden {
		t.Fatalf("member chat expected non-403, got %d body=%s", wChatMember.Code, wChatMember.Body.String())
	}

	// outsider 无角色，访问 chat 应为 403。
	reqChatOutsider := httptest.NewRequest(http.MethodPost, "/api/chat", bytes.NewReader(chatRaw))
	reqChatOutsider.Header.Set("Authorization", "Bearer "+outsiderToken)
	reqChatOutsider.Header.Set("Content-Type", "application/json")
	wChatOutsider := httptest.NewRecorder()
	router.ServeHTTP(wChatOutsider, reqChatOutsider)
	if wChatOutsider.Code != http.StatusForbidden {
		t.Fatalf("outsider chat expected 403, got %d body=%s", wChatOutsider.Code, wChatOutsider.Body.String())
	}
	chatOutsiderEnv := decodeEnvelope(t, wChatOutsider)
	if chatOutsiderEnv.Code != "AUTH_FORBIDDEN" {
		t.Fatalf("unexpected outsider chat code: %s body=%s", chatOutsiderEnv.Code, wChatOutsider.Body.String())
	}
}
