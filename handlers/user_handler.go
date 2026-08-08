package handlers

import (
	"GoAI/db"
	"GoAI/middlewares"
	"GoAI/models"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

// UserHandler 处理用户认证与用户管理接口，并显式持有数据库依赖。
type UserHandler struct {
	database   *gorm.DB
	rbacEnable bool
}

// NewUserHandler 创建用户接口处理器，并显式注入 RBAC 开关。
func NewUserHandler(database *gorm.DB, rbacEnable bool) *UserHandler {
	return &UserHandler{database: database, rbacEnable: rbacEnable}
}

// RegisterUser 处理用户注册请求
func (h *UserHandler) RegisterUser(c *gin.Context) {
	var req userWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid user payload", nil))
		return
	}
	normalizeUserWriteRequest(&req)
	if appErr := validateUserWriteRequest(req, true); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}

	var existingUser models.User
	if result := h.database.Where("username = ? OR email = ?", req.Username, req.Email).First(&existingUser); result.RowsAffected > 0 {
		middlewares.AbortWithError(c, middlewares.UserAlreadyExistsError())
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("password hash failed", err))
		return
	}

	user := models.User{
		Username: req.Username,
		Email:    req.Email,
		Password: string(hashedPassword),
	}
	if err := h.createUser(&user); err != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("create user failed", err))
		return
	}

	middlewares.Success(c, http.StatusCreated, gin.H{
		"user_id":  user.ID,
		"username": user.Username,
	}, "register success")
}

// LoginUser 处理用户登录请求
func (h *UserHandler) LoginUser(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid login payload", nil))
		return
	}
	normalizeLoginRequest(&req)
	if appErr := validateLoginRequest(req); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}

	var user models.User
	if result := h.database.Where("username = ?", req.Username).First(&user); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidCredentials())
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)); err != nil {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidCredentials())
		return
	}

	token, err := middlewares.GenerateToken(user.ID)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("generate token failed", err))
		return
	}

	middlewares.Success(c, http.StatusOK, gin.H{"token": token}, "login success")
}

// CreateUser 处理创建用户的请求
func (h *UserHandler) CreateUser(c *gin.Context) {
	var req userWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid user payload", nil))
		return
	}
	normalizeUserWriteRequest(&req)
	if appErr := validateUserWriteRequest(req, true); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}

	var existingUser models.User
	if result := h.database.Where("username = ? OR email = ?", req.Username, req.Email).First(&existingUser); result.RowsAffected > 0 {
		middlewares.AbortWithError(c, middlewares.UserAlreadyExistsError())
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("password hash failed", err))
		return
	}

	user := models.User{
		Username: req.Username,
		Email:    req.Email,
		Password: string(hashedPassword),
	}
	if err := h.createUser(&user); err != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("create user failed", err))
		return
	}

	middlewares.Success(c, http.StatusCreated, buildUserPayload(user.ID, user.Username, user.Email, user.CreatedAt, user.UpdatedAt), "success")
}

func (h *UserHandler) createUser(user *models.User) error {
	if user == nil {
		return gorm.ErrInvalidData
	}
	return h.database.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(user).Error; err != nil {
			return err
		}
		if !h.rbacEnable {
			return nil
		}
		return db.AssignMemberRole(tx, uint64(user.ID))
	})
}

// GetUserByID 处理根据 ID 获取用户的请求
func (h *UserHandler) GetUserByID(c *gin.Context) {
	id := c.Param("id")
	userID, err := strconv.ParseUint(id, 10, 32)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.InvalidIDError())
		return
	}

	var user models.User
	if result := h.database.First(&user, userID); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.UserNotFoundError())
		return
	}

	middlewares.Success(c, http.StatusOK, buildUserPayload(user.ID, user.Username, user.Email, user.CreatedAt, user.UpdatedAt), "success")
}

// UpdateUser 处理更新用户的请求
func (h *UserHandler) UpdateUser(c *gin.Context) {
	id := c.Param("id")
	userID, err := strconv.ParseUint(id, 10, 32)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.InvalidIDError())
		return
	}

	var user models.User
	if result := h.database.First(&user, userID); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.UserNotFoundError())
		return
	}

	var req userWriteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed("invalid user payload", nil))
		return
	}
	normalizeUserWriteRequest(&req)
	if appErr := validateUserWriteRequest(req, false); appErr != nil {
		middlewares.AbortWithError(c, appErr)
		return
	}

	var existingUser models.User
	if result := h.database.Where("(username = ? OR email = ?) AND id <> ?", req.Username, req.Email, user.ID).First(&existingUser); result.RowsAffected > 0 {
		middlewares.AbortWithError(c, middlewares.UserAlreadyExistsError())
		return
	}

	user.Username = req.Username
	user.Email = req.Email
	if req.Password != "" {
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
		if err != nil {
			middlewares.AbortWithError(c, middlewares.InternalError("password hash failed", err))
			return
		}
		user.Password = string(hashedPassword)
	}

	if result := h.database.Save(&user); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("update user failed", result.Error))
		return
	}

	middlewares.Success(c, http.StatusOK, buildUserPayload(user.ID, user.Username, user.Email, user.CreatedAt, user.UpdatedAt), "success")
}

// DeleteUser 处理删除用户的请求
func (h *UserHandler) DeleteUser(c *gin.Context) {
	id := c.Param("id")
	userID, err := strconv.ParseUint(id, 10, 32)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.InvalidIDError())
		return
	}

	if result := h.database.Delete(&models.User{}, userID); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("delete user failed", result.Error))
		return
	}

	middlewares.Success(c, http.StatusOK, gin.H{"id": userID}, "user deleted")
}

// ListUsers 处理获取所有用户的请求
func (h *UserHandler) ListUsers(c *gin.Context) {
	var users []models.User
	if result := h.database.Find(&users); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("list users failed", result.Error))
		return
	}

	payloads := make([]userPayload, 0, len(users))
	for _, user := range users {
		payloads = append(payloads, buildUserPayload(user.ID, user.Username, user.Email, user.CreatedAt, user.UpdatedAt))
	}
	middlewares.Success(c, http.StatusOK, payloads, "success")
}
