package handlers

import (
	"net/http"
	"strconv"

	"GoAI/db"
	"GoAI/middlewares"
	"GoAI/models"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

// RegisterUser 处理用户注册请求
func RegisterUser(c *gin.Context) {
	var user models.User
	if err := c.ShouldBindJSON(&user); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed(err.Error(), nil))
		return
	}

	// 检查用户是否已存在
	var existingUser models.User
	if result := db.DB.Where("username = ? OR email = ?", user.Username, user.Email).First(&existingUser); result.RowsAffected > 0 {
		middlewares.AbortWithError(c, middlewares.UserAlreadyExistsError())
		return
	}

	// 密码加密
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(user.Password), bcrypt.DefaultCost)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("password hash failed", err))
		return
	}
	user.Password = string(hashedPassword)

	if result := db.DB.Create(&user); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("create user failed", result.Error))
		return
	}

	middlewares.Success(c, http.StatusCreated, gin.H{
		"user_id":  user.ID,
		"username": user.Username,
	}, "register success")
}

// LoginUser 处理用户登录请求
func LoginUser(c *gin.Context) {
	var loginInfo struct {
		Username string `json:"username" binding:"required"`
		Password string `json:"password" binding:"required"`
	}

	if err := c.ShouldBindJSON(&loginInfo); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed(err.Error(), nil))
		return
	}

	var user models.User
	if result := db.DB.Where("username = ?", loginInfo.Username).First(&user); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidCredentials())
		return
	}

	// 验证密码
	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(loginInfo.Password)); err != nil {
		middlewares.AbortWithError(c, middlewares.UnauthorizedInvalidCredentials())
		return
	}

	// 生成 JWT Token
	token, err := middlewares.GenerateToken(user.ID)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("generate token failed", err))
		return
	}

	middlewares.Success(c, http.StatusOK, gin.H{"token": token}, "login success")
}

// CreateUser 处理创建用户的请求
func CreateUser(c *gin.Context) {
	var user models.User
	if err := c.ShouldBindJSON(&user); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed(err.Error(), nil))
		return
	}

	var existingUser models.User
	if result := db.DB.Where("username = ? OR email = ?", user.Username, user.Email).First(&existingUser); result.RowsAffected > 0 {
		middlewares.AbortWithError(c, middlewares.UserAlreadyExistsError())
		return
	}

	// 密码加密
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(user.Password), bcrypt.DefaultCost)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("password hash failed", err))
		return
	}
	user.Password = string(hashedPassword)

	if result := db.DB.Create(&user); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("create user failed", result.Error))
		return
	}

	middlewares.Success(c, http.StatusCreated, buildUserPayload(user.ID, user.Username, user.Email, user.CreatedAt, user.UpdatedAt), "success")
}

// GetUserByID 处理根据 ID 获取用户的请求
func GetUserByID(c *gin.Context) {
	id := c.Param("id")
	userID, err := strconv.ParseUint(id, 10, 32)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.InvalidIDError())
		return
	}

	var user models.User
	if result := db.DB.First(&user, userID); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.UserNotFoundError())
		return
	}

	middlewares.Success(c, http.StatusOK, buildUserPayload(user.ID, user.Username, user.Email, user.CreatedAt, user.UpdatedAt), "success")
}

// UpdateUser 处理更新用户的请求
func UpdateUser(c *gin.Context) {
	id := c.Param("id")
	userID, err := strconv.ParseUint(id, 10, 32)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.InvalidIDError())
		return
	}

	var user models.User
	if result := db.DB.First(&user, userID); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.UserNotFoundError())
		return
	}

	var req models.User
	if err := c.ShouldBindJSON(&req); err != nil {
		middlewares.AbortWithError(c, middlewares.ValidationFailed(err.Error(), nil))
		return
	}

	var existingUser models.User
	if result := db.DB.Where("(username = ? OR email = ?) AND id <> ?", req.Username, req.Email, user.ID).First(&existingUser); result.RowsAffected > 0 {
		middlewares.AbortWithError(c, middlewares.UserAlreadyExistsError())
		return
	}

	user.Username = req.Username
	user.Email = req.Email
	user.Password = req.Password

	// 如果请求中包含密码，则重新加密
	if user.Password != "" {
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(user.Password), bcrypt.DefaultCost)
		if err != nil {
			middlewares.AbortWithError(c, middlewares.InternalError("password hash failed", err))
			return
		}
		user.Password = string(hashedPassword)
	}

	if result := db.DB.Save(&user); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("update user failed", result.Error))
		return
	}

	middlewares.Success(c, http.StatusOK, buildUserPayload(user.ID, user.Username, user.Email, user.CreatedAt, user.UpdatedAt), "success")
}

// DeleteUser 处理删除用户的请求
func DeleteUser(c *gin.Context) {
	id := c.Param("id")
	userID, err := strconv.ParseUint(id, 10, 32)
	if err != nil {
		middlewares.AbortWithError(c, middlewares.InvalidIDError())
		return
	}

	if result := db.DB.Delete(&models.User{}, userID); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("delete user failed", result.Error))
		return
	}

	middlewares.Success(c, http.StatusOK, gin.H{"id": userID}, "user deleted")
}

// ListUsers 处理获取所有用户的请求
func ListUsers(c *gin.Context) {
	var users []models.User
	if result := db.DB.Find(&users); result.Error != nil {
		middlewares.AbortWithError(c, middlewares.InternalError("list users failed", result.Error))
		return
	}

	payloads := make([]userPayload, 0, len(users))
	for _, user := range users {
		payloads = append(payloads, buildUserPayload(user.ID, user.Username, user.Email, user.CreatedAt, user.UpdatedAt))
	}
	middlewares.Success(c, http.StatusOK, payloads, "success")
}
