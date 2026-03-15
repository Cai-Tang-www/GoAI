package db

import (
	"GoAI/config"
	"fmt"
	"log"

	"GoAI/models"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// DB 全局数据库连接实例
var DB *gorm.DB

// InitDB 初始化MySQL数据库连接
func InitDB() {
	dsn := fmt.Sprintf("%s:%s@tcp(localhost:3306)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		"root", // 你的 MySQL 用户名
		config.AppConfig.MySQLRootPassword,
		config.AppConfig.MySQLDatabase,
	)
	var err error
	//打开gorm连接
	DB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// 自动迁移数据库表结构
	err = DB.AutoMigrate(&models.Task{}, &models.User{}, &models.DialogueMessage{})
	if err != nil {
		log.Fatalf("Failed to auto migrate database: %v", err)
	}

	log.Println("Database connected successfully!")
}
