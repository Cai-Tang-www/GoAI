package db

import (
	"GoAI/config"
	"fmt"
	"log"

	"GoAI/models"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var DB *gorm.DB

func InitDB() {
	dsn := fmt.Sprintf("%s:%s@tcp(localhost:3306)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		"root", // 你的 MySQL 用户名
		config.AppConfig.MySQLRootPassword,
		config.AppConfig.MySQLDatabase,
	)

	var err error
	DB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	err = DB.AutoMigrate(&models.User{})
	if err != nil {
		log.Fatalf("Failed to auto migrate database: %v", err)
	}

	log.Println("Database connected successfully!")
}
