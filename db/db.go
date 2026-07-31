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

// InitDB 初始化 MySQL 数据库连接并执行表迁移。
func InitDB() {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		config.AppConfig.MySQLUser,
		config.AppConfig.MySQLRootPassword,
		config.AppConfig.MySQLHost,
		config.AppConfig.MySQLPort,
		config.AppConfig.MySQLDatabase,
	)
	var err error
	DB, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	err = DB.AutoMigrate(
		&models.User{},
		&models.Role{},
		&models.Permission{},
		&models.UserRole{},
		&models.RolePermission{},
		&models.Agent{},
		&models.Workflow{},
		&models.Run{},
		&models.RunStep{},
		&models.RunIdempotency{},
	)
	if err != nil {
		log.Fatalf("Failed to auto migrate database: %v", err)
	}

	if err = SeedRBAC(); err != nil {
		log.Fatalf("Failed to seed RBAC data: %v", err)
	}

	log.Println("Database connected successfully!")
}

// Close 关闭 GORM 底层的数据库连接池。
func Close() error {
	if DB == nil {
		return nil
	}
	sqlDB, err := DB.DB()
	if err != nil {
		return fmt.Errorf("getting database connection pool: %w", err)
	}
	return sqlDB.Close()
}
