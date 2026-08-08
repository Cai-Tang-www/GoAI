package db

import (
	"fmt"

	"GoAI/config"
	"GoAI/models"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// New 根据配置创建数据库连接实例。连接生命周期由调用方持有和关闭。
func New(cfg *config.Config) (*gorm.DB, error) {
	if cfg == nil {
		return nil, fmt.Errorf("creating database: config is nil")
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.MySQLUser,
		cfg.MySQLRootPassword,
		cfg.MySQLHost,
		cfg.MySQLPort,
		cfg.MySQLDatabase,
	)
	database, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	return database, nil
}

// Migrate 执行 GoAI 当前持久化模型的数据库迁移。
func Migrate(database *gorm.DB) error {
	if database == nil {
		return fmt.Errorf("migrating database: database is nil")
	}
	if err := database.AutoMigrate(
		&models.User{},
		&models.Role{},
		&models.Permission{},
		&models.UserRole{},
		&models.RolePermission{},
		&models.Agent{},
		&models.AgentEndpoint{},
		&models.AgentCapability{},
		&models.MCPServer{},
		&models.MCPTool{},
		&models.Workflow{},
		&models.Thread{},
		&models.Message{},
		&models.Delegation{},
		&models.DelegationGroup{},
		&models.A2APushConfig{},
		&models.Run{},
		&models.RunStep{},
		&models.RunInterrupt{},
		&models.RunIdempotency{},
		&models.LoopRecord{},
		&models.LoopEvaluation{},
	); err != nil {
		return fmt.Errorf("auto migrating database: %w", err)
	}
	return nil
}

// Close 关闭指定 GORM 实例底层的数据库连接池。
func Close(database *gorm.DB) error {
	if database == nil {
		return nil
	}
	sqlDB, err := database.DB()
	if err != nil {
		return fmt.Errorf("getting database connection pool: %w", err)
	}
	if err := sqlDB.Close(); err != nil {
		return fmt.Errorf("closing database connection pool: %w", err)
	}
	return nil
}
