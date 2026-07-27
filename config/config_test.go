package config

import (
	"strings"
	"testing"
)

// TestValidateStartupSuccess 验证完整关键配置可以通过启动期校验。
func TestValidateStartupSuccess(t *testing.T) {
	cfg := &Config{
		MySQLHost:             "localhost",
		MySQLPort:             3306,
		MySQLUser:             "root",
		MySQLDatabase:         "goai",
		RedisHost:             "localhost",
		RedisPort:             6379,
		ServerPort:            "8080",
		KafkaBootstrapServers: "localhost:9092",
		KafkaRunTopic:         "run_execute",
		KafkaRunGroupID:       "run-worker-group",
		JWTSecret:             "test-secret",
	}
	if err := cfg.ValidateStartup(); err != nil {
		t.Fatalf("expected config valid, got %v", err)
	}
}

// TestValidateStartupReportsMissingCriticalFields 验证缺失关键配置时会返回可读错误。
func TestValidateStartupReportsMissingCriticalFields(t *testing.T) {
	cfg := &Config{}
	err := cfg.ValidateStartup()
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	message := err.Error()
	for _, want := range []string{"MYSQL_DATABASE", "REDIS_PORT", "KAFKA_BOOTSTRAP_SERVERS", "JWT_SECRET"} {
		if !strings.Contains(message, want) {
			t.Fatalf("expected error message contains %s, got %s", want, message)
		}
	}
}

// TestValidateStartupRequiresDefaultProviderProfile 验证启用 provider 配置时必须存在完整默认 profile。
func TestValidateStartupRequiresDefaultProviderProfile(t *testing.T) {
	cfg := &Config{
		MySQLHost:             "localhost",
		MySQLPort:             3306,
		MySQLUser:             "root",
		MySQLDatabase:         "goai",
		RedisHost:             "localhost",
		RedisPort:             6379,
		ServerPort:            "8080",
		KafkaBootstrapServers: "localhost:9092",
		KafkaRunTopic:         "run_execute",
		KafkaRunGroupID:       "run-worker-group",
		JWTSecret:             "test-secret",
		ModelProviderDefault:  "deepseek",
		ModelProviders: map[string]ModelProviderConfig{
			"deepseek": {
				Driver:       "openai_compatible",
				BaseURL:      "https://api.deepseek.com",
				APIKey:       "",
				DefaultModel: "deepseek-chat",
			},
		},
	}
	err := cfg.ValidateStartup()
	if err == nil {
		t.Fatal("expected provider validation error, got nil")
	}
	if !strings.Contains(err.Error(), "MODEL_API_KEY") {
		t.Fatalf("expected provider api key error, got %v", err)
	}
}

// TestLoadConfigRejectsInvalidPortEnv 验证端口环境变量非法时会在加载阶段直接失败。
func TestLoadConfigRejectsInvalidPortEnv(t *testing.T) {
	t.Setenv("MYSQL_HOST", "localhost")
	t.Setenv("MYSQL_PORT", "3306")
	t.Setenv("MYSQL_USER", "root")
	t.Setenv("MYSQL_DATABASE", "goai")
	t.Setenv("REDIS_HOST", "localhost")
	t.Setenv("REDIS_PORT", "bad-port")
	t.Setenv("SERVER_PORT", "8080")
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	t.Setenv("KAFKA_RUN_TOPIC", "run_execute")
	t.Setenv("KAFKA_RUN_GROUP_ID", "run-worker-group")
	t.Setenv("JWT_SECRET", "test-secret")
	t.Setenv("MODEL_PROVIDER_DEFAULT", "")

	err := LoadConfig()
	if err == nil {
		t.Fatal("expected load config error, got nil")
	}
	if !strings.Contains(err.Error(), "REDIS_PORT") {
		t.Fatalf("expected REDIS_PORT error, got %v", err)
	}
}
