package config

import (
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type ModelProviderConfig struct {
	Driver       string
	BaseURL      string
	APIKey       string
	DefaultModel string
	EndpointPath string
}

// Config 配置结构体
type Config struct {
	MySQLRootPassword          string
	MySQLDatabase              string
	RedisPassword              string
	RedisPort                  int
	KafkaPort                  int
	KafkaJMXPort               int
	KafkaClusterID             string
	KafkaBootstrapServers      string
	KafkaZookeeperConnect      string
	KafkaTopic                 string
	KafkaRunTopic              string
	KafkaRunGroupID            string
	JWTSecret                  string
	RBACEnable                 bool
	RBACBootstrapAdminUsername string
	ModelScopeKey              string
	ModelScopeModel            string
	ModelProviderDefault       string
	ModelProviders             map[string]ModelProviderConfig
}

// AppConfig 全局配置
var AppConfig *Config

// LoadConfig 加载配置
func LoadConfig() error {
	err := godotenv.Load()
	if err != nil {
	}

	AppConfig = &Config{
		MySQLRootPassword:          os.Getenv("MYSQL_ROOT_PASSWORD"),
		MySQLDatabase:              os.Getenv("MYSQL_DATABASE"),
		RedisPassword:              os.Getenv("REDIS_PASSWORD"),
		KafkaBootstrapServers:      os.Getenv("KAFKA_BOOTSTRAP_SERVERS"),
		KafkaZookeeperConnect:      os.Getenv("KAFKA_ZOOKEEPER_CONNECT"),
		KafkaClusterID:             os.Getenv("KAFKA_CLUSTER_ID"),
		JWTSecret:                  os.Getenv("JWT_SECRET"),
		KafkaTopic:                 os.Getenv("KAFKA_TOPIC"),
		KafkaRunTopic:              strings.TrimSpace(os.Getenv("KAFKA_RUN_TOPIC")),
		KafkaRunGroupID:            strings.TrimSpace(os.Getenv("KAFKA_RUN_GROUP_ID")),
		RBACEnable:                 true,
		RBACBootstrapAdminUsername: strings.TrimSpace(os.Getenv("RBAC_BOOTSTRAP_ADMIN_USERNAME")),
		ModelScopeKey:              os.Getenv("MODELSCOPE_KEY"),
		ModelScopeModel:            os.Getenv("MODELSCOPE_MODEL"),
		ModelProviderDefault:       strings.ToLower(strings.TrimSpace(os.Getenv("MODEL_PROVIDER_DEFAULT"))),
		ModelProviders:             make(map[string]ModelProviderConfig),
	}

	// 转换 int 类型
	if portStr := os.Getenv("REDIS_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			AppConfig.RedisPort = port
		}
	}
	if portStr := os.Getenv("KAFKA_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			AppConfig.KafkaPort = port
		}
	}
	if portStr := os.Getenv("KAFKA_JMX_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			AppConfig.KafkaJMXPort = port
		}
	}
	if AppConfig.KafkaRunTopic == "" {
		AppConfig.KafkaRunTopic = "run_execute"
	}
	if AppConfig.KafkaRunGroupID == "" {
		AppConfig.KafkaRunGroupID = "run-worker-group"
	}
	if enableStr := strings.TrimSpace(os.Getenv("RBAC_ENABLE")); enableStr != "" {
		enableLower := strings.ToLower(enableStr)
		AppConfig.RBACEnable = enableLower == "1" || enableLower == "true" || enableLower == "yes" || enableLower == "on"
	}

	providers := []string{"mimo", "deepseek", "qwen", "modelscope", "openai"}
	for _, name := range providers {
		upper := strings.ToUpper(name)
		cfg := ModelProviderConfig{
			Driver:       strings.TrimSpace(os.Getenv("MODEL_DRIVER_" + upper)),
			BaseURL:      strings.TrimSpace(os.Getenv("MODEL_BASE_URL_" + upper)),
			APIKey:       strings.TrimSpace(os.Getenv("MODEL_API_KEY_" + upper)),
			DefaultModel: strings.TrimSpace(os.Getenv("MODEL_NAME_DEFAULT_" + upper)),
			EndpointPath: strings.TrimSpace(os.Getenv("MODEL_ENDPOINT_PATH_" + upper)),
		}
		if cfg.Driver == "" {
			cfg.Driver = "openai_compatible"
		}
		if cfg.EndpointPath == "" {
			cfg.EndpointPath = defaultEndpointPath(name)
		}
		if cfg.BaseURL != "" || cfg.APIKey != "" || cfg.DefaultModel != "" {
			AppConfig.ModelProviders[name] = cfg
		}
	}

	// 兼容旧配置：若没有显式的 modelscope 配置，复用旧 MODELSCOPE_* 环境变量。
	if _, exists := AppConfig.ModelProviders["modelscope"]; !exists &&
		(AppConfig.ModelScopeKey != "" || AppConfig.ModelScopeModel != "") {
		baseURL := strings.TrimSpace(os.Getenv("MODEL_BASE_URL_MODELSCOPE"))
		if baseURL == "" {
			baseURL = "https://api-inference.modelscope.cn"
		}
		AppConfig.ModelProviders["modelscope"] = ModelProviderConfig{
			Driver:       "openai_compatible",
			BaseURL:      baseURL,
			APIKey:       AppConfig.ModelScopeKey,
			DefaultModel: AppConfig.ModelScopeModel,
			EndpointPath: "/v1/chat/completions",
		}
	}

	return nil
}

func defaultEndpointPath(providerName string) string {
	switch strings.ToLower(strings.TrimSpace(providerName)) {
	case "deepseek":
		return "/v1/chat/completions"
	default:
		return "/chat/completions"
	}
}
