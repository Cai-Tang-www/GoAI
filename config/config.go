package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

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
	MySQLHost                  string
	MySQLPort                  int
	MySQLUser                  string
	MySQLRootPassword          string
	MySQLDatabase              string
	RedisHost                  string
	RedisPassword              string
	RedisPort                  int
	ServerPort                 string
	ServerShutdownTimeout      time.Duration
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

const defaultShutdownTimeout = 15 * time.Second

// AppConfig 全局配置
var AppConfig *Config

// LoadConfig 加载环境变量、填充默认值并执行启动期关键配置校验。
func LoadConfig() error {
	_ = godotenv.Load()

	appConfig := &Config{
		MySQLHost:                  strings.TrimSpace(os.Getenv("MYSQL_HOST")),
		MySQLUser:                  strings.TrimSpace(os.Getenv("MYSQL_USER")),
		MySQLRootPassword:          os.Getenv("MYSQL_ROOT_PASSWORD"),
		MySQLDatabase:              strings.TrimSpace(os.Getenv("MYSQL_DATABASE")),
		RedisHost:                  strings.TrimSpace(os.Getenv("REDIS_HOST")),
		RedisPassword:              os.Getenv("REDIS_PASSWORD"),
		ServerPort:                 strings.TrimSpace(os.Getenv("SERVER_PORT")),
		KafkaBootstrapServers:      strings.TrimSpace(os.Getenv("KAFKA_BOOTSTRAP_SERVERS")),
		KafkaZookeeperConnect:      strings.TrimSpace(os.Getenv("KAFKA_ZOOKEEPER_CONNECT")),
		KafkaClusterID:             strings.TrimSpace(os.Getenv("KAFKA_CLUSTER_ID")),
		JWTSecret:                  strings.TrimSpace(os.Getenv("JWT_SECRET")),
		KafkaTopic:                 strings.TrimSpace(os.Getenv("KAFKA_TOPIC")),
		KafkaRunTopic:              strings.TrimSpace(os.Getenv("KAFKA_RUN_TOPIC")),
		KafkaRunGroupID:            strings.TrimSpace(os.Getenv("KAFKA_RUN_GROUP_ID")),
		RBACEnable:                 true,
		RBACBootstrapAdminUsername: strings.TrimSpace(os.Getenv("RBAC_BOOTSTRAP_ADMIN_USERNAME")),
		ModelScopeKey:              strings.TrimSpace(os.Getenv("MODELSCOPE_KEY")),
		ModelScopeModel:            strings.TrimSpace(os.Getenv("MODELSCOPE_MODEL")),
		ModelProviderDefault:       strings.ToLower(strings.TrimSpace(os.Getenv("MODEL_PROVIDER_DEFAULT"))),
		ModelProviders:             make(map[string]ModelProviderConfig),
	}

	var err error
	if appConfig.MySQLPort, err = loadIntEnv("MYSQL_PORT", 3306); err != nil {
		return err
	}
	if appConfig.RedisPort, err = loadIntEnv("REDIS_PORT", 6379); err != nil {
		return err
	}
	shutdownTimeoutSeconds, err := loadIntEnv("SERVER_SHUTDOWN_TIMEOUT_SECONDS", int(defaultShutdownTimeout/time.Second))
	if err != nil {
		return err
	}
	appConfig.ServerShutdownTimeout = time.Duration(shutdownTimeoutSeconds) * time.Second
	if appConfig.KafkaPort, err = loadIntEnv("KAFKA_PORT", 9092); err != nil {
		return err
	}
	if appConfig.KafkaJMXPort, err = loadIntEnv("KAFKA_JMX_PORT", 9991); err != nil {
		return err
	}
	if appConfig.MySQLHost == "" {
		appConfig.MySQLHost = "localhost"
	}
	if appConfig.MySQLUser == "" {
		appConfig.MySQLUser = "root"
	}
	if appConfig.RedisHost == "" {
		appConfig.RedisHost = "localhost"
	}
	if appConfig.ServerPort == "" {
		appConfig.ServerPort = "8080"
	}
	if appConfig.KafkaRunTopic == "" {
		appConfig.KafkaRunTopic = "run_execute"
	}
	if appConfig.KafkaRunGroupID == "" {
		appConfig.KafkaRunGroupID = "run-worker-group"
	}
	if enableStr := strings.TrimSpace(os.Getenv("RBAC_ENABLE")); enableStr != "" {
		appConfig.RBACEnable = parseBoolEnv(enableStr)
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
			appConfig.ModelProviders[name] = cfg
		}
	}

	if _, exists := appConfig.ModelProviders["modelscope"]; !exists &&
		(appConfig.ModelScopeKey != "" || appConfig.ModelScopeModel != "") {
		baseURL := strings.TrimSpace(os.Getenv("MODEL_BASE_URL_MODELSCOPE"))
		if baseURL == "" {
			baseURL = "https://api-inference.modelscope.cn"
		}
		appConfig.ModelProviders["modelscope"] = ModelProviderConfig{
			Driver:       "openai_compatible",
			BaseURL:      baseURL,
			APIKey:       appConfig.ModelScopeKey,
			DefaultModel: appConfig.ModelScopeModel,
			EndpointPath: "/v1/chat/completions",
		}
	}

	if err := appConfig.ValidateStartup(); err != nil {
		return err
	}
	AppConfig = appConfig
	return nil
}

// ValidateStartup 校验应用启动所需的关键配置，避免服务启动后才暴露基础错误。
func (c *Config) ValidateStartup() error {
	var problems []string
	if c == nil {
		return fmt.Errorf("invalid config: config is nil")
	}

	if strings.TrimSpace(c.MySQLHost) == "" {
		problems = append(problems, "MYSQL_HOST is required")
	}
	if c.MySQLPort <= 0 {
		problems = append(problems, "MYSQL_PORT must be greater than 0")
	}
	if strings.TrimSpace(c.MySQLUser) == "" {
		problems = append(problems, "MYSQL_USER is required")
	}
	if strings.TrimSpace(c.MySQLDatabase) == "" {
		problems = append(problems, "MYSQL_DATABASE is required")
	}
	if strings.TrimSpace(c.RedisHost) == "" {
		problems = append(problems, "REDIS_HOST is required")
	}
	if c.RedisPort <= 0 {
		problems = append(problems, "REDIS_PORT must be greater than 0")
	}
	if strings.TrimSpace(c.ServerPort) == "" {
		problems = append(problems, "SERVER_PORT is required")
	}
	if c.ServerShutdownTimeout <= 0 {
		problems = append(problems, "SERVER_SHUTDOWN_TIMEOUT_SECONDS must be greater than 0")
	}
	if strings.TrimSpace(c.KafkaBootstrapServers) == "" {
		problems = append(problems, "KAFKA_BOOTSTRAP_SERVERS is required")
	}
	if strings.TrimSpace(c.KafkaRunTopic) == "" {
		problems = append(problems, "KAFKA_RUN_TOPIC is required")
	}
	if strings.TrimSpace(c.KafkaRunGroupID) == "" {
		problems = append(problems, "KAFKA_RUN_GROUP_ID is required")
	}
	if strings.TrimSpace(c.JWTSecret) == "" {
		problems = append(problems, "JWT_SECRET is required")
	}

	if err := validateProviderStartup(c); err != nil {
		problems = append(problems, err.Error())
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(problems, "; "))
	}
	return nil
}

// loadIntEnv 读取整型环境变量，空值时回退到默认值，非法值直接报错。
func loadIntEnv(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid config: %s must be an integer", key)
	}
	return parsed, nil
}

// parseBoolEnv 将常见的布尔环境变量写法统一为 true 或 false。
func parseBoolEnv(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(value))
	return normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on"
}

// validateProviderStartup 校验默认 Provider 及其关键字段在启动时是完整可用的。
func validateProviderStartup(c *Config) error {
	if len(c.ModelProviders) == 0 && c.ModelProviderDefault == "" {
		return nil
	}
	if c.ModelProviderDefault == "" {
		return fmt.Errorf("MODEL_PROVIDER_DEFAULT is required when provider profiles are configured")
	}
	profile, exists := c.ModelProviders[c.ModelProviderDefault]
	if !exists {
		return fmt.Errorf("MODEL_PROVIDER_DEFAULT %q does not match any configured provider profile", c.ModelProviderDefault)
	}
	var problems []string
	if strings.TrimSpace(profile.Driver) == "" {
		problems = append(problems, "MODEL_DRIVER is required for default provider")
	}
	if strings.TrimSpace(profile.BaseURL) == "" {
		problems = append(problems, "MODEL_BASE_URL is required for default provider")
	}
	if strings.TrimSpace(profile.APIKey) == "" {
		problems = append(problems, "MODEL_API_KEY is required for default provider")
	}
	if strings.TrimSpace(profile.DefaultModel) == "" {
		problems = append(problems, "MODEL_NAME_DEFAULT is required for default provider")
	}
	if len(problems) > 0 {
		return fmt.Errorf("%s", strings.Join(problems, "; "))
	}
	return nil
}

// defaultEndpointPath 返回不同 provider 的默认聊天端点路径。
func defaultEndpointPath(providerName string) string {
	switch strings.ToLower(strings.TrimSpace(providerName)) {
	case "deepseek":
		return "/v1/chat/completions"
	default:
		return "/chat/completions"
	}
}
