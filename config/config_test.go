package config

import (
	"strings"
	"testing"
	"time"
)

// TestValidateStartupSuccess 验证完整关键配置可以通过启动期校验。
func TestValidateStartupSuccess(t *testing.T) {
	cfg := &Config{
		MySQLHost:                  "localhost",
		MySQLPort:                  3306,
		MySQLUser:                  "root",
		MySQLDatabase:              "goai",
		RedisHost:                  "localhost",
		RedisPort:                  6379,
		ServerPort:                 "8080",
		ServerShutdownTimeout:      15 * time.Second,
		RunResumeLeaseDuration:     30 * time.Second,
		RunResumeHeartbeatInterval: 10 * time.Second,
		RunRecoveryScanInterval:    5 * time.Second,
		RunRecoveryBatchSize:       100,
		KafkaBootstrapServers:      "localhost:9092",
		KafkaRunTopic:              "run_execute",
		KafkaRunGroupID:            "run-worker-group",
		JWTSecret:                  "test-secret",
	}
	if err := cfg.ValidateStartup(); err != nil {
		t.Fatalf("expected config valid, got %v", err)
	}
}

// TestValidateStartupDefaultsResumeQueue 验证直接构造配置时也会补齐恢复队列默认值。
func TestValidateStartupDefaultsResumeQueue(t *testing.T) {
	cfg := validStartupConfig()
	cfg.KafkaRunResumeTopic = ""
	cfg.KafkaRunResumeGroupID = ""

	if err := cfg.ValidateStartup(); err != nil {
		t.Fatalf("expected config valid, got %v", err)
	}
	if cfg.KafkaRunResumeTopic != "run_resume" {
		t.Fatalf("expected default resume topic, got %q", cfg.KafkaRunResumeTopic)
	}
	if cfg.KafkaRunResumeGroupID != "run-resume-worker-group" {
		t.Fatalf("expected default resume group, got %q", cfg.KafkaRunResumeGroupID)
	}
}

// TestValidateStartupValidatesA2ACallbackURL 验证 callback 地址只允许 loopback HTTP 或远程 HTTPS。
func TestValidateStartupValidatesA2ACallbackURL(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantErr string
	}{
		{name: "loopback IPv4 HTTP", rawURL: "http://127.0.0.1:8080"},
		{name: "loopback IPv6 HTTP", rawURL: "http://[::1]:8080"},
		{name: "localhost HTTP", rawURL: "http://localhost:8080"},
		{name: "remote HTTPS", rawURL: "https://agents.example.com"},
		{name: "remote HTTP", rawURL: "http://agents.example.com", wantErr: "remote callback URL must use HTTPS"},
		{name: "relative URL", rawURL: "/a2a/callback", wantErr: "must be an absolute URL"},
		{name: "user info", rawURL: "https://user:password@agents.example.com", wantErr: "must not contain user info"},
		{name: "query", rawURL: "https://agents.example.com?token=secret", wantErr: "query and fragment are not allowed"},
		{name: "fragment", rawURL: "https://agents.example.com#callback", wantErr: "query and fragment are not allowed"},
		{name: "unsupported scheme", rawURL: "ftp://agents.example.com", wantErr: "unsupported scheme"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validStartupConfig()
			cfg.A2ACallbackBaseURL = tt.rawURL

			err := cfg.ValidateStartup()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected URL valid, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
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
	for _, want := range []string{"MYSQL_DATABASE", "REDIS_PORT", "SERVER_SHUTDOWN_TIMEOUT_SECONDS", "KAFKA_BOOTSTRAP_SERVERS", "JWT_SECRET"} {
		if !strings.Contains(message, want) {
			t.Fatalf("expected error message contains %s, got %s", want, message)
		}
	}
}

// TestValidateStartupRequiresDefaultProviderProfile 验证启用 provider 配置时必须存在完整默认 profile。
func TestValidateStartupRequiresDefaultProviderProfile(t *testing.T) {
	cfg := &Config{
		MySQLHost:                  "localhost",
		MySQLPort:                  3306,
		MySQLUser:                  "root",
		MySQLDatabase:              "goai",
		RedisHost:                  "localhost",
		RedisPort:                  6379,
		ServerPort:                 "8080",
		ServerShutdownTimeout:      15 * time.Second,
		RunResumeLeaseDuration:     30 * time.Second,
		RunResumeHeartbeatInterval: 10 * time.Second,
		RunRecoveryScanInterval:    5 * time.Second,
		RunRecoveryBatchSize:       100,
		KafkaBootstrapServers:      "localhost:9092",
		KafkaRunTopic:              "run_execute",
		KafkaRunGroupID:            "run-worker-group",
		JWTSecret:                  "test-secret",
		ModelProviderDefault:       "deepseek",
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

// TestValidateStartupRejectsNonPositiveShutdownTimeout 验证关闭超时必须为正数。
func TestValidateStartupRejectsNonPositiveShutdownTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		cfg := &Config{
			MySQLHost:                  "localhost",
			MySQLPort:                  3306,
			MySQLUser:                  "root",
			MySQLDatabase:              "goai",
			RedisHost:                  "localhost",
			RedisPort:                  6379,
			ServerPort:                 "8080",
			ServerShutdownTimeout:      timeout,
			RunResumeLeaseDuration:     30 * time.Second,
			RunResumeHeartbeatInterval: 10 * time.Second,
			RunRecoveryScanInterval:    5 * time.Second,
			RunRecoveryBatchSize:       100,
			KafkaBootstrapServers:      "localhost:9092",
			KafkaRunTopic:              "run_execute",
			KafkaRunGroupID:            "run-worker-group",
			JWTSecret:                  "test-secret",
		}
		err := cfg.ValidateStartup()
		if err == nil || !strings.Contains(err.Error(), "SERVER_SHUTDOWN_TIMEOUT_SECONDS") {
			t.Fatalf("timeout %s: expected shutdown timeout validation error, got %v", timeout, err)
		}
	}
}

// TestLoadConfigDefaultsShutdownTimeout 验证未配置关闭超时时采用 15 秒默认值。
func TestLoadConfigDefaultsShutdownTimeout(t *testing.T) {
	t.Setenv("MYSQL_HOST", "localhost")
	t.Setenv("MYSQL_PORT", "3306")
	t.Setenv("MYSQL_USER", "root")
	t.Setenv("MYSQL_DATABASE", "goai")
	t.Setenv("REDIS_HOST", "localhost")
	t.Setenv("REDIS_PORT", "6379")
	t.Setenv("SERVER_PORT", "8080")
	t.Setenv("SERVER_SHUTDOWN_TIMEOUT_SECONDS", "")
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	t.Setenv("KAFKA_RUN_TOPIC", "run_execute")
	t.Setenv("KAFKA_RUN_GROUP_ID", "run-worker-group")
	t.Setenv("JWT_SECRET", "test-secret")
	t.Setenv("MODEL_PROVIDER_DEFAULT", "")

	if err := LoadConfig(); err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if AppConfig.ServerShutdownTimeout != 15*time.Second {
		t.Fatalf("expected 15s shutdown timeout, got %s", AppConfig.ServerShutdownTimeout)
	}
	if AppConfig.RunResumeLeaseDuration != 30*time.Second || AppConfig.RunResumeHeartbeatInterval != 10*time.Second {
		t.Fatalf("unexpected resume lease defaults: lease=%s heartbeat=%s", AppConfig.RunResumeLeaseDuration, AppConfig.RunResumeHeartbeatInterval)
	}
	if AppConfig.RunRecoveryScanInterval != 5*time.Second || AppConfig.RunRecoveryBatchSize != 100 {
		t.Fatalf("unexpected recovery defaults: interval=%s batch=%d", AppConfig.RunRecoveryScanInterval, AppConfig.RunRecoveryBatchSize)
	}
}

// TestValidateStartupRejectsInvalidResumeRecoveryConfig 验证恢复租约与扫描参数的边界关系。
func TestValidateStartupRejectsInvalidResumeRecoveryConfig(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{name: "non-positive lease", mutate: func(cfg *Config) { cfg.RunResumeLeaseDuration = 0 }, wantErr: "RUN_RESUME_LEASE_SECONDS"},
		{name: "non-positive heartbeat", mutate: func(cfg *Config) { cfg.RunResumeHeartbeatInterval = 0 }, wantErr: "RUN_RESUME_HEARTBEAT_SECONDS"},
		{name: "heartbeat equals lease", mutate: func(cfg *Config) { cfg.RunResumeHeartbeatInterval = cfg.RunResumeLeaseDuration }, wantErr: "must be less than RUN_RESUME_LEASE_SECONDS"},
		{name: "non-positive scan interval", mutate: func(cfg *Config) { cfg.RunRecoveryScanInterval = 0 }, wantErr: "RUN_RECOVERY_SCAN_INTERVAL_SECONDS"},
		{name: "non-positive batch size", mutate: func(cfg *Config) { cfg.RunRecoveryBatchSize = 0 }, wantErr: "RUN_RECOVERY_BATCH_SIZE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validStartupConfig()
			tt.mutate(cfg)
			err := cfg.ValidateStartup()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

// TestValidateStartupRejectsInvalidGovernanceConfig 验证启用服务治理时关键参数必须为正数。
func TestValidateStartupRejectsInvalidGovernanceConfig(t *testing.T) {
	cfg := &Config{
		MySQLHost:                  "localhost",
		MySQLPort:                  3306,
		MySQLUser:                  "root",
		MySQLDatabase:              "goai",
		RedisHost:                  "localhost",
		RedisPort:                  6379,
		ServerPort:                 "8080",
		ServerShutdownTimeout:      15 * time.Second,
		KafkaBootstrapServers:      "localhost:9092",
		KafkaRunTopic:              "run_execute",
		KafkaRunGroupID:            "run-worker-group",
		JWTSecret:                  "test-secret",
		ServiceGovernanceEnabled:   true,
		RateLimitRequestsPerSecond: 0,
		RateLimitBurst:             40,
		RateLimitMaxKeys:           100,
		DownstreamRequestTimeout:   30 * time.Second,
		CircuitFailureThreshold:    3,
		CircuitOpenTimeout:         10 * time.Second,
	}
	if err := cfg.ValidateStartup(); err == nil || !strings.Contains(err.Error(), "RATE_LIMIT_REQUESTS_PER_SECOND") {
		t.Fatalf("expected governance validation error, got %v", err)
	}
}

// TestValidateStartupSkipsGovernanceWhenDisabled 验证关闭治理时不强制要求治理参数。
func TestValidateStartupSkipsGovernanceWhenDisabled(t *testing.T) {
	cfg := &Config{
		MySQLHost:                  "localhost",
		MySQLPort:                  3306,
		MySQLUser:                  "root",
		MySQLDatabase:              "goai",
		RedisHost:                  "localhost",
		RedisPort:                  6379,
		ServerPort:                 "8080",
		ServerShutdownTimeout:      15 * time.Second,
		RunResumeLeaseDuration:     30 * time.Second,
		RunResumeHeartbeatInterval: 10 * time.Second,
		RunRecoveryScanInterval:    5 * time.Second,
		RunRecoveryBatchSize:       100,
		KafkaBootstrapServers:      "localhost:9092",
		KafkaRunTopic:              "run_execute",
		KafkaRunGroupID:            "run-worker-group",
		JWTSecret:                  "test-secret",
	}
	if err := cfg.ValidateStartup(); err != nil {
		t.Fatalf("disabled governance should pass without governance settings: %v", err)
	}
}

func TestLoadFloatEnvRejectsNonFiniteValues(t *testing.T) {
	for _, value := range []string{"NaN", "+Inf", "-Inf"} {
		t.Setenv("GOAI_TEST_FLOAT", value)
		if _, err := loadFloatEnv("GOAI_TEST_FLOAT", 1); err == nil {
			t.Fatalf("value %q should be rejected", value)
		}
	}
}

func TestParseStrictBoolEnvRejectsUnknownValue(t *testing.T) {
	if _, err := parseStrictBoolEnv("SERVICE_GOVERNANCE_ENABLE", "maybe"); err == nil {
		t.Fatal("unknown boolean value should be rejected")
	}
}

func TestParseScopesStrictRejectsUnknownScope(t *testing.T) {
	if _, err := parseScopesStrict("api,unknown"); err == nil {
		t.Fatal("unknown governance scope should be rejected")
	}
}

func TestLoadConfigParsesA2AAuthentication(t *testing.T) {
	t.Setenv("MYSQL_HOST", "localhost")
	t.Setenv("MYSQL_USER", "root")
	t.Setenv("MYSQL_DATABASE", "goai")
	t.Setenv("REDIS_HOST", "localhost")
	t.Setenv("SERVER_PORT", "8080")
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	t.Setenv("KAFKA_RUN_TOPIC", "run_execute")
	t.Setenv("KAFKA_RUN_GROUP_ID", "run-worker-group")
	t.Setenv("JWT_SECRET", "test-secret")
	t.Setenv("MODEL_PROVIDER_DEFAULT", "")
	t.Setenv("A2A_AUTH_REQUIRED", "true")
	t.Setenv("A2A_AUTH_MAX_CLOCK_SKEW_SECONDS", "120")
	t.Setenv("A2A_AUTH_CREDENTIALS_JSON", `{"planner-key":"test-only-a2a-secret-at-least-32-bytes-long"}`)
	if err := LoadConfig(); err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if !AppConfig.A2AAuthRequired || AppConfig.A2AAuthMaxClockSkew != 2*time.Minute || len(AppConfig.A2ACredentials) != 1 {
		t.Fatalf("unexpected A2A auth config: required=%v skew=%s credentials=%d", AppConfig.A2AAuthRequired, AppConfig.A2AAuthMaxClockSkew, len(AppConfig.A2ACredentials))
	}
}

func TestLoadConfigRejectsMalformedA2ACredentialsWithoutLeakingValue(t *testing.T) {
	t.Setenv("A2A_AUTH_CREDENTIALS_JSON", `{"secret":"sensitive-value"`)
	if err := LoadConfig(); err == nil || !strings.Contains(err.Error(), "A2A_AUTH_CREDENTIALS_JSON") || strings.Contains(err.Error(), "sensitive-value") {
		t.Fatalf("unexpected credentials parse error: %v", err)
	}
}

func validStartupConfig() *Config {
	return &Config{
		MySQLHost:                  "localhost",
		MySQLPort:                  3306,
		MySQLUser:                  "root",
		MySQLDatabase:              "goai",
		RedisHost:                  "localhost",
		RedisPort:                  6379,
		ServerPort:                 "8080",
		ServerShutdownTimeout:      15 * time.Second,
		RunResumeLeaseDuration:     30 * time.Second,
		RunResumeHeartbeatInterval: 10 * time.Second,
		RunRecoveryScanInterval:    5 * time.Second,
		RunRecoveryBatchSize:       100,
		KafkaBootstrapServers:      "localhost:9092",
		KafkaRunTopic:              "run_execute",
		KafkaRunGroupID:            "run-worker-group",
		KafkaRunResumeTopic:        "run_resume",
		KafkaRunResumeGroupID:      "run-resume-worker-group",
		JWTSecret:                  "test-secret",
		A2ACallbackBaseURL:         "http://127.0.0.1:8080",
	}
}
