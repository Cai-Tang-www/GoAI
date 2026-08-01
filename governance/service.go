package governance

import (
	"errors"
	"math"
	"time"
)

// Config 描述 HTTP 入口和下游客户端使用的进程内治理配置。
type Config struct {
	Enabled                    bool
	RateLimitRequestsPerSecond float64
	RateLimitBurst             int
	RateLimitMaxKeys           int
	DownstreamRequestTimeout   time.Duration
	CircuitFailureThreshold    int
	CircuitOpenTimeout         time.Duration
	CircuitMaxTargets          int
	OnEvent                    EventSink
}

// Service 聚合应用使用的限流器和下游治理传输层。
type Service struct {
	Enabled   bool
	Limiter   *KeyedLimiter
	Transport *RoundTripper
	OnEvent   EventSink
}

// New 创建服务治理实例。关闭治理时返回空操作服务，保持本地开发和测试的依赖装配不变。
func New(config Config) (*Service, error) {
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	if !config.Enabled {
		return &Service{}, nil
	}
	service := &Service{Enabled: true, OnEvent: config.OnEvent}
	limiter, err := NewKeyedLimiter(RateLimitConfig{
		RequestsPerSecond: config.RateLimitRequestsPerSecond,
		Burst:             config.RateLimitBurst,
		MaxKeys:           config.RateLimitMaxKeys,
	})
	if err != nil {
		return nil, err
	}
	transport, err := NewRoundTripper(TransportConfig{
		RequestTimeout:   config.DownstreamRequestTimeout,
		FailureThreshold: config.CircuitFailureThreshold,
		OpenTimeout:      config.CircuitOpenTimeout,
		MaxTargets:       config.CircuitMaxTargets,
		OnEvent:          service.Emit,
	})
	if err != nil {
		return nil, err
	}
	service.Limiter = limiter
	service.Transport = transport
	return service, nil
}

// ValidateConfig 在不创建客户端的情况下校验启用状态下的治理配置。
func ValidateConfig(config Config) error {
	if !config.Enabled {
		return nil
	}
	if config.RateLimitRequestsPerSecond <= 0 || math.IsNaN(config.RateLimitRequestsPerSecond) || math.IsInf(config.RateLimitRequestsPerSecond, 0) {
		return errors.New("rate limit requests per second must be greater than zero")
	}
	if config.RateLimitBurst <= 0 {
		return errors.New("rate limit burst must be greater than zero")
	}
	if config.RateLimitMaxKeys <= 0 {
		return errors.New("rate limit max keys must be greater than zero")
	}
	if config.DownstreamRequestTimeout <= 0 {
		return errors.New("downstream request timeout must be greater than zero")
	}
	if config.CircuitFailureThreshold <= 0 {
		return errors.New("circuit failure threshold must be greater than zero")
	}
	if config.CircuitOpenTimeout <= 0 {
		return errors.New("circuit open timeout must be greater than zero")
	}
	if config.CircuitMaxTargets == 0 {
		return errors.New("circuit max targets must be greater than zero")
	}
	if config.CircuitMaxTargets < 0 {
		return errors.New("circuit max targets must not be negative")
	}
	return nil
}

// Emit 将治理事件发送给配置的观测事件接收器。
func (s *Service) Emit(event Event) {
	if s == nil || s.OnEvent == nil {
		return
	}
	s.OnEvent(event)
}
