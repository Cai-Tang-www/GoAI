package governance

import (
	"errors"
	"math"
	"strings"
	"sync"
	"time"
)

var (
	// ErrRateLimited 表示指定 key 超过了配置的请求额度。
	ErrRateLimited = errors.New("rate limit exceeded")
)

// RateLimitConfig 配置进程内按 key 隔离的令牌桶。
type RateLimitConfig struct {
	RequestsPerSecond float64
	Burst             int
	MaxKeys           int
}

type tokenBucket struct {
	tokens  float64
	updated time.Time
}

// KeyedLimiter 为每个逻辑 key 使用独立令牌桶。
// 当前实现限定在单进程内，后续可在不改变中间件契约的前提下替换为分布式限流器。
type KeyedLimiter struct {
	mu      sync.Mutex
	config  RateLimitConfig
	buckets map[string]tokenBucket
}

// NewKeyedLimiter 创建并校验配置后的按 key 限流器。
func NewKeyedLimiter(config RateLimitConfig) (*KeyedLimiter, error) {
	if config.RequestsPerSecond <= 0 || math.IsNaN(config.RequestsPerSecond) || math.IsInf(config.RequestsPerSecond, 0) {
		return nil, errors.New("rate limit requests per second must be greater than zero")
	}
	if config.Burst <= 0 {
		return nil, errors.New("rate limit burst must be greater than zero")
	}
	if config.MaxKeys <= 0 {
		return nil, errors.New("rate limit max keys must be greater than zero")
	}
	return &KeyedLimiter{
		config:  config,
		buckets: make(map[string]tokenBucket),
	}, nil
}

// Allow consumes one token for key and returns a retry delay when rejected.
func (l *KeyedLimiter) Allow(key string) (allowed bool, retryAfter time.Duration) {
	if l == nil {
		return true, 0
	}
	key = normalizeLimitKey(key)
	if key == "" {
		key = "default"
	}

	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()

	bucket, exists := l.buckets[key]
	if !exists {
		if len(l.buckets) >= l.config.MaxKeys {
			l.evictOldest()
		}
		bucket = tokenBucket{tokens: float64(l.config.Burst), updated: now}
	}

	elapsed := now.Sub(bucket.updated).Seconds()
	if elapsed > 0 {
		bucket.tokens = math.Min(float64(l.config.Burst), bucket.tokens+elapsed*l.config.RequestsPerSecond)
		bucket.updated = now
	}
	if bucket.tokens < 1 {
		l.buckets[key] = bucket
		missing := 1 - bucket.tokens
		return false, time.Duration(math.Ceil(missing / l.config.RequestsPerSecond * float64(time.Second)))
	}

	bucket.tokens--
	l.buckets[key] = bucket
	return true, 0
}

// KeyCount reports the number of tracked keys and is intended for tests and diagnostics.
func (l *KeyedLimiter) KeyCount() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// evictOldest bounds memory while allowing a newly observed client to enter the
// limiter. The bucket timestamp is refreshed on every Allow call, so the oldest
// entry is the least recently used one for this process-local limiter.
func (l *KeyedLimiter) evictOldest() {
	var oldestKey string
	var oldestAt time.Time
	for key, bucket := range l.buckets {
		if oldestKey == "" || bucket.updated.Before(oldestAt) {
			oldestKey = key
			oldestAt = bucket.updated
		}
	}
	if oldestKey != "" {
		delete(l.buckets, oldestKey)
	}
}

func normalizeLimitKey(key string) string {
	return strings.TrimSpace(key)
}
