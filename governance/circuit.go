package governance

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrCircuitOpen 表示下游目标暂时不可用。
	ErrCircuitOpen = errors.New("circuit breaker is open")
	// ErrDownstreamTimeout 表示治理层为下游请求设置的截止时间已到。
	ErrDownstreamTimeout = errors.New("downstream request timeout")
	// ErrCircuitRegistryFull 表示已跟踪的目标都仍在使用中，暂时无法加入新目标。
	ErrCircuitRegistryFull = errors.New("circuit breaker registry is full")
)

// CircuitState 描述下游熔断器的生命周期状态。
type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half_open"
)

const defaultMaxCircuitTargets = 1024

// CircuitConfig 控制失败计数、恢复探测和注册表容量。
type CircuitConfig struct {
	FailureThreshold int
	OpenTimeout      time.Duration
	MaxTargets       int
}

// Event 描述用于日志和指标的低基数治理事件。
type Event struct {
	Type    string
	Target  string
	Status  string
	Latency time.Duration
	Error   error
}

// EventSink 接收治理生命周期事件，应保持非阻塞或足够轻量。
type EventSink func(Event)

// CircuitBreaker 保护一个下游目标。
type CircuitBreaker struct {
	mu            sync.Mutex
	config        CircuitConfig
	state         CircuitState
	failures      int
	openedAt      time.Time
	probeInFlight bool
}

// NewCircuitBreaker 创建并校验一个初始为 closed 的熔断器。
func NewCircuitBreaker(config CircuitConfig) (*CircuitBreaker, error) {
	if config.FailureThreshold <= 0 {
		return nil, errors.New("circuit failure threshold must be greater than zero")
	}
	if config.OpenTimeout <= 0 {
		return nil, errors.New("circuit open timeout must be greater than zero")
	}
	return &CircuitBreaker{config: config, state: CircuitClosed}, nil
}

// Allow 声明一次执行请求；熔断恢复时最多允许一个 half-open 探测。
func (b *CircuitBreaker) Allow(now time.Time) (bool, CircuitState, bool) {
	if b == nil {
		return true, CircuitClosed, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case CircuitClosed:
		return true, b.state, false
	case CircuitOpen:
		if now.Sub(b.openedAt) < b.config.OpenTimeout {
			return false, b.state, false
		}
		if b.probeInFlight {
			return false, b.state, false
		}
		b.state = CircuitHalfOpen
		b.probeInFlight = true
		return true, b.state, true
	case CircuitHalfOpen:
		if b.probeInFlight {
			return false, b.state, false
		}
		b.probeInFlight = true
		return true, b.state, true
	default:
		b.state = CircuitOpen
		b.openedAt = now
		return false, b.state, false
	}
}

// Success 记录一次成功请求，并返回熔断器是否刚从 half-open 恢复。
func (b *CircuitBreaker) Success() (recovered bool) {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	recovered = b.state == CircuitHalfOpen
	b.state = CircuitClosed
	b.failures = 0
	b.probeInFlight = false
	b.openedAt = time.Time{}
	return recovered
}

// Failure 记录一次失败请求，并返回熔断器是否刚刚打开。
func (b *CircuitBreaker) Failure(now time.Time) (opened bool) {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	b.probeInFlight = false
	if b.state == CircuitHalfOpen {
		b.state = CircuitOpen
		b.openedAt = now
		b.failures = b.config.FailureThreshold
		return true
	}
	if b.state == CircuitOpen {
		return false
	}

	b.failures++
	if b.failures < b.config.FailureThreshold {
		return false
	}
	b.state = CircuitOpen
	b.openedAt = now
	return true
}

// CancelProbe 在调用方取消请求时释放 half-open 探测，但不计为下游失败。
func (b *CircuitBreaker) CancelProbe() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == CircuitHalfOpen {
		b.state = CircuitOpen
	}
	b.probeInFlight = false
}

// State 返回当前状态，供诊断和测试使用。
func (b *CircuitBreaker) State() CircuitState {
	if b == nil {
		return CircuitClosed
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

type breakerEntry struct {
	breaker  *CircuitBreaker
	lastUsed time.Time
	active   int
}

type breakerLease struct {
	registry *BreakerRegistry
	target   string
	breaker  *CircuitBreaker
	once     sync.Once
}

func (l *breakerLease) release() {
	if l == nil || l.registry == nil {
		return
	}
	l.once.Do(func() {
		l.registry.release(l.target, l.breaker)
	})
}

// BreakerRegistry 按下游目标懒加载熔断器，并限制目标集合大小。
type BreakerRegistry struct {
	mu       sync.Mutex
	config   CircuitConfig
	maxKeys  int
	breakers map[string]breakerEntry
}

// NewBreakerRegistry 创建一个按目标隔离且有界的熔断器注册表。
func NewBreakerRegistry(config CircuitConfig) (*BreakerRegistry, error) {
	if _, err := NewCircuitBreaker(config); err != nil {
		return nil, err
	}
	maxKeys := config.MaxTargets
	if maxKeys <= 0 {
		maxKeys = defaultMaxCircuitTargets
	}
	return &BreakerRegistry{config: config, maxKeys: maxKeys, breakers: make(map[string]breakerEntry)}, nil
}

// Get 返回目标对应的熔断器；该方法不声明活跃请求，主要用于诊断和测试。
func (r *BreakerRegistry) Get(target string) (*CircuitBreaker, error) {
	if r == nil {
		return nil, errors.New("circuit breaker registry is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	if entry, ok := r.breakers[target]; ok {
		entry.lastUsed = now
		r.breakers[target] = entry
		return entry.breaker, nil
	}
	if len(r.breakers) >= r.maxKeys && !r.evictOldest() {
		return nil, ErrCircuitRegistryFull
	}
	breaker, err := NewCircuitBreaker(r.config)
	if err != nil {
		return nil, err
	}
	r.breakers[target] = breakerEntry{breaker: breaker, lastUsed: now}
	return breaker, nil
}

func (r *BreakerRegistry) acquire(target string) (*breakerLease, error) {
	if r == nil {
		return nil, errors.New("circuit breaker registry is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	entry, ok := r.breakers[target]
	if !ok {
		if len(r.breakers) >= r.maxKeys && !r.evictOldest() {
			return nil, ErrCircuitRegistryFull
		}
		breaker, err := NewCircuitBreaker(r.config)
		if err != nil {
			return nil, err
		}
		entry = breakerEntry{breaker: breaker}
	}
	entry.lastUsed = now
	entry.active++
	r.breakers[target] = entry
	return &breakerLease{registry: r, target: target, breaker: entry.breaker}, nil
}

func (r *BreakerRegistry) release(target string, breaker *CircuitBreaker) {
	if r == nil || breaker == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.breakers[target]
	if !ok || entry.breaker != breaker {
		return
	}
	if entry.active > 0 {
		entry.active--
	}
	entry.lastUsed = time.Now()
	r.breakers[target] = entry
}

func (r *BreakerRegistry) evictOldest() bool {
	var oldestKey string
	var oldest time.Time
	for key, entry := range r.breakers {
		if entry.active > 0 {
			continue
		}
		if oldestKey == "" || entry.lastUsed.Before(oldest) {
			oldestKey = key
			oldest = entry.lastUsed
		}
	}
	if oldestKey == "" {
		return false
	}
	delete(r.breakers, oldestKey)
	return true
}

// KeyCount 返回当前跟踪的下游目标数量。
func (r *BreakerRegistry) KeyCount() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.breakers)
}
