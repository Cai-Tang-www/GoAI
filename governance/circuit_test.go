package governance

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreakerOpensAndRecovers(t *testing.T) {
	breaker, err := NewCircuitBreaker(CircuitConfig{FailureThreshold: 2, OpenTimeout: time.Second})
	if err != nil {
		t.Fatalf("create breaker: %v", err)
	}
	now := time.Now()
	if opened := breaker.Failure(now); opened {
		t.Fatal("breaker should not open before threshold")
	}
	if opened := breaker.Failure(now); !opened {
		t.Fatal("breaker should open at threshold")
	}
	if breaker.State() != CircuitOpen {
		t.Fatalf("expected open state, got %s", breaker.State())
	}
	if allowed, _, _ := breaker.Allow(now.Add(500 * time.Millisecond)); allowed {
		t.Fatal("open breaker should reject requests before recovery timeout")
	}
	allowed, state, probe := breaker.Allow(now.Add(2 * time.Second))
	if !allowed || state != CircuitHalfOpen || !probe {
		t.Fatalf("expected one half-open probe, allowed=%v state=%s probe=%v", allowed, state, probe)
	}
	if allowed, _, _ := breaker.Allow(now.Add(2 * time.Second)); allowed {
		t.Fatal("second half-open probe should be rejected")
	}
	if recovered := breaker.Success(); !recovered {
		t.Fatal("successful half-open probe should recover breaker")
	}
	if breaker.State() != CircuitClosed {
		t.Fatalf("expected closed state after recovery, got %s", breaker.State())
	}
}

func TestCircuitBreakerHalfOpenFailureReopens(t *testing.T) {
	breaker, err := NewCircuitBreaker(CircuitConfig{FailureThreshold: 1, OpenTimeout: time.Second})
	if err != nil {
		t.Fatalf("create breaker: %v", err)
	}
	now := time.Now()
	if !breaker.Failure(now) {
		t.Fatal("breaker should open at first failure")
	}
	if allowed, _, _ := breaker.Allow(now.Add(2 * time.Second)); !allowed {
		t.Fatal("recovery probe should be allowed")
	}
	if opened := breaker.Failure(now.Add(2 * time.Second)); !opened {
		t.Fatal("failed recovery probe should reopen breaker")
	}
	if breaker.State() != CircuitOpen {
		t.Fatalf("expected open state, got %s", breaker.State())
	}
}

func TestBreakerRegistryCreatesOneBreakerPerTarget(t *testing.T) {
	registry, err := NewBreakerRegistry(CircuitConfig{FailureThreshold: 1, OpenTimeout: time.Second})
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}
	first, err := registry.Get("https://one.example")
	if err != nil {
		t.Fatalf("get first breaker: %v", err)
	}
	second, err := registry.Get("https://one.example")
	if err != nil {
		t.Fatalf("get second breaker: %v", err)
	}
	if first != second || registry.KeyCount() != 1 {
		t.Fatal("expected target lookups to reuse one breaker")
	}
}

func TestCircuitBreakerCancelProbeCanRetry(t *testing.T) {
	breaker, err := NewCircuitBreaker(CircuitConfig{FailureThreshold: 1, OpenTimeout: time.Second})
	if err != nil {
		t.Fatalf("create breaker: %v", err)
	}
	now := time.Now()
	if !breaker.Failure(now) {
		t.Fatal("breaker should open after failure")
	}
	allowed, state, probe := breaker.Allow(now.Add(2 * time.Second))
	if !allowed || state != CircuitHalfOpen || !probe {
		t.Fatalf("expected half-open probe, allowed=%v state=%s probe=%v", allowed, state, probe)
	}
	breaker.CancelProbe()
	if breaker.State() != CircuitOpen {
		t.Fatalf("cancelled probe should return to open, got %s", breaker.State())
	}
	allowed, state, probe = breaker.Allow(now.Add(3 * time.Second))
	if !allowed || state != CircuitHalfOpen || !probe {
		t.Fatalf("breaker should allow a later probe, allowed=%v state=%s probe=%v", allowed, state, probe)
	}
}

func TestBreakerRegistryEvictsOldestTarget(t *testing.T) {
	registry, err := NewBreakerRegistry(CircuitConfig{FailureThreshold: 1, OpenTimeout: time.Second, MaxTargets: 2})
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}
	if _, err := registry.Get("target-1"); err != nil {
		t.Fatalf("get target-1: %v", err)
	}
	registry.mu.Lock()
	entry := registry.breakers["target-1"]
	entry.lastUsed = time.Unix(0, 0)
	registry.breakers["target-1"] = entry
	registry.mu.Unlock()
	if _, err := registry.Get("target-2"); err != nil {
		t.Fatalf("get target-2: %v", err)
	}
	if _, err := registry.Get("target-3"); err != nil {
		t.Fatalf("get target-3: %v", err)
	}
	if registry.KeyCount() != 2 {
		t.Fatalf("registry key count = %d, want 2", registry.KeyCount())
	}
	if _, err := registry.Get("target-1"); err != nil {
		t.Fatalf("recreate evicted target-1: %v", err)
	}
	if registry.KeyCount() != 2 {
		t.Fatalf("registry should stay bounded after recreation, got %d", registry.KeyCount())
	}
}

func TestBreakerRegistryDoesNotEvictActiveLease(t *testing.T) {
	registry, err := NewBreakerRegistry(CircuitConfig{FailureThreshold: 1, OpenTimeout: time.Second, MaxTargets: 1})
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}
	lease, err := registry.acquire("target-1")
	if err != nil {
		t.Fatalf("acquire target-1: %v", err)
	}
	if _, err := registry.acquire("target-2"); !errors.Is(err, ErrCircuitRegistryFull) {
		t.Fatalf("expected full registry while target-1 is active, got %v", err)
	}
	lease.release()
	if _, err := registry.acquire("target-2"); err != nil {
		t.Fatalf("released target should be evictable: %v", err)
	}
}

func TestBreakerRegistryAllActiveTargetsFailFastWithoutGrowing(t *testing.T) {
	registry, err := NewBreakerRegistry(CircuitConfig{FailureThreshold: 1, OpenTimeout: time.Second, MaxTargets: 2})
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}
	first, err := registry.acquire("target-1")
	if err != nil {
		t.Fatalf("acquire target-1: %v", err)
	}
	second, err := registry.acquire("target-2")
	if err != nil {
		t.Fatalf("acquire target-2: %v", err)
	}
	if _, err := registry.acquire("target-3"); !errors.Is(err, ErrCircuitRegistryFull) {
		t.Fatalf("expected bounded fast failure, got %v", err)
	}
	if got := registry.KeyCount(); got != 2 {
		t.Fatalf("registry grew beyond max targets: %d", got)
	}
	first.release()
	second.release()
}
