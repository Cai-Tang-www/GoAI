package governance

import (
	"math"
	"sync"
	"testing"
	"time"
)

func TestKeyedLimiterBurstAndRefill(t *testing.T) {
	now := time.Unix(100, 0)
	limiter, err := newKeyedLimiter(RateLimitConfig{RequestsPerSecond: 100, Burst: 2, MaxKeys: 10}, func() time.Time {
		return now
	})
	if err != nil {
		t.Fatalf("create limiter: %v", err)
	}

	if allowed, _ := limiter.Allow("client"); !allowed {
		t.Fatal("first request should be allowed")
	}
	if allowed, _ := limiter.Allow("client"); !allowed {
		t.Fatal("burst request should be allowed")
	}
	if allowed, retryAfter := limiter.Allow("client"); allowed || retryAfter <= 0 {
		t.Fatalf("third immediate request should be rejected with retry delay, allowed=%v retry_after=%s", allowed, retryAfter)
	}

	now = now.Add(20 * time.Millisecond)
	if allowed, _ := limiter.Allow("client"); !allowed {
		t.Fatal("refilled request should be allowed")
	}
}

func TestKeyedLimiterMaxKeys(t *testing.T) {
	now := time.Unix(100, 0)
	limiter, err := newKeyedLimiter(RateLimitConfig{RequestsPerSecond: 1, Burst: 1, MaxKeys: 2}, func() time.Time {
		return now
	})
	if err != nil {
		t.Fatalf("create limiter: %v", err)
	}
	if allowed, _ := limiter.Allow("first"); !allowed {
		t.Fatal("first key should be allowed")
	}
	now = now.Add(time.Nanosecond)
	if allowed, _ := limiter.Allow("second"); !allowed {
		t.Fatal("second key should be allowed")
	}
	now = now.Add(time.Nanosecond)
	if allowed, _ := limiter.Allow("third"); !allowed {
		t.Fatal("new key should evict the oldest bucket and be allowed")
	}
	if got := limiter.KeyCount(); got != 2 {
		t.Fatalf("expected max key count to remain bounded at two, got %d", got)
	}
	if _, ok := limiter.buckets["first"]; ok {
		t.Fatal("expected oldest bucket to be evicted")
	}
}

func TestKeyedLimiterConcurrentAccess(t *testing.T) {
	limiter, err := NewKeyedLimiter(RateLimitConfig{RequestsPerSecond: 100000, Burst: 100000, MaxKeys: 100})
	if err != nil {
		t.Fatalf("create limiter: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				limiter.Allow("client-" + string(rune('a'+i%26)))
			}
		}(i)
	}
	wg.Wait()
	if limiter.KeyCount() == 0 {
		t.Fatal("expected concurrent calls to track keys")
	}
}

func TestNewKeyedLimiterRejectsInvalidConfig(t *testing.T) {
	cases := []RateLimitConfig{
		{RequestsPerSecond: 0, Burst: 1, MaxKeys: 1},
		{RequestsPerSecond: math.NaN(), Burst: 1, MaxKeys: 1},
		{RequestsPerSecond: math.Inf(1), Burst: 1, MaxKeys: 1},
		{RequestsPerSecond: math.Inf(-1), Burst: 1, MaxKeys: 1},
		{RequestsPerSecond: 1, Burst: 0, MaxKeys: 1},
		{RequestsPerSecond: 1, Burst: 1, MaxKeys: 0},
	}
	for _, config := range cases {
		if _, err := NewKeyedLimiter(config); err == nil {
			t.Fatalf("expected invalid config error for %+v", config)
		}
	}
}
