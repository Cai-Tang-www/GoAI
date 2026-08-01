package governance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TransportConfig 配置下游请求超时和按目标隔离的熔断保护。
type TransportConfig struct {
	BaseTransport    http.RoundTripper
	RequestTimeout   time.Duration
	FailureThreshold int
	OpenTimeout      time.Duration
	MaxTargets       int
	OnEvent          EventSink
}

// RoundTripper 为 A2A 和 Provider 客户端提供统一的下游治理。
type RoundTripper struct {
	base           http.RoundTripper
	requestTimeout time.Duration
	breakers       *BreakerRegistry
	onEvent        EventSink
}

// NewRoundTripper 创建治理 Transport，并校验下游请求配置。
func NewRoundTripper(config TransportConfig) (*RoundTripper, error) {
	if config.RequestTimeout <= 0 {
		return nil, errors.New("downstream request timeout must be greater than zero")
	}
	breakers, err := NewBreakerRegistry(CircuitConfig{
		FailureThreshold: config.FailureThreshold,
		OpenTimeout:      config.OpenTimeout,
		MaxTargets:       config.MaxTargets,
	})
	if err != nil {
		return nil, err
	}
	base := config.BaseTransport
	if base == nil {
		base = http.DefaultTransport
	}
	return &RoundTripper{
		base:           base,
		requestTimeout: config.RequestTimeout,
		breakers:       breakers,
		onEvent:        config.OnEvent,
	}, nil
}

// RoundTrip 实现 http.RoundTripper，且不会改写协议请求和响应负载。
func (t *RoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if t == nil {
		return nil, errors.New("governed transport is nil")
	}
	if req == nil || req.URL == nil {
		return nil, errors.New("downstream request URL is required")
	}
	target := targetKey(req)
	lease, err := t.breakers.acquire(target)
	if err != nil {
		return nil, fmt.Errorf("acquire circuit breaker for %s: %w", target, err)
	}
	breaker := lease.breaker

	allowed, state, halfOpen := breaker.Allow(time.Now())
	if !allowed {
		lease.release()
		err := fmt.Errorf("%w: %s", ErrCircuitOpen, target)
		t.emit(Event{Type: "circuit_rejected", Target: target, Status: string(state), Error: err})
		return nil, err
	}
	if halfOpen {
		t.emit(Event{Type: "circuit_half_open", Target: target, Status: string(state)})
	}

	parentContext := req.Context()
	ctx, cancel := context.WithTimeout(parentContext, t.requestTimeout)
	startedAt := time.Now()
	resp, err := t.base.RoundTrip(req.WithContext(ctx))
	latency := time.Since(startedAt)
	governedErr := ctx.Err()
	callerErr := parentContext.Err()
	if err != nil {
		cancel()
		if callerErr != nil || errors.Is(err, context.Canceled) {
			// 调用方主动取消或请求被取消，不应惩罚下游目标。
			t.emit(Event{Type: "downstream_cancelled", Target: target, Status: "cancelled", Latency: latency, Error: err})
			breaker.CancelProbe()
			lease.release()
			return nil, err
		}
		if errors.Is(governedErr, context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("%w: %s", ErrDownstreamTimeout, target)
			t.emit(Event{Type: "downstream_timeout", Target: target, Status: "timeout", Latency: latency, Error: err})
		} else {
			t.emit(Event{Type: "downstream_failure", Target: target, Status: "network_error", Latency: latency, Error: err})
		}
		if breaker.Failure(time.Now()) {
			t.emit(Event{Type: "circuit_opened", Target: target, Status: "open", Latency: latency, Error: err})
		}
		lease.release()
		return nil, err
	}

	status := strconv.Itoa(resp.StatusCode)
	headerFailure := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError
	if resp.Body == nil {
		cancel()
		if headerFailure {
			t.recordResponseFailure(breaker, target, status, latency)
		} else if recovered := breaker.Success(); recovered {
			t.emit(Event{Type: "circuit_recovered", Target: target, Status: "closed", Latency: latency})
		}
		lease.release()
		return resp, nil
	}

	body := &governedBody{
		ReadCloser:      resp.Body,
		cancel:          cancel,
		breaker:         breaker,
		lease:           lease,
		parentContext:   parentContext,
		governedContext: ctx,
		target:          target,
		status:          status,
		headerFailure:   headerFailure,
		startedAt:       startedAt,
		emit:            t.emit,
	}
	resp.Body = body
	if headerFailure {
		t.recordResponseFailure(breaker, target, status, latency)
		body.settled = true
	}
	return resp, nil
}

func (t *RoundTripper) recordResponseFailure(breaker *CircuitBreaker, target, status string, latency time.Duration) {
	t.emit(Event{Type: "downstream_failure", Target: target, Status: status, Latency: latency})
	if breaker.Failure(time.Now()) {
		t.emit(Event{Type: "circuit_opened", Target: target, Status: "open", Latency: latency})
	}
}

type governedBody struct {
	io.ReadCloser
	cancel          context.CancelFunc
	breaker         *CircuitBreaker
	lease           *breakerLease
	parentContext   context.Context
	governedContext context.Context
	target          string
	status          string
	headerFailure   bool
	settled         bool
	startedAt       time.Time
	emit            func(Event)
	once            sync.Once
}

func (b *governedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == nil {
		return n, nil
	}
	if errors.Is(err, io.EOF) {
		b.completeSuccess()
		return n, err
	}
	b.completeReadFailure(err)
	return n, err
}

func (b *governedBody) Close() error {
	err := b.ReadCloser.Close()
	// Close without EOF means the consumer stopped reading. It must release a
	// half-open probe and the registry lease, but is not evidence of a bad target.
	b.completeCancellation()
	return err
}

func (b *governedBody) completeSuccess() {
	b.once.Do(func() {
		if !b.settled {
			if recovered := b.breaker.Success(); recovered {
				b.emit(Event{Type: "circuit_recovered", Target: b.target, Status: "closed", Latency: time.Since(b.startedAt)})
			}
		}
		b.cancel()
		b.lease.release()
	})
}

func (b *governedBody) completeCancellation() {
	b.once.Do(func() {
		if !b.settled {
			b.breaker.CancelProbe()
		}
		b.cancel()
		b.lease.release()
	})
}

func (b *governedBody) completeReadFailure(readErr error) {
	b.once.Do(func() {
		if b.settled {
			b.cancel()
			b.lease.release()
			return
		}
		callerErr := b.parentContext.Err()
		if callerErr != nil || errors.Is(readErr, context.Canceled) {
			b.emit(Event{Type: "downstream_cancelled", Target: b.target, Status: "cancelled", Latency: time.Since(b.startedAt), Error: readErr})
			b.breaker.CancelProbe()
			b.cancel()
			b.lease.release()
			return
		}
		failureErr := readErr
		status := "stream_error"
		if errors.Is(b.governedContext.Err(), context.DeadlineExceeded) || errors.Is(readErr, context.DeadlineExceeded) {
			failureErr = fmt.Errorf("%w: %s", ErrDownstreamTimeout, b.target)
			status = "timeout"
			b.emit(Event{Type: "downstream_timeout", Target: b.target, Status: status, Latency: time.Since(b.startedAt), Error: failureErr})
		} else {
			b.emit(Event{Type: "downstream_failure", Target: b.target, Status: status, Latency: time.Since(b.startedAt), Error: failureErr})
		}
		if b.breaker.Failure(time.Now()) {
			b.emit(Event{Type: "circuit_opened", Target: b.target, Status: "open", Latency: time.Since(b.startedAt), Error: failureErr})
		}
		b.cancel()
		b.lease.release()
	})
}

// BreakerCount 返回当前 Transport 跟踪的下游目标数量。
func (t *RoundTripper) BreakerCount() int {
	if t == nil || t.breakers == nil {
		return 0
	}
	return t.breakers.KeyCount()
}

func (t *RoundTripper) emit(event Event) {
	if t == nil || t.onEvent == nil {
		return
	}
	t.onEvent(event)
}

func targetKey(req *http.Request) string {
	if req == nil || req.URL == nil {
		return "unknown"
	}
	scheme := strings.ToLower(strings.TrimSpace(req.URL.Scheme))
	host := strings.ToLower(strings.TrimSpace(req.URL.Host))
	if scheme == "" {
		return host
	}
	return scheme + "://" + host
}
