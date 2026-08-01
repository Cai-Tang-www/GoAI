package governance

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newTransportTestRequest(t *testing.T) *http.Request {
	t.Helper()
	u, err := url.Parse("https://downstream.example/v1")
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	return &http.Request{Method: http.MethodGet, URL: u, Header: make(http.Header), Body: io.NopCloser(nil)}
}

func TestRoundTripperOpensCircuitAfterFailures(t *testing.T) {
	var mu sync.Mutex
	var events []Event
	transport, err := NewRoundTripper(TransportConfig{
		BaseTransport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusBadGateway, Body: io.NopCloser(nil), Request: req}, nil
		}),
		RequestTimeout:   time.Second,
		FailureThreshold: 2,
		OpenTimeout:      time.Minute,
		OnEvent: func(event Event) {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, event)
		},
	})
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}

	for i := 0; i < 2; i++ {
		response, err := transport.RoundTrip(newTransportTestRequest(t))
		if err != nil {
			t.Fatalf("round trip %d: %v", i, err)
		}
		if response.StatusCode != http.StatusBadGateway {
			t.Fatalf("expected 502, got %d", response.StatusCode)
		}
		response.Body.Close()
	}
	if _, err := transport.RoundTrip(newTransportTestRequest(t)); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected open circuit error, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) < 3 {
		t.Fatalf("expected failure, opened, and rejected events, got %d", len(events))
	}
}

func TestRoundTripperReportsTimeout(t *testing.T) {
	var got Event
	transport, err := NewRoundTripper(TransportConfig{
		BaseTransport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
		RequestTimeout:   5 * time.Millisecond,
		FailureThreshold: 3,
		OpenTimeout:      time.Second,
		OnEvent: func(event Event) {
			if event.Type == "downstream_timeout" {
				got = event
			}
		},
	})
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}
	_, err = transport.RoundTrip(newTransportTestRequest(t))
	if !errors.Is(err, ErrDownstreamTimeout) {
		t.Fatalf("expected timeout error, got %v", err)
	}
	if got.Type != "downstream_timeout" || got.Status != "timeout" {
		t.Fatalf("unexpected timeout event: %+v", got)
	}
}

func TestServiceTransportUsesCurrentEventSink(t *testing.T) {
	service, err := New(Config{
		Enabled:                    true,
		RateLimitRequestsPerSecond: 10,
		RateLimitBurst:             1,
		RateLimitMaxKeys:           10,
		DownstreamRequestTimeout:   time.Second,
		CircuitFailureThreshold:    1,
		CircuitOpenTimeout:         time.Minute,
		CircuitMaxTargets:          10,
	})
	if err != nil {
		t.Fatalf("create service: %v", err)
	}
	var mu sync.Mutex
	var events []Event
	service.OnEvent = func(event Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, event)
	}
	service.Transport.base = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusInternalServerError, Body: io.NopCloser(nil), Request: req}, nil
	})
	response, err := service.Transport.RoundTrip(newTransportTestRequest(t))
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	response.Body.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatal("expected transport event to use service event sink")
	}
}

func TestRoundTripperPreservesRequestContextCancellation(t *testing.T) {
	transport, err := NewRoundTripper(TransportConfig{
		BaseTransport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if err := req.Context().Err(); err != nil {
				return nil, err
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(nil), Request: req}, nil
		}),
		RequestTimeout:   time.Second,
		FailureThreshold: 1,
		OpenTimeout:      time.Second,
	})
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := newTransportTestRequest(t).WithContext(ctx)
	_, err = transport.RoundTrip(req)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled request error, got %v", err)
	}
	response, err := transport.RoundTrip(newTransportTestRequest(t))
	if err != nil {
		t.Fatalf("cancellation should not open circuit: %v", err)
	}
	response.Body.Close()
}

func TestRoundTripperKeepsResponseContextUntilBodyClose(t *testing.T) {
	var downstreamContext context.Context
	transport, err := NewRoundTripper(TransportConfig{
		BaseTransport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			downstreamContext = req.Context()
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("chunk")), Request: req}, nil
		}),
		RequestTimeout:   time.Second,
		FailureThreshold: 1,
		OpenTimeout:      time.Second,
	})
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}
	response, err := transport.RoundTrip(newTransportTestRequest(t))
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	if downstreamContext.Err() != nil {
		t.Fatalf("response context cancelled before body consumption: %v", downstreamContext.Err())
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if downstreamContext.Err() != context.Canceled {
		t.Fatalf("response context after EOF = %v, want context.Canceled", downstreamContext.Err())
	}
}

type scriptedReadCloser struct {
	reads int
}

func (b *scriptedReadCloser) Read(p []byte) (int, error) {
	if b.reads == 0 {
		b.reads++
		return copy(p, "chunk"), nil
	}
	return 0, errors.New("stream read failed")
}

func (b *scriptedReadCloser) Close() error { return nil }

func TestRoundTripperCallerDeadlineDoesNotOpenCircuit(t *testing.T) {
	var calls int
	var callsMu sync.Mutex
	transport, err := NewRoundTripper(TransportConfig{
		BaseTransport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			callsMu.Lock()
			calls++
			current := calls
			callsMu.Unlock()
			if current == 1 {
				<-req.Context().Done()
				return nil, req.Context().Err()
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok")), Request: req}, nil
		}),
		RequestTimeout:   time.Second,
		FailureThreshold: 1,
		OpenTimeout:      time.Second,
	})
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	_, err = transport.RoundTrip(newTransportTestRequest(t).WithContext(ctx))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected caller deadline error, got %v", err)
	}
	if errors.Is(err, ErrDownstreamTimeout) {
		t.Fatalf("caller deadline must not be classified as downstream timeout: %v", err)
	}

	response, err := transport.RoundTrip(newTransportTestRequest(t))
	if err != nil {
		t.Fatalf("caller deadline must not open circuit: %v", err)
	}
	response.Body.Close()
}

func TestRoundTripperCountsStreamReadFailure(t *testing.T) {
	transport, err := NewRoundTripper(TransportConfig{
		BaseTransport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: &scriptedReadCloser{}, Request: req}, nil
		}),
		RequestTimeout:   time.Second,
		FailureThreshold: 1,
		OpenTimeout:      time.Minute,
	})
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}
	response, err := transport.RoundTrip(newTransportTestRequest(t))
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	if _, err := io.ReadAll(response.Body); err == nil || !strings.Contains(err.Error(), "stream read failed") {
		t.Fatalf("expected stream read failure, got %v", err)
	}
	if _, err := transport.RoundTrip(newTransportTestRequest(t)); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("stream read failure should open circuit, got %v", err)
	}
}

func TestRoundTripperCallerCancellationDuringBodyDoesNotOpenCircuit(t *testing.T) {
	transport, err := NewRoundTripper(TransportConfig{
		BaseTransport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: contextBody{ctx: req.Context()}, Request: req}, nil
		}),
		RequestTimeout:   time.Second,
		FailureThreshold: 1,
		OpenTimeout:      time.Minute,
	})
	if err != nil {
		t.Fatalf("create transport: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	response, err := transport.RoundTrip(newTransportTestRequest(t).WithContext(ctx))
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	cancel()
	if _, err := io.ReadAll(response.Body); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected caller cancellation from body, got %v", err)
	}

	response, err = transport.RoundTrip(newTransportTestRequest(t))
	if err != nil {
		t.Fatalf("caller cancellation must not open circuit: %v", err)
	}
	response.Body.Close()
}

type contextBody struct{ ctx context.Context }

func (b contextBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (contextBody) Close() error { return nil }
