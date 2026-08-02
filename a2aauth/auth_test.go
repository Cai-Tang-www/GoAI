package a2aauth

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

const testSecret = "0123456789abcdef0123456789abcdef"

type captureTransport struct {
	request *http.Request
	body    []byte
}

func (t *captureTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.request = request
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	t.body = body
	request.Body = io.NopCloser(bytes.NewReader(body))
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(nil)), Request: request}, nil
}

func TestSignerAndVerifierRoundTrip(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	resolver, err := NewStaticCredentialResolver(map[string]string{"planner-key": testSecret})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	capture := &captureTransport{}
	signer, err := NewSigner(capture, resolver, "planner", "planner-key",
		WithSignerClock(func() time.Time { return now }),
		WithNonceGenerator(func() (string, error) { return "nonce-0123456789", nil }),
	)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	request, err := http.NewRequest(http.MethodPost, "https://agent.example/a2a/agents/writer/message:send?mode=sync", bytes.NewBufferString(`{"prompt":"write"}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if _, err := signer.RoundTrip(request); err != nil {
		t.Fatalf("sign request: %v", err)
	}
	if got := string(capture.body); got != `{"prompt":"write"}` {
		t.Fatalf("body changed: %q", got)
	}
	verifier, err := NewVerifier(resolver, NewMemoryNonceStore(), 5*time.Minute, WithVerifierClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	if agent, err := verifier.Verify(capture.request, "planner-key"); err != nil || agent != "planner" {
		t.Fatalf("verify agent=%q err=%v", agent, err)
	}
	if got, _ := io.ReadAll(capture.request.Body); string(got) != `{"prompt":"write"}` {
		t.Fatalf("verified body was not restored: %q", got)
	}
}

func TestVerifierRejectsTamperingExpiryAndReplay(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	tests := []struct {
		name   string
		mutate func(*http.Request)
		want   error
	}{
		{name: "path", mutate: func(request *http.Request) { request.URL.Path = "/a2a/agents/other/message:send" }, want: ErrInvalidAuthentication},
		{name: "query", mutate: func(request *http.Request) { request.URL.RawQuery = "mode=other" }, want: ErrInvalidAuthentication},
		{name: "source", mutate: func(request *http.Request) { request.Header.Set(HeaderAgentCode, "forged") }, want: ErrInvalidAuthentication},
		{name: "nonce", mutate: func(request *http.Request) { request.Header.Set(HeaderNonce, "different-valid-nonce") }, want: ErrInvalidAuthentication},
		{name: "body", mutate: func(request *http.Request) {
			request.Body = io.NopCloser(bytes.NewBufferString(`{"prompt":"tampered"}`))
		}, want: ErrInvalidAuthentication},
		{name: "timestamp", mutate: func(request *http.Request) { request.Header.Set(HeaderTimestamp, "1699999000") }, want: ErrExpiredRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			signed, resolver := signedRequest(t, now, "nonce-0123456789")
			test.mutate(signed)
			verifier, err := NewVerifier(resolver, NewMemoryNonceStore(), 5*time.Minute, WithVerifierClock(func() time.Time { return now }))
			if err != nil {
				t.Fatalf("new verifier: %v", err)
			}
			if _, err := verifier.Verify(signed, "planner-key"); !errors.Is(err, test.want) {
				t.Fatalf("got %v, want %v", err, test.want)
			}
		})
	}

	signed, resolver := signedRequest(t, now, "nonce-replay-1234")
	store := NewMemoryNonceStore()
	verifier, err := NewVerifier(resolver, store, 5*time.Minute, WithVerifierClock(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	body, _ := io.ReadAll(signed.Body)
	signed.Body = io.NopCloser(bytes.NewReader(body))
	if _, err := verifier.Verify(signed, "planner-key"); err != nil {
		t.Fatalf("first verification: %v", err)
	}
	signed.Body = io.NopCloser(bytes.NewReader(body))
	if _, err := verifier.Verify(signed, "planner-key"); !errors.Is(err, ErrReplayDetected) {
		t.Fatalf("replay got %v, want %v", err, ErrReplayDetected)
	}
}

func TestVerifierRejectsReplayAtClockSkewBoundary(t *testing.T) {
	const maxClockSkew = 5 * time.Minute
	requestTime := time.Unix(1_700_000_000, 0)
	verifyTime := requestTime.Add(maxClockSkew)
	signed, resolver := signedRequest(t, requestTime, "boundary-replay-nonce")
	verifier, err := NewVerifier(resolver, NewMemoryNonceStore(), maxClockSkew, WithVerifierClock(func() time.Time { return verifyTime }))
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	body, err := io.ReadAll(signed.Body)
	if err != nil {
		t.Fatalf("read signed body: %v", err)
	}
	for attempt, want := range []error{nil, ErrReplayDetected} {
		signed.Body = io.NopCloser(bytes.NewReader(body))
		_, verifyErr := verifier.Verify(signed, "planner-key")
		if !errors.Is(verifyErr, want) {
			t.Fatalf("attempt %d got %v, want %v", attempt+1, verifyErr, want)
		}
	}
}

func TestMemoryNonceStoreClaimsOnceConcurrently(t *testing.T) {
	store := NewMemoryNonceStore()
	now := time.Unix(1_700_000_000, 0)
	var wg sync.WaitGroup
	results := make(chan bool, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, err := store.Claim(context.Background(), "planner", "same-nonce-12345", now, now.Add(time.Minute))
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			results <- claimed
		}()
	}
	wg.Wait()
	close(results)
	claimed := 0
	for result := range results {
		if result {
			claimed++
		}
	}
	if claimed != 1 {
		t.Fatalf("claimed=%d, want 1", claimed)
	}
}

func TestStaticCredentialResolverValidationAndCopies(t *testing.T) {
	if _, err := NewStaticCredentialResolver(map[string]string{"short": "secret"}); err == nil {
		t.Fatal("expected short credential error")
	}
	if _, err := NewStaticCredentialResolver(map[string]string{"blank": strings.Repeat(" ", minimumSecretBytes)}); err == nil {
		t.Fatal("expected blank credential error")
	}
	resolver, err := NewStaticCredentialResolver(map[string]string{"key": testSecret})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	first, err := resolver.Resolve(context.Background(), "key")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	first[0] = 'x'
	second, err := resolver.Resolve(context.Background(), "key")
	if err != nil {
		t.Fatalf("resolve again: %v", err)
	}
	if string(second) != testSecret {
		t.Fatal("resolver returned mutable backing storage")
	}
	if _, err := resolver.Resolve(context.Background(), "missing"); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("missing credential got %v", err)
	}
}

func signedRequest(t *testing.T, now time.Time, nonce string) (*http.Request, CredentialResolver) {
	t.Helper()
	resolver, err := NewStaticCredentialResolver(map[string]string{"planner-key": testSecret})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	capture := &captureTransport{}
	signer, err := NewSigner(capture, resolver, "planner", "planner-key",
		WithSignerClock(func() time.Time { return now }),
		WithNonceGenerator(func() (string, error) { return nonce, nil }),
	)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	request, err := http.NewRequest(http.MethodPost, "https://agent.example/a2a/agents/writer/message:send?mode=sync", bytes.NewBufferString(`{"prompt":"write"}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if _, err := signer.RoundTrip(request); err != nil {
		t.Fatalf("sign request: %v", err)
	}
	return capture.request, resolver
}
