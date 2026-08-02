// Package a2aauth 提供 GoAI A2A HTTP 请求的机器身份认证、签名和重放保护。
package a2aauth

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// AuthTypeNone 表示 Endpoint 不要求 Agent 机器身份认证，仅允许显式关闭认证的开发环境使用。
	AuthTypeNone = "none"
	// AuthTypeHMACSHA256 表示 Endpoint 使用 GoAI HMAC-SHA256 请求签名。
	AuthTypeHMACSHA256 = "goai_hmac_sha256"

	AuthorizationScheme = "GoAI-HMAC-SHA256"
	HeaderAgentCode     = "X-GoAI-Agent-Code"
	HeaderTimestamp     = "X-GoAI-Timestamp"
	HeaderNonce         = "X-GoAI-Nonce"
	HeaderContentSHA256 = "X-GoAI-Content-SHA256"

	minimumSecretBytes  = 32
	defaultMaxBodyBytes = 4 << 20
)

var (
	ErrCredentialNotFound    = errors.New("A2A credential not found")
	ErrMissingAuthentication = errors.New("A2A authentication is missing")
	ErrInvalidAuthentication = errors.New("A2A authentication is invalid")
	ErrExpiredRequest        = errors.New("A2A authentication timestamp is outside the allowed window")
	ErrReplayDetected        = errors.New("A2A request replay detected")
	ErrRequestBodyTooLarge   = errors.New("A2A request body is too large")
)

// CredentialResolver 根据逻辑引用解析真实密钥；实现不得把密钥写回数据库或日志。
type CredentialResolver interface {
	Resolve(context.Context, string) ([]byte, error)
}

// StaticCredentialResolver 是配置驱动的只读凭据解析器。
type StaticCredentialResolver struct {
	credentials map[string][]byte
}

// NewStaticCredentialResolver 校验并复制凭据，避免调用方后续修改底层数据。
func NewStaticCredentialResolver(credentials map[string]string) (*StaticCredentialResolver, error) {
	resolved := make(map[string][]byte, len(credentials))
	for rawRef, rawSecret := range credentials {
		ref := strings.TrimSpace(rawRef)
		if ref == "" {
			return nil, errors.New("creating A2A credential resolver: credential reference is empty")
		}
		if strings.TrimSpace(rawSecret) == "" {
			return nil, fmt.Errorf("creating A2A credential resolver: credential %q must not be blank", ref)
		}
		secret := []byte(rawSecret)
		if len(secret) < minimumSecretBytes {
			return nil, fmt.Errorf("creating A2A credential resolver: credential %q must contain at least %d bytes", ref, minimumSecretBytes)
		}
		if _, exists := resolved[ref]; exists {
			return nil, fmt.Errorf("creating A2A credential resolver: duplicate credential reference %q", ref)
		}
		resolved[ref] = append([]byte(nil), secret...)
	}
	return &StaticCredentialResolver{credentials: resolved}, nil
}

// Resolve 返回指定引用的密钥副本。
func (r *StaticCredentialResolver) Resolve(ctx context.Context, ref string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil {
		return nil, ErrCredentialNotFound
	}
	secret, ok := r.credentials[strings.TrimSpace(ref)]
	if !ok {
		return nil, ErrCredentialNotFound
	}
	return append([]byte(nil), secret...), nil
}

// NonceStore 原子声明一次性 nonce；返回 false 表示同一身份已使用该 nonce。
type NonceStore interface {
	Claim(context.Context, string, string, time.Time, time.Time) (bool, error)
}

// MemoryNonceStore 是单进程并发安全的 nonce store，适合 V1 和测试环境。
type MemoryNonceStore struct {
	mu      sync.Mutex
	expires map[string]time.Time
}

// NewMemoryNonceStore 创建进程内 nonce store。
func NewMemoryNonceStore() *MemoryNonceStore {
	return &MemoryNonceStore{expires: make(map[string]time.Time)}
}

// Claim 在清理过期记录后原子声明 nonce。
func (s *MemoryNonceStore) Claim(ctx context.Context, agentCode, nonce string, now, expiresAt time.Time) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s == nil {
		return false, errors.New("claiming A2A nonce: store is nil")
	}
	key := strings.TrimSpace(agentCode) + "\x00" + nonce
	s.mu.Lock()
	defer s.mu.Unlock()
	for existing, expiry := range s.expires {
		if !expiry.After(now) {
			delete(s.expires, existing)
		}
	}
	if expiry, exists := s.expires[key]; exists && expiry.After(now) {
		return false, nil
	}
	s.expires[key] = expiresAt
	return true, nil
}

// Signer 使用来源 Agent 的凭据为每次 HTTP RoundTrip 生成独立签名。
type Signer struct {
	base          http.RoundTripper
	resolver      CredentialResolver
	agentCode     string
	credentialRef string
	now           func() time.Time
	nonce         func() (string, error)
	maxBodyBytes  int64
}

// SignerOption 配置签名器的可测试依赖。
type SignerOption func(*Signer)

// WithSignerClock 注入签名时间来源。
func WithSignerClock(now func() time.Time) SignerOption {
	return func(signer *Signer) {
		if now != nil {
			signer.now = now
		}
	}
}

// WithNonceGenerator 注入 nonce 生成器。
func WithNonceGenerator(generate func() (string, error)) SignerOption {
	return func(signer *Signer) {
		if generate != nil {
			signer.nonce = generate
		}
	}
}

// NewSigner 创建出站请求签名 RoundTripper。
func NewSigner(base http.RoundTripper, resolver CredentialResolver, agentCode, credentialRef string, options ...SignerOption) (*Signer, error) {
	if base == nil {
		base = http.DefaultTransport
	}
	if resolver == nil {
		return nil, errors.New("creating A2A signer: credential resolver is nil")
	}
	agentCode = strings.TrimSpace(agentCode)
	if agentCode == "" {
		return nil, errors.New("creating A2A signer: agent code is empty")
	}
	credentialRef = strings.TrimSpace(credentialRef)
	if credentialRef == "" {
		return nil, errors.New("creating A2A signer: credential reference is empty")
	}
	signer := &Signer{
		base:          base,
		resolver:      resolver,
		agentCode:     agentCode,
		credentialRef: credentialRef,
		now:           time.Now,
		nonce:         randomNonce,
		maxBodyBytes:  defaultMaxBodyBytes,
	}
	for _, option := range options {
		if option != nil {
			option(signer)
		}
	}
	return signer, nil
}

// RoundTrip 克隆请求、计算正文摘要并按最终 URL 生成签名。
func (s *Signer) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil {
		return nil, errors.New("signing A2A request: request is nil")
	}
	body, err := requestBody(request, s.maxBodyBytes)
	if err != nil {
		return nil, fmt.Errorf("signing A2A request: %w", err)
	}
	secret, err := s.resolver.Resolve(request.Context(), s.credentialRef)
	if err != nil {
		return nil, fmt.Errorf("signing A2A request: resolving credential: %w", err)
	}
	nonce, err := s.nonce()
	if err != nil {
		return nil, fmt.Errorf("signing A2A request: generating nonce: %w", err)
	}
	timestamp := strconv.FormatInt(s.now().UTC().Unix(), 10)
	digest := sha256.Sum256(body)
	digestHex := hex.EncodeToString(digest[:])
	signature := calculateSignature(secret, request.Method, request.URL.RequestURI(), timestamp, nonce, s.agentCode, digestHex)

	cloned := cloneRequestWithBody(request, body)
	cloned.Header.Set("Authorization", AuthorizationScheme+" "+signature)
	cloned.Header.Set(HeaderAgentCode, s.agentCode)
	cloned.Header.Set(HeaderTimestamp, timestamp)
	cloned.Header.Set(HeaderNonce, nonce)
	cloned.Header.Set(HeaderContentSHA256, digestHex)
	return s.base.RoundTrip(cloned)
}

// Verifier 校验入站 HMAC 请求并通过 NonceStore 防止有效请求重放。
type Verifier struct {
	resolver     CredentialResolver
	nonces       NonceStore
	maxClockSkew time.Duration
	now          func() time.Time
	maxBodyBytes int64
}

// VerifierOption 配置验签器的可测试依赖。
type VerifierOption func(*Verifier)

// WithVerifierClock 注入验签时间来源。
func WithVerifierClock(now func() time.Time) VerifierOption {
	return func(verifier *Verifier) {
		if now != nil {
			verifier.now = now
		}
	}
}

// NewVerifier 创建入站请求验签器。
func NewVerifier(resolver CredentialResolver, nonces NonceStore, maxClockSkew time.Duration, options ...VerifierOption) (*Verifier, error) {
	if resolver == nil {
		return nil, errors.New("creating A2A verifier: credential resolver is nil")
	}
	if nonces == nil {
		return nil, errors.New("creating A2A verifier: nonce store is nil")
	}
	if maxClockSkew <= 0 {
		return nil, errors.New("creating A2A verifier: max clock skew must be greater than zero")
	}
	verifier := &Verifier{resolver: resolver, nonces: nonces, maxClockSkew: maxClockSkew, now: time.Now, maxBodyBytes: defaultMaxBodyBytes}
	for _, option := range options {
		if option != nil {
			option(verifier)
		}
	}
	return verifier, nil
}

// Verify 校验请求头、正文摘要、时间窗、签名和 nonce，并返回认证后的来源 Agent。
func (v *Verifier) Verify(request *http.Request, credentialRef string) (string, error) {
	if request == nil {
		return "", ErrMissingAuthentication
	}
	agentCode := strings.TrimSpace(request.Header.Get(HeaderAgentCode))
	timestamp := strings.TrimSpace(request.Header.Get(HeaderTimestamp))
	nonce := strings.TrimSpace(request.Header.Get(HeaderNonce))
	contentDigest := strings.ToLower(strings.TrimSpace(request.Header.Get(HeaderContentSHA256)))
	authorization := strings.TrimSpace(request.Header.Get("Authorization"))
	if agentCode == "" || timestamp == "" || nonce == "" || contentDigest == "" || authorization == "" {
		return "", ErrMissingAuthentication
	}
	if !validNonce(nonce) || len(contentDigest) != sha256.Size*2 {
		return "", ErrInvalidAuthentication
	}
	providedSignature, ok := strings.CutPrefix(authorization, AuthorizationScheme+" ")
	if !ok || len(providedSignature) != sha256.Size*2 {
		return "", ErrInvalidAuthentication
	}
	providedSignatureBytes, err := hex.DecodeString(providedSignature)
	if err != nil {
		return "", ErrInvalidAuthentication
	}
	requestTimeUnix, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return "", ErrInvalidAuthentication
	}
	now := v.now().UTC()
	requestTime := time.Unix(requestTimeUnix, 0).UTC()
	if delta := now.Sub(requestTime); delta > v.maxClockSkew || delta < -v.maxClockSkew {
		return "", ErrExpiredRequest
	}
	body, err := requestBody(request, v.maxBodyBytes)
	if err != nil {
		return "", err
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	digest := sha256.Sum256(body)
	actualDigest := hex.EncodeToString(digest[:])
	if !hmac.Equal([]byte(actualDigest), []byte(contentDigest)) {
		return "", ErrInvalidAuthentication
	}
	secret, err := v.resolver.Resolve(request.Context(), strings.TrimSpace(credentialRef))
	if err != nil {
		return "", fmt.Errorf("verifying A2A request: resolving credential: %w", err)
	}
	expectedSignatureHex := calculateSignature(secret, request.Method, request.URL.RequestURI(), timestamp, nonce, agentCode, actualDigest)
	expectedSignature, _ := hex.DecodeString(expectedSignatureHex)
	if !hmac.Equal(expectedSignature, providedSignatureBytes) {
		return "", ErrInvalidAuthentication
	}
	claimed, err := v.nonces.Claim(request.Context(), agentCode, nonce, now, now.Add(v.maxClockSkew))
	if err != nil {
		return "", fmt.Errorf("verifying A2A request: claiming nonce: %w", err)
	}
	if !claimed {
		return "", ErrReplayDetected
	}
	return agentCode, nil
}

type authenticatedAgentContextKey struct{}

// WithAuthenticatedAgent 把可信来源 Agent 写入请求上下文。
func WithAuthenticatedAgent(ctx context.Context, agentCode string) context.Context {
	return context.WithValue(ctx, authenticatedAgentContextKey{}, strings.TrimSpace(agentCode))
}

// AuthenticatedAgentFromContext 返回验签后写入的来源 Agent。
func AuthenticatedAgentFromContext(ctx context.Context) (string, bool) {
	agentCode, ok := ctx.Value(authenticatedAgentContextKey{}).(string)
	agentCode = strings.TrimSpace(agentCode)
	return agentCode, ok && agentCode != ""
}

func calculateSignature(secret []byte, method, pathAndQuery, timestamp, nonce, agentCode, contentDigest string) string {
	canonical := strings.Join([]string{
		strings.ToUpper(strings.TrimSpace(method)),
		pathAndQuery,
		timestamp,
		nonce,
		agentCode,
		contentDigest,
	}, "\n")
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}

func requestBody(request *http.Request, maxBytes int64) ([]byte, error) {
	if request.Body == nil || request.Body == http.NoBody {
		return nil, nil
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	if int64(len(body)) > maxBytes {
		return nil, ErrRequestBodyTooLarge
	}
	return body, nil
}

func cloneRequestWithBody(request *http.Request, body []byte) *http.Request {
	cloned := request.Clone(request.Context())
	cloned.Header = request.Header.Clone()
	if len(body) == 0 {
		cloned.Body = http.NoBody
		cloned.GetBody = func() (io.ReadCloser, error) { return http.NoBody, nil }
		cloned.ContentLength = 0
		return cloned
	}
	cloned.Body = io.NopCloser(bytes.NewReader(body))
	cloned.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	cloned.ContentLength = int64(len(body))
	return cloned
}

func randomNonce() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func validNonce(value string) bool {
	if len(value) < 16 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '-' || char == '_' {
			continue
		}
		return false
	}
	return true
}
