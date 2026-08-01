// Package a2aclient 实现 GoAI Runtime 到目标 Agent 的官方 A2A 出站调用。
package a2aclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"GoAI/a2aprotocol"
	"GoAI/observability"
	"GoAI/requestctx"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
	sdkclient "github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aclient/agentcard"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const defaultRequestTimeout = 30 * time.Second

// Client 使用 Agent Card 发现和官方 HTTP+JSON transport 执行跨 Agent 委派。
type Client struct {
	httpClient   *http.Client
	pollInterval time.Duration
	telemetry    *observability.Bundle
}

// Option 配置 A2A 出站客户端的可选依赖。
type Option func(*Client) error

// WithObservability 注入 A2A 出站调用的日志、指标和 Trace 能力。
func WithObservability(bundle *observability.Bundle) Option {
	return func(client *Client) error {
		if bundle == nil {
			return errors.New("configuring A2A client: observability bundle is nil")
		}
		client.telemetry = bundle
		return nil
	}
}

// New 创建安全的 A2A 出站客户端。HTTP 仅允许 loopback，远程地址必须使用 HTTPS。
func New(httpClient *http.Client, requestTimeout, pollInterval time.Duration, options ...Option) (*Client, error) {
	if requestTimeout <= 0 {
		requestTimeout = defaultRequestTimeout
	}
	if pollInterval <= 0 {
		return nil, errors.New("creating A2A client: poll interval must be greater than zero")
	}
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	cloned := *httpClient
	cloned.Timeout = requestTimeout
	previousRedirectPolicy := cloned.CheckRedirect
	cloned.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if err := validateURL(req.URL); err != nil {
			return fmt.Errorf("rejecting unsafe A2A redirect: %w", err)
		}
		if previousRedirectPolicy != nil {
			return previousRedirectPolicy(req, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	client := &Client{httpClient: &cloned, pollInterval: pollInterval}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(client); err != nil {
			return nil, err
		}
	}
	return client, nil
}

// Invoke 发现目标 Agent、校验能力并等待 A2A Task 进入终态。
func (c *Client) Invoke(ctx context.Context, request services.AgentInvocationRequest) (*services.AgentInvocationResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateRequest(request); err != nil {
		return nil, invocationError(err, false)
	}

	startedAt := time.Now()
	status := "success"
	var span oteltrace.Span
	if c != nil && c.telemetry != nil && c.telemetry.Tracer != nil {
		ctx, span = c.telemetry.Tracer.Start(ctx, "a2a.invoke", observability.SpanAttributes(
			requestctx.TraceIDFromContext(ctx), request.ParentRunID, request.ThreadID, "")...)
		defer span.End()
	}
	defer func() {
		if c == nil || c.telemetry == nil {
			return
		}
		if c.telemetry.Metrics != nil {
			c.telemetry.Metrics.ObserveA2A("invoke", status)
		}
		if c.telemetry.Logger != nil {
			c.telemetry.Logger.InfoContext(ctx, "a2a invocation",
				slog.String("trace_id", requestctx.TraceIDFromContext(ctx)),
				slog.String("source_agent", request.SourceAgentCode),
				slog.String("target_agent", request.TargetAgentCode),
				slog.String("capability", request.CapabilityCode),
				slog.String("parent_run_id", request.ParentRunID),
				slog.String("status", status),
				slog.Int64("latency_ms", time.Since(startedAt).Milliseconds()),
			)
		}
	}()

	var failures []error
	for _, endpoint := range request.Endpoints {
		result, err := c.invokeEndpoint(ctx, request, endpoint)
		if err == nil {
			return result, nil
		}
		status = "error"
		if span != nil {
			observability.MarkSpanError(span, err)
		}
		failures = append(failures, err)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !isRetryable(err) {
			return nil, err
		}
	}
	return nil, invocationError(fmt.Errorf("all A2A endpoints failed: %w", errors.Join(failures...)), true)
}

func (c *Client) invokeEndpoint(ctx context.Context, request services.AgentInvocationRequest, endpoint services.AgentInvocationEndpoint) (*services.AgentInvocationResult, error) {
	baseURL, err := parseEndpoint(endpoint)
	if err != nil {
		return nil, invocationError(err, false)
	}
	card, err := agentcard.NewResolver(c.httpClient).Resolve(ctx, strings.TrimRight(baseURL.String(), "/"))
	if err != nil {
		return nil, invocationError(fmt.Errorf("discovering target agent card: %w", err), true)
	}
	if err := validateCard(card, request.CapabilityCode, baseURL); err != nil {
		return nil, invocationError(err, false)
	}

	client, err := sdkclient.NewFromCard(
		ctx,
		card,
		sdkclient.WithDefaultsDisabled(),
		sdkclient.WithRESTTransport(c.httpClient),
		sdkclient.WithConfig(sdkclient.Config{
			PreferredTransports: []a2a.TransportProtocol{a2a.TransportProtocolHTTPJSON},
			AcceptedOutputModes: []string{"application/json", "text/plain"},
		}),
	)
	if err != nil {
		return nil, invocationError(fmt.Errorf("creating A2A transport: %w", err), true)
	}

	var payload any
	if err := json.Unmarshal([]byte(request.InputJSON), &payload); err != nil {
		return nil, invocationError(fmt.Errorf("decoding invocation input: %w", err), false)
	}
	message := &a2a.Message{
		ID:         request.MessageID,
		ContextID:  request.ThreadID,
		TaskID:     a2a.TaskID(request.TaskID),
		Role:       a2a.MessageRoleUser,
		Parts:      a2a.ContentParts{a2a.NewDataPart(payload)},
		Extensions: []string{a2aprotocol.DelegationExtensionURI},
		Metadata: map[string]any{
			a2aprotocol.DelegationExtensionURI: map[string]any{
				"sourceAgentCode": request.SourceAgentCode,
				"capabilityCode":  request.CapabilityCode,
				"parentRunId":     request.ParentRunID,
			},
		},
	}
	result, err := client.SendMessage(ctx, &a2a.SendMessageRequest{Message: message})
	if err != nil {
		return nil, invocationError(fmt.Errorf("sending A2A message: %w", err), true)
	}
	switch value := result.(type) {
	case *a2a.Message:
		return resultFromMessage(request.TaskID, value)
	case *a2a.Task:
		return c.waitForTask(ctx, client, request.TaskID, value)
	default:
		return nil, invocationError(fmt.Errorf("unsupported A2A response type %T", result), false)
	}
}

func (c *Client) waitForTask(ctx context.Context, client *sdkclient.Client, expectedTaskID string, task *a2a.Task) (*services.AgentInvocationResult, error) {
	for {
		if task == nil {
			return nil, invocationError(errors.New("target agent returned an empty task"), true)
		}
		if string(task.ID) != expectedTaskID {
			return nil, invocationError(fmt.Errorf("target task id mismatch: expected %s, got %s", expectedTaskID, task.ID), false)
		}
		if task.Status.State.Terminal() {
			return resultFromTask(task)
		}
		if task.Status.State == a2a.TaskStateInputRequired || task.Status.State == a2a.TaskStateAuthRequired {
			return nil, invocationError(fmt.Errorf("target agent requires unsupported interaction: %s", task.Status.State), false)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.pollInterval):
		}
		var err error
		task, err = client.GetTask(ctx, &a2a.GetTaskRequest{ID: a2a.TaskID(expectedTaskID)})
		if err != nil {
			return nil, invocationError(fmt.Errorf("polling A2A task: %w", err), true)
		}
	}
}

func resultFromTask(task *a2a.Task) (*services.AgentInvocationResult, error) {
	if task.Status.State != a2a.TaskStateCompleted {
		message := messageText(task.Status.Message)
		if message == "" {
			message = "target agent ended in state " + string(task.Status.State)
		}
		return nil, invocationError(errors.New(message), false)
	}
	output, message, err := taskOutput(task)
	if err != nil {
		return nil, invocationError(err, false)
	}
	return &services.AgentInvocationResult{TaskID: string(task.ID), State: string(task.Status.State), OutputJSON: output, Message: message}, nil
}

func resultFromMessage(taskID string, message *a2a.Message) (*services.AgentInvocationResult, error) {
	output, text, err := partsOutput(message.Parts)
	if err != nil {
		return nil, invocationError(err, false)
	}
	return &services.AgentInvocationResult{TaskID: taskID, State: string(a2a.TaskStateCompleted), OutputJSON: output, Message: text}, nil
}

func taskOutput(task *a2a.Task) (string, string, error) {
	for _, artifact := range task.Artifacts {
		if artifact == nil {
			continue
		}
		output, message, err := partsOutput(artifact.Parts)
		if err != nil {
			return "", "", err
		}
		if output != "{}" || message != "" {
			return output, message, nil
		}
	}
	if task.Status.Message != nil {
		return partsOutput(task.Status.Message.Parts)
	}
	for index := len(task.History) - 1; index >= 0; index-- {
		if task.History[index] != nil && task.History[index].Role == a2a.MessageRoleAgent {
			return partsOutput(task.History[index].Parts)
		}
	}
	return "{}", "", nil
}

func partsOutput(parts a2a.ContentParts) (string, string, error) {
	for _, part := range parts {
		if part == nil {
			continue
		}
		if data := part.Data(); data != nil {
			encoded, err := json.Marshal(data)
			if err != nil {
				return "", "", fmt.Errorf("encoding A2A result data: %w", err)
			}
			return string(encoded), "", nil
		}
		if text := strings.TrimSpace(part.Text()); text != "" {
			encoded, err := json.Marshal(map[string]string{"message": text})
			if err != nil {
				return "", "", err
			}
			return string(encoded), text, nil
		}
	}
	return "{}", "", nil
}

func messageText(message *a2a.Message) string {
	if message == nil {
		return ""
	}
	for _, part := range message.Parts {
		if part != nil && strings.TrimSpace(part.Text()) != "" {
			return strings.TrimSpace(part.Text())
		}
	}
	return ""
}

func validateRequest(request services.AgentInvocationRequest) error {
	if strings.TrimSpace(request.SourceAgentCode) == "" || strings.TrimSpace(request.TargetAgentCode) == "" {
		return errors.New("source and target agent codes are required")
	}
	if strings.TrimSpace(request.CapabilityCode) == "" || strings.TrimSpace(request.ParentRunID) == "" {
		return errors.New("capability and parent run id are required")
	}
	if strings.TrimSpace(request.TaskID) == "" || strings.TrimSpace(request.MessageID) == "" {
		return errors.New("task id and message id are required")
	}
	if len(request.TaskID) > 64 || len(request.MessageID) > 64 {
		return errors.New("task id and message id must not exceed 64 characters")
	}
	if !json.Valid([]byte(request.InputJSON)) {
		return errors.New("agent invocation input must be valid JSON")
	}
	if len(request.Endpoints) == 0 {
		return errors.New("target agent has no active A2A endpoint")
	}
	return nil
}

func parseEndpoint(endpoint services.AgentInvocationEndpoint) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint.Address))
	if err != nil {
		return nil, fmt.Errorf("parsing A2A endpoint: %w", err)
	}
	if err := validateURL(parsed); err != nil {
		return nil, err
	}
	if endpoint.Transport != "" && !strings.EqualFold(endpoint.Transport, parsed.Scheme) {
		return nil, fmt.Errorf("A2A endpoint transport %s does not match URL scheme %s", endpoint.Transport, parsed.Scheme)
	}
	return parsed, nil
}

func validateURL(parsed *url.URL) error {
	if parsed == nil || !parsed.IsAbs() || parsed.Host == "" {
		return errors.New("A2A endpoint must be an absolute URL")
	}
	if parsed.User != nil {
		return errors.New("A2A endpoint must not contain user info")
	}
	switch strings.ToLower(parsed.Scheme) {
	case "https":
		return nil
	case "http":
		if isLoopbackHost(parsed.Hostname()) {
			return nil
		}
		return errors.New("remote A2A endpoint must use HTTPS")
	default:
		return fmt.Errorf("unsupported A2A endpoint scheme %q", parsed.Scheme)
	}
}

func validateCard(card *a2a.AgentCard, capability string, requestedBase *url.URL) error {
	if card == nil {
		return errors.New("target agent card is empty")
	}
	foundExtension := false
	for _, extension := range card.Capabilities.Extensions {
		if extension.URI == a2aprotocol.DelegationExtensionURI {
			foundExtension = true
			break
		}
	}
	if !foundExtension {
		return errors.New("target agent card does not support GoAI delegation extension")
	}
	foundCapability := false
	for _, skill := range card.Skills {
		if skill.ID == capability {
			foundCapability = true
			break
		}
	}
	if !foundCapability {
		return fmt.Errorf("target agent card does not expose capability %q", capability)
	}
	validInterfaces := make([]*a2a.AgentInterface, 0, len(card.SupportedInterfaces))
	for _, endpoint := range card.SupportedInterfaces {
		if endpoint == nil || endpoint.ProtocolBinding != a2a.TransportProtocolHTTPJSON {
			continue
		}
		parsed, err := url.Parse(endpoint.URL)
		if err != nil || validateURL(parsed) != nil || !sameOrigin(parsed, requestedBase) {
			continue
		}
		validInterfaces = append(validInterfaces, endpoint)
	}
	if len(validInterfaces) == 0 {
		return errors.New("target agent card has no safe HTTP+JSON interface")
	}
	card.SupportedInterfaces = validInterfaces
	return nil
}

func sameOrigin(left, right *url.URL) bool {
	return left != nil && right != nil && strings.EqualFold(left.Scheme, right.Scheme) && strings.EqualFold(left.Host, right.Host)
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type clientError struct {
	err       error
	retryable bool
}

func (e *clientError) Error() string   { return e.err.Error() }
func (e *clientError) Unwrap() error   { return e.err }
func (e *clientError) Retryable() bool { return e.retryable }

func invocationError(err error, retryable bool) error {
	return &clientError{err: err, retryable: retryable}
}

func isRetryable(err error) bool {
	var typed interface{ Retryable() bool }
	return errors.As(err, &typed) && typed.Retryable()
}
