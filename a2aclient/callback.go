package a2aclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"GoAI/a2aauth"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

const notificationTokenHeader = "A2A-Notification-Token"

// CallbackSender 通过签名的 A2A HTTP(S) 请求发送终态 Push Notification。
type CallbackSender struct {
	httpClient   *http.Client
	resolver     a2aauth.CredentialResolver
	authRequired bool
}

// NewCallbackSender 创建拒绝重定向的终态 callback 发送器，避免 Token 跨主机泄漏。
func NewCallbackSender(httpClient *http.Client, resolver a2aauth.CredentialResolver, authRequired bool) (*CallbackSender, error) {
	if httpClient == nil {
		return nil, errors.New("creating A2A callback sender: HTTP client is nil")
	}
	if authRequired && resolver == nil {
		return nil, errors.New("creating A2A callback sender: credential resolver is nil")
	}
	cloned := *httpClient
	cloned.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &CallbackSender{httpClient: &cloned, resolver: resolver, authRequired: authRequired}, nil
}

// SendDelegationCallback 实现 services.DelegationCallbackSender。
func (s *CallbackSender) SendDelegationCallback(ctx context.Context, delivery services.DelegationCallbackDelivery) error {
	event, err := callbackEvent(delivery)
	if err != nil {
		return err
	}
	body, err := json.Marshal(a2a.StreamResponse{Event: event})
	if err != nil {
		return fmt.Errorf("marshalling A2A callback: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSpace(delivery.CallbackURL), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating A2A callback request: %w", err)
	}
	if err := validateURL(request.URL); err != nil {
		return fmt.Errorf("validating A2A callback URL: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(notificationTokenHeader, strings.TrimSpace(delivery.NotificationToken))
	if traceID := strings.TrimSpace(delivery.TraceID); traceID != "" {
		request.Header.Set("X-Trace-ID", traceID)
	}
	client := s.httpClient
	if s.authRequired {
		if strings.TrimSpace(delivery.SenderAgentCode) == "" || strings.TrimSpace(delivery.SenderCredentialRef) == "" {
			return errors.New("sending A2A callback: sender machine identity is not configured")
		}
		signer, err := a2aauth.NewSigner(client.Transport, s.resolver, delivery.SenderAgentCode, delivery.SenderCredentialRef)
		if err != nil {
			return err
		}
		cloned := *client
		cloned.Transport = signer
		client = &cloned
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("sending A2A callback: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		limited, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("sending A2A callback: status=%d body=%s", response.StatusCode, strings.TrimSpace(string(limited)))
	}
	return nil
}

func callbackEvent(delivery services.DelegationCallbackDelivery) (a2a.Event, error) {
	state := a2a.TaskStateCompleted
	if delivery.State == services.DelegationCallbackStateFailed {
		state = a2a.TaskStateFailed
	}
	if delivery.State == services.DelegationCallbackStateCancelled {
		state = a2a.TaskStateCanceled
	}
	task := &a2a.Task{ID: a2a.TaskID(delivery.TaskID), ContextID: delivery.ThreadID, Status: a2a.TaskStatus{State: state}}
	if state == a2a.TaskStateCompleted {
		var payload any
		if err := json.Unmarshal([]byte(delivery.OutputJSON), &payload); err != nil {
			return nil, fmt.Errorf("decoding callback output: %w", err)
		}
		task.Artifacts = []*a2a.Artifact{{ID: a2a.ArtifactID("result"), Parts: a2a.ContentParts{a2a.NewDataPart(payload)}}}
	} else if strings.TrimSpace(delivery.ErrorMessage) != "" {
		task.Status.Message = &a2a.Message{ID: "callback-status", ContextID: delivery.ThreadID, TaskID: a2a.TaskID(delivery.TaskID), Role: a2a.MessageRoleAgent, Parts: a2a.ContentParts{a2a.NewTextPart(delivery.ErrorMessage)}}
	}
	return task, nil
}

var _ services.DelegationCallbackSender = (*CallbackSender)(nil)
