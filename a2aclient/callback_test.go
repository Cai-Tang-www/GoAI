package a2aclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"GoAI/a2aauth"
	"GoAI/services"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

func TestCallbackSenderSendsSignedCompletedTask(t *testing.T) {
	const secret = "callback-sender-test-secret-at-least-32-bytes"
	resolver, err := a2aauth.NewStaticCredentialResolver(map[string]string{"writer-key": secret})
	if err != nil {
		t.Fatalf("create resolver failed: %v", err)
	}
	verifier, err := a2aauth.NewVerifier(resolver, a2aauth.NewMemoryNonceStore(), time.Minute)
	if err != nil {
		t.Fatalf("create verifier failed: %v", err)
	}
	var received a2a.StreamResponse
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agentCode, verifyErr := verifier.Verify(r, "writer-key")
		if verifyErr != nil || agentCode != "writer" {
			t.Errorf("verify callback signature: agent=%q err=%v", agentCode, verifyErr)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if got := r.Header.Get(notificationTokenHeader); got != "notification-token" {
			t.Errorf("notification token=%q", got)
		}
		if got := r.Header.Get("X-Trace-ID"); got != "trace-callback" {
			t.Errorf("trace header=%q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode callback body: %v", err)
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	sender, err := NewCallbackSender(server.Client(), resolver, true)
	if err != nil {
		t.Fatalf("create callback sender failed: %v", err)
	}
	delivery := services.DelegationCallbackDelivery{
		CallbackURL: server.URL + "/a2a/agents/planner/callbacks/tasks/task-1", NotificationToken: "notification-token",
		SenderAgentCode: "writer", SenderCredentialRef: "writer-key", TaskID: "task-1", ThreadID: "thread-1",
		State: services.DelegationCallbackStateSucceeded, OutputJSON: `{"answer":"done"}`, TraceID: "trace-callback",
	}
	if err := sender.SendDelegationCallback(context.Background(), delivery); err != nil {
		t.Fatalf("send callback failed: %v", err)
	}
	task, ok := received.Event.(*a2a.Task)
	if !ok || task.Status.State != a2a.TaskStateCompleted || len(task.Artifacts) != 1 {
		t.Fatalf("unexpected callback event: %#v", received.Event)
	}
}

func TestCallbackEventMapsFailedAndCancelledStatus(t *testing.T) {
	for _, test := range []struct {
		state string
		want  a2a.TaskState
	}{
		{state: services.DelegationCallbackStateFailed, want: a2a.TaskStateFailed},
		{state: services.DelegationCallbackStateCancelled, want: a2a.TaskStateCanceled},
	} {
		event, err := callbackEvent(services.DelegationCallbackDelivery{TaskID: "task-1", ThreadID: "thread-1", State: test.state, ErrorMessage: "terminal error"})
		if err != nil {
			t.Fatalf("create callback event failed: %v", err)
		}
		task, ok := event.(*a2a.Task)
		if !ok || task.Status.State != test.want || task.Status.Message == nil {
			t.Fatalf("unexpected terminal callback event: %#v", event)
		}
	}
}

func TestCallbackSenderRejectsRedirectAndNonSuccess(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected.Add(1)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	sender, err := NewCallbackSender(redirect.Client(), nil, false)
	if err != nil {
		t.Fatalf("create callback sender failed: %v", err)
	}
	delivery := services.DelegationCallbackDelivery{CallbackURL: redirect.URL, TaskID: "task-1", State: services.DelegationCallbackStateSucceeded, OutputJSON: `{}`}
	if err := sender.SendDelegationCallback(context.Background(), delivery); err == nil || !strings.Contains(err.Error(), "status=307") {
		t.Fatalf("redirect error=%v", err)
	}
	if redirected.Load() != 0 {
		t.Fatalf("callback sender followed redirect %d times", redirected.Load())
	}

	failure := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "callback rejected", http.StatusConflict)
	}))
	defer failure.Close()
	sender, err = NewCallbackSender(failure.Client(), nil, false)
	if err != nil {
		t.Fatalf("create failure sender failed: %v", err)
	}
	delivery.CallbackURL = failure.URL
	if err := sender.SendDelegationCallback(context.Background(), delivery); err == nil || !strings.Contains(err.Error(), "status=409") || !strings.Contains(err.Error(), "callback rejected") {
		t.Fatalf("non-success error=%v", err)
	}
}

func TestCallbackSenderRejectsUnsafeRemoteHTTPURL(t *testing.T) {
	var calls atomic.Int32
	sender, err := NewCallbackSender(&http.Client{Transport: countingRoundTripper{base: http.DefaultTransport, calls: &calls}}, nil, false)
	if err != nil {
		t.Fatalf("create callback sender failed: %v", err)
	}
	err = sender.SendDelegationCallback(context.Background(), services.DelegationCallbackDelivery{
		CallbackURL: "http://agents.example.com/callback", TaskID: "task-1",
		State: services.DelegationCallbackStateSucceeded, OutputJSON: "{}",
	})
	if err == nil || !strings.Contains(err.Error(), "remote A2A endpoint must use HTTPS") {
		t.Fatalf("unsafe callback URL error=%v", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("unsafe callback reached HTTP transport %d times", calls.Load())
	}
}

func TestCallbackSenderHonorsContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	sender, err := NewCallbackSender(server.Client(), nil, false)
	if err != nil {
		t.Fatalf("create callback sender failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = sender.SendDelegationCallback(ctx, services.DelegationCallbackDelivery{
		CallbackURL: server.URL, TaskID: "task-1", State: services.DelegationCallbackStateSucceeded, OutputJSON: `{}`,
	})
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("context cancellation error=%v", err)
	}
}
