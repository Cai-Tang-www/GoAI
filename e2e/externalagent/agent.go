// Package externalagent 提供不依赖 GoAI 内部服务的 A2A 互操作测试 Agent。
package externalagent

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

const (
	authorizationScheme = "GoAI-HMAC-SHA256"
	headerAgentCode     = "X-GoAI-Agent-Code"
	headerTimestamp     = "X-GoAI-Timestamp"
	headerNonce         = "X-GoAI-Nonce"
	headerContentSHA256 = "X-GoAI-Content-SHA256"
	notificationToken   = "A2A-Notification-Token"
)

// Agent 是一个独立的标准 A2A HTTP+JSON 测试 Agent，不依赖 GoAI 内部领域包。
type Agent struct {
	code          string
	name          string
	capability    string
	signingSecret []byte
	trusted       map[string][]byte

	mu        sync.Mutex
	tasks     map[string]*taskRecord
	messages  []*a2a.Message
	callbacks []Callback
	nonces    map[string]time.Time
	nextID    atomic.Uint64
}

type taskRecord struct {
	task          *a2a.Task
	pushURL       string
	pushToken     string
	callbackState *Callback
}

// Callback 记录外部 Agent 收到的终态 Push Notification。
type Callback struct {
	TaskID string
	State  a2a.TaskState
	Output any
	Error  string
	Token  string
}

// New 创建一个独立的 A2A 测试 Agent。
func New(code, capability string, signingSecret []byte, trustedSecrets map[string][]byte) (*Agent, error) {
	code = strings.TrimSpace(code)
	capability = strings.TrimSpace(capability)
	if code == "" || capability == "" {
		return nil, errors.New("external agent code and capability are required")
	}
	if len(signingSecret) < 32 {
		return nil, errors.New("external agent signing secret must contain at least 32 bytes")
	}
	trusted := make(map[string][]byte, len(trustedSecrets))
	for source, secret := range trustedSecrets {
		if strings.TrimSpace(source) == "" || len(secret) < 32 {
			return nil, errors.New("external agent trusted secrets must contain valid identities")
		}
		trusted[strings.TrimSpace(source)] = append([]byte(nil), secret...)
	}
	return &Agent{
		code:          code,
		name:          code,
		capability:    capability,
		signingSecret: append([]byte(nil), signingSecret...),
		trusted:       trusted,
		tasks:         make(map[string]*taskRecord),
		nonces:        make(map[string]time.Time),
	}, nil
}

// ServeHTTP 实现标准 Agent Card、Task API、CancelTask 和 Push callback。
func (a *Agent) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if a == nil {
		http.Error(w, "external agent is nil", http.StatusInternalServerError)
		return
	}
	prefix := "/a2a/agents/" + a.code
	if !strings.HasPrefix(r.URL.Path, prefix) {
		http.NotFound(w, r)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, prefix)
	switch {
	case r.Method == http.MethodGet && path == "/.well-known/agent-card.json":
		a.serveCard(w, r)
	case r.Method == http.MethodPost && path == "/message:send":
		a.sendMessage(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/tasks/"):
		a.getTask(w, r, strings.TrimPrefix(path, "/tasks/"))
	case r.Method == http.MethodPost && strings.HasPrefix(path, "/tasks/") && strings.HasSuffix(path, ":cancel"):
		taskID := strings.TrimSuffix(strings.TrimPrefix(path, "/tasks/"), ":cancel")
		a.cancelTask(w, r, taskID)
	case r.Method == http.MethodPost && strings.HasPrefix(path, "/callbacks/tasks/"):
		a.receiveCallback(w, r, strings.TrimPrefix(path, "/callbacks/tasks/"))
	default:
		http.NotFound(w, r)
	}
}

func (a *Agent) serveCard(w http.ResponseWriter, r *http.Request) {
	baseURL := "http://" + r.Host + "/a2a/agents/" + a.code
	if r.TLS != nil {
		baseURL = "https://" + r.Host + "/a2a/agents/" + a.code
	}
	const schemeName a2a.SecuritySchemeName = "goaiHMACSHA256"
	card := &a2a.AgentCard{
		Name:                a.name,
		Description:         "Independent A2A conformance agent",
		Version:             "1.0",
		SupportedInterfaces: []*a2a.AgentInterface{a2a.NewAgentInterface(baseURL, a2a.TransportProtocolHTTPJSON)},
		Capabilities: a2a.AgentCapabilities{
			PushNotifications: true,
		},
		SecuritySchemes: a2a.NamedSecuritySchemes{
			schemeName: a2a.HTTPAuthSecurityScheme{Scheme: authorizationScheme},
		},
		SecurityRequirements: a2a.SecurityRequirementsOptions{
			a2a.SecurityRequirements{schemeName: a2a.SecuritySchemeScopes{}},
		},
		DefaultInputModes:  []string{"application/json", "text/plain"},
		DefaultOutputModes: []string{"application/json"},
		Skills:             []a2a.AgentSkill{{ID: a.capability, Name: a.capability}},
	}
	writeJSON(w, http.StatusOK, card)
}

func (a *Agent) sendMessage(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil || a.verify(r, body) != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "A2A authentication failed"})
		return
	}
	var request a2a.SendMessageRequest
	if err := json.Unmarshal(body, &request); err != nil || request.Message == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "message is required"})
		return
	}
	taskID := strings.TrimSpace(string(request.Message.TaskID))
	if taskID == "" {
		taskID = fmt.Sprintf("external-task-%d", a.nextID.Add(1))
	}
	contextID := strings.TrimSpace(request.Message.ContextID)
	if contextID == "" {
		contextID = "external-context-" + taskID
	}
	pushURL, pushToken := "", ""
	if request.Config != nil && request.Config.PushConfig != nil {
		pushURL = strings.TrimSpace(request.Config.PushConfig.URL)
		pushToken = strings.TrimSpace(request.Config.PushConfig.Token)
	}
	task := &a2a.Task{ID: a2a.TaskID(taskID), ContextID: contextID, Status: a2a.TaskStatus{State: a2a.TaskStateWorking}}
	a.mu.Lock()
	a.tasks[taskID] = &taskRecord{task: task, pushURL: pushURL, pushToken: pushToken}
	a.messages = append(a.messages, request.Message)
	a.mu.Unlock()
	writeJSON(w, http.StatusOK, &a2a.StreamResponse{Event: task})
}

func (a *Agent) getTask(w http.ResponseWriter, r *http.Request, taskID string) {
	body, err := readBody(r)
	if err != nil || a.verify(r, body) != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "A2A authentication failed"})
		return
	}
	task, ok := a.copyTask(strings.TrimSpace(taskID))
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (a *Agent) cancelTask(w http.ResponseWriter, r *http.Request, taskID string) {
	body, err := readBody(r)
	if err != nil || a.verify(r, body) != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "A2A authentication failed"})
		return
	}
	task, ok := a.updateTask(strings.TrimSpace(taskID), a2a.TaskStateCanceled, nil, "cancelled by caller")
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (a *Agent) receiveCallback(w http.ResponseWriter, r *http.Request, taskID string) {
	body, err := readBody(r)
	if err != nil || a.verify(r, body) != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "A2A authentication failed"})
		return
	}
	if got := strings.TrimSpace(r.Header.Get(notificationToken)); got == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "notification token is required"})
		return
	}
	var response a2a.StreamResponse
	if err := json.Unmarshal(body, &response); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid callback"})
		return
	}
	task, ok := response.Event.(*a2a.Task)
	if !ok || string(task.ID) != strings.TrimSpace(taskID) || !task.Status.State.Terminal() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid terminal callback"})
		return
	}
	output, callbackErr := taskOutput(task)
	callback := Callback{TaskID: string(task.ID), State: task.Status.State, Output: output, Error: callbackStatus(task), Token: strings.TrimSpace(r.Header.Get(notificationToken))}
	a.mu.Lock()
	a.callbacks = append(a.callbacks, callback)
	if record := a.tasks[string(task.ID)]; record != nil {
		record.callbackState = &callback
		record.task = task
	}
	a.mu.Unlock()
	if callbackErr != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": callbackErr.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

// Complete 将一个外部任务收敛为终态，并按 PushConfig 回送 A2A callback。
func (a *Agent) Complete(ctx context.Context, taskID string, state a2a.TaskState, output any, message string) error {
	if !state.Terminal() || state == a2a.TaskStateCanceled && output != nil {
		return errors.New("complete requires a terminal task state")
	}
	task, pushURL, pushToken, ok := a.updateTaskWithPush(taskID, state, output, message)
	if !ok {
		return errors.New("task not found")
	}
	if pushURL == "" {
		return nil
	}
	body, err := json.Marshal(&a2a.StreamResponse{Event: task})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, pushURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(notificationToken, pushToken)
	if err := a.sign(request, body); err != nil {
		return err
	}
	response, err := (&http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		limited, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("callback status=%d body=%s", response.StatusCode, strings.TrimSpace(string(limited)))
	}
	return nil
}

// SendMessage 从独立 Agent 向 GoAI 或其他 A2A Agent 发起标准消息。
func (a *Agent) SendMessage(ctx context.Context, client *http.Client, endpoint, taskID, contextID, messageID, callbackURL, callbackToken string, input any) (*a2a.Task, error) {
	if client == nil {
		client = http.DefaultClient
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		taskID = fmt.Sprintf("external-task-%d", a.nextID.Add(1))
	}
	message := &a2a.Message{ID: messageID, TaskID: a2a.TaskID(taskID), ContextID: contextID, Role: a2a.MessageRoleUser, Parts: a2a.ContentParts{a2a.NewDataPart(input)}}
	requestPayload := &a2a.SendMessageRequest{Message: message, Config: &a2a.SendMessageConfig{ReturnImmediately: true}}
	if strings.TrimSpace(callbackURL) != "" {
		requestPayload.Config.PushConfig = &a2a.PushConfig{TaskID: a2a.TaskID(taskID), ID: "external-push-" + taskID, URL: callbackURL, Token: callbackToken}
	}
	body, err := json.Marshal(requestPayload)
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(endpoint), "/") + "/message:send")
	if err != nil {
		return nil, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, parsed.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if err := a.sign(httpRequest, body); err != nil {
		return nil, err
	}
	response, err := client.Do(httpRequest)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		limited, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("message:send status=%d body=%s", response.StatusCode, strings.TrimSpace(string(limited)))
	}
	var stream a2a.StreamResponse
	if err := json.NewDecoder(response.Body).Decode(&stream); err != nil {
		return nil, err
	}
	task, ok := stream.Event.(*a2a.Task)
	if !ok {
		return nil, fmt.Errorf("message:send returned %T, want Task", stream.Event)
	}
	a.mu.Lock()
	a.tasks[taskID] = &taskRecord{task: task, pushURL: callbackURL, pushToken: callbackToken}
	a.mu.Unlock()
	return task, nil
}

// GetRemoteTask 查询另一个 A2A Agent 的 Task。
func (a *Agent) GetRemoteTask(ctx context.Context, client *http.Client, endpoint, taskID string) (*a2a.Task, error) {
	return a.doRemoteTaskRequest(ctx, client, http.MethodGet, strings.TrimRight(strings.TrimSpace(endpoint), "/")+"/tasks/"+url.PathEscape(strings.TrimSpace(taskID)), nil)
}

// CancelRemoteTask 调用另一个 A2A Agent 的 CancelTask 方法。
func (a *Agent) CancelRemoteTask(ctx context.Context, client *http.Client, endpoint, taskID string) (*a2a.Task, error) {
	return a.doRemoteTaskRequest(ctx, client, http.MethodPost, strings.TrimRight(strings.TrimSpace(endpoint), "/")+"/tasks/"+url.PathEscape(strings.TrimSpace(taskID))+":cancel", nil)
}

// Task 返回一个任务的当前快照。
func (a *Agent) Task(taskID string) (*a2a.Task, bool) { return a.copyTask(strings.TrimSpace(taskID)) }

// Callbacks 返回已收到的终态 callback 快照。
func (a *Agent) Callbacks() []Callback {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]Callback(nil), a.callbacks...)
}

// LastMessage 返回最近收到的 A2A Message 快照。
func (a *Agent) LastMessage() *a2a.Message {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.messages) == 0 {
		return nil
	}
	message := *a.messages[len(a.messages)-1]
	return &message
}

func (a *Agent) copyTask(taskID string) (*a2a.Task, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	record, ok := a.tasks[taskID]
	if !ok || record.task == nil {
		return nil, false
	}
	copy := *record.task
	return &copy, true
}

func (a *Agent) updateTask(taskID string, state a2a.TaskState, output any, message string) (*a2a.Task, bool) {
	task, _, _, ok := a.updateTaskWithPush(taskID, state, output, message)
	return task, ok
}

func (a *Agent) updateTaskWithPush(taskID string, state a2a.TaskState, output any, message string) (*a2a.Task, string, string, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	record, ok := a.tasks[strings.TrimSpace(taskID)]
	if !ok || record.task == nil {
		return nil, "", "", false
	}
	record.task.Status.State = state
	if strings.TrimSpace(message) != "" {
		record.task.Status.Message = a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart(message))
	}
	if state == a2a.TaskStateCompleted {
		record.task.Artifacts = []*a2a.Artifact{{ID: "external-result", Parts: a2a.ContentParts{a2a.NewDataPart(output)}}}
	}
	return record.task, record.pushURL, record.pushToken, true
}

func (a *Agent) sign(request *http.Request, body []byte) error {
	if request == nil {
		return errors.New("request is nil")
	}
	timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	nonce := fmt.Sprintf("external-nonce-%d", a.nextID.Add(1))
	digest := sha256.Sum256(body)
	digestHex := hex.EncodeToString(digest[:])
	canonical := strings.Join([]string{request.Method, request.URL.RequestURI(), timestamp, nonce, a.code, digestHex}, "\n")
	mac := hmac.New(sha256.New, a.signingSecret)
	_, _ = mac.Write([]byte(canonical))
	request.Header.Set("Authorization", authorizationScheme+" "+hex.EncodeToString(mac.Sum(nil)))
	request.Header.Set(headerAgentCode, a.code)
	request.Header.Set(headerTimestamp, timestamp)
	request.Header.Set(headerNonce, nonce)
	request.Header.Set(headerContentSHA256, digestHex)
	return nil
}

func (a *Agent) doRemoteTaskRequest(ctx context.Context, client *http.Client, method, rawURL string, body []byte) (*a2a.Task, error) {
	if client == nil {
		client = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, method, rawURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	if err := a.sign(request, body); err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		limited, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return nil, fmt.Errorf("task request status=%d body=%s", response.StatusCode, strings.TrimSpace(string(limited)))
	}
	var task a2a.Task
	if err := json.NewDecoder(response.Body).Decode(&task); err != nil {
		return nil, err
	}
	return &task, nil
}

func (a *Agent) verify(request *http.Request, body []byte) error {
	if request == nil {
		return errors.New("request is nil")
	}
	source := strings.TrimSpace(request.Header.Get(headerAgentCode))
	secret, ok := a.trusted[source]
	if !ok {
		return errors.New("unknown source")
	}
	timestamp := strings.TrimSpace(request.Header.Get(headerTimestamp))
	nonce := strings.TrimSpace(request.Header.Get(headerNonce))
	digestHex := strings.ToLower(strings.TrimSpace(request.Header.Get(headerContentSHA256)))
	authorization := strings.TrimSpace(request.Header.Get("Authorization"))
	if timestamp == "" || nonce == "" || digestHex == "" || authorization == "" {
		return errors.New("missing authentication")
	}
	parsedTimestamp, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || time.Since(time.Unix(parsedTimestamp, 0)) > 5*time.Minute || time.Until(time.Unix(parsedTimestamp, 0)) > 5*time.Minute {
		return errors.New("expired authentication")
	}
	digest := sha256.Sum256(body)
	actualDigest := hex.EncodeToString(digest[:])
	if actualDigest != digestHex {
		return errors.New("body digest mismatch")
	}
	provided, ok := strings.CutPrefix(authorization, authorizationScheme+" ")
	if !ok {
		return errors.New("invalid authorization")
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(strings.Join([]string{request.Method, request.URL.RequestURI(), timestamp, nonce, source, actualDigest}, "\n")))
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(provided), []byte(expected)) {
		return errors.New("invalid signature")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	nonceKey := source + "\x00" + nonce
	if expiry, exists := a.nonces[nonceKey]; exists && expiry.After(time.Now()) {
		return errors.New("nonce replay")
	}
	a.nonces[nonceKey] = time.Now().Add(5 * time.Minute)
	return nil
}

func readBody(request *http.Request) ([]byte, error) {
	if request == nil || request.Body == nil {
		return nil, nil
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}

func taskOutput(task *a2a.Task) (any, error) {
	if task == nil || len(task.Artifacts) == 0 || len(task.Artifacts[0].Parts) == 0 {
		return nil, nil
	}
	part := task.Artifacts[0].Parts[0]
	if data := part.Data(); data != nil {
		return data, nil
	}
	if text := part.Text(); text != "" {
		return text, nil
	}
	return nil, errors.New("callback artifact has no supported content")
}

func callbackStatus(task *a2a.Task) string {
	if task == nil || task.Status.Message == nil {
		return ""
	}
	var values []string
	for _, part := range task.Status.Message.Parts {
		if part == nil {
			continue
		}
		if text := part.Text(); text != "" {
			values = append(values, text)
		}
	}
	return strings.Join(values, " ")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
