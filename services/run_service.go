package services

import (
	"GoAI/ai"
	"GoAI/db"
	"GoAI/domain/runstate"
	"GoAI/kafka"
	"GoAI/models"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

var (
	errRunNotFound           = errors.New("run not found")
	errRunForbidden          = errors.New("run does not belong to current user")
	errRunDispatchFailed     = errors.New("run execute event publish failed")
	errInvalidRunTransition  = errors.New("invalid run status transition")
	errInvalidStepTransition = errors.New("invalid step status transition")
	publishRunExecuteEvent   = func(ctx context.Context, runID string) error {
		return kafka.SendRunExecuteEvent(ctx, runID)
	}
	stepRetryBackoffs = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
)

func ErrRunNotFound() error {
	return errRunNotFound
}

func ErrRunForbidden() error {
	return errRunForbidden
}

// ErrRunDispatchFailed 返回 Run 入队失败的统一 sentinel error。
func ErrRunDispatchFailed() error {
	return errRunDispatchFailed
}

// SetPublishRunExecuteEventForTest 允许测试替换 Kafka 投递函数。
func SetPublishRunExecuteEventForTest(fn func(ctx context.Context, runID string) error) {
	if fn == nil {
		publishRunExecuteEvent = func(ctx context.Context, runID string) error {
			return kafka.SendRunExecuteEvent(ctx, runID)
		}
		return
	}
	publishRunExecuteEvent = fn
}

func SetStepRetryBackoffsForTest(backoffs []time.Duration) {
	if len(backoffs) == 0 {
		stepRetryBackoffs = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
		return
	}
	stepRetryBackoffs = backoffs
}

type CreateRunRequest struct {
	AgentCode       string          `json:"agent_code"`
	WorkflowVersion int             `json:"workflow_version"`
	ThreadID        string          `json:"thread_id"`
	TriggerType     string          `json:"trigger_type"`
	Input           json.RawMessage `json:"input"`
	Provider        string          `json:"provider"`
	Model           string          `json:"model"`
}

type CreateRunResponse struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

func CreateRun(ctx context.Context, userID uint64, req CreateRunRequest) (*models.Run, error) {
	agentCode := strings.TrimSpace(req.AgentCode)
	if agentCode == "" {
		return nil, errors.New("agent_code is required")
	}

	var agent models.Agent
	if err := db.DB.WithContext(ctx).
		Where("agent_code = ? AND status = ?", agentCode, models.AgentStatusActive).
		First(&agent).Error; err != nil {
		return nil, fmt.Errorf("find active agent by code: %w", err)
	}

	workflow, err := resolveWorkflow(ctx, agent.ID, req.WorkflowVersion)
	if err != nil {
		return nil, err
	}
	if _, err := ParseAndValidateWorkflowDefinition(workflow.DefinitionJSON); err != nil {
		return nil, err
	}

	inputJSON := strings.TrimSpace(string(req.Input))
	if inputJSON == "" {
		inputJSON = "{}"
	}
	triggerType := strings.TrimSpace(req.TriggerType)
	if triggerType == "" {
		triggerType = "api"
	}

	run := &models.Run{
		RunID:       newRunID(),
		ThreadID:    strings.TrimSpace(req.ThreadID),
		AgentID:     agent.ID,
		WorkflowID:  workflow.ID,
		UserID:      userID,
		TriggerType: triggerType,
		InputJSON:   inputJSON,
		Status:      models.RunStatusPending,
		Provider:    strings.TrimSpace(req.Provider),
		Model:       strings.TrimSpace(req.Model),
	}

	if err := db.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(run).Error; err != nil {
			return err
		}
		return transitionRunStatus(ctx, tx, run, models.RunStatusQueued, "")
	}); err != nil {
		return nil, err
	}

	if err := publishRunExecuteEvent(ctx, run.RunID); err != nil {
		_ = failRunWithMessage(ctx, run.RunID, fmt.Sprintf("dispatch run message failed: %v", err))
		return run, fmt.Errorf("%w: %v", errRunDispatchFailed, err)
	}

	return run, nil
}

func GetRunByRunID(ctx context.Context, userID uint64, isAdmin bool, runID string) (*models.Run, error) {
	run, err := fetchRunByRunID(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run.UserID != userID && !isAdmin {
		return nil, errRunForbidden
	}
	return run, nil
}

func GetRunStepsByRunID(ctx context.Context, userID uint64, isAdmin bool, runID string) ([]models.RunStep, error) {
	run, err := fetchRunByRunID(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run.UserID != userID && !isAdmin {
		return nil, errRunForbidden
	}

	var steps []models.RunStep
	if err := db.DB.WithContext(ctx).
		Where("run_id = ?", runID).
		Order("created_at ASC").
		Order("attempt ASC").
		Find(&steps).Error; err != nil {
		return nil, err
	}
	return steps, nil
}

func ReplayRun(ctx context.Context, userID uint64, isAdmin bool, runID string) (*models.Run, error) {
	origin, err := GetRunByRunID(ctx, userID, isAdmin, runID)
	if err != nil {
		return nil, err
	}
	req := CreateRunRequest{
		AgentCode:       "",
		WorkflowVersion: 0,
		ThreadID:        origin.ThreadID,
		TriggerType:     "replay",
		Input:           json.RawMessage(origin.InputJSON),
		Provider:        origin.Provider,
		Model:           origin.Model,
	}

	// 回放走 agent_id + workflow_id 直建，避免 workflow 版本漂移影响重放。
	clone := &models.Run{
		RunID:       newRunID(),
		ThreadID:    req.ThreadID,
		AgentID:     origin.AgentID,
		WorkflowID:  origin.WorkflowID,
		UserID:      userID,
		TriggerType: req.TriggerType,
		InputJSON:   strings.TrimSpace(string(req.Input)),
		Status:      models.RunStatusPending,
		Provider:    req.Provider,
		Model:       req.Model,
	}

	if clone.InputJSON == "" {
		clone.InputJSON = "{}"
	}

	if err := db.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(clone).Error; err != nil {
			return err
		}
		return transitionRunStatus(ctx, tx, clone, models.RunStatusQueued, "")
	}); err != nil {
		return nil, err
	}
	if err := publishRunExecuteEvent(ctx, clone.RunID); err != nil {
		_ = failRunWithMessage(ctx, clone.RunID, fmt.Sprintf("dispatch replay message failed: %v", err))
		return clone, fmt.Errorf("%w: %v", errRunDispatchFailed, err)
	}
	return clone, nil
}

func HandleRunExecuteMessage(ctx context.Context, msg kafka.RunExecuteMessage) error {
	run, err := fetchRunByRunID(ctx, msg.RunID)
	if err != nil {
		return err
	}
	if run.Status != models.RunStatusQueued {
		return nil
	}

	var workflow models.Workflow
	if err := db.DB.WithContext(ctx).First(&workflow, "id = ?", run.WorkflowID).Error; err != nil {
		return err
	}

	def, err := ParseAndValidateWorkflowDefinition(workflow.DefinitionJSON)
	if err != nil {
		_ = failRunWithMessage(ctx, run.RunID, err.Error())
		return err
	}

	order, err := ResolveExecutionOrder(def)
	if err != nil {
		_ = failRunWithMessage(ctx, run.RunID, err.Error())
		return err
	}

	if err := db.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := transitionRunStatus(ctx, tx, run, models.RunStatusRunning, ""); err != nil {
			return err
		}
		for _, node := range order {
			if err := tx.Model(&models.Run{}).
				Where("run_id = ?", run.RunID).
				Update("current_step", node.Key).Error; err != nil {
				return err
			}
			if execErr := executeNodeWithRetry(ctx, tx, run, node); execErr != nil {
				return transitionRunStatus(ctx, tx, run, models.RunStatusFailed, execErr.Error())
			}
		}
		return transitionRunStatus(ctx, tx, run, models.RunStatusSuccess, "")
	}); err != nil {
		return err
	}
	return nil
}

func executeNodeWithRetry(ctx context.Context, tx *gorm.DB, run *models.Run, node WorkflowNode) error {
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		startedAt := time.Now()
		step := &models.RunStep{
			RunID:     run.RunID,
			StepKey:   strings.TrimSpace(node.Key),
			StepType:  strings.TrimSpace(node.Type),
			Attempt:   attempt,
			Status:    models.RunStepStatusRunning,
			InputJSON: run.InputJSON,
			Provider:  run.Provider,
			Model:     run.Model,
			StartedAt: &startedAt,
		}
		if err := tx.Create(step).Error; err != nil {
			return err
		}

		outputJSON, err := executeWorkflowNode(ctx, run, node, attempt)
		finishedAt := time.Now()
		latencyMS := finishedAt.Sub(startedAt).Milliseconds()
		if err != nil {
			lastErr = err
			if updateErr := transitionStepStatus(ctx, tx, step, models.RunStepStatusFailed, "", err.Error(), latencyMS, &finishedAt); updateErr != nil {
				return updateErr
			}
			if err := tx.Model(&models.Run{}).
				Where("run_id = ?", run.RunID).
				Update("retry_count", gorm.Expr("retry_count + ?", 1)).Error; err != nil {
				return err
			}
			if attempt < len(stepRetryBackoffs)+1 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(stepRetryBackoffs[attempt-1]):
				}
			}
			continue
		}
		if updateErr := transitionStepStatus(ctx, tx, step, models.RunStepStatusSuccess, outputJSON, "", latencyMS, &finishedAt); updateErr != nil {
			return updateErr
		}
		return nil
	}
	if lastErr == nil {
		return errors.New("node execution failed after retries")
	}
	return lastErr
}

func executeWorkflowNode(ctx context.Context, run *models.Run, node WorkflowNode, attempt int) (string, error) {
	var conf map[string]any
	if len(node.Config) > 0 {
		if err := json.Unmarshal(node.Config, &conf); err != nil {
			return "", fmt.Errorf("invalid node config for %s: %w", node.Key, err)
		}
	}
	if forceFail, ok := conf["force_fail"].(bool); ok && forceFail {
		return "", fmt.Errorf("node %s forced failure", node.Key)
	}
	if failAttempts, ok := conf["fail_attempts"].(float64); ok && attempt <= int(failAttempts) {
		return "", fmt.Errorf("node %s failed on attempt %d", node.Key, attempt)
	}

	nodeType := strings.ToLower(strings.TrimSpace(node.Type))
	switch nodeType {
	case "llm":
		return executeLLMNode(ctx, run)
	case "tool", "noop", "planner":
		resp := map[string]any{
			"step_key": node.Key,
			"type":     nodeType,
			"result":   "ok",
		}
		bs, _ := json.Marshal(resp)
		return string(bs), nil
	default:
		resp := map[string]any{
			"step_key": node.Key,
			"type":     nodeType,
			"result":   "skipped_unknown_type",
		}
		bs, _ := json.Marshal(resp)
		return string(bs), nil
	}
}

func executeLLMNode(ctx context.Context, run *models.Run) (string, error) {
	var input struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
		Prompt string `json:"prompt"`
	}
	if err := json.Unmarshal([]byte(run.InputJSON), &input); err != nil {
		return "", fmt.Errorf("invalid run input json: %w", err)
	}
	messages := make([]ai.Message, 0, len(input.Messages))
	for _, m := range input.Messages {
		if strings.TrimSpace(m.Role) == "" || strings.TrimSpace(m.Content) == "" {
			continue
		}
		messages = append(messages, ai.Message{Role: m.Role, Content: m.Content})
	}
	if len(messages) == 0 && strings.TrimSpace(input.Prompt) != "" {
		messages = append(messages, ai.Message{Role: "user", Content: input.Prompt})
	}
	if len(messages) == 0 {
		return `{"message":"llm node executed without input messages"}`, nil
	}

	stream, err := Chat(ctx, messages, run.Provider, run.Model)
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	chunks := stream.Chunks
	errs := stream.Errs
	for chunks != nil || errs != nil {
		select {
		case content, ok := <-chunks:
			if !ok {
				chunks = nil
				continue
			}
			builder.WriteString(content)
		case streamErr, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if streamErr != nil {
				return "", streamErr
			}
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	result := map[string]any{
		"message": builder.String(),
	}
	bs, _ := json.Marshal(result)
	return string(bs), nil
}

func transitionRunStatus(ctx context.Context, tx *gorm.DB, run *models.Run, nextStatus string, errMsg string) error {
	if !isValidRunTransition(run.Status, nextStatus) {
		return fmt.Errorf("%w: %s -> %s", errInvalidRunTransition, run.Status, nextStatus)
	}
	updates := map[string]any{
		"status":        nextStatus,
		"error_message": errMsg,
	}
	now := time.Now()
	if nextStatus == models.RunStatusRunning && run.StartedAt == nil {
		updates["started_at"] = now
		run.StartedAt = &now
	}
	if nextStatus == models.RunStatusSuccess || nextStatus == models.RunStatusFailed || nextStatus == models.RunStatusCancelled {
		updates["finished_at"] = now
		run.FinishedAt = &now
	}
	if err := tx.WithContext(ctx).
		Model(&models.Run{}).
		Where("run_id = ?", run.RunID).
		Updates(updates).Error; err != nil {
		return err
	}
	run.Status = nextStatus
	run.ErrorMessage = errMsg
	return nil
}

func transitionStepStatus(ctx context.Context, tx *gorm.DB, step *models.RunStep, nextStatus string, outputJSON, errMsg string, latencyMS int64, finishedAt *time.Time) error {
	if !isValidRunStepTransition(step.Status, nextStatus) {
		return fmt.Errorf("%w: %s -> %s", errInvalidStepTransition, step.Status, nextStatus)
	}
	updates := map[string]any{
		"status":        nextStatus,
		"output_json":   outputJSON,
		"error_message": errMsg,
		"latency_ms":    latencyMS,
		"finished_at":   finishedAt,
	}
	if err := tx.WithContext(ctx).
		Model(&models.RunStep{}).
		Where("id = ?", step.ID).
		Updates(updates).Error; err != nil {
		return err
	}
	step.Status = nextStatus
	return nil
}

func isValidRunTransition(from, to string) bool {
	return runstate.IsValidRunTransition(from, to)
}

func isValidRunStepTransition(from, to string) bool {
	return runstate.IsValidRunStepTransition(from, to)
}

func resolveWorkflow(ctx context.Context, agentID uint64, version int) (*models.Workflow, error) {
	var workflow models.Workflow
	query := db.DB.WithContext(ctx).Where("agent_id = ?", agentID)
	if version > 0 {
		err := query.Where("version = ?", version).First(&workflow).Error
		if err != nil {
			return nil, fmt.Errorf("workflow version not found: %w", err)
		}
		return &workflow, nil
	}
	err := query.Where("is_active = ?", true).Order("version DESC").First(&workflow).Error
	if err != nil {
		return nil, fmt.Errorf("active workflow not found: %w", err)
	}
	return &workflow, nil
}

func fetchRunByRunID(ctx context.Context, runID string) (*models.Run, error) {
	var run models.Run
	if err := db.DB.WithContext(ctx).Where("run_id = ?", strings.TrimSpace(runID)).First(&run).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errRunNotFound
		}
		return nil, err
	}
	return &run, nil
}

func failRunWithMessage(ctx context.Context, runID, msg string) error {
	return db.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var run models.Run
		if err := tx.Where("run_id = ?", runID).First(&run).Error; err != nil {
			return err
		}
		if run.Status == models.RunStatusSuccess || run.Status == models.RunStatusFailed || run.Status == models.RunStatusCancelled {
			return nil
		}
		if run.Status == models.RunStatusQueued || run.Status == models.RunStatusRunning {
			return transitionRunStatus(ctx, tx, &run, models.RunStatusFailed, msg)
		}
		return fmt.Errorf("%w: %s -> %s", errInvalidRunTransition, run.Status, models.RunStatusFailed)
	})
}

func newRunID() string {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("run_%d", time.Now().UnixNano())
	}
	return "run_" + hex.EncodeToString(buf)
}
