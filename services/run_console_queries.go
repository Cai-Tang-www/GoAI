package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"GoAI/models"

	"gorm.io/gorm"
)

// 控制台只读列表查询的统一上限，V1 不做分页，用固定窗口保证响应可控。
const (
	consoleThreadListLimit  = 200
	consoleRunListDefault   = 50
	consoleRunListMaxLimit  = 200
	consoleMessageListLimit = 500
)

// ThreadSummaryView 是控制台会话列表的只读视图，附带最近一次 Run 的归属信息。
type ThreadSummaryView struct {
	ThreadID      string    `json:"thread_id"`
	Title         string    `json:"title"`
	Status        string    `json:"status"`
	AgentCode     string    `json:"agent_code"`
	AgentName     string    `json:"agent_name"`
	LastRunID     string    `json:"last_run_id"`
	LastRunStatus string    `json:"last_run_status"`
	RunCount      int64     `json:"run_count"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// ThreadMessageView 是 Thread 消息历史的只读视图，Role/Content 与 Thread 回放语义保持一致。
type ThreadMessageView struct {
	MessageID   string          `json:"message_id"`
	RunID       string          `json:"run_id"`
	SenderType  string          `json:"sender_type"`
	SenderID    string          `json:"sender_id"`
	MessageType string          `json:"message_type"`
	Role        string          `json:"role"`
	Content     string          `json:"content"`
	ContentJSON json.RawMessage `json:"content_json"`
	Status      string          `json:"status"`
	CreatedAt   time.Time       `json:"created_at"`
}

// RunListItemView 是控制台 Run 列表的只读视图，附带 Agent 归属信息。
type RunListItemView struct {
	RunID        string     `json:"run_id"`
	ThreadID     string     `json:"thread_id"`
	AgentCode    string     `json:"agent_code"`
	AgentName    string     `json:"agent_name"`
	TriggerType  string     `json:"trigger_type"`
	Status       string     `json:"status"`
	CurrentStep  string     `json:"current_step"`
	ErrorMessage string     `json:"error_message"`
	StartedAt    *time.Time `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at"`
	CreatedAt    time.Time  `json:"created_at"`
}

// RunWorkflowView 是 Run 执行图查询的只读视图：Run 实际执行的那份 Workflow 定义。
type RunWorkflowView struct {
	RunID      string          `json:"run_id"`
	AgentCode  string          `json:"agent_code"`
	AgentName  string          `json:"agent_name"`
	WorkflowID uint64          `json:"workflow_id"`
	Version    int             `json:"version"`
	Definition json.RawMessage `json:"definition"`
	Checksum   string          `json:"checksum"`
	IsActive   bool            `json:"is_active"`
}

// RunListFilter 描述控制台 Run 列表的可选过滤条件。
type RunListFilter struct {
	ThreadID  string
	AgentCode string
	Status    string
	Limit     int
}

// ListThreads 返回当前用户可见的会话列表；admin 可跨 owner 查看。
func (s *RunService) ListThreads(ctx context.Context, userID uint64, isAdmin bool) ([]ThreadSummaryView, error) {
	query := s.database.WithContext(ctx).Order("updated_at DESC").Order("id DESC").Limit(consoleThreadListLimit)
	if !isAdmin {
		query = query.Where("owner_user_id = ?", userID)
	}
	var threads []models.Thread
	if err := query.Find(&threads).Error; err != nil {
		return nil, fmt.Errorf("listing threads: %w", err)
	}
	views := make([]ThreadSummaryView, 0, len(threads))
	if len(threads) == 0 {
		return views, nil
	}

	threadIDs := make([]string, 0, len(threads))
	for _, thread := range threads {
		threadIDs = append(threadIDs, thread.ThreadID)
	}

	type runAggregate struct {
		ThreadID string
		MaxID    uint64
		RunCount int64
	}
	var aggregates []runAggregate
	if err := s.database.WithContext(ctx).Model(&models.Run{}).
		Select("thread_id AS thread_id, MAX(id) AS max_id, COUNT(*) AS run_count").
		Where("thread_id IN ?", threadIDs).
		Group("thread_id").
		Scan(&aggregates).Error; err != nil {
		return nil, fmt.Errorf("aggregating thread runs: %w", err)
	}
	latestRunIDs := make([]uint64, 0, len(aggregates))
	aggregateByThread := make(map[string]runAggregate, len(aggregates))
	for _, aggregate := range aggregates {
		aggregateByThread[aggregate.ThreadID] = aggregate
		latestRunIDs = append(latestRunIDs, aggregate.MaxID)
	}

	latestRunByThread := make(map[string]models.Run, len(latestRunIDs))
	agentByID := make(map[uint64]models.Agent)
	if len(latestRunIDs) > 0 {
		var latestRuns []models.Run
		if err := s.database.WithContext(ctx).Where("id IN ?", latestRunIDs).Find(&latestRuns).Error; err != nil {
			return nil, fmt.Errorf("loading latest thread runs: %w", err)
		}
		agentIDs := make([]uint64, 0, len(latestRuns))
		for _, run := range latestRuns {
			latestRunByThread[run.ThreadID] = run
			agentIDs = append(agentIDs, run.AgentID)
		}
		if len(agentIDs) > 0 {
			var agents []models.Agent
			if err := s.database.WithContext(ctx).Where("id IN ?", agentIDs).Find(&agents).Error; err != nil {
				return nil, fmt.Errorf("loading thread agents: %w", err)
			}
			for _, agent := range agents {
				agentByID[agent.ID] = agent
			}
		}
	}

	firstInputByThread, err := s.loadFirstUserInputs(ctx, threadIDs)
	if err != nil {
		return nil, err
	}

	for _, thread := range threads {
		title := thread.Title
		if title == "" {
			title = firstInputByThread[thread.ThreadID]
		}
		view := ThreadSummaryView{
			ThreadID:  thread.ThreadID,
			Title:     title,
			Status:    thread.Status,
			CreatedAt: thread.CreatedAt,
			UpdatedAt: thread.UpdatedAt,
		}
		if aggregate, ok := aggregateByThread[thread.ThreadID]; ok {
			view.RunCount = aggregate.RunCount
		}
		if run, ok := latestRunByThread[thread.ThreadID]; ok {
			view.LastRunID = run.RunID
			view.LastRunStatus = run.Status
			if agent, ok := agentByID[run.AgentID]; ok {
				view.AgentCode = agent.AgentCode
				view.AgentName = agent.Name
			}
		}
		views = append(views, view)
	}
	return views, nil
}

// loadFirstUserInputs 为会话列表派生标题：取每个 Thread 第一条用户输入消息的文本摘要。
func (s *RunService) loadFirstUserInputs(ctx context.Context, threadIDs []string) (map[string]string, error) {
	if len(threadIDs) == 0 {
		return map[string]string{}, nil
	}
	type firstMessage struct {
		ThreadID string
		MinID    uint64
	}
	var firsts []firstMessage
	if err := s.database.WithContext(ctx).Model(&models.Message{}).
		Select("thread_id AS thread_id, MIN(id) AS min_id").
		Where("thread_id IN ? AND message_type = ? AND sender_type = ?", threadIDs, models.MessageTypeInput, models.MessageSenderUser).
		Group("thread_id").
		Scan(&firsts).Error; err != nil {
		return nil, fmt.Errorf("aggregating thread first inputs: %w", err)
	}
	if len(firsts) == 0 {
		return map[string]string{}, nil
	}
	messageIDs := make([]uint64, 0, len(firsts))
	for _, first := range firsts {
		messageIDs = append(messageIDs, first.MinID)
	}
	var messages []models.Message
	if err := s.database.WithContext(ctx).Where("id IN ?", messageIDs).Find(&messages).Error; err != nil {
		return nil, fmt.Errorf("loading thread first inputs: %w", err)
	}
	titles := make(map[string]string, len(messages))
	for _, message := range messages {
		content := replayMessageContent(message.ContentJSON)
		content = strings.TrimSpace(content)
		if runes := []rune(content); len(runes) > 60 {
			content = string(runes[:60])
		}
		titles[message.ThreadID] = content
	}
	return titles, nil
}

// ListThreadMessages 返回单个 Thread 的持久化消息历史，非 owner 且非 admin 拒绝访问。
func (s *RunService) ListThreadMessages(ctx context.Context, userID uint64, isAdmin bool, threadID string) ([]ThreadMessageView, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return nil, errThreadNotFound
	}
	var thread models.Thread
	if err := s.database.WithContext(ctx).Where("thread_id = ?", threadID).First(&thread).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errThreadNotFound
		}
		return nil, fmt.Errorf("loading thread: %w", err)
	}
	if !isAdmin && thread.OwnerUserID != userID {
		return nil, errRunForbidden
	}

	var messages []models.Message
	if err := s.database.WithContext(ctx).
		Where("thread_id = ?", threadID).
		Order("created_at ASC").Order("id ASC").
		Limit(consoleMessageListLimit).
		Find(&messages).Error; err != nil {
		return nil, fmt.Errorf("loading thread messages: %w", err)
	}

	views := make([]ThreadMessageView, 0, len(messages))
	for _, message := range messages {
		contentJSON := strings.TrimSpace(message.ContentJSON)
		if contentJSON == "" || !json.Valid([]byte(contentJSON)) {
			contentJSON = "null"
		}
		views = append(views, ThreadMessageView{
			MessageID:   message.MessageID,
			RunID:       message.RunID,
			SenderType:  message.SenderType,
			SenderID:    message.SenderID,
			MessageType: message.MessageType,
			Role:        replayMessageRole(message),
			Content:     replayMessageContent(contentJSON),
			ContentJSON: json.RawMessage(contentJSON),
			Status:      message.Status,
			CreatedAt:   message.CreatedAt,
		})
	}
	return views, nil
}

// GetRunWorkflow 返回 Run 实际执行的 Workflow 定义，访问控制与 Run 详情一致（owner 或 admin）。
// Run 记录了执行时的 workflow_id，因此即使 Agent 后续发布了新版本，这里仍返回当时的定义。
func (s *RunService) GetRunWorkflow(ctx context.Context, userID uint64, isAdmin bool, runID string) (*RunWorkflowView, error) {
	run, err := s.GetRunByRunID(ctx, userID, isAdmin, runID)
	if err != nil {
		return nil, err
	}
	var workflow models.Workflow
	if err := s.database.WithContext(ctx).Where("id = ?", run.WorkflowID).First(&workflow).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errWorkflowNotFound
		}
		return nil, fmt.Errorf("loading run workflow: %w", err)
	}
	view := &RunWorkflowView{
		RunID:      run.RunID,
		WorkflowID: workflow.ID,
		Version:    workflow.Version,
		Definition: json.RawMessage(workflow.DefinitionJSON),
		Checksum:   workflow.Checksum,
		IsActive:   workflow.IsActive,
	}
	var agent models.Agent
	if err := s.database.WithContext(ctx).Where("id = ?", workflow.AgentID).First(&agent).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("loading run workflow agent: %w", err)
		}
	} else {
		view.AgentCode = agent.AgentCode
		view.AgentName = agent.Name
	}
	return view, nil
}

// ListRuns 返回当前用户可见的 Run 列表；admin 可跨 owner 查看，支持按 Thread/Agent/状态过滤。
func (s *RunService) ListRuns(ctx context.Context, userID uint64, isAdmin bool, filter RunListFilter) ([]RunListItemView, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = consoleRunListDefault
	}
	if limit > consoleRunListMaxLimit {
		limit = consoleRunListMaxLimit
	}

	query := s.database.WithContext(ctx).Order("created_at DESC").Order("id DESC").Limit(limit)
	if !isAdmin {
		query = query.Where("user_id = ?", userID)
	}
	if threadID := strings.TrimSpace(filter.ThreadID); threadID != "" {
		query = query.Where("thread_id = ?", threadID)
	}
	if status := strings.TrimSpace(filter.Status); status != "" {
		query = query.Where("status = ?", status)
	}
	if agentCode := strings.TrimSpace(filter.AgentCode); agentCode != "" {
		var agent models.Agent
		if err := s.database.WithContext(ctx).Where("agent_code = ?", agentCode).First(&agent).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return []RunListItemView{}, nil
			}
			return nil, fmt.Errorf("resolving run filter agent: %w", err)
		}
		query = query.Where("agent_id = ?", agent.ID)
	}

	var runs []models.Run
	if err := query.Find(&runs).Error; err != nil {
		return nil, fmt.Errorf("listing runs: %w", err)
	}

	agentByID := make(map[uint64]models.Agent)
	if len(runs) > 0 {
		agentIDs := make([]uint64, 0, len(runs))
		seen := make(map[uint64]bool, len(runs))
		for _, run := range runs {
			if !seen[run.AgentID] {
				seen[run.AgentID] = true
				agentIDs = append(agentIDs, run.AgentID)
			}
		}
		var agents []models.Agent
		if err := s.database.WithContext(ctx).Where("id IN ?", agentIDs).Find(&agents).Error; err != nil {
			return nil, fmt.Errorf("loading run agents: %w", err)
		}
		for _, agent := range agents {
			agentByID[agent.ID] = agent
		}
	}

	views := make([]RunListItemView, 0, len(runs))
	for _, run := range runs {
		view := RunListItemView{
			RunID:        run.RunID,
			ThreadID:     run.ThreadID,
			TriggerType:  run.TriggerType,
			Status:       run.Status,
			CurrentStep:  run.CurrentStep,
			ErrorMessage: run.ErrorMessage,
			StartedAt:    run.StartedAt,
			FinishedAt:   run.FinishedAt,
			CreatedAt:    run.CreatedAt,
		}
		if agent, ok := agentByID[run.AgentID]; ok {
			view.AgentCode = agent.AgentCode
			view.AgentName = agent.Name
		}
		views = append(views, view)
	}
	return views, nil
}
