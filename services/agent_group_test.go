package services

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"GoAI/models"

	"gorm.io/gorm"
)

type coordinatedAgentGroupInvoker struct {
	mu          sync.Mutex
	requests    []AgentInvocationRequest
	callsByKey  map[string]int
	active      int
	maxActive   int
	barrierSize int
	barrier     chan struct{}
	barrierOnce sync.Once
	invoke      func(AgentInvocationRequest, int) (*AgentInvocationResult, error)
}

func newCoordinatedAgentGroupInvoker(barrierSize int, invoke func(AgentInvocationRequest, int) (*AgentInvocationResult, error)) *coordinatedAgentGroupInvoker {
	return &coordinatedAgentGroupInvoker{
		callsByKey:  make(map[string]int),
		barrierSize: barrierSize,
		barrier:     make(chan struct{}),
		invoke:      invoke,
	}
}

func (i *coordinatedAgentGroupInvoker) Invoke(ctx context.Context, request AgentInvocationRequest) (*AgentInvocationResult, error) {
	i.mu.Lock()
	i.requests = append(i.requests, request)
	i.callsByKey[request.GroupMemberKey]++
	call := i.callsByKey[request.GroupMemberKey]
	i.active++
	if i.active > i.maxActive {
		i.maxActive = i.active
	}
	if i.barrierSize > 0 && i.active >= i.barrierSize {
		i.barrierOnce.Do(func() { close(i.barrier) })
	}
	barrier := i.barrier
	barrierSize := i.barrierSize
	i.mu.Unlock()

	if barrierSize > 0 {
		select {
		case <-barrier:
		case <-ctx.Done():
			i.mu.Lock()
			i.active--
			i.mu.Unlock()
			return nil, ctx.Err()
		}
	}

	result, err := i.invoke(request, call)
	i.mu.Lock()
	i.active--
	i.mu.Unlock()
	return result, err
}

func (i *coordinatedAgentGroupInvoker) snapshot() ([]AgentInvocationRequest, map[string]int, int) {
	i.mu.Lock()
	defer i.mu.Unlock()
	requests := append([]AgentInvocationRequest(nil), i.requests...)
	calls := make(map[string]int, len(i.callsByKey))
	for key, count := range i.callsByKey {
		calls[key] = count
	}
	return requests, calls, i.maxActive
}

func seedAgentGroupWorkflow(t *testing.T, database *gorm.DB, definition string) (models.Agent, models.Workflow) {
	t.Helper()
	source, workflow := seedAgentWorkflow(t, database)
	workflow.DefinitionJSON = definition
	if err := database.Save(&workflow).Error; err != nil {
		t.Fatalf("update agent group workflow failed: %v", err)
	}
	for _, target := range []struct {
		code       string
		capability string
	}{
		{code: "security-reviewer", capability: "review"},
		{code: "quality-reviewer", capability: "review"},
	} {
		agent := models.Agent{AgentCode: target.code, Name: target.code, OwnerUserID: 1, Status: models.AgentStatusActive}
		if err := database.Create(&agent).Error; err != nil {
			t.Fatalf("create target agent %s failed: %v", target.code, err)
		}
		capability := models.AgentCapability{
			AgentID: agent.ID, CapabilityCode: target.capability, Name: target.capability,
			CapabilityType: models.AgentCapabilityTypeWorkflow, Version: "1", Status: models.AgentCapabilityStatusActive,
		}
		if err := database.Create(&capability).Error; err != nil {
			t.Fatalf("create target capability %s failed: %v", target.code, err)
		}
		endpoint := models.AgentEndpoint{
			AgentID: agent.ID, EndpointCode: target.code + "-loopback", Protocol: models.AgentEndpointProtocolA2A,
			Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1:18080/a2a/agents/" + target.code,
			Status: models.AgentEndpointStatusActive,
		}
		if err := database.Create(&endpoint).Error; err != nil {
			t.Fatalf("create target endpoint %s failed: %v", target.code, err)
		}
	}
	return source, workflow
}

func TestHandleRunExecuteAgentGroupFansOutConcurrentlyAndPersistsStableMembers(t *testing.T) {
	database, service, _ := setupRunTestService(t)
	source, workflow := seedAgentGroupWorkflow(t, database, `{
		"entry_node":"draft",
		"nodes":[
			{"key":"draft","type":"planner"},
			{"key":"parallel_review","type":"agent_group","config":{
				"strategy":"all","input_from":["draft"],
				"members":[
					{"key":"security","target_agent":"security-reviewer","capability":"review","timeout_ms":2000},
					{"key":"quality","target_agent":"quality-reviewer","capability":"review","timeout_ms":2000}
				]
			}}
		],
		"edges":[{"from":"draft","to":"parallel_review"}]
	}`)
	invoker := newCoordinatedAgentGroupInvoker(2, func(request AgentInvocationRequest, _ int) (*AgentInvocationResult, error) {
		return &AgentInvocationResult{
			TaskID: request.TaskID, State: AgentInvocationStateCompleted,
			OutputJSON: `{"reviewer":"` + request.GroupMemberKey + `","approved":true}`,
		}, nil
	})
	service.agentInvoker = invoker
	service.stepRetryBackoffs = []time.Duration{0, 0, 0}
	run := models.Run{
		RunID: "run_agent_group_sync", ThreadID: "thread-agent-group", AgentID: source.ID, WorkflowID: workflow.ID,
		UserID: 1, TriggerType: "api", InputJSON: `{"draft":"review me"}`, Status: models.RunStatusQueued, TraceID: "trace-agent-group",
	}
	if err := database.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	if err := service.HandleRunExecute(context.Background(), run.RunID); err != nil {
		t.Fatalf("execute agent group run failed: %v", err)
	}
	requests, calls, maxActive := invoker.snapshot()
	if len(requests) != 2 || calls["security"] != 1 || calls["quality"] != 1 {
		t.Fatalf("unexpected A2A fan-out calls: requests=%+v calls=%v", requests, calls)
	}
	if maxActive < 2 {
		t.Fatalf("agent group calls were not concurrent: max_active=%d", maxActive)
	}
	sort.Slice(requests, func(i, j int) bool { return requests[i].GroupMemberKey < requests[j].GroupMemberKey })
	groupID := stableA2AID("group", run.RunID, "parallel_review")
	for _, request := range requests {
		if request.DelegationGroupID != groupID || request.DelegationID == "" || request.TaskID == "" || request.MessageID == "" {
			t.Fatalf("missing stable group protocol identifiers: %+v", request)
		}
		if request.InputJSON != requests[0].InputJSON || request.InputJSON == run.InputJSON {
			t.Fatalf("members did not receive the same resolved input: %+v", requests)
		}
		if len(request.Endpoints) != 1 || request.Endpoints[0].Transport != models.AgentEndpointTransportHTTP {
			t.Fatalf("member was not routed through an A2A endpoint: %+v", request)
		}
	}

	var storedRun models.Run
	if err := database.Where("run_id = ?", run.RunID).First(&storedRun).Error; err != nil {
		t.Fatalf("reload run failed: %v", err)
	}
	if storedRun.Status != models.RunStatusSuccess {
		t.Fatalf("run status=%s want=%s", storedRun.Status, models.RunStatusSuccess)
	}
	var group models.DelegationGroup
	if err := database.Where("group_id = ?", groupID).First(&group).Error; err != nil {
		t.Fatalf("load delegation group failed: %v", err)
	}
	if group.Status != models.DelegationGroupStatusSucceeded || group.SucceededMembers != 2 || group.ErrorMessage != "" {
		t.Fatalf("unexpected delegation group: %+v", group)
	}
	var delegations []models.Delegation
	if err := database.Where("delegation_group_id = ?", groupID).Order("group_member_position ASC").Find(&delegations).Error; err != nil {
		t.Fatalf("load group delegations failed: %v", err)
	}
	if len(delegations) != 2 {
		t.Fatalf("delegation count=%d want=2", len(delegations))
	}
	for position, delegation := range delegations {
		memberKey := []string{"security", "quality"}[position]
		if delegation.GroupMemberKey == nil || *delegation.GroupMemberKey != memberKey || delegation.GroupMemberPosition != position {
			t.Fatalf("member order mismatch at %d: %+v", position, delegation)
		}
		if delegation.DelegationID != stableA2AID("delegation", run.RunID, "parallel_review#"+memberKey) || delegation.Status != models.DelegationStatusSucceeded {
			t.Fatalf("member identifiers/status are not stable: %+v", delegation)
		}
	}
	var messageCount int64
	if err := database.Model(&models.Message{}).Where("run_id = ?", run.RunID).Count(&messageCount).Error; err != nil {
		t.Fatalf("count group messages failed: %v", err)
	}
	if messageCount != 4 {
		t.Fatalf("message count=%d want=4 request/result messages", messageCount)
	}
}

func TestHandleRunExecuteAgentGroupRetriesOnlyPendingMembers(t *testing.T) {
	database, service, _ := setupRunTestService(t)
	source, workflow := seedAgentGroupWorkflow(t, database, `{
		"entry_node":"parallel_review",
		"nodes":[{"key":"parallel_review","type":"agent_group","config":{
			"strategy":"all",
			"members":[
				{"key":"security","target_agent":"security-reviewer","capability":"review"},
				{"key":"quality","target_agent":"quality-reviewer","capability":"review"}
			]
		}}],
		"edges":[]
	}`)
	invoker := newCoordinatedAgentGroupInvoker(0, func(request AgentInvocationRequest, call int) (*AgentInvocationResult, error) {
		if request.GroupMemberKey == "quality" && call == 1 {
			return nil, errors.New("quality endpoint temporarily unavailable")
		}
		return &AgentInvocationResult{TaskID: request.TaskID, State: AgentInvocationStateCompleted, OutputJSON: `{"ok":true}`}, nil
	})
	service.agentInvoker = invoker
	service.stepRetryBackoffs = []time.Duration{0, 0, 0}
	run := models.Run{
		RunID: "run_agent_group_retry", ThreadID: "thread-agent-group-retry", AgentID: source.ID, WorkflowID: workflow.ID,
		UserID: 1, TriggerType: "api", InputJSON: `{"draft":"review me"}`, Status: models.RunStatusQueued,
	}
	if err := database.Create(&run).Error; err != nil {
		t.Fatalf("create run failed: %v", err)
	}

	if err := service.HandleRunExecute(context.Background(), run.RunID); err != nil {
		t.Fatalf("execute retried agent group failed: %v", err)
	}
	_, calls, _ := invoker.snapshot()
	if calls["security"] != 1 || calls["quality"] != 2 {
		t.Fatalf("completed member was invoked again: calls=%v", calls)
	}
	var delegationCount int64
	if err := database.Model(&models.Delegation{}).Where("parent_run_id = ?", run.RunID).Count(&delegationCount).Error; err != nil {
		t.Fatalf("count delegations failed: %v", err)
	}
	if delegationCount != 2 {
		t.Fatalf("retry created duplicate delegations: count=%d", delegationCount)
	}
	var steps []models.RunStep
	if err := database.Where("run_id = ? AND step_key = ?", run.RunID, "parallel_review").Order("attempt ASC").Find(&steps).Error; err != nil {
		t.Fatalf("load run steps failed: %v", err)
	}
	if len(steps) != 2 || steps[0].Status != models.RunStepStatusFailed || steps[1].Status != models.RunStepStatusSuccess {
		t.Fatalf("unexpected retry steps: %+v", steps)
	}
	var requestMessages []models.Message
	if err := database.Where("run_id = ? AND message_type = ?", run.RunID, models.MessageTypeDelegation).Find(&requestMessages).Error; err != nil {
		t.Fatalf("load request messages failed: %v", err)
	}
	if len(requestMessages) != 2 {
		t.Fatalf("request message count=%d want=2", len(requestMessages))
	}
	for _, message := range requestMessages {
		if message.Status != models.MessageStatusDelivered {
			t.Fatalf("request message did not recover to delivered: %+v", message)
		}
	}
}

func TestSuspendRunForTerminalAgentGroupDoesNotEnterWaitingState(t *testing.T) {
	tests := []struct {
		name           string
		groupStatus    string
		wantRunStatus  string
		wantStepStatus string
		wantSuspended  bool
		wantTerminal   string
	}{
		{name: "success continues graph", groupStatus: models.DelegationGroupStatusSucceeded, wantRunStatus: models.RunStatusRunning, wantStepStatus: models.RunStepStatusSuccess},
		{name: "failure stops graph", groupStatus: models.DelegationGroupStatusFailed, wantRunStatus: models.RunStatusFailed, wantStepStatus: models.RunStepStatusFailed, wantTerminal: models.RunStatusFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, service, _ := setupRunTestService(t)
			source, workflow := seedAgentWorkflow(t, database)
			target := models.Agent{AgentCode: "race-reviewer", Name: "Race Reviewer", OwnerUserID: 1, Status: models.AgentStatusActive}
			if err := database.Create(&target).Error; err != nil {
				t.Fatalf("create race target failed: %v", err)
			}
			startedAt := time.Now()
			run := models.Run{
				RunID: "run_group_suspend_" + test.groupStatus, ThreadID: "thread-group-suspend-" + test.groupStatus,
				AgentID: source.ID, WorkflowID: workflow.ID, UserID: 1, TriggerType: "api",
				InputJSON: `{"draft":"review"}`, Status: models.RunStatusRunning, TraceID: "trace-group-suspend", StartedAt: &startedAt,
			}
			if err := database.Create(&run).Error; err != nil {
				t.Fatalf("create race run failed: %v", err)
			}
			node := WorkflowNode{Key: "parallel_review", Type: "agent_group"}
			step, err := service.startRunStep(context.Background(), &run, node, 1)
			if err != nil {
				t.Fatalf("start race step failed: %v", err)
			}
			groupID := "group_suspend_" + test.groupStatus
			delegationID := "delegation_suspend_" + test.groupStatus
			memberKey := "security"
			taskID := "task_suspend_" + test.groupStatus
			group := models.DelegationGroup{
				GroupID: groupID, ThreadID: run.ThreadID, ParentRunID: run.RunID, ParentStepKey: node.Key,
				CoordinatorDelegationID: delegationID, TraceID: run.TraceID, Strategy: models.DelegationGroupStrategyAll,
				TotalMembers: 2, SucceededMembers: 2, Status: test.groupStatus,
				ResultJSON: `{"status":"` + test.groupStatus + `","members":[]}`, ErrorMessage: "review policy failed", StartedAt: &startedAt,
			}
			if err := database.Create(&group).Error; err != nil {
				t.Fatalf("create terminal group failed: %v", err)
			}
			delegation := models.Delegation{
				DelegationID: delegationID, ThreadID: run.ThreadID, ParentRunID: run.RunID, ChildRunID: taskID, A2ATaskID: &taskID,
				TraceID: run.TraceID, SourceAgentID: source.ID, TargetAgentID: target.ID, CapabilityCode: "review",
				RequestMessageID: "message_suspend_" + test.groupStatus, ParentStepKey: node.Key,
				DelegationGroupID: &groupID, GroupMemberKey: &memberKey, InputJSON: run.InputJSON,
				OutputJSON: `{"ok":true}`, Status: models.DelegationStatusSucceeded,
			}
			if err := database.Create(&delegation).Error; err != nil {
				t.Fatalf("create terminal coordinator failed: %v", err)
			}
			settlement, err := service.suspendRunForAgentInvocation(context.Background(), &run, step, node, "finalize", &agentInvocationAcceptedError{
				TaskID: taskID, DelegationID: delegationID, MessageID: delegation.RequestMessageID,
				SourceAgentID: source.ID, TargetAgentID: target.ID, CapabilityCode: "review", OutputJSON: `{"waiting":true}`, GroupID: groupID,
			})
			if err != nil {
				t.Fatalf("settle terminal group before suspend failed: %v", err)
			}
			if settlement.Suspended != test.wantSuspended || settlement.TerminalStatus != test.wantTerminal {
				t.Fatalf("unexpected settlement: %+v", settlement)
			}
			var storedRun models.Run
			if err := database.Where("run_id = ?", run.RunID).First(&storedRun).Error; err != nil {
				t.Fatalf("reload race run failed: %v", err)
			}
			var storedStep models.RunStep
			if err := database.First(&storedStep, step.ID).Error; err != nil {
				t.Fatalf("reload race step failed: %v", err)
			}
			if storedRun.Status != test.wantRunStatus || storedStep.Status != test.wantStepStatus {
				t.Fatalf("terminal group entered invalid parent state: run=%s step=%s", storedRun.Status, storedStep.Status)
			}
			if storedRun.Status == models.RunStatusWaitingExternal || storedStep.Status == models.RunStepStatusWaitingExternal {
				t.Fatalf("terminal group was incorrectly suspended: run=%s step=%s", storedRun.Status, storedStep.Status)
			}
			if test.groupStatus == models.DelegationGroupStatusSucceeded {
				wantOutput, err := normalizeNodeOutput(group.ResultJSON)
				if err != nil {
					t.Fatalf("normalize expected group output failed: %v", err)
				}
				if settlement.OutputJSON != wantOutput {
					t.Fatalf("success settlement output=%s want=%s", settlement.OutputJSON, wantOutput)
				}
			}
		})
	}
}

func TestGetRunDetailReturnsOrderedAgentGroupMembers(t *testing.T) {
	database, service, _ := setupRunTestService(t)
	source, workflow := seedAgentGroupWorkflow(t, database, `{"entry_node":"parallel_review","nodes":[{"key":"parallel_review","type":"agent_group","config":{"strategy":"all","members":[{"key":"security","target_agent":"security-reviewer","capability":"review"},{"key":"quality","target_agent":"quality-reviewer","capability":"review"}]}}],"edges":[]}`)
	run := models.Run{
		RunID: "run_group_detail", ThreadID: "thread-group-detail", AgentID: source.ID,
		WorkflowID: workflow.ID, UserID: 1, TriggerType: "api", InputJSON: `{}`,
		Status: models.RunStatusWaitingExternal, TraceID: "trace-group-detail",
	}
	if err := database.Create(&run).Error; err != nil {
		t.Fatalf("create group detail run failed: %v", err)
	}

	var security, quality models.Agent
	if err := database.Where("agent_code = ?", "security-reviewer").First(&security).Error; err != nil {
		t.Fatalf("load security reviewer failed: %v", err)
	}
	if err := database.Where("agent_code = ?", "quality-reviewer").First(&quality).Error; err != nil {
		t.Fatalf("load quality reviewer failed: %v", err)
	}
	groupID := "group_detail"
	securityKey, qualityKey := "security", "quality"
	group := models.DelegationGroup{
		GroupID: groupID, ThreadID: run.ThreadID, ParentRunID: run.RunID, ParentStepKey: "parallel_review",
		CoordinatorDelegationID: "delegation_security", TraceID: run.TraceID,
		Strategy: models.DelegationGroupStrategyAll, TotalMembers: 2, SucceededMembers: 1,
		Status: models.DelegationGroupStatusWaiting, ResultJSON: `{"status":"waiting"}`,
	}
	if err := database.Create(&group).Error; err != nil {
		t.Fatalf("create delegation group failed: %v", err)
	}
	members := []models.Delegation{
		{
			DelegationID: "delegation_quality", ThreadID: run.ThreadID, ParentRunID: run.RunID, ChildRunID: "child_quality",
			TraceID: run.TraceID, SourceAgentID: source.ID, TargetAgentID: quality.ID, CapabilityCode: "review",
			RequestMessageID: "message_quality", ParentStepKey: "parallel_review", DelegationGroupID: &groupID,
			GroupMemberKey: &qualityKey, GroupMemberPosition: 1, InputJSON: `{}`, Status: models.DelegationStatusAccepted,
		},
		{
			DelegationID: "delegation_security", ThreadID: run.ThreadID, ParentRunID: run.RunID, ChildRunID: "child_security",
			TraceID: run.TraceID, SourceAgentID: source.ID, TargetAgentID: security.ID, CapabilityCode: "review",
			RequestMessageID: "message_security", ParentStepKey: "parallel_review", DelegationGroupID: &groupID,
			GroupMemberKey: &securityKey, GroupMemberPosition: 0, InputJSON: `{}`, OutputJSON: `{"approved":true}`, Status: models.DelegationStatusSucceeded,
		},
	}
	if err := database.Create(&members).Error; err != nil {
		t.Fatalf("create delegation group members failed: %v", err)
	}

	detail, err := service.GetRunDetailByRunID(context.Background(), run.UserID, false, run.RunID)
	if err != nil {
		t.Fatalf("get run detail failed: %v", err)
	}
	if len(detail.DelegationGroups) != 1 {
		t.Fatalf("delegation group count=%d want=1", len(detail.DelegationGroups))
	}
	state := detail.DelegationGroups[0]
	if state.CoordinatorDelegationID != "delegation_security" || state.Strategy != models.DelegationGroupStrategyAll || len(state.Members) != 2 {
		t.Fatalf("unexpected delegation group state: %+v", state)
	}
	if state.Members[0].Position != 0 || state.Members[0].MemberKey != "security" || state.Members[0].TargetAgent != "security-reviewer" {
		t.Fatalf("unexpected first member: %+v", state.Members[0])
	}
	if state.Members[1].Position != 1 || state.Members[1].MemberKey != "quality" || state.Members[1].TargetAgent != "quality-reviewer" {
		t.Fatalf("unexpected second member: %+v", state.Members[1])
	}

	plainRun := models.Run{
		RunID: "run_without_group", ThreadID: "thread-without-group", AgentID: source.ID,
		WorkflowID: workflow.ID, UserID: 1, TriggerType: "api", InputJSON: `{}`, Status: models.RunStatusPending,
	}
	if err := database.Create(&plainRun).Error; err != nil {
		t.Fatalf("create plain run failed: %v", err)
	}
	plainDetail, err := service.GetRunDetailByRunID(context.Background(), plainRun.UserID, false, plainRun.RunID)
	if err != nil {
		t.Fatalf("get plain run detail failed: %v", err)
	}
	if plainDetail.DelegationGroups != nil {
		t.Fatalf("plain run delegation groups=%+v want nil", plainDetail.DelegationGroups)
	}
}
