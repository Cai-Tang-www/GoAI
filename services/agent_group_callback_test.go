package services

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"GoAI/models"

	"gorm.io/gorm"
)

type agentGroupCallbackFixture struct {
	database  *gorm.DB
	service   *RunService
	runtime   *RuntimeService
	publisher *recordingResumePublisher
	source    models.Agent
	run       models.Run
	group     models.DelegationGroup
	members   map[string]models.Delegation
	tokens    map[string]string
}

func setupAgentGroupCallbackFixture(t *testing.T, strategy string, requiredSuccesses int, memberKeys []string, completed map[string]bool) agentGroupCallbackFixture {
	t.Helper()
	database, service, _ := setupRunTestService(t)
	targets := map[string]string{
		"security":   "security-reviewer",
		"quality":    "quality-reviewer",
		"compliance": "compliance-reviewer",
	}
	membersJSON := ""
	for index, key := range memberKeys {
		if index > 0 {
			membersJSON += ","
		}
		membersJSON += `{"key":"` + key + `","target_agent":"` + targets[key] + `","capability":"review"}`
	}
	requiredJSON := ""
	if strategy == models.DelegationGroupStrategyQuorum {
		requiredJSON = `,"required_successes":` + strconv.Itoa(requiredSuccesses)
	}
	definition := `{"entry_node":"parallel_review","nodes":[{"key":"parallel_review","type":"agent_group","config":{"strategy":"` + strategy + `"` + requiredJSON + `,"members":[` + membersJSON + `]}}],"edges":[]}`
	source, workflow := seedAgentWorkflow(t, database)
	workflow.DefinitionJSON = definition
	if err := database.Save(&workflow).Error; err != nil {
		t.Fatalf("update callback workflow failed: %v", err)
	}
	for _, key := range memberKeys {
		target := models.Agent{AgentCode: targets[key], Name: targets[key], OwnerUserID: 1, Status: models.AgentStatusActive}
		if err := database.Create(&target).Error; err != nil {
			t.Fatalf("create callback target %s failed: %v", key, err)
		}
		if err := database.Create(&models.AgentCapability{
			AgentID: target.ID, CapabilityCode: "review", Name: "Review", CapabilityType: models.AgentCapabilityTypeWorkflow,
			Version: "1", Status: models.AgentCapabilityStatusActive,
		}).Error; err != nil {
			t.Fatalf("create callback capability %s failed: %v", key, err)
		}
		if err := database.Create(&models.AgentEndpoint{
			AgentID: target.ID, EndpointCode: key + "-loopback", Protocol: models.AgentEndpointProtocolA2A,
			Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1:18080/a2a/agents/" + targets[key],
			Status: models.AgentEndpointStatusActive,
		}).Error; err != nil {
			t.Fatalf("create callback endpoint %s failed: %v", key, err)
		}
	}
	tokens := make(map[string]string, len(memberKeys))
	var tokensMu sync.Mutex
	invoker := newCoordinatedAgentGroupInvoker(0, func(request AgentInvocationRequest, _ int) (*AgentInvocationResult, error) {
		token := "token-" + request.GroupMemberKey
		tokensMu.Lock()
		tokens[request.GroupMemberKey] = token
		tokensMu.Unlock()
		state := AgentInvocationStateAccepted
		output := `{"accepted":true}`
		if completed[request.GroupMemberKey] {
			state = AgentInvocationStateCompleted
			output = `{"member":"` + request.GroupMemberKey + `","sync":true}`
		}
		return &AgentInvocationResult{TaskID: request.TaskID, State: state, OutputJSON: output, NotificationToken: token}, nil
	})
	service.agentInvoker = invoker
	service.stepRetryBackoffs = []time.Duration{0, 0, 0}
	run := models.Run{
		RunID: "run_group_callback_" + strategy, ThreadID: "thread-group-callback-" + strategy,
		AgentID: source.ID, WorkflowID: workflow.ID, UserID: 1, TriggerType: "api",
		InputJSON: `{"draft":"review"}`, Status: models.RunStatusQueued, TraceID: "trace-group-" + strategy,
	}
	if err := database.Create(&run).Error; err != nil {
		t.Fatalf("create callback run failed: %v", err)
	}
	if err := service.HandleRunExecute(context.Background(), run.RunID); err != nil {
		t.Fatalf("execute callback group failed: %v", err)
	}
	if err := database.Where("run_id = ?", run.RunID).First(&run).Error; err != nil {
		t.Fatalf("reload callback run failed: %v", err)
	}
	var group models.DelegationGroup
	if err := database.Where("parent_run_id = ?", run.RunID).First(&group).Error; err != nil {
		t.Fatalf("load callback group failed: %v", err)
	}
	var stored []models.Delegation
	if err := database.Where("delegation_group_id = ?", group.GroupID).Order("group_member_position ASC").Find(&stored).Error; err != nil {
		t.Fatalf("load callback members failed: %v", err)
	}
	members := make(map[string]models.Delegation, len(stored))
	for _, member := range stored {
		if member.GroupMemberKey == nil {
			t.Fatalf("callback member key is nil: %+v", member)
		}
		members[*member.GroupMemberKey] = member
	}
	publisher := &recordingResumePublisher{}
	runtimeService, err := NewRuntimeService(database, service, WithRunResumePublisher(publisher))
	if err != nil {
		t.Fatalf("create callback runtime failed: %v", err)
	}
	return agentGroupCallbackFixture{
		database: database, service: service, runtime: runtimeService, publisher: publisher,
		source: source, run: run, group: group, members: members, tokens: tokens,
	}
}

func (f agentGroupCallbackFixture) command(memberKey, state string) DelegationCallbackCommand {
	member := f.members[memberKey]
	return DelegationCallbackCommand{
		SourceAgentCode: f.source.AgentCode,
		TargetAgentCode: map[string]string{
			"security": "security-reviewer", "quality": "quality-reviewer", "compliance": "compliance-reviewer",
		}[memberKey],
		TaskID: *member.A2ATaskID, State: state,
		OutputJSON:        `{"member":"` + memberKey + `","state":"` + state + `"}`,
		ErrorMessage:      "member " + memberKey + " " + state,
		NotificationToken: f.tokens[memberKey], EventJSON: `{"member":"` + memberKey + `","terminal":"` + state + `"}`,
	}
}

func TestAgentGroupCallbackCombinesSynchronousAndAsynchronousMembers(t *testing.T) {
	fixture := setupAgentGroupCallbackFixture(t, models.DelegationGroupStrategyAll, 0, []string{"security", "quality"}, map[string]bool{"security": true})
	if fixture.run.Status != models.RunStatusWaitingExternal || fixture.members["security"].Status != models.DelegationStatusSucceeded || fixture.members["quality"].Status != models.DelegationStatusAccepted {
		t.Fatalf("unexpected mixed group setup: run=%+v members=%+v", fixture.run, fixture.members)
	}
	command := fixture.command("quality", DelegationCallbackStateSucceeded)
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), command); err != nil {
		t.Fatalf("accept mixed group callback failed: %v", err)
	}
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), command); err != nil {
		t.Fatalf("duplicate mixed group callback failed: %v", err)
	}
	calls := fixture.publisher.snapshot()
	if len(calls) != 1 || calls[0].delegationID != fixture.group.CoordinatorDelegationID || calls[0].runID != fixture.run.RunID {
		t.Fatalf("group resume was not published exactly once by coordinator: %+v", calls)
	}
	var group models.DelegationGroup
	if err := fixture.database.Where("group_id = ?", fixture.group.GroupID).First(&group).Error; err != nil {
		t.Fatalf("reload mixed group failed: %v", err)
	}
	if group.Status != models.DelegationGroupStatusSucceeded || group.SucceededMembers != 2 || group.ErrorMessage != "" {
		t.Fatalf("unexpected completed mixed group: %+v", group)
	}
	var step models.RunStep
	if err := fixture.database.Where("run_id = ? AND step_key = ?", fixture.run.RunID, group.ParentStepKey).First(&step).Error; err != nil {
		t.Fatalf("load mixed group step failed: %v", err)
	}
	if step.Status != models.RunStepStatusSuccess || step.OutputJSON != group.ResultJSON {
		t.Fatalf("parent checkpoint does not match terminal aggregate: step=%+v group=%+v", step, group)
	}
}

func TestAgentGroupAnyLateCallbackUpdatesAuditWithoutChangingCheckpoint(t *testing.T) {
	fixture := setupAgentGroupCallbackFixture(t, models.DelegationGroupStrategyAny, 0, []string{"security", "quality"}, nil)
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), fixture.command("quality", DelegationCallbackStateSucceeded)); err != nil {
		t.Fatalf("accept early any success failed: %v", err)
	}
	var checkpoint models.RunStep
	if err := fixture.database.Where("run_id = ? AND step_key = ?", fixture.run.RunID, fixture.group.ParentStepKey).First(&checkpoint).Error; err != nil {
		t.Fatalf("load any checkpoint failed: %v", err)
	}
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), fixture.command("security", DelegationCallbackStateFailed)); err != nil {
		t.Fatalf("accept late any callback failed: %v", err)
	}
	var after models.RunStep
	if err := fixture.database.First(&after, checkpoint.ID).Error; err != nil {
		t.Fatalf("reload any checkpoint failed: %v", err)
	}
	if after.OutputJSON != checkpoint.OutputJSON || after.Status != models.RunStepStatusSuccess {
		t.Fatalf("late callback rewrote parent checkpoint: before=%+v after=%+v", checkpoint, after)
	}
	var group models.DelegationGroup
	if err := fixture.database.Where("group_id = ?", fixture.group.GroupID).First(&group).Error; err != nil {
		t.Fatalf("reload any group failed: %v", err)
	}
	if group.Status != models.DelegationGroupStatusSucceeded || group.SucceededMembers != 1 || group.FailedMembers != 1 || group.ResultJSON == checkpoint.OutputJSON {
		t.Fatalf("late callback was not retained only in audit aggregate: group=%+v checkpoint=%s", group, checkpoint.OutputJSON)
	}
	if calls := fixture.publisher.snapshot(); len(calls) != 1 {
		t.Fatalf("late callback published another resume: %+v", calls)
	}
}

func TestAgentGroupConcurrentCallbacksConvergeAndPublishOnce(t *testing.T) {
	fixture := setupAgentGroupCallbackFixture(t, models.DelegationGroupStrategyAll, 0, []string{"security", "quality"}, nil)
	sqlDB, err := fixture.database.DB()
	if err != nil {
		t.Fatalf("get callback SQL database failed: %v", err)
	}
	sqlDB.SetMaxOpenConns(1)
	commands := []DelegationCallbackCommand{
		fixture.command("security", DelegationCallbackStateSucceeded),
		fixture.command("quality", DelegationCallbackStateSucceeded),
	}
	var wait sync.WaitGroup
	errorsCh := make(chan error, len(commands))
	for _, command := range commands {
		command := command
		wait.Add(1)
		go func() {
			defer wait.Done()
			errorsCh <- fixture.runtime.AcceptDelegationCallback(context.Background(), command)
		}()
	}
	wait.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("concurrent group callback failed: %v", err)
		}
	}
	if calls := fixture.publisher.snapshot(); len(calls) != 1 || calls[0].delegationID != fixture.group.CoordinatorDelegationID {
		t.Fatalf("concurrent callbacks published invalid resumes: %+v", calls)
	}
	var group models.DelegationGroup
	if err := fixture.database.Where("group_id = ?", fixture.group.GroupID).First(&group).Error; err != nil {
		t.Fatalf("reload concurrent group failed: %v", err)
	}
	if group.Status != models.DelegationGroupStatusSucceeded || group.SucceededMembers != 2 {
		t.Fatalf("concurrent callbacks did not converge: %+v", group)
	}
}

func TestAgentGroupQuorumImpossibleFailsParentAndKeepsLateResultForAudit(t *testing.T) {
	fixture := setupAgentGroupCallbackFixture(t, models.DelegationGroupStrategyQuorum, 2, []string{"security", "quality", "compliance"}, nil)
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), fixture.command("security", DelegationCallbackStateFailed)); err != nil {
		t.Fatalf("accept first quorum failure failed: %v", err)
	}
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), fixture.command("quality", DelegationCallbackStateCancelled)); err != nil {
		t.Fatalf("accept quorum impossible callback failed: %v", err)
	}
	var runBefore models.Run
	if err := fixture.database.Where("run_id = ?", fixture.run.RunID).First(&runBefore).Error; err != nil {
		t.Fatalf("load failed quorum run failed: %v", err)
	}
	if runBefore.Status != models.RunStatusFailed {
		t.Fatalf("quorum impossible run status=%s want=%s", runBefore.Status, models.RunStatusFailed)
	}
	var stepBefore models.RunStep
	if err := fixture.database.Where("run_id = ? AND step_key = ?", fixture.run.RunID, fixture.group.ParentStepKey).First(&stepBefore).Error; err != nil {
		t.Fatalf("load failed quorum step failed: %v", err)
	}
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), fixture.command("compliance", DelegationCallbackStateSucceeded)); err != nil {
		t.Fatalf("accept late quorum callback failed: %v", err)
	}
	var stepAfter models.RunStep
	if err := fixture.database.First(&stepAfter, stepBefore.ID).Error; err != nil {
		t.Fatalf("reload failed quorum step failed: %v", err)
	}
	if stepAfter.OutputJSON != stepBefore.OutputJSON || stepAfter.Status != models.RunStepStatusFailed {
		t.Fatalf("late quorum callback rewrote failed checkpoint: before=%+v after=%+v", stepBefore, stepAfter)
	}
	var group models.DelegationGroup
	if err := fixture.database.Where("group_id = ?", fixture.group.GroupID).First(&group).Error; err != nil {
		t.Fatalf("reload failed quorum group failed: %v", err)
	}
	if group.Status != models.DelegationGroupStatusFailed || group.SucceededMembers != 1 || group.FailedMembers != 1 || group.CancelledMembers != 1 {
		t.Fatalf("unexpected failed quorum audit: %+v", group)
	}
	if calls := fixture.publisher.snapshot(); len(calls) != 0 {
		t.Fatalf("failed quorum published resume: %+v", calls)
	}
}

func TestAgentGroupResumePublishFailureRecoversThroughCoordinator(t *testing.T) {
	fixture := setupAgentGroupCallbackFixture(t, models.DelegationGroupStrategyAll, 0, []string{"security", "quality"}, map[string]bool{"security": true})
	fixture.publisher.setError(errors.New("kafka unavailable"))
	if err := fixture.runtime.AcceptDelegationCallback(context.Background(), fixture.command("quality", DelegationCallbackStateSucceeded)); err == nil {
		t.Fatal("expected group resume publish failure")
	}
	var coordinator models.Delegation
	if err := fixture.database.Where("delegation_id = ?", fixture.group.CoordinatorDelegationID).First(&coordinator).Error; err != nil {
		t.Fatalf("load failed group coordinator failed: %v", err)
	}
	if coordinator.ResumeStatus != models.DelegationResumeStatusPublishFailed || coordinator.ResumeAttemptCount != 1 {
		t.Fatalf("group publish failure was not persisted on coordinator: %+v", coordinator)
	}
	fixture.publisher.setError(nil)
	restarted, err := NewRuntimeService(fixture.database, fixture.service, WithRunResumePublisher(fixture.publisher))
	if err != nil {
		t.Fatalf("recreate group runtime failed: %v", err)
	}
	if err := restarted.RecoverPendingResumes(context.Background(), 10); err != nil {
		t.Fatalf("recover group resume failed: %v", err)
	}
	if calls := fixture.publisher.snapshot(); len(calls) != 2 || calls[1].delegationID != fixture.group.CoordinatorDelegationID {
		t.Fatalf("group resume recovery did not use coordinator: %+v", calls)
	}

	staleAt := time.Now().Add(-2 * resumeRepublishDelay)
	if err := fixture.database.Model(&models.Delegation{}).Where("delegation_id = ?", fixture.group.CoordinatorDelegationID).UpdateColumns(map[string]any{
		"resume_status": models.DelegationResumeStatusPublished, "resume_published_at": staleAt, "updated_at": staleAt,
	}).Error; err != nil {
		t.Fatalf("mark group coordinator publish stale failed: %v", err)
	}
	if err := restarted.RecoverPendingResumes(context.Background(), 10); err != nil {
		t.Fatalf("recover stale group publish failed: %v", err)
	}
	if calls := fixture.publisher.snapshot(); len(calls) != 3 || calls[2].delegationID != fixture.group.CoordinatorDelegationID {
		t.Fatalf("stale group recovery did not use coordinator: %+v", calls)
	}

	expiredAt := time.Now().Add(-time.Second)
	if err := fixture.database.Model(&models.Delegation{}).Where("delegation_id = ?", fixture.group.CoordinatorDelegationID).UpdateColumns(map[string]any{
		"resume_status": models.DelegationResumeStatusClaimed, "resume_lease_owner": "crashed-worker",
		"resume_lease_expires_at": expiredAt, "updated_at": expiredAt,
	}).Error; err != nil {
		t.Fatalf("expire group coordinator claim failed: %v", err)
	}
	if err := restarted.RecoverPendingResumes(context.Background(), 10); err != nil {
		t.Fatalf("recover expired group claim failed: %v", err)
	}
	if calls := fixture.publisher.snapshot(); len(calls) != 4 || calls[3].delegationID != fixture.group.CoordinatorDelegationID {
		t.Fatalf("expired group claim recovery did not use coordinator: %+v", calls)
	}
	if err := restarted.RecoverPendingResumes(context.Background(), 10); err != nil {
		t.Fatalf("repeat group recovery scan failed: %v", err)
	}
	if calls := fixture.publisher.snapshot(); len(calls) != 4 {
		t.Fatalf("fresh group resume was republished: %+v", calls)
	}
}
