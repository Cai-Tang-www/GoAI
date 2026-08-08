package db

import (
	"testing"

	"GoAI/models"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func openDomainModelTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	database, err := gorm.Open(
		sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)},
	)
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := Migrate(database); err != nil {
		t.Fatalf("migrate domain models failed: %v", err)
	}
	t.Cleanup(func() {
		if err := Close(database); err != nil {
			t.Errorf("close sqlite failed: %v", err)
		}
	})
	return database
}

func TestMigrateCreatesUnifiedDomainModels(t *testing.T) {
	database := openDomainModelTestDB(t)

	// AutoMigrate 必须可重复执行，保证服务重复启动时不会破坏已有结构。
	if err := Migrate(database); err != nil {
		t.Fatalf("second migration failed: %v", err)
	}

	for _, table := range []string{
		"threads",
		"messages",
		"delegations",
		"delegation_groups",
		"agent_endpoints",
		"agent_capabilities",
		"workflows",
		"mcp_servers",
		"mcp_tools",
		"loop_records",
		"loop_evaluations",
	} {
		if !database.Migrator().HasTable(table) {
			t.Errorf("expected table %s", table)
		}
	}

	indexes := []struct {
		model any
		name  string
	}{
		{&models.Thread{}, "idx_threads_thread_id"},
		{&models.Message{}, "idx_messages_message_id"},
		{&models.Delegation{}, "idx_delegations_delegation_id"},
		{&models.Delegation{}, "idx_delegations_child_run_id"},
		{&models.Delegation{}, "uidx_delegation_group_member"},
		{&models.DelegationGroup{}, "idx_delegation_groups_group_id"},
		{&models.DelegationGroup{}, "uidx_delegation_group_parent_step"},
		{&models.DelegationGroup{}, "uidx_delegation_group_coordinator"},
		{&models.AgentEndpoint{}, "idx_agent_endpoint_unique"},
		{&models.AgentCapability{}, "idx_agent_capability_unique"},
		{&models.Workflow{}, "uidx_workflows_agent_version"},
		{&models.MCPServer{}, "idx_mcp_server_owner_code"},
		{&models.MCPTool{}, "idx_mcp_tool_server_name"},
		{&models.LoopRecord{}, "idx_loop_records_loop_id"},
		{&models.LoopEvaluation{}, "idx_loop_evaluation_unique"},
	}
	for _, index := range indexes {
		if !database.Migrator().HasIndex(index.model, index.name) {
			t.Errorf("expected index %s for %T", index.name, index.model)
		}
	}
}

func TestUnifiedDomainModelUniqueConstraints(t *testing.T) {
	database := openDomainModelTestDB(t)

	assertDuplicateRejected := func(t *testing.T, first, duplicate any) {
		t.Helper()
		if err := database.Create(first).Error; err != nil {
			t.Fatalf("create first record failed: %v", err)
		}
		if err := database.Create(duplicate).Error; err == nil {
			t.Fatal("expected duplicate record to be rejected")
		}
	}

	t.Run("thread id", func(t *testing.T) {
		assertDuplicateRejected(t,
			&models.Thread{ThreadID: "thread-1", OwnerUserID: 1, Status: models.ThreadStatusActive},
			&models.Thread{ThreadID: "thread-1", OwnerUserID: 2, Status: models.ThreadStatusActive},
		)
	})

	t.Run("message id", func(t *testing.T) {
		assertDuplicateRejected(t,
			&models.Message{MessageID: "message-1", ThreadID: "thread-1", SenderType: models.MessageSenderUser, MessageType: models.MessageTypeInput, ContentType: "application/json", ContentJSON: `{}`, Status: models.MessageStatusPending},
			&models.Message{MessageID: "message-1", ThreadID: "thread-2", SenderType: models.MessageSenderUser, MessageType: models.MessageTypeInput, ContentType: "application/json", ContentJSON: `{}`, Status: models.MessageStatusPending},
		)
	})

	t.Run("delegation id", func(t *testing.T) {
		assertDuplicateRejected(t,
			newTestDelegation("delegation-1", "child-run-1"),
			newTestDelegation("delegation-1", "child-run-2"),
		)
	})

	t.Run("delegation child run", func(t *testing.T) {
		assertDuplicateRejected(t,
			newTestDelegation("delegation-2", "child-run-3"),
			newTestDelegation("delegation-3", "child-run-3"),
		)
	})

	t.Run("ungrouped delegations allow null group keys", func(t *testing.T) {
		if err := database.Create(newTestDelegation("delegation-null-1", "child-run-null-1")).Error; err != nil {
			t.Fatalf("create first ungrouped delegation: %v", err)
		}
		if err := database.Create(newTestDelegation("delegation-null-2", "child-run-null-2")).Error; err != nil {
			t.Fatalf("create second ungrouped delegation: %v", err)
		}
	})

	t.Run("delegation group member", func(t *testing.T) {
		groupID := "group-1"
		memberKey := "security"
		first := newTestDelegation("delegation-group-1", "child-run-group-1")
		first.DelegationGroupID = &groupID
		first.GroupMemberKey = &memberKey
		duplicate := newTestDelegation("delegation-group-2", "child-run-group-2")
		duplicate.DelegationGroupID = &groupID
		duplicate.GroupMemberKey = &memberKey
		assertDuplicateRejected(t, first, duplicate)
	})

	t.Run("delegation group parent step", func(t *testing.T) {
		assertDuplicateRejected(t,
			newTestDelegationGroup("group-parent-1", "review"),
			newTestDelegationGroup("group-parent-2", "review"),
		)
	})

	t.Run("delegation group coordinator", func(t *testing.T) {
		testFirst := newTestDelegationGroup("group-coordinator-1", "review-1")
		testDuplicate := newTestDelegationGroup("group-coordinator-2", "review-2")
		testFirst.CoordinatorDelegationID = "coordinator-unique"
		testDuplicate.CoordinatorDelegationID = "coordinator-unique"
		testFirst.ParentStepKey = "review-1"
		testDuplicate.ParentStepKey = "review-2"
		testFirst.ParentRunID = "parent-run-coordinator"
		testDuplicate.ParentRunID = "parent-run-coordinator"
		assertDuplicateRejected(t, testFirst, testDuplicate)
	})
	t.Run("agent endpoint code", func(t *testing.T) {
		assertDuplicateRejected(t,
			&models.AgentEndpoint{AgentID: 1, EndpointCode: "primary", Protocol: models.AgentEndpointProtocolA2A, Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1:8080/api/a2a", Status: models.AgentEndpointStatusActive},
			&models.AgentEndpoint{AgentID: 1, EndpointCode: "primary", Protocol: models.AgentEndpointProtocolA2A, Transport: models.AgentEndpointTransportHTTPS, Address: "https://agent.example/a2a", Status: models.AgentEndpointStatusActive},
		)
		if err := database.Create(&models.AgentEndpoint{AgentID: 2, EndpointCode: "primary", Protocol: models.AgentEndpointProtocolA2A, Transport: models.AgentEndpointTransportHTTP, Address: "http://127.0.0.1:8081/api/a2a", Status: models.AgentEndpointStatusActive}).Error; err != nil {
			t.Fatalf("same endpoint code on another agent should be allowed: %v", err)
		}
	})

	t.Run("agent capability code", func(t *testing.T) {
		assertDuplicateRejected(t,
			&models.AgentCapability{AgentID: 1, CapabilityCode: "summarize", Name: "Summarize", CapabilityType: models.AgentCapabilityTypeWorkflow, Version: "v1", Status: models.AgentCapabilityStatusActive},
			&models.AgentCapability{AgentID: 1, CapabilityCode: "summarize", Name: "Summarize v2", CapabilityType: models.AgentCapabilityTypeWorkflow, Version: "v2", Status: models.AgentCapabilityStatusActive},
		)
		if err := database.Create(&models.AgentCapability{AgentID: 2, CapabilityCode: "summarize", Name: "Summarize", CapabilityType: models.AgentCapabilityTypeWorkflow, Version: "v1", Status: models.AgentCapabilityStatusActive}).Error; err != nil {
			t.Fatalf("same capability code on another agent should be allowed: %v", err)
		}
	})

	t.Run("workflow agent and version", func(t *testing.T) {
		first := &models.Workflow{AgentID: 1, Version: 1, DefinitionJSON: `{}`, Checksum: "v1", CreatedBy: 1}
		duplicate := &models.Workflow{AgentID: 1, Version: 1, DefinitionJSON: `{}`, Checksum: "v1-duplicate", CreatedBy: 2}
		assertDuplicateRejected(t, first, duplicate)
		if err := database.Create(&models.Workflow{AgentID: 1, Version: 2, DefinitionJSON: `{}`, Checksum: "v2", CreatedBy: 1}).Error; err != nil {
			t.Fatalf("different workflow version should be allowed: %v", err)
		}
		if err := database.Create(&models.Workflow{AgentID: 2, Version: 1, DefinitionJSON: `{}`, Checksum: "other-v1", CreatedBy: 2}).Error; err != nil {
			t.Fatalf("same workflow version on another agent should be allowed: %v", err)
		}
	})

	t.Run("mcp server owner and code", func(t *testing.T) {
		assertDuplicateRejected(t,
			&models.MCPServer{OwnerUserID: 1, ServerCode: "search", Name: "Search", Transport: models.MCPServerTransportStreamableHTTP, Endpoint: "https://mcp.example.com", AuthType: models.MCPServerAuthTypeNone, Status: models.MCPServerStatusInactive},
			&models.MCPServer{OwnerUserID: 1, ServerCode: "search", Name: "Search v2", Transport: models.MCPServerTransportStreamableHTTP, Endpoint: "https://mcp.example.com/v2", AuthType: models.MCPServerAuthTypeNone, Status: models.MCPServerStatusInactive},
		)
		if err := database.Create(&models.MCPServer{OwnerUserID: 2, ServerCode: "search", Name: "Other Search", Transport: models.MCPServerTransportStreamableHTTP, Endpoint: "https://other.example.com", AuthType: models.MCPServerAuthTypeNone, Status: models.MCPServerStatusInactive}).Error; err != nil {
			t.Fatalf("same server code under another owner must be allowed: %v", err)
		}
	})

	t.Run("mcp tool server and name", func(t *testing.T) {
		assertDuplicateRejected(t,
			&models.MCPTool{ServerID: 1, ToolName: "search", InputSchemaJSON: "{}"},
			&models.MCPTool{ServerID: 1, ToolName: "search", InputSchemaJSON: "{}"},
		)
		if err := database.Create(&models.MCPTool{ServerID: 2, ToolName: "search", InputSchemaJSON: "{}"}).Error; err != nil {
			t.Fatalf("same tool name under another server must be allowed: %v", err)
		}
	})

	t.Run("loop record id and evaluation pair", func(t *testing.T) {
		assertDuplicateRejected(t,
			&models.LoopRecord{LoopID: "loop-1", RunID: "run-1", AgentID: 1, LoopType: models.LoopTypeRun, Status: models.LoopStatusRunning, InputSnapshotJSON: `{}`, OutputSnapshotJSON: `{}`},
			&models.LoopRecord{LoopID: "loop-1", RunID: "run-2", AgentID: 1, LoopType: models.LoopTypeRun, Status: models.LoopStatusRunning, InputSnapshotJSON: `{}`, OutputSnapshotJSON: `{}`},
		)
		assertDuplicateRejected(t,
			&models.LoopEvaluation{LoopID: "loop-1", EvaluatorCode: "quality-v1", Status: models.EvaluationStatusPending},
			&models.LoopEvaluation{LoopID: "loop-1", EvaluatorCode: "quality-v1", Status: models.EvaluationStatusPending},
		)
	})
}

func newTestDelegation(delegationID, childRunID string) *models.Delegation {
	return &models.Delegation{
		DelegationID:     delegationID,
		ThreadID:         "thread-1",
		ParentRunID:      "parent-run-1",
		ChildRunID:       childRunID,
		SourceAgentID:    1,
		TargetAgentID:    2,
		CapabilityCode:   "summarize",
		RequestMessageID: "request-message-1",
		InputJSON:        `{}`,
		Status:           models.DelegationStatusPending,
	}
}

func newTestDelegationGroup(groupID, parentStepKey string) *models.DelegationGroup {
	return &models.DelegationGroup{
		GroupID: groupID, ThreadID: "thread-1", ParentRunID: "parent-run-group", ParentStepKey: parentStepKey,
		CoordinatorDelegationID: "coordinator-1", Strategy: models.DelegationGroupStrategyAll, RequiredSuccesses: 2,
		TotalMembers: 2, Status: models.DelegationGroupStatusWaiting, ResultJSON: "{}",
	}
}
