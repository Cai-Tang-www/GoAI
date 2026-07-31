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
		"agent_endpoints",
		"agent_capabilities",
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
		{&models.AgentEndpoint{}, "idx_agent_endpoint_unique"},
		{&models.AgentCapability{}, "idx_agent_capability_unique"},
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

	t.Run("agent endpoint code", func(t *testing.T) {
		assertDuplicateRejected(t,
			&models.AgentEndpoint{AgentID: 1, EndpointCode: "primary", Protocol: models.AgentEndpointProtocolA2A, Transport: models.AgentEndpointTransportLocal, Address: "local://agent-1", Status: models.AgentEndpointStatusActive},
			&models.AgentEndpoint{AgentID: 1, EndpointCode: "primary", Protocol: models.AgentEndpointProtocolA2A, Transport: models.AgentEndpointTransportHTTPS, Address: "https://agent.example/a2a", Status: models.AgentEndpointStatusActive},
		)
		if err := database.Create(&models.AgentEndpoint{AgentID: 2, EndpointCode: "primary", Protocol: models.AgentEndpointProtocolA2A, Transport: models.AgentEndpointTransportLocal, Address: "local://agent-2", Status: models.AgentEndpointStatusActive}).Error; err != nil {
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
