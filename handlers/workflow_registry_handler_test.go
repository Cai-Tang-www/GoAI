package handlers_test

import (
	"bytes"
	"net/http"
	"testing"
)

func TestWorkflowRegistryAPIUsesRBACAndVersionLifecycle(t *testing.T) {
	fixture := setupRegistryHTTPFixture(t)
	createAgent := registryRequest(t, fixture.router, http.MethodPost, "/api/agents", fixture.ownerToken, map[string]any{
		"agent_code": "writer", "name": "Writer", "description": "writes articles",
	})
	requireRegistryResponse(t, createAgent, http.StatusCreated, "OK")
	definition := map[string]any{
		"entry_node": "start",
		"nodes":      []map[string]any{{"key": "start", "type": "noop"}},
		"edges":      []any{},
	}

	create := registryRequest(t, fixture.router, http.MethodPost, "/api/agents/writer/workflows", fixture.ownerToken, map[string]any{
		"version": 1, "definition": definition,
	})
	created := requireRegistryResponse(t, create, http.StatusCreated, "OK")
	if !bytes.Contains(created.Data, []byte(`"is_active":false`)) || !bytes.Contains(created.Data, []byte(`"checksum"`)) {
		t.Fatalf("workflow create response missing normalized metadata: %s", create.Body.String())
	}

	duplicate := registryRequest(t, fixture.router, http.MethodPost, "/api/agents/writer/workflows", fixture.ownerToken, map[string]any{
		"version": 1, "definition": definition,
	})
	requireRegistryResponse(t, duplicate, http.StatusConflict, "WORKFLOW_ALREADY_EXISTS")

	invalid := registryRequest(t, fixture.router, http.MethodPost, "/api/agents/writer/workflows", fixture.ownerToken, map[string]any{
		"version": 2, "definition": map[string]any{"entry_node": "missing", "nodes": []any{}, "edges": []any{}},
	})
	requireRegistryResponse(t, invalid, http.StatusBadRequest, "VALIDATION_FAILED")

	foreignGet := registryRequest(t, fixture.router, http.MethodGet, "/api/agents/writer/workflows", fixture.otherToken, nil)
	requireRegistryResponse(t, foreignGet, http.StatusForbidden, "AUTH_FORBIDDEN")
	adminGet := registryRequest(t, fixture.router, http.MethodGet, "/api/agents/writer/workflows/1", fixture.adminToken, nil)
	requireRegistryResponse(t, adminGet, http.StatusOK, "OK")

	activate := registryRequest(t, fixture.router, http.MethodPost, "/api/agents/writer/workflows/1/activate", fixture.ownerToken, nil)
	requireRegistryResponse(t, activate, http.StatusOK, "OK")
	updateActive := registryRequest(t, fixture.router, http.MethodPut, "/api/agents/writer/workflows/1", fixture.ownerToken, map[string]any{
		"definition": definition,
	})
	requireRegistryResponse(t, updateActive, http.StatusConflict, "WORKFLOW_INVALID_STATE")

	list := registryRequest(t, fixture.router, http.MethodGet, "/api/agents/writer/workflows", fixture.ownerToken, nil)
	listEnvelope := requireRegistryResponse(t, list, http.StatusOK, "OK")
	if !bytes.Contains(listEnvelope.Data, []byte(`"version":1`)) {
		t.Fatalf("workflow list missing version: %s", list.Body.String())
	}
}

func TestWorkflowRegistryAPIRejectsInvalidVersionPath(t *testing.T) {
	fixture := setupRegistryHTTPFixture(t)
	createAgent := registryRequest(t, fixture.router, http.MethodPost, "/api/agents", fixture.ownerToken, map[string]any{
		"agent_code": "writer", "name": "Writer",
	})
	requireRegistryResponse(t, createAgent, http.StatusCreated, "OK")
	response := registryRequest(t, fixture.router, http.MethodGet, "/api/agents/writer/workflows/not-a-version", fixture.ownerToken, nil)
	requireRegistryResponse(t, response, http.StatusBadRequest, "VALIDATION_FAILED")
}
