package docs_test

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestOpenAPIContractIsReadable verifies that the checked-in contract remains a usable OpenAPI document.
func TestOpenAPIContractIsReadable(t *testing.T) {
	raw, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatalf("read OpenAPI contract: %v", err)
	}

	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatalf("parse OpenAPI YAML: %v", err)
	}
	for _, key := range []string{"openapi", "info", "paths", "components"} {
		if _, ok := document[key]; !ok {
			t.Fatalf("OpenAPI document is missing %q", key)
		}
	}
	if version, ok := document["openapi"].(string); !ok || version != "3.0.3" {
		t.Fatalf("unexpected OpenAPI version: %#v", document["openapi"])
	}

	paths, ok := document["paths"].(map[string]any)
	if !ok {
		t.Fatalf("OpenAPI paths must be an object")
	}
	for _, path := range []string{
		"/api/agents/{agent_code}/agui",
		"/a2a/agents/{agent_code}/message:send",
		"/api/runs/{run_id}",
		"/api/agents/{agent_code}/workflows/{version}",
		"/api/agents/{agent_code}/capabilities",
		"/api/agents/{agent_code}/endpoints",
		"/api/mcp/servers/{server_code}/tools",
		"/a2a/agents/{agent_code}/callbacks/tasks/{task_id}",
	} {
		if _, ok := paths[path]; !ok {
			t.Fatalf("OpenAPI contract is missing required path %q", path)
		}
	}
	callback, ok := paths["/a2a/agents/{agent_code}/callbacks/tasks/{task_id}"].(map[string]any)
	if !ok {
		t.Fatalf("A2A callback path must be an object")
	}
	post, ok := callback["post"].(map[string]any)
	if !ok {
		t.Fatalf("A2A callback path must define POST")
	}
	parameters, ok := post["parameters"].([]any)
	if !ok {
		t.Fatalf("A2A callback POST must define parameters")
	}
	assertParameterReference(t, parameters, "#/components/parameters/A2ANotificationToken")
	assertParameterReference(t, parameters, "#/components/parameters/TraceID")
	assertOpenAPIReferences(t, document, document)
}

func assertParameterReference(t *testing.T, parameters []any, want string) {
	t.Helper()
	for _, parameter := range parameters {
		if value, ok := parameter.(map[string]any); ok && value["$ref"] == want {
			return
		}
	}
	t.Fatalf("OpenAPI operation is missing parameter reference %q", want)
}

func assertOpenAPIReferences(t *testing.T, root map[string]any, value any) {
	t.Helper()
	switch typed := value.(type) {
	case map[string]any:
		if reference, ok := typed["$ref"].(string); ok {
			const prefix = "#/components/"
			if !strings.HasPrefix(reference, prefix) {
				t.Fatalf("unsupported OpenAPI reference %q", reference)
			}
			parts := strings.Split(strings.TrimPrefix(reference, prefix), "/")
			if len(parts) != 2 {
				t.Fatalf("malformed OpenAPI reference %q", reference)
			}
			components, ok := root["components"].(map[string]any)
			if !ok {
				t.Fatalf("OpenAPI components must be an object")
			}
			section, ok := components[parts[0]].(map[string]any)
			if !ok {
				t.Fatalf("OpenAPI component section %q is missing", parts[0])
			}
			if _, ok := section[parts[1]]; !ok {
				t.Fatalf("OpenAPI reference %q is unresolved", reference)
			}
		}
		for _, child := range typed {
			assertOpenAPIReferences(t, root, child)
		}
	case []any:
		for _, child := range typed {
			assertOpenAPIReferences(t, root, child)
		}
	}
}
