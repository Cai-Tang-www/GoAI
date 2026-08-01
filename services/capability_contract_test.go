package services

import "testing"

func TestValidateJSONSchemaCapabilityContract(t *testing.T) {
	schema := `{"type":"object","required":["prompt"],"properties":{"prompt":{"type":"string"}},"additionalProperties":false}`
	if err := validateJSONSchema(schema, `{"prompt":"draft"}`); err != nil {
		t.Fatalf("valid payload rejected: %v", err)
	}
	for name, payload := range map[string]string{
		"missing required": `{}`,
		"wrong type":       `{"prompt":42}`,
		"additional field": `{"prompt":"draft","extra":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateJSONSchema(schema, payload); err == nil {
				t.Fatal("expected schema validation error")
			}
		})
	}
}

func TestValidateJSONSchemaAllowsUnconfiguredContract(t *testing.T) {
	if err := validateJSONSchema("", `{"anything":true}`); err != nil {
		t.Fatalf("empty schema should be optional: %v", err)
	}
}

func TestValidateJSONSchemaRejectsMalformedSchema(t *testing.T) {
	for name, schema := range map[string]string{
		"required is not an array":         `{"type":"object","required":"prompt"}`,
		"required contains an empty name":  `{"type":"object","required":[""]}`,
		"unknown type":                     `{"type":"map"}`,
		"properties is not an object":      `{"type":"object","properties":[]}`,
		"additionalProperties is not bool": `{"type":"object","additionalProperties":"false"}`,
		"items is not an object":           `{"type":"array","items":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateJSONSchema(schema, `{}`); err == nil {
				t.Fatal("expected malformed schema error")
			}
		})
	}
}
