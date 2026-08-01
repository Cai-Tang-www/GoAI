package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"GoAI/models"

	"gorm.io/gorm"
)

// validateCapabilityInput 校验委派请求是否满足目标 capability 声明的输入契约。
func validateCapabilityInput(schemaJSON, inputJSON string) error {
	return validateJSONSchema(schemaJSON, inputJSON)
}

// validateRunOutputContract 校验 Child Run 的最终输出是否满足其委派 capability 的输出契约。
func (s *RunService) validateRunOutputContract(ctx context.Context, run *models.Run, outputJSON string) error {
	if run == nil {
		return errors.New("run is nil")
	}
	var delegation models.Delegation
	if err := s.database.WithContext(ctx).Where("child_run_id = ?", run.RunID).First(&delegation).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return fmt.Errorf("loading child delegation for output contract: %w", err)
	}
	var capability models.AgentCapability
	if err := s.database.WithContext(ctx).
		Where("agent_id = ? AND capability_code = ?", delegation.TargetAgentID, delegation.CapabilityCode).
		First(&capability).Error; err != nil {
		return fmt.Errorf("loading child capability for output contract: %w", err)
	}
	if err := validateJSONSchema(capability.OutputSchemaJSON, outputJSON); err != nil {
		return fmt.Errorf("child output violates capability %s contract: %w", capability.CapabilityCode, err)
	}
	return nil
}

// validateJSONSchema implements the small, stable JSON Schema subset used by V1
// capability contracts: type, required, properties, items and additionalProperties.
// It intentionally avoids coupling the protocol boundary to a third-party schema engine.
func validateJSONSchema(schemaJSON, documentJSON string) error {
	if strings.TrimSpace(schemaJSON) == "" {
		return nil
	}
	var schema any
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		return fmt.Errorf("capability schema is invalid JSON: %w", err)
	}
	var document any
	if err := json.Unmarshal([]byte(documentJSON), &document); err != nil {
		return fmt.Errorf("capability payload is invalid JSON: %w", err)
	}
	return validateJSONSchemaValue(schema, document, "$")
}

func validateJSONSchemaValue(schema, value any, path string) error {
	if schema == nil {
		return nil
	}
	object, ok := schema.(map[string]any)
	if !ok {
		return fmt.Errorf("schema at %s must be an object", path)
	}
	if err := validateJSONSchemaObject(object, path); err != nil {
		return err
	}
	if required, exists := object["required"]; exists {
		requiredFields := required.([]any)
		valueObject, isObject := value.(map[string]any)
		if !isObject {
			return fmt.Errorf("%s must be an object for required fields", path)
		}
		for _, item := range requiredFields {
			name, isString := item.(string)
			if _, exists := valueObject[name]; !exists {
				return fmt.Errorf("%s.%s is required", path, name)
			}
			if !isString {
				return fmt.Errorf("schema required at %s must contain strings", path)
			}
		}
	}
	if typeName, exists := object["type"]; exists && !jsonTypeMatches(typeName.(string), value) {
		return fmt.Errorf("%s must be %s", path, typeName)
	}

	if properties, exists := object["properties"]; exists {
		propertySchemas := properties.(map[string]any)
		valueObject, isObject := value.(map[string]any)
		if !isObject {
			return fmt.Errorf("%s must be an object", path)
		}
		for name, propertySchema := range propertySchemas {
			property, exists := valueObject[name]
			if !exists {
				continue
			}
			if err := validateJSONSchemaValue(propertySchema, property, path+"."+name); err != nil {
				return err
			}
		}
		if additional := object["additionalProperties"]; additional != nil && !additional.(bool) {
			for name := range valueObject {
				if _, declared := propertySchemas[name]; !declared {
					return fmt.Errorf("%s.%s is not allowed", path, name)
				}
			}
		}
	}
	if items, exists := object["items"]; exists {
		values, isArray := value.([]any)
		if !isArray {
			return fmt.Errorf("%s must be an array", path)
		}
		for index, item := range values {
			if err := validateJSONSchemaValue(items, item, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateJSONSchemaObject(object map[string]any, path string) error {
	if rawType, exists := object["type"]; exists {
		typeName, ok := rawType.(string)
		if !ok || !isSupportedJSONType(typeName) {
			return fmt.Errorf("schema type at %s must be a supported string", path)
		}
	}
	if rawRequired, exists := object["required"]; exists {
		required, ok := rawRequired.([]any)
		if !ok {
			return fmt.Errorf("schema required at %s must be an array", path)
		}
		for _, item := range required {
			name, ok := item.(string)
			if !ok || strings.TrimSpace(name) == "" {
				return fmt.Errorf("schema required at %s must contain non-empty strings", path)
			}
		}
	}
	if rawProperties, exists := object["properties"]; exists {
		properties, ok := rawProperties.(map[string]any)
		if !ok {
			return fmt.Errorf("schema properties at %s must be an object", path)
		}
		for name, propertySchema := range properties {
			if _, ok := propertySchema.(map[string]any); !ok {
				return fmt.Errorf("schema property %s.%s must be an object", path, name)
			}
		}
	}
	if rawAdditional, exists := object["additionalProperties"]; exists {
		if _, ok := rawAdditional.(bool); !ok {
			return fmt.Errorf("schema additionalProperties at %s must be boolean", path)
		}
	}
	if rawItems, exists := object["items"]; exists {
		if _, ok := rawItems.(map[string]any); !ok {
			return fmt.Errorf("schema items at %s must be an object", path)
		}
	}
	return nil
}

func isSupportedJSONType(typeName string) bool {
	switch typeName {
	case "object", "array", "string", "number", "integer", "boolean", "null":
		return true
	default:
		return false
	}
}

func jsonTypeMatches(typeName string, value any) bool {
	switch typeName {
	case "object":
		_, ok := value.(map[string]any)
		return ok
	case "array":
		_, ok := value.([]any)
		return ok
	case "string":
		_, ok := value.(string)
		return ok
	case "number":
		_, ok := value.(float64)
		return ok
	case "integer":
		number, ok := value.(float64)
		return ok && number == float64(int64(number))
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "null":
		return value == nil
	default:
		return false
	}
}
