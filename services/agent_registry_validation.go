package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"GoAI/models"

	"gorm.io/gorm"
)

var registryCodePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

func normalizeCreateAgentCommand(command CreateAgentCommand) CreateAgentCommand {
	command.AgentCode = strings.TrimSpace(command.AgentCode)
	command.Name = strings.TrimSpace(command.Name)
	command.Description = strings.TrimSpace(command.Description)
	return command
}

func validateCreateAgentCommand(command CreateAgentCommand) error {
	if !registryCodePattern.MatchString(command.AgentCode) {
		return fmt.Errorf("%w: invalid agent_code", errAgentRegistryValidation)
	}
	if command.Name == "" || len(command.Name) > 128 {
		return fmt.Errorf("%w: invalid agent name", errAgentRegistryValidation)
	}
	if len(command.Description) > 4000 {
		return fmt.Errorf("%w: agent description is too long", errAgentRegistryValidation)
	}
	return nil
}

func normalizeCapabilityCommand(command UpsertCapabilityCommand) (UpsertCapabilityCommand, error) {
	command.CapabilityCode = strings.TrimSpace(command.CapabilityCode)
	command.Name = strings.TrimSpace(command.Name)
	command.Description = strings.TrimSpace(command.Description)
	command.CapabilityType = strings.TrimSpace(command.CapabilityType)
	command.Version = strings.TrimSpace(command.Version)
	command.Status = strings.TrimSpace(command.Status)
	if command.Status == "" {
		command.Status = models.AgentCapabilityStatusActive
	}
	var err error
	if command.InputSchemaJSON, err = normalizeJSONObject(command.InputSchemaJSON); err != nil {
		return command, fmt.Errorf("%w: input_schema_json %v", errAgentRegistryValidation, err)
	}
	if command.OutputSchemaJSON, err = normalizeJSONObject(command.OutputSchemaJSON); err != nil {
		return command, fmt.Errorf("%w: output_schema_json %v", errAgentRegistryValidation, err)
	}
	if command.ConfigJSON, err = normalizeJSONObject(command.ConfigJSON); err != nil {
		return command, fmt.Errorf("%w: config_json %v", errAgentRegistryValidation, err)
	}
	return command, nil
}

func validateCapabilityCommand(ctx context.Context, database *gorm.DB, agent models.Agent, command UpsertCapabilityCommand) error {
	if !registryCodePattern.MatchString(command.CapabilityCode) {
		return fmt.Errorf("%w: invalid capability_code", errAgentRegistryValidation)
	}
	if command.Name == "" || len(command.Name) > 128 || len(command.Description) > 4000 {
		return fmt.Errorf("%w: invalid capability metadata", errAgentRegistryValidation)
	}
	if command.Version == "" || len(command.Version) > 32 {
		return fmt.Errorf("%w: invalid capability version", errAgentRegistryValidation)
	}
	if command.Status != models.AgentCapabilityStatusActive && command.Status != models.AgentCapabilityStatusInactive {
		return fmt.Errorf("%w: invalid capability status", errAgentRegistryValidation)
	}
	switch command.CapabilityType {
	case models.AgentCapabilityTypeWorkflow:
		if command.WorkflowID == nil || *command.WorkflowID == 0 {
			return fmt.Errorf("%w: workflow capability requires workflow_id", errAgentRegistryValidation)
		}
		var workflow models.Workflow
		if err := database.WithContext(ctx).
			Where("id = ? AND agent_id = ? AND is_active = ?", *command.WorkflowID, agent.ID, true).
			First(&workflow).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("%w: workflow must belong to the agent and be active", errAgentRegistryValidation)
			}
			return fmt.Errorf("validating capability workflow: %w", err)
		}
		if command.Version != strconv.Itoa(workflow.Version) {
			return fmt.Errorf("%w: capability version must match workflow version", errAgentRegistryValidation)
		}
	case models.AgentCapabilityTypeTool, models.AgentCapabilityTypeCustom:
		if command.WorkflowID != nil {
			return fmt.Errorf("%w: non-workflow capability cannot reference workflow_id", errAgentRegistryValidation)
		}
	default:
		return fmt.Errorf("%w: invalid capability_type", errAgentRegistryValidation)
	}
	return nil
}

func normalizeEndpointCommand(command UpsertEndpointCommand, authRequired bool) (UpsertEndpointCommand, error) {
	command.EndpointCode = strings.TrimSpace(command.EndpointCode)
	command.Protocol = strings.ToLower(strings.TrimSpace(command.Protocol))
	command.Transport = strings.ToLower(strings.TrimSpace(command.Transport))
	command.Address = strings.TrimRight(strings.TrimSpace(command.Address), "/")
	command.AuthType = strings.ToLower(strings.TrimSpace(command.AuthType))
	command.CredentialRef = strings.TrimSpace(command.CredentialRef)
	if command.Protocol == "" {
		command.Protocol = models.AgentEndpointProtocolA2A
	}
	if command.AuthType == "" {
		if authRequired {
			command.AuthType = models.AgentEndpointAuthTypeHMACSHA256
		} else {
			command.AuthType = models.AgentEndpointAuthTypeNone
		}
	}
	var err error
	command.ConfigJSON, err = normalizeJSONObject(command.ConfigJSON)
	if err != nil {
		return command, fmt.Errorf("%w: config_json %v", errAgentRegistryValidation, err)
	}
	if err := validateEndpointConfigJSON(command.ConfigJSON); err != nil {
		return command, fmt.Errorf("%w: config_json %v", errAgentRegistryValidation, err)
	}
	return command, nil
}

func validateEndpointCommand(command UpsertEndpointCommand, authRequired bool) error {
	if !registryCodePattern.MatchString(command.EndpointCode) {
		return fmt.Errorf("%w: invalid endpoint_code", errAgentRegistryValidation)
	}
	if command.Address == "" || len(command.Address) > 512 {
		return fmt.Errorf("%w: invalid endpoint address", errAgentRegistryValidation)
	}
	if command.Protocol != models.AgentEndpointProtocolA2A {
		return fmt.Errorf("%w: endpoint protocol must be a2a", errAgentRegistryValidation)
	}
	parsed, err := url.Parse(command.Address)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("%w: invalid endpoint address", errAgentRegistryValidation)
	}
	switch command.Transport {
	case models.AgentEndpointTransportHTTP:
		if parsed.Scheme != "http" || !isRegistryLoopbackHost(parsed.Hostname()) {
			return fmt.Errorf("%w: HTTP endpoint must use a loopback host", errAgentRegistryValidation)
		}
	case models.AgentEndpointTransportHTTPS:
		if parsed.Scheme != "https" {
			return fmt.Errorf("%w: HTTPS endpoint must use https", errAgentRegistryValidation)
		}
	default:
		return fmt.Errorf("%w: invalid endpoint transport", errAgentRegistryValidation)
	}
	switch command.AuthType {
	case models.AgentEndpointAuthTypeNone:
		if authRequired {
			return fmt.Errorf("%w: A2A authentication requires HMAC endpoints", errAgentRegistryValidation)
		}
		if command.CredentialRef != "" {
			return fmt.Errorf("%w: anonymous endpoint cannot set credential_ref", errAgentRegistryValidation)
		}
	case models.AgentEndpointAuthTypeHMACSHA256:
		if command.CredentialRef == "" || len(command.CredentialRef) > 255 {
			return fmt.Errorf("%w: HMAC endpoint requires credential_ref", errAgentRegistryValidation)
		}
	default:
		return fmt.Errorf("%w: invalid endpoint auth_type", errAgentRegistryValidation)
	}
	return nil
}

func normalizeJSONObject(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "{}", nil
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(raw), &object); err != nil {
		return "", errors.New("must be a valid JSON object")
	}
	normalized, err := json.Marshal(object)
	if err != nil {
		return "", fmt.Errorf("normalizing JSON object: %w", err)
	}
	return string(normalized), nil
}

var endpointSensitiveConfigKeySuffixes = []string{
	"secret",
	"secretref",
	"secretkey",
	"password",
	"passwordref",
	"token",
	"tokenref",
	"apikey",
	"apikeyref",
	"accesskey",
	"privatekey",
	"privatekeyref",
	"credential",
	"credentialref",
	"authorization",
	"jwt",
}

func validateEndpointConfigJSON(raw string) error {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return errors.New("must be a valid JSON object")
	}
	return rejectEndpointSecrets(value, "")
}

func rejectEndpointSecrets(value any, path string) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if isSensitiveEndpointConfigKey(key) {
				fieldPath := joinJSONPath(path, key)
				return fmt.Errorf("contains sensitive field %q; store secrets through credential_ref", fieldPath)
			}
			if err := rejectEndpointSecrets(child, joinJSONPath(path, key)); err != nil {
				return err
			}
		}
	case []any:
		for index, child := range typed {
			if err := rejectEndpointSecrets(child, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	}
	return nil
}

func isSensitiveEndpointConfigKey(key string) bool {
	normalized := strings.NewReplacer("_", "", "-", "", ".", "", " ", "").Replace(strings.ToLower(key))
	if normalized == "credentials" {
		return true
	}
	for _, suffix := range endpointSensitiveConfigKeySuffixes {
		if strings.HasSuffix(normalized, suffix) {
			return true
		}
	}
	return false
}

func safeEndpointConfigJSON(raw string) string {
	normalized, err := normalizeJSONObject(raw)
	if err != nil || validateEndpointConfigJSON(normalized) != nil {
		return "{}"
	}
	return normalized
}

func joinJSONPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return prefix + "." + key
}

func isRegistryLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
