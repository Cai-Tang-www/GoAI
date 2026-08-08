package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"GoAI/models"

	"gorm.io/gorm"
)

type preparedAgentInvocation struct {
	source         models.Agent
	sourceEndpoint models.AgentEndpoint
	target         models.Agent
	capability     models.AgentCapability
	route          *AgentRoute
	request        AgentInvocationRequest
	routingPolicy  string
	timeout        time.Duration
}

func (s *RunService) prepareAgentInvocation(ctx context.Context, run *models.Run, node WorkflowNode, targetAgent, capabilityCode, routingPolicy string, timeoutMS int) (*preparedAgentInvocation, error) {
	if run == nil {
		return nil, errors.New("preparing Agent invocation: run is nil")
	}
	var source models.Agent
	if err := s.database.WithContext(ctx).Where("id = ? AND status = ?", run.AgentID, models.AgentStatusActive).First(&source).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errAgentNotFound
		}
		return nil, fmt.Errorf("loading source agent: %w", err)
	}
	targetAgent = strings.TrimSpace(targetAgent)
	if targetAgent != "" && source.AgentCode == targetAgent {
		return nil, fmt.Errorf("%w: agent node %s cannot target source agent %s", errAgentRouteInvalid, node.Key, source.AgentCode)
	}
	if s.agentRouter == nil {
		return nil, fmt.Errorf("%w: agent router is not configured", errAgentRouteInvalid)
	}
	route, err := s.agentRouter.Route(ctx, AgentRouteRequest{
		SourceAgentID: source.ID, CapabilityCode: strings.TrimSpace(capabilityCode), PreferredAgentCode: targetAgent,
	})
	if err != nil {
		return nil, err
	}
	if route == nil {
		return nil, fmt.Errorf("%w: agent router returned an empty route", errAgentRouteInvalid)
	}
	target := route.Agent
	if source.ID == target.ID {
		return nil, fmt.Errorf("%w: agent node %s cannot target source agent %s", errAgentRouteInvalid, node.Key, source.AgentCode)
	}
	var sourceEndpoint models.AgentEndpoint
	if err := s.database.WithContext(ctx).
		Where("agent_id = ? AND protocol = ? AND status = ?", source.ID, models.AgentEndpointProtocolA2A, models.AgentEndpointStatusActive).
		Order("id ASC").First(&sourceEndpoint).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			sourceEndpoint.AuthType = models.AgentEndpointAuthTypeNone
		} else {
			return nil, fmt.Errorf("loading source A2A identity endpoint: %w", err)
		}
	}
	if timeoutMS < 0 || timeoutMS > 300000 {
		return nil, fmt.Errorf("agent node %s timeout_ms must be between 0 and 300000", node.Key)
	}
	timeout := 120 * time.Second
	if timeoutMS > 0 {
		timeout = time.Duration(timeoutMS) * time.Millisecond
	}
	if routingPolicy == "" {
		routingPolicy = "explicit"
	}
	return &preparedAgentInvocation{
		source: source, sourceEndpoint: sourceEndpoint, target: target, capability: route.Capability, route: route,
		routingPolicy: routingPolicy, timeout: timeout,
		request: AgentInvocationRequest{
			SourceAgentCode: source.AgentCode, SourceAuthType: sourceEndpoint.AuthType, SourceCredentialRef: sourceEndpoint.CredentialRef,
			TargetAgentCode: target.AgentCode, CapabilityCode: route.Capability.CapabilityCode, ParentRunID: run.RunID,
			TraceID: run.TraceID, DelegationID: stableA2AID("delegation", run.RunID, node.Key), ThreadID: run.ThreadID,
			TaskID: stableA2AID("task", run.RunID, node.Key), MessageID: stableA2AID("message", run.RunID, node.Key),
			InputJSON: run.InputJSON, Endpoints: []AgentInvocationEndpoint{{Address: route.Endpoint.Address, Transport: route.Endpoint.Transport}},
		},
	}, nil
}

func (s *RunService) newAgentAsToolForNode(ctx context.Context, run *models.Run, node WorkflowNode, targetAgent, capabilityCode, routingPolicy string, toolName string, timeoutMS int, resultType string) (*AgentAsTool, error) {
	prepared, err := s.prepareAgentInvocation(ctx, run, node, targetAgent, capabilityCode, routingPolicy, timeoutMS)
	if err != nil {
		return nil, err
	}
	if toolName == "" {
		toolName = generatedAgentToolName(prepared.target.AgentCode, prepared.capability.CapabilityCode)
	}
	return NewAgentAsTool(AgentAsToolConfig{
		ToolName: toolName, Description: prepared.capability.Description,
		InputSchemaJSON: prepared.capability.InputSchemaJSON, OutputSchemaJSON: prepared.capability.OutputSchemaJSON,
		Invoker: s.agentInvoker, Invocation: prepared.request, SourceAgentID: prepared.source.ID, TargetAgentID: prepared.target.ID,
		SelectionReason: prepared.route.SelectionReason, WorkflowVersion: prepared.route.Workflow.Version,
		RoutingPolicy: prepared.routingPolicy,
		Timeout:       prepared.timeout, ResultType: resultType,
	})
}

func generatedAgentToolName(targetAgent, capability string) string {
	name := "agent_" + strings.TrimSpace(targetAgent) + "_" + strings.TrimSpace(capability)
	if len(name) <= 64 {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	return "agent_" + hex.EncodeToString(sum[:])[:56]
}
