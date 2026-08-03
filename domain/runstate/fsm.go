package runstate

import "GoAI/models"

// IsValidRunTransition 判断 Run 状态是否允许显式迁移。
func IsValidRunTransition(from, to string) bool {
	transitions := map[string]map[string]struct{}{
		models.RunStatusPending: {
			models.RunStatusQueued: {},
		},
		models.RunStatusQueued: {
			models.RunStatusRunning:   {},
			models.RunStatusFailed:    {},
			models.RunStatusCancelled: {},
		},
		models.RunStatusRunning: {
			models.RunStatusWaitingExternal: {},
			models.RunStatusSuccess:         {},
			models.RunStatusFailed:          {},
			models.RunStatusCancelled:       {},
		},
		models.RunStatusWaitingExternal: {
			models.RunStatusRunning:   {},
			models.RunStatusFailed:    {},
			models.RunStatusCancelled: {},
		},
	}
	next, ok := transitions[from]
	if !ok {
		return false
	}
	_, allowed := next[to]
	return allowed
}

// IsValidRunStepTransition 判断 RunStep 状态是否允许显式迁移。
func IsValidRunStepTransition(from, to string) bool {
	transitions := map[string]map[string]struct{}{
		models.RunStepStatusPending: {
			models.RunStepStatusRunning: {},
			models.RunStepStatusSkipped: {},
		},
		models.RunStepStatusRunning: {
			models.RunStepStatusWaitingExternal: {},
			models.RunStepStatusSuccess:         {},
			models.RunStepStatusFailed:          {},
			models.RunStepStatusSkipped:         {},
		},
		models.RunStepStatusWaitingExternal: {
			models.RunStepStatusSuccess: {},
			models.RunStepStatusFailed:  {},
			models.RunStepStatusSkipped: {},
		},
	}
	next, ok := transitions[from]
	if !ok {
		return false
	}
	_, allowed := next[to]
	return allowed
}

// IsValidDelegationTransition 判断多 Agent 委派状态是否允许显式迁移。
func IsValidDelegationTransition(from, to string) bool {
	transitions := map[string]map[string]struct{}{
		models.DelegationStatusPending: {
			models.DelegationStatusAccepted:  {},
			models.DelegationStatusSucceeded: {},
			models.DelegationStatusFailed:    {},
			models.DelegationStatusCancelled: {},
		},
		models.DelegationStatusAccepted: {
			models.DelegationStatusRunning:   {},
			models.DelegationStatusSucceeded: {},
			models.DelegationStatusFailed:    {},
			models.DelegationStatusCancelled: {},
		},
		models.DelegationStatusRunning: {
			models.DelegationStatusSucceeded: {},
			models.DelegationStatusFailed:    {},
			models.DelegationStatusCancelled: {},
		},
	}
	next, ok := transitions[from]
	if !ok {
		return false
	}
	_, allowed := next[to]
	return allowed
}
