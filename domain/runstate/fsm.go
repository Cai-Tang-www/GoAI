package runstate

import "GoAI/models"

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
			models.RunStatusSuccess:   {},
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

func IsValidRunStepTransition(from, to string) bool {
	transitions := map[string]map[string]struct{}{
		models.RunStepStatusPending: {
			models.RunStepStatusRunning: {},
			models.RunStepStatusSkipped: {},
		},
		models.RunStepStatusRunning: {
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
