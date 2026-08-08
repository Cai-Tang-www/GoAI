package runstate

import (
	"testing"

	"GoAI/models"
)

func TestIsValidDelegationTransition(t *testing.T) {
	tests := []struct {
		name string
		from string
		to   string
		want bool
	}{
		{name: "pending to accepted", from: models.DelegationStatusPending, to: models.DelegationStatusAccepted, want: true},
		{name: "pending to success for fast callback", from: models.DelegationStatusPending, to: models.DelegationStatusSucceeded, want: true},
		{name: "pending to failed", from: models.DelegationStatusPending, to: models.DelegationStatusFailed, want: true},
		{name: "accepted to running", from: models.DelegationStatusAccepted, to: models.DelegationStatusRunning, want: true},
		{name: "accepted to success", from: models.DelegationStatusAccepted, to: models.DelegationStatusSucceeded, want: true},
		{name: "running to success", from: models.DelegationStatusRunning, to: models.DelegationStatusSucceeded, want: true},
		{name: "running to failed", from: models.DelegationStatusRunning, to: models.DelegationStatusFailed, want: true},
		{name: "running to cancelled", from: models.DelegationStatusRunning, to: models.DelegationStatusCancelled, want: true},
		{name: "terminal cannot restart", from: models.DelegationStatusSucceeded, to: models.DelegationStatusRunning, want: false},
		{name: "cannot skip backward", from: models.DelegationStatusRunning, to: models.DelegationStatusAccepted, want: false},
		{name: "unknown source", from: "unknown", to: models.DelegationStatusFailed, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsValidDelegationTransition(test.from, test.to); got != test.want {
				t.Fatalf("IsValidDelegationTransition(%q, %q)=%v want=%v", test.from, test.to, got, test.want)
			}
		})
	}
}

func TestRunWaitingExternalTransitions(t *testing.T) {
	tests := []struct {
		name string
		from string
		to   string
		want bool
	}{
		{name: "running can suspend", from: models.RunStatusRunning, to: models.RunStatusWaitingExternal, want: true},
		{name: "waiting can resume", from: models.RunStatusWaitingExternal, to: models.RunStatusRunning, want: true},
		{name: "waiting cannot skip claim", from: models.RunStatusWaitingExternal, to: models.RunStatusSuccess, want: false},
		{name: "terminal cannot suspend", from: models.RunStatusSuccess, to: models.RunStatusWaitingExternal, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsValidRunTransition(test.from, test.to); got != test.want {
				t.Fatalf("IsValidRunTransition(%q, %q)=%v want=%v", test.from, test.to, got, test.want)
			}
		})
	}
}

func TestRunCancellationTransitions(t *testing.T) {
	tests := []struct {
		name string
		from string
		want bool
	}{
		{name: "pending can cancel", from: models.RunStatusPending, want: true},
		{name: "queued can cancel", from: models.RunStatusQueued, want: true},
		{name: "running can cancel", from: models.RunStatusRunning, want: true},
		{name: "waiting can cancel", from: models.RunStatusWaitingExternal, want: true},
		{name: "success cannot cancel", from: models.RunStatusSuccess, want: false},
		{name: "failed cannot cancel", from: models.RunStatusFailed, want: false},
		{name: "cancelled cannot cancel again", from: models.RunStatusCancelled, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsValidRunTransition(test.from, models.RunStatusCancelled); got != test.want {
				t.Fatalf("IsValidRunTransition(%q, %q)=%v want=%v", test.from, models.RunStatusCancelled, got, test.want)
			}
		})
	}
}

func TestRunStepWaitingExternalTransitions(t *testing.T) {
	tests := []struct {
		name string
		from string
		to   string
		want bool
	}{
		{name: "running can suspend", from: models.RunStepStatusRunning, to: models.RunStepStatusWaitingExternal, want: true},
		{name: "waiting can complete", from: models.RunStepStatusWaitingExternal, to: models.RunStepStatusSuccess, want: true},
		{name: "waiting can fail", from: models.RunStepStatusWaitingExternal, to: models.RunStepStatusFailed, want: true},
		{name: "completed cannot suspend", from: models.RunStepStatusSuccess, to: models.RunStepStatusWaitingExternal, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := IsValidRunStepTransition(test.from, test.to); got != test.want {
				t.Fatalf("IsValidRunStepTransition(%q, %q)=%v want=%v", test.from, test.to, got, test.want)
			}
		})
	}
}
