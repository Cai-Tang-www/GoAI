package services

import "testing"

func TestRunStatusTransitions(t *testing.T) {
	cases := []struct {
		from    string
		to      string
		allowed bool
	}{
		{"pending", "queued", true},
		{"queued", "running", true},
		{"running", "success", true},
		{"running", "failed", true},
		{"pending", "running", false},
		{"success", "running", false},
	}
	for _, tc := range cases {
		got := isValidRunTransition(tc.from, tc.to)
		if got != tc.allowed {
			t.Fatalf("transition %s->%s expected %v got %v", tc.from, tc.to, tc.allowed, got)
		}
	}
}

func TestRunStepStatusTransitions(t *testing.T) {
	cases := []struct {
		from    string
		to      string
		allowed bool
	}{
		{"pending", "running", true},
		{"running", "success", true},
		{"running", "failed", true},
		{"success", "failed", false},
		{"pending", "success", false},
	}
	for _, tc := range cases {
		got := isValidRunStepTransition(tc.from, tc.to)
		if got != tc.allowed {
			t.Fatalf("step transition %s->%s expected %v got %v", tc.from, tc.to, tc.allowed, got)
		}
	}
}
