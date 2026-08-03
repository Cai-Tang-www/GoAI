package delegationgroup

import (
	"testing"

	"GoAI/models"
)

func TestEvaluateDelegationGroupStrategies(t *testing.T) {
	tests := []struct {
		name     string
		strategy string
		required int
		statuses []string
		want     string
		ready    bool
	}{
		{name: "all waiting", strategy: models.DelegationGroupStrategyAll, statuses: []string{models.DelegationStatusSucceeded, models.DelegationStatusAccepted}, want: models.DelegationGroupStatusWaiting},
		{name: "all succeeded", strategy: models.DelegationGroupStrategyAll, statuses: []string{models.DelegationStatusSucceeded, models.DelegationStatusSucceeded}, want: models.DelegationGroupStatusSucceeded, ready: true},
		{name: "all failed fast", strategy: models.DelegationGroupStrategyAll, statuses: []string{models.DelegationStatusFailed, models.DelegationStatusAccepted}, want: models.DelegationGroupStatusFailed, ready: true},
		{name: "all cancelled fast", strategy: models.DelegationGroupStrategyAll, statuses: []string{models.DelegationStatusCancelled, models.DelegationStatusAccepted}, want: models.DelegationGroupStatusCancelled, ready: true},
		{name: "any succeeds early", strategy: models.DelegationGroupStrategyAny, statuses: []string{models.DelegationStatusSucceeded, models.DelegationStatusAccepted}, want: models.DelegationGroupStatusSucceeded, ready: true},
		{name: "any waits for active", strategy: models.DelegationGroupStrategyAny, statuses: []string{models.DelegationStatusFailed, models.DelegationStatusAccepted}, want: models.DelegationGroupStatusWaiting},
		{name: "any fails terminal", strategy: models.DelegationGroupStrategyAny, statuses: []string{models.DelegationStatusFailed, models.DelegationStatusCancelled}, want: models.DelegationGroupStatusFailed, ready: true},
		{name: "quorum succeeds", strategy: models.DelegationGroupStrategyQuorum, required: 2, statuses: []string{models.DelegationStatusSucceeded, models.DelegationStatusSucceeded, models.DelegationStatusAccepted}, want: models.DelegationGroupStatusSucceeded, ready: true},
		{name: "quorum impossible", strategy: models.DelegationGroupStrategyQuorum, required: 2, statuses: []string{models.DelegationStatusSucceeded, models.DelegationStatusFailed, models.DelegationStatusCancelled}, want: models.DelegationGroupStatusFailed, ready: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decision, err := Evaluate(test.strategy, test.required, test.statuses)
			if err != nil {
				t.Fatalf("Evaluate() error: %v", err)
			}
			if decision.Status != test.want || decision.Ready != test.ready {
				t.Fatalf("decision got status=%s ready=%v want status=%s ready=%v", decision.Status, decision.Ready, test.want, test.ready)
			}
		})
	}
}

func TestEvaluateDelegationGroupRejectsInvalidInput(t *testing.T) {
	if _, err := Evaluate(models.DelegationGroupStrategyAll, 0, nil); err == nil {
		t.Fatal("expected empty members error")
	}
	if _, err := Evaluate(models.DelegationGroupStrategyQuorum, 3, []string{models.DelegationStatusAccepted, models.DelegationStatusAccepted}); err == nil {
		t.Fatal("expected invalid quorum error")
	}
	if _, err := Evaluate("unknown", 0, []string{models.DelegationStatusAccepted}); err == nil {
		t.Fatal("expected unknown strategy error")
	}
	if _, err := Evaluate(models.DelegationGroupStrategyAll, 0, []string{"unknown"}); err == nil {
		t.Fatal("expected unknown member status error")
	}
}
