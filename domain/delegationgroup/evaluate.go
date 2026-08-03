package delegationgroup

import (
	"errors"
	"fmt"

	"GoAI/models"
)

// Counts 汇总 DelegationGroup 中各类成员状态。
type Counts struct {
	Total     int
	Succeeded int
	Failed    int
	Cancelled int
	Active    int
}

// Decision 描述当前成员集合是否已经满足 fan-in 终态条件。
type Decision struct {
	Status string
	Ready  bool
	Counts Counts
}

// Evaluate 根据聚合策略计算 group 是否成功、失败、取消或继续等待。
func Evaluate(strategy string, requiredSuccesses int, memberStatuses []string) (Decision, error) {
	if len(memberStatuses) == 0 {
		return Decision{}, errors.New("delegation group members are empty")
	}
	counts := Counts{Total: len(memberStatuses)}
	for _, status := range memberStatuses {
		switch status {
		case models.DelegationStatusSucceeded:
			counts.Succeeded++
		case models.DelegationStatusFailed:
			counts.Failed++
		case models.DelegationStatusCancelled:
			counts.Cancelled++
		case models.DelegationStatusPending, models.DelegationStatusAccepted, models.DelegationStatusRunning:
			counts.Active++
		default:
			return Decision{}, fmt.Errorf("unsupported delegation member status %q", status)
		}
	}

	decision := Decision{Status: models.DelegationGroupStatusWaiting, Counts: counts}
	switch strategy {
	case models.DelegationGroupStrategyAll:
		if counts.Failed > 0 {
			decision.Status, decision.Ready = models.DelegationGroupStatusFailed, true
		} else if counts.Cancelled > 0 {
			decision.Status, decision.Ready = models.DelegationGroupStatusCancelled, true
		} else if counts.Succeeded == counts.Total {
			decision.Status, decision.Ready = models.DelegationGroupStatusSucceeded, true
		}
	case models.DelegationGroupStrategyAny:
		if counts.Succeeded > 0 {
			decision.Status, decision.Ready = models.DelegationGroupStatusSucceeded, true
		} else if counts.Active == 0 {
			decision.Status, decision.Ready = terminalFailureStatus(counts), true
		}
	case models.DelegationGroupStrategyQuorum:
		if requiredSuccesses < 1 || requiredSuccesses > counts.Total {
			return Decision{}, fmt.Errorf("required successes must be between 1 and %d", counts.Total)
		}
		if counts.Succeeded >= requiredSuccesses {
			decision.Status, decision.Ready = models.DelegationGroupStatusSucceeded, true
		} else if counts.Succeeded+counts.Active < requiredSuccesses {
			decision.Status, decision.Ready = terminalFailureStatus(counts), true
		}
	default:
		return Decision{}, fmt.Errorf("unsupported delegation group strategy %q", strategy)
	}
	return decision, nil
}

func terminalFailureStatus(counts Counts) string {
	if counts.Failed > 0 {
		return models.DelegationGroupStatusFailed
	}
	return models.DelegationGroupStatusCancelled
}
