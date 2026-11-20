package routing

import "time"

// Verdict describes how the alternative performed relative to the configured default.
type Verdict string

const (
	Better       Verdict = "better"
	Worse        Verdict = "worse"
	Inconclusive Verdict = "inconclusive"
)

// Assessment records the observations used to judge one pair on a held-out task set.
type Assessment struct {
	SatisfiedRequirements []string `json:"satisfied_requirements"`
	CriticalMistakes      []string `json:"critical_mistakes"`
}

// Evidence records a decision 11 comparison on representative held-out tasks.
type Evidence struct {
	TaskSetID         string     `json:"task_set_id"`
	TaskKind          string     `json:"task_kind"`
	Default           Pair       `json:"default"`
	Alternative       Pair       `json:"alternative"`
	DefaultResult     Assessment `json:"default_result"`
	AlternativeResult Assessment `json:"alternative_result"`
	Verdict           Verdict    `json:"verdict"`
	RecordedAt        time.Time  `json:"recorded_at"`
}

// bestAlternative finds the eligible alternative with the newest better comparison.
func bestAlternative(req Request) *Evidence {
	latest := make(map[Pair]*Evidence)
	for i := range req.Evidence {
		comparison := &req.Evidence[i]
		if !comparison.applies(req) {
			continue
		}
		prior := latest[comparison.Alternative]
		// A later or equally recent inconclusive result must not leave a stale preference active.
		if prior == nil || comparison.RecordedAt.After(prior.RecordedAt) ||
			comparison.RecordedAt.Equal(prior.RecordedAt) && comparison.Verdict != Better {
			latest[comparison.Alternative] = comparison
		}
	}
	var best *Evidence
	// Configuration order breaks ties between equally recent comparisons for different pairs.
	for _, candidate := range req.Candidates {
		comparison := latest[candidate.Pair]
		if comparison != nil && comparison.Verdict == Better &&
			(best == nil || comparison.RecordedAt.After(best.RecordedAt)) && eligible(req, candidate.Pair) {
			best = comparison
		}
	}
	return best
}

// applies checks that the comparison is recorded and relevant to this routing request.
func (e Evidence) applies(req Request) bool {
	return req.TaskKind != "" && e.TaskKind == req.TaskKind && e.TaskSetID != "" &&
		req.Default.Harness != "" && req.Default.Model != "" && e.Default == req.Default &&
		e.Alternative != e.Default && !e.RecordedAt.IsZero() &&
		(e.Verdict == Better || e.Verdict == Worse || e.Verdict == Inconclusive)
}
