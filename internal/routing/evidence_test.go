package routing

import (
	"testing"
	"time"
)

// TestEvidenceRelevance covers R4's task-kind, baseline, recorded evidence, and verdict rules.
func TestEvidenceRelevance(t *testing.T) {
	baseline := Pair{Harness: "codex", Model: "default"}
	alternate := Pair{Harness: "kimi", Model: "alternate"}
	better := comparisonFor(baseline, alternate)
	otherTask := better
	otherTask.TaskKind = "review"
	otherDefault := comparisonFor(Pair{Harness: "codex", Model: "old-default"}, alternate)
	missingTaskSet := better
	missingTaskSet.TaskSetID = ""
	unrecorded := better
	unrecorded.RecordedAt = time.Time{}
	latest := better
	latest.RecordedAt = better.RecordedAt.Add(time.Second)
	latest.Verdict = Inconclusive
	conflicting := better
	conflicting.Verdict = Worse
	for _, test := range []struct {
		name     string
		evidence []Evidence
		want     Pair
	}{
		{"same kind and default", []Evidence{better}, alternate},
		{"wrong kind", []Evidence{otherTask}, baseline},
		{"wrong default", []Evidence{otherDefault}, baseline},
		{"missing task set", []Evidence{missingTaskSet}, baseline},
		{"unrecorded comparison", []Evidence{unrecorded}, baseline},
		{"newer inconclusive comparison", []Evidence{latest, better}, baseline},
		{"equally recent worse comparison", []Evidence{conflicting, better}, baseline},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := Select(Request{
				HostOS: "windows", TaskKind: "build", Default: baseline, Evidence: test.evidence, Permitted: allowPair,
				Candidates: []Candidate{qualifiedCandidate(baseline), qualifiedCandidate(alternate)},
			})
			if !got.Eligible() || got.Pair != test.want {
				t.Fatalf("Select() = %+v, want %v", got, test.want)
			}
		})
	}
}

// TestMultipleAlternatives covers R2 when more than one eligible pair has better evidence.
func TestMultipleAlternatives(t *testing.T) {
	baseline := Pair{Harness: "codex", Model: "default"}
	first := Pair{Harness: "claude-code", Model: "first"}
	second := Pair{Harness: "kimi", Model: "second"}
	older := comparisonFor(baseline, first)
	newer := comparisonFor(baseline, second)
	newer.RecordedAt = older.RecordedAt.Add(time.Second)
	req := Request{
		HostOS: "windows", TaskKind: "build", Default: baseline, Permitted: allowPair,
		Candidates: []Candidate{qualifiedCandidate(baseline), qualifiedCandidate(first), qualifiedCandidate(second)},
		Evidence:   []Evidence{newer, older},
	}
	if got := Select(req); got.Pair != second {
		t.Fatalf("Select() = %+v, want newest comparison for %v", got, second)
	}
	req.Permitted = func(pair Pair) bool { return pair != second }
	if got := Select(req); got.Pair != first {
		t.Fatalf("Select() = %+v, want eligible alternative %v", got, first)
	}
	req.Permitted = allowPair
	req.Evidence[0].RecordedAt = older.RecordedAt
	if got := Select(req); got.Pair != first {
		t.Fatalf("Select() = %+v, want first configured alternative %v", got, first)
	}
}
