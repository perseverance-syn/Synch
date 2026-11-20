package routing

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"synch/internal/engine"
	"synch/internal/thread"
)

// --- Selection matrix: exercise pin, default, and alternative precedence ---

// TestSelectionMatrix covers R2, R3, and R5 across pin sources, eligibility, and evidence verdicts.
func TestSelectionMatrix(t *testing.T) {
	baseline := Pair{Harness: "codex", Model: "default"}
	alternate := Pair{Harness: "claude-code", Model: "alternate"}
	pinned := Pair{Harness: "kimi", Model: "pinned"}
	options := []string{"qualified", "missing", "gap", "denied"}
	for _, source := range []string{"none", "pin", "replacement", "both"} {
		for _, pinStatus := range options {
			for _, defaultStatus := range options {
				for _, altStatus := range options {
					for _, verdict := range []Verdict{"", Better, Worse, Inconclusive} {
						name := fmt.Sprintf("%s/%s/%s/%s/%s", source, pinStatus, defaultStatus, altStatus, verdict)
						t.Run(name, func(t *testing.T) {
							statuses := map[Pair]string{baseline: defaultStatus, alternate: altStatus, pinned: pinStatus}
							req := Request{
								HostOS: "windows", TaskKind: "build", Default: baseline,
								Permitted: func(pair Pair) bool { return statuses[pair] != "denied" },
							}
							for _, pair := range []Pair{baseline, alternate, pinned} {
								if statuses[pair] == "missing" {
									continue
								}
								candidate := qualifiedCandidate(pair)
								if statuses[pair] == "gap" {
									delete(candidate.Capabilities.Ops, engine.SkillDeliveryOp)
								}
								req.Candidates = append(req.Candidates, candidate)
							}
							if source == "pin" {
								req.Pin = &pinned
							}
							if source == "replacement" || source == "both" {
								req.SelectedHarness = &thread.HarnessSelection{Harness: pinned.Harness, Model: pinned.Model}
							}
							if source == "both" {
								req.Pin = &baseline
							}
							if verdict != "" {
								evidence := comparisonFor(baseline, alternate)
								evidence.Verdict = verdict
								req.Evidence = []Evidence{evidence}
							}
							var want Pair
							switch {
							case source != "none":
								if pinStatus == "qualified" {
									want = pinned
								}
							case altStatus == "qualified" && verdict == Better:
								want = alternate
							case defaultStatus == "qualified":
								want = baseline
							}
							got := Select(req)
							outcome := NoEligiblePair
							if want != (Pair{}) {
								outcome = Selected
							}
							if got.Outcome != outcome || got.Eligible() != (outcome == Selected) || got.Pair != want ||
								got.Pinned != (source != "none") || got.RoutingReason == "" {
								t.Fatalf("Select() = %+v, want %s %v, pinned=%t and a reason", got, outcome, want, source != "none")
							}
							if want == alternate && !strings.Contains(got.RoutingReason, "build-set-1") {
								t.Fatalf("routing reason lacks the task set: %q", got.RoutingReason)
							}
						})
					}
				}
			}
		}
	}
}

// --- Qualification: check operations and their declaration context ---

// TestRequiredQualifications proves R1 requires each operation to be qualified for a single declaration.
func TestRequiredQualifications(t *testing.T) {
	pair := Pair{Harness: "codex", Model: "default"}
	for _, op := range []string{engine.StartOp, engine.SendOp, engine.StreamOp, engine.StopOp, engine.SkillDeliveryOp} {
		for _, status := range []engine.Status{"", engine.Untested, engine.Unavailable, engine.Qualified} {
			t.Run(op+"/"+string(status), func(t *testing.T) {
				candidate := qualifiedCandidate(pair)
				delete(candidate.Capabilities.Ops, op)
				if status != "" {
					candidate.Capabilities.Ops[op] = engine.Capability{Status: status}
				}
				partial := qualifiedCandidate(pair)
				partial.Capabilities.Ops = map[string]engine.Capability{op: {Status: engine.Qualified}}
				req := Request{HostOS: "windows", Default: pair, Candidates: []Candidate{partial, candidate}, Permitted: allowPair}
				if got := Select(req); got.Eligible() != (status == engine.Qualified) {
					t.Fatalf("Select() = %+v for %s with status %q", got, op, status)
				}
			})
		}
	}
}

// TestQualificationContext proves R1's platform, adapter, model, installed-version, and policy constraints.
func TestQualificationContext(t *testing.T) {
	pair := Pair{Harness: "codex", Model: "default"}
	for _, test := range []struct {
		name, host, adapter, model, installed, bounds string
		permit, want                                  bool
	}{
		{"lower inclusive", "windows", "codex", "default", "2.0.0", ">=2.0 <3.0", true, true},
		{"upper exclusive", "windows", "codex", "default", "3.0.0", ">=2.0 <3.0", true, false},
		{"below lower", "windows", "codex", "default", "1.99.99", ">=2.0 <3.0", true, false},
		{"numeric ordering", "windows", "codex", "default", "2.10.0", ">2.9 >=2.0 <=2.10.0", true, true},
		{"exact patch", "windows", "codex", "default", "0.169.0", "0.169.0", true, true},
		{"codex window", "windows", "codex", "default", "0.169.0", ">=0.169.0 <0.169.1", true, true},
		{"invalid range", "windows", "codex", "default", "2.1.0", ">=2.0 || <3.0", true, false},
		{"no version", "windows", "codex", "default", "", ">=2.0 <3.0", true, false},
		{"prerelease", "windows", "codex", "default", "2.1.0-alpha", ">=2.0 <3.0", true, false},
		{"platform mismatch", "linux", "codex", "default", "2.1.0", ">=2.0 <3.0", true, false},
		{"adapter mismatch", "windows", "kimi", "default", "2.1.0", ">=2.0 <3.0", true, false},
		{"undeclared model", "windows", "codex", "other", "2.1.0", ">=2.0 <3.0", true, false},
		{"no policy", "windows", "codex", "default", "2.1.0", ">=2.0 <3.0", false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := qualifiedCandidate(pair)
			candidate.HarnessVersion = test.installed
			candidate.Capabilities.Adapter = test.adapter
			candidate.Capabilities.Models = []string{test.model}
			candidate.Capabilities.HarnessVersions = test.bounds
			req := Request{HostOS: test.host, Default: pair, Candidates: []Candidate{candidate}}
			if test.permit {
				req.Permitted = allowPair
			}
			if got := Select(req); got.Eligible() != test.want {
				t.Fatalf("Select() = %+v, want eligibility %t", got, test.want)
			}
		})
	}
}

// --- Fixtures: build capability and comparison records for routing tests ---

// qualifiedCandidate builds a declaration with the required operations and no optional ones.
func qualifiedCandidate(pair Pair) Candidate {
	ops := make(map[string]engine.Capability)
	for _, op := range []string{engine.StartOp, engine.SendOp, engine.StreamOp, engine.StopOp, engine.SkillDeliveryOp} {
		ops[op] = engine.Capability{Status: engine.Qualified}
	}
	return Candidate{
		Pair: pair, HarnessVersion: "2.1.0",
		Capabilities: engine.CapabilitySet{
			Adapter: pair.Harness, OS: "windows", Models: []string{pair.Model},
			HarnessVersions: ">=2.0 <3.0", Ops: ops,
		},
	}
}

// comparisonFor builds a recorded comparison for the build task kind.
func comparisonFor(baseline, alternate Pair) Evidence {
	return Evidence{
		TaskSetID: "build-set-1", TaskKind: "build", Default: baseline, Alternative: alternate,
		Verdict: Better, RecordedAt: time.Unix(100, 0),
	}
}

// allowPair supplies an affirmative policy decision for all test pairs.
func allowPair(Pair) bool { return true }
