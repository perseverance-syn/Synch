package findings

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// TestGroup verifies F4 canonical fingerprints, tool identity and distinct provenance.
func TestGroup(t *testing.T) {
	component := Component{Type: "file", Name: "<>&\u2028"}
	location := Location{Path: "a\n\\\".txt"}
	canonical := []byte(`{"component":{"name":"<>&` + "\u2028" + `","type":"file","version":""},"location":{"path":"a\n\\\".txt","start_line":0},"rule":"rule","tool":"trivy"}`)
	hash := Fingerprint("trivy", "rule", component, location)
	if hash != Digest(canonical) {
		t.Fatalf("fingerprint = %s, want %s", hash, Digest(canonical))
	}
	otherHash := Fingerprint("gitleaks", "rule", component, location)
	if otherHash == hash {
		t.Fatal("scanner-driver section 4 requires the tool in the fingerprint")
	}
	report := ReportRef{URI: "report://fixture", Digest: Digest(nil), Redacted: true}
	first := Provenance{ScanID: "first", Tool: "trivy", Report: report}
	second := Provenance{ScanID: "second", Tool: "trivy", Report: report}
	other := Provenance{ScanID: "third", Tool: "gitleaks", Report: report}
	input := []Finding{
		{Fingerprint: hash, ID: "original", Provenance: []Provenance{first}},
		{Fingerprint: hash, ID: "repeated", Provenance: []Provenance{first, second}},
		{Fingerprint: otherHash, ID: "different tool", Provenance: []Provenance{other}},
	}
	grouped := Group(input)
	if len(grouped) != 2 || grouped[0].ID != "original" ||
		!slices.Equal(grouped[0].Provenance, []Provenance{first, second}) ||
		!slices.Equal(grouped[1].Provenance, []Provenance{other}) ||
		len(input[0].Provenance) != 1 {
		t.Fatalf("Group = %+v", grouped)
	}
}

// TestTriage verifies scanner-driver section 4 requires a reason and actor for dispositions.
func TestTriage(t *testing.T) {
	var finding Finding
	for _, status := range []string{"false_positive", "accepted_risk"} {
		if err := finding.SetTriage(Triage{Status: status, Reason: "reviewed"}); !errors.Is(err, ErrInvalidTriage) {
			t.Fatalf("SetTriage without actor = %v", err)
		}
		if err := finding.SetTriage(Triage{Status: status, Reason: "reviewed", By: "security"}); err != nil || finding.Triage.Status != status {
			t.Fatalf("SetTriage = %v, %+v", err, finding.Triage)
		}
	}
}

// TestRunFix verifies F5 reruns named checks against the exact fix revision and records failed attempts.
func TestRunFix(t *testing.T) {
	before := ReportRef{URI: "report://before", Digest: Digest([]byte("before")), Redacted: true}
	after := ReportRef{URI: "report://after", Digest: Digest([]byte("after")), Redacted: true}
	finding := Finding{ID: "finding-id", Revision: "original", Evidence: Evidence{Summary: "before", Report: before}}
	var recorded []FixAttempt
	record := func(ctx context.Context, attempt FixAttempt) error {
		if ctx.Err() != nil {
			t.Fatal("recording inherited canceled rerun context")
		}
		recorded = append(recorded, attempt)
		return nil
	}
	for _, tc := range []struct {
		name, actualRevision, checkStatus, attemptStatus string
		want                                             error
	}{
		{"check failed", "fix", "failed", "failed", nil},
		{"check passed", "fix", "passed", "passed", nil},
		{"wrong revision", "wrong", "passed", "error", ErrRerunResult},
		{"unredacted evidence", "fix", "passed", "error", ErrRerunResult},
		{"runner stopped", "", "", "error", ErrRerun},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := FixRequest{AttemptID: tc.name, Change: "review-change", Revision: "fix", Checks: []string{"secrets", "vulnerabilities"}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			run := func(_ context.Context, revision string, checks []string) ([]CheckResult, error) {
				if revision != req.Revision || !slices.Equal(checks, req.Checks) {
					t.Fatalf("reran %v on %s", checks, revision)
				}
				if tc.want == ErrRerun {
					cancel()
					return nil, context.Canceled
				}
				evidence := after
				if tc.name == "unredacted evidence" {
					evidence.Redacted = false
				}
				return []CheckResult{
					{Name: "secrets", Revision: tc.actualRevision, Status: tc.checkStatus, Evidence: []ReportRef{evidence}},
					{Name: "vulnerabilities", Revision: tc.actualRevision, Status: tc.checkStatus, Evidence: []ReportRef{evidence}},
				}, nil
			}
			attempt, err := RunFix(ctx, finding, req, run, record)
			if !errors.Is(err, tc.want) || attempt.Status != tc.attemptStatus || attempt.Before != finding.Evidence ||
				attempt.Revision != req.Revision || attempt.BeforeRevision != finding.Revision ||
				attempt.FindingID != finding.ID || attempt.Change != req.Change {
				t.Fatalf("RunFix = %+v, %v", attempt, err)
			}
			if tc.want == nil {
				if len(attempt.After) != 2 || !slices.Equal(attempt.After[0].Evidence, []ReportRef{after}) {
					t.Fatalf("missing rerun evidence: %+v", attempt.After)
				}
			} else if len(attempt.After) != 0 {
				t.Fatalf("invalid evidence in attempt: %+v", attempt.After)
			}
		})
	}
	if len(recorded) != 5 || recorded[0].Status != "failed" || recorded[1].Status != "passed" || recorded[4].Reason != "rerun_failed" {
		t.Fatalf("attempt history = %+v", recorded)
	}
}

// TestRunFixRequiresAfterEvidence verifies M3: a passed check with no evidence is an error.
func TestRunFixRequiresAfterEvidence(t *testing.T) {
	finding := Finding{ID: "f", Revision: "original", Evidence: Evidence{Report: ReportRef{URI: "report://b", Redacted: true}}}
	req := FixRequest{AttemptID: "a", Change: "c", Revision: "fix", Checks: []string{"secrets"}}
	run := func(context.Context, string, []string) ([]CheckResult, error) {
		return []CheckResult{{Name: "secrets", Revision: "fix", Status: "passed"}}, nil
	}
	attempt, err := RunFix(t.Context(), finding, req, run, func(context.Context, FixAttempt) error { return nil })
	if !errors.Is(err, ErrRerunResult) || attempt.Status != "error" || attempt.Reason != "missing_after_evidence" {
		t.Fatalf("RunFix = %+v, %v", attempt, err)
	}
}
