package findings

import (
	"context"
	"errors"
	"slices"
)

var (
	ErrInvalidFix  = errors.New("invalid fix request")
	ErrRerun       = errors.New("fix checks could not be rerun")
	ErrRerunResult = errors.New("fix checks returned incomplete or mismatched results")
	ErrFixRecord   = errors.New("fix attempt could not be recorded")
)

// --- Types ---

// FixRequest identifies a Builder change and the exact revision Security will review.
type FixRequest struct {
	AttemptID, Change, Revision string
	Checks                      []string
}

// CheckResult identifies the actual revision checked and carries redacted evidence.
type CheckResult struct {
	Name, Revision, Status string
	Evidence               []ReportRef
}

// FixAttempt records before evidence and the exact-revision outcome for review.
type FixAttempt struct {
	FixRequest
	FindingID, BeforeRevision string
	Before                    Evidence
	After                     []CheckResult
	Status, Reason            string
}

type CheckRunner func(context.Context, string, []string) ([]CheckResult, error)
type AttemptRecorder func(context.Context, FixAttempt) error

// --- Fix loop and result validation ---

// RunFix reruns the requested checks for a normalized finding and records every
// outcome, including a runner error or cancellation. The recorder must persist
// by attempt ID.
func RunFix(ctx context.Context, finding Finding, req FixRequest, run CheckRunner, record AttemptRecorder) (FixAttempt, error) {
	if finding.ID == "" || finding.Revision == "" || req.AttemptID == "" || req.Change == "" || req.Revision == "" ||
		!validChecks(req.Checks) || !finding.Evidence.Report.Redacted || run == nil || record == nil {
		return FixAttempt{}, ErrInvalidFix
	}
	req.Checks = slices.Clone(req.Checks)
	attempt := FixAttempt{FixRequest: req, FindingID: finding.ID, BeforeRevision: finding.Revision, Before: finding.Evidence}
	results, err := run(ctx, req.Revision, slices.Clone(req.Checks))
	attempt.After = matchingChecks(req, results)
	switch {
	case err != nil:
		attempt.Status, attempt.Reason = "error", "rerun_failed"
		err = ErrRerun
	case len(attempt.After) != len(req.Checks):
		attempt.Status, attempt.Reason = "error", "invalid_rerun_result"
		err = ErrRerunResult
	case missingEvidence(attempt.After):
		attempt.Status, attempt.Reason = "error", "missing_after_evidence"
		err = ErrRerunResult
	default:
		attempt.Status = "passed"
		for _, result := range attempt.After {
			if result.Status != "passed" {
				attempt.Status, attempt.Reason = "failed", "checks_not_passed"
			}
		}
	}
	// Cancellation of the checks must not cancel recording their failed attempt.
	if record(context.WithoutCancel(ctx), attempt) != nil {
		err = errors.Join(err, ErrFixRecord)
	}
	return attempt, err
}

// missingEvidence reports a passed check with no after-evidence report attached.
func missingEvidence(results []CheckResult) bool {
	return slices.ContainsFunc(results, func(r CheckResult) bool { return r.Status == "passed" && len(r.Evidence) == 0 })
}

// validChecks requires unique, nonempty names so each result has one expected identity.
func validChecks(checks []string) bool {
	seen := make(map[string]bool, len(checks))
	for _, name := range checks {
		if name == "" || seen[name] {
			return false
		}
		seen[name] = true
	}
	return len(checks) != 0
}

// matchingChecks preserves valid evidence and excludes ambiguous, unchecked and
// wrong-revision results from the review record.
func matchingChecks(req FixRequest, results []CheckResult) []CheckResult {
	byName := make(map[string][]CheckResult, len(results))
	for _, result := range results {
		byName[result.Name] = append(byName[result.Name], result)
	}
	var matched []CheckResult
	for _, name := range req.Checks {
		records := byName[name]
		if len(records) != 1 || records[0].Revision != req.Revision {
			continue
		}
		switch result := records[0]; result.Status {
		case "passed", "failed", "error", "partial", "not_run":
			if !slices.ContainsFunc(result.Evidence, unredactedReport) {
				result.Evidence = slices.Clone(result.Evidence)
				matched = append(matched, result)
			}
		}
	}
	return matched
}

// unredactedReport identifies rerun evidence that cannot be recorded.
func unredactedReport(ref ReportRef) bool {
	return !ref.Redacted
}
