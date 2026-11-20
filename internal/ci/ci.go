package ci

import (
	"context"
	"errors"
	"fmt"

	"synch/internal/gates"
)

// --- Provider interface, request and result types (ci-driver.md §2) ---

// Sentinel errors callers can test with errors.Is.
var (
	// ErrRejected means the provider explicitly refused the request before
	// executing it (HTTP 4xx other than 408).
	ErrRejected = errors.New("ci: provider rejected the request")
	// ErrTransient means the outcome is not known: timeout, 5xx, disconnect.
	ErrTransient = errors.New("ci: provider outcome not known")
	// ErrInvalid means the request failed local validation before any dispatch.
	ErrInvalid = errors.New("ci: invalid request")
)

// TokenBroker supplies provider tokens at the moment of use and lists every
// secret value the redactor must remove. It is a minimal interpretation of
// the credentials broker of card 401 and is to be replaced by it.
type TokenBroker interface {
	// Token returns the provider token for an endpoint.
	Token(ctx context.Context, endpoint string) (string, error)
	// KnownValues returns every secret value that must never be retained.
	KnownValues() []string
}

// RunRef identifies one provider run (GitHub workflow run, GitLab pipeline).
// ID is empty when the provider accepted a trigger without naming the run.
type RunRef struct {
	Project string
	ID      string
}

// TriggerRequest asks for a run on the exact commit SHA at Ref. Ref must
// currently point at SHA; Token is set by the Dispatcher (synch-<request id>).
type TriggerRequest struct {
	Project  string
	Ref      string
	SHA      string
	Workflow string // GitHub workflow file name; unused by GitLab
	Token    string
}

// Verdict is the provider-neutral state of a run or job.
type Verdict string

// Neutral verdicts; ci-driver.md §4 maps each to a check status.
const (
	VerdictPassed    Verdict = "passed"
	VerdictFailed    Verdict = "failed"
	VerdictCancelled Verdict = "cancelled"
	VerdictTimedOut  Verdict = "timed_out"
	VerdictSkipped   Verdict = "skipped"
	VerdictPending   Verdict = "pending"
	VerdictOther     Verdict = "other"
)

// RunStatus is a run state and the commit SHA it used. Raw is the provider
// own state text, kept for reasons.
type RunStatus struct {
	SHA     string
	Verdict Verdict
	Raw     string
}

// Job is one CI job; by contract its Name equals a gate check id. SHA may be
// empty when the provider does not report it per job.
type Job struct {
	ID      string
	Name    string
	SHA     string
	Verdict Verdict
	Raw     string
}

// Provider is implemented by every CI integration.
type Provider interface {
	// Name is the driver name used in external_request records.
	Name() string
	// Trigger starts a run; it must send req.Token to the provider.
	Trigger(ctx context.Context, req TriggerRequest) (RunRef, error)
	// FindByToken lists runs carrying req.Token (reconciliation).
	FindByToken(ctx context.Context, req TriggerRequest) ([]RunRef, error)
	// Status reads the run state and the SHA it used.
	Status(ctx context.Context, run RunRef) (RunStatus, error)
	// Jobs lists the jobs of the run with verdicts.
	Jobs(ctx context.Context, run RunRef) ([]Job, error)
	// Logs returns one job log, at most MaxRetainedLog+1 bytes.
	Logs(ctx context.Context, run RunRef, jobID string) ([]byte, error)
	// Cancel requests cancellation of a run.
	Cancel(ctx context.Context, run RunRef) error
}

// --- Verdict to check status mapping and commit binding (ci-driver.md §4, §5) ---

// Map converts a verdict to a gate check status and reason. Only a
// VerdictPassed yields StatusPassed; anything unrecognised is an error.
func Map(v Verdict, raw string) (gates.Status, string) {
	switch v {
	case VerdictPassed:
		return gates.StatusPassed, ""
	case VerdictFailed:
		return gates.StatusFailed, "CI job failed"
	case VerdictCancelled:
		return gates.StatusError, "cancelled"
	case VerdictTimedOut:
		return gates.StatusError, "timed out"
	case VerdictSkipped:
		return gates.StatusNotRun, "skipped by CI"
	case VerdictPending:
		return gates.StatusNotRun, "job not finished"
	default:
		return gates.StatusError, fmt.Sprintf("unrecognised CI state %q", raw)
	}
}

// Results reads a run and returns one gates.Result per job, bound to wantSHA.
// It never returns passed for a cancelled or timed-out run, for a run whose
// SHA differs from wantSHA, or when status cannot be read (a single error
// record check "ci-run" is returned instead). Reasons are redacted.
func Results(ctx context.Context, p Provider, broker TokenBroker, run RunRef, wantSHA string) []gates.Result {
	secrets := broker.KnownValues()
	unreadable := func(err error) []gates.Result {
		return []gates.Result{{Check: "ci-run", Status: gates.StatusError, Revision: wantSHA,
			Reason: "status unreadable: " + Redact(err.Error(), secrets)}}
	}
	st, err := p.Status(ctx, run)
	if err != nil {
		return unreadable(err)
	}
	jobs, err := p.Jobs(ctx, run)
	if err != nil {
		return unreadable(err)
	}
	if len(jobs) == 0 {
		return []gates.Result{{Check: "ci-run", Status: gates.StatusNotRun, Revision: st.SHA, Reason: "no jobs reported"}}
	}
	// Binding is all-or-nothing: one wrong SHA makes the whole run untrusted.
	used := st.SHA
	bad := st.SHA != wantSHA
	for _, j := range jobs {
		if j.SHA != "" && j.SHA != wantSHA {
			bad, used = true, j.SHA
		}
	}
	out := make([]gates.Result, 0, len(jobs))
	for _, j := range jobs {
		r := gates.Result{Check: j.Name, Revision: st.SHA}
		if bad {
			r.Status = gates.StatusError
			r.Reason = fmt.Sprintf("commit mismatch: run used %q, requested %q", used, wantSHA)
		} else {
			r.Status, r.Reason = Map(j.Verdict, j.Raw)
			// A cancelled or timed-out run is incomplete evidence even if this job finished.
			if r.Status == gates.StatusPassed && st.Verdict == VerdictCancelled {
				r.Status, r.Reason = gates.StatusError, "run cancelled"
			}
			if r.Status == gates.StatusPassed && st.Verdict == VerdictTimedOut {
				r.Status, r.Reason = gates.StatusError, "run timed out"
			}
		}
		r.Reason = Redact(r.Reason, secrets)
		out = append(out, r)
	}
	return out
}
