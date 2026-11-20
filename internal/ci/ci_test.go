package ci

import (
	"context"
	"errors"
	"strings"
	"testing"

	"synch/internal/gates"
)

// --- Test doubles ---

const (
	secret = "ghp_PLANTEDSECRET123"
	sha1   = "0123456789abcdef0123456789abcdef01234567"
	sha2   = "fedcba9876543210fedcba9876543210fedcba98"
)

// fakeBroker is a TokenBroker with one known secret.
type fakeBroker struct{}

// Token returns the planted secret.
func (fakeBroker) Token(context.Context, string) (string, error) { return secret, nil }

// KnownValues returns the planted secret.
func (fakeBroker) KnownValues() []string { return []string{secret} }

// memJournal records journal calls in order.
type memJournal struct {
	events     []string
	resolved   []Resolution
	persistErr error
}

// Persist records the persisted event or fails when persistErr is set.
func (j *memJournal) Persist(_ context.Context, r Record) error {
	if j.persistErr != nil {
		return j.persistErr
	}
	j.events = append(j.events, "persisted:"+r.Operation+":"+r.Resource)
	return nil
}

// BindTask records the bound task.
func (j *memJournal) BindTask(_ context.Context, _, _, task string) error {
	j.events = append(j.events, "bound:"+task)
	return nil
}

// Resolve records the resolution.
func (j *memJournal) Resolve(_ context.Context, r Resolution) error {
	j.events = append(j.events, "resolved:"+string(r.Outcome))
	j.resolved = append(j.resolved, r)
	return nil
}

// fakeProvider is a scriptable Provider.
type fakeProvider struct {
	j         *memJournal
	triggers  int
	trigRun   RunRef
	trigErr   error
	found     []RunRef
	findErr   error
	status    RunStatus
	statusErr error
	jobs      []Job
	log       []byte
	sawEvents []string
}

// Name returns the fake driver name.
func (*fakeProvider) Name() string { return "fake" }

// Trigger counts calls and snapshots the journal to prove persist-first.
func (f *fakeProvider) Trigger(context.Context, TriggerRequest) (RunRef, error) {
	f.triggers++
	if f.j != nil {
		f.sawEvents = append([]string(nil), f.j.events...)
	}
	return f.trigRun, f.trigErr
}

// FindByToken returns the scripted runs.
func (f *fakeProvider) FindByToken(context.Context, TriggerRequest) ([]RunRef, error) {
	return f.found, f.findErr
}

// Status returns the scripted status.
func (f *fakeProvider) Status(context.Context, RunRef) (RunStatus, error) {
	return f.status, f.statusErr
}

// Jobs returns the scripted jobs.
func (f *fakeProvider) Jobs(context.Context, RunRef) ([]Job, error) { return f.jobs, nil }

// Logs returns the scripted log.
func (f *fakeProvider) Logs(context.Context, RunRef, string) ([]byte, error) { return f.log, nil }

// Cancel does nothing.
func (*fakeProvider) Cancel(context.Context, RunRef) error { return nil }

// newDispatcher wires a Dispatcher around a fake provider.
func newDispatcher(f *fakeProvider) (*Dispatcher, *memJournal) {
	j := &memJournal{}
	f.j = j
	return &Dispatcher{Provider: f, Journal: j, Endpoint: "ep", NewID: func() string { return "01JABCDEFGHJKMNPQRSTVWXYZ0" }}, j
}

// --- Decision 18 dispatcher tests (P3) ---

// TestDispatchPersistsBeforeDispatch proves P3: persisted is written before the provider is called, then resolved.
func TestDispatchPersistsBeforeDispatch(t *testing.T) {
	f := &fakeProvider{trigRun: RunRef{Project: "o/r", ID: "7"}}
	d, j := newDispatcher(f)
	res, err := d.Trigger(context.Background(), TriggerRequest{Project: "o/r", Ref: "main", SHA: sha1})
	if err != nil || res.Outcome != Succeeded || res.Via != ViaResponse {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if len(f.sawEvents) != 1 || !strings.HasPrefix(f.sawEvents[0], "persisted:trigger:o/r@"+sha1) {
		t.Fatalf("provider called before persist: %v", f.sawEvents)
	}
	if res.Token != "synch-01JABCDEFGHJKMNPQRSTVWXYZ0" || strings.Join(j.events[1:], ",") != "bound:7,resolved:succeeded" {
		t.Fatalf("token=%q events=%v", res.Token, j.events)
	}
}

// TestDispatchPersistFailureSendsNothing proves P3: a failed persist means no dispatch at all.
func TestDispatchPersistFailureSendsNothing(t *testing.T) {
	f := &fakeProvider{}
	d, j := newDispatcher(f)
	j.persistErr = errors.New("disk full")
	if _, err := d.Trigger(context.Background(), TriggerRequest{Project: "o/r", Ref: "main", SHA: sha1}); err == nil {
		t.Fatal("expected error")
	}
	if f.triggers != 0 {
		t.Fatalf("provider called %d times", f.triggers)
	}
}

// TestDispatchLostResponseReconcilesByToken proves P3: a lost response is settled by the token lookup, never re-triggered.
func TestDispatchLostResponseReconcilesByToken(t *testing.T) {
	f := &fakeProvider{trigErr: ErrTransient, found: []RunRef{{Project: "o/r", ID: "9"}}}
	d, j := newDispatcher(f)
	res, err := d.Trigger(context.Background(), TriggerRequest{Project: "o/r", Ref: "main", SHA: sha1})
	if err != nil || res.Outcome != Succeeded || res.Via != ViaResultingRun || res.Run.ID != "9" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if f.triggers != 1 || j.resolved[0].TaskID != "9" {
		t.Fatalf("triggers=%d resolved=%+v", f.triggers, j.resolved)
	}
}

// TestDispatchLostResponseUnknown proves P3: no evidence, many runs or a lookup error is unknown, with no retry.
func TestDispatchLostResponseUnknown(t *testing.T) {
	cases := map[string]*fakeProvider{
		"none":  {trigErr: ErrTransient},
		"many":  {trigErr: ErrTransient, found: []RunRef{{ID: "1"}, {ID: "2"}}},
		"error": {trigErr: ErrTransient, findErr: ErrTransient},
	}
	for name, f := range cases {
		d, _ := newDispatcher(f)
		res, err := d.Trigger(context.Background(), TriggerRequest{Project: "o/r", Ref: "main", SHA: sha1})
		if err != nil || res.Outcome != Unknown || res.Via != ViaNone || res.Run.ID != "" || f.triggers != 1 {
			t.Fatalf("%s: res=%+v err=%v triggers=%d", name, res, err, f.triggers)
		}
	}
}

// TestDispatchRejectedIsNotExecuted proves P3: an explicit provider rejection is not_executed via response.
func TestDispatchRejectedIsNotExecuted(t *testing.T) {
	f := &fakeProvider{trigErr: ErrRejected}
	d, _ := newDispatcher(f)
	res, err := d.Trigger(context.Background(), TriggerRequest{Project: "o/r", Ref: "main", SHA: sha1})
	if err != nil || res.Outcome != NotExecuted || res.Via != ViaResponse {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

// TestDispatchRejectsAbbreviatedSHA proves P2 binding: only a full commit id may be requested.
func TestDispatchRejectsAbbreviatedSHA(t *testing.T) {
	d, _ := newDispatcher(&fakeProvider{})
	if _, err := d.Trigger(context.Background(), TriggerRequest{Project: "o/r", Ref: "main", SHA: "0123abc"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("err=%v", err)
	}
}

// --- Result mapping tests (P2) ---

// results runs Results against a fake provider with the given jobs.
func results(st RunStatus, statusErr error, jobs ...Job) []gates.Result {
	f := &fakeProvider{status: st, statusErr: statusErr, jobs: jobs}
	return Results(context.Background(), f, fakeBroker{}, RunRef{Project: "o/r", ID: "1"}, sha1)
}

// TestResultsMapsStatuses proves P2: verdicts map to the exact check statuses, with a reason whenever not passed.
func TestResultsMapsStatuses(t *testing.T) {
	want := map[Verdict]gates.Status{
		VerdictPassed: gates.StatusPassed, VerdictFailed: gates.StatusFailed, VerdictCancelled: gates.StatusError,
		VerdictTimedOut: gates.StatusError, VerdictSkipped: gates.StatusNotRun, VerdictPending: gates.StatusNotRun,
		VerdictOther: gates.StatusError,
	}
	for v, status := range want {
		r := results(RunStatus{SHA: sha1, Verdict: VerdictPassed}, nil, Job{Name: "unit", SHA: sha1, Verdict: v, Raw: "x"})[0]
		if r.Status != status || r.Check != "unit" || r.Revision != sha1 || (status != gates.StatusPassed && r.Reason == "") {
			t.Fatalf("%s: %+v", v, r)
		}
	}
}

// TestResultsCommitMismatchIsError proves P2: a run or job on another commit is error for every job, never passed.
func TestResultsCommitMismatchIsError(t *testing.T) {
	jobs := []Job{{Name: "a", Verdict: VerdictPassed}, {Name: "b", Verdict: VerdictPassed}}
	for _, rs := range results(RunStatus{SHA: sha2, Verdict: VerdictPassed}, nil, jobs...) {
		if rs.Status != gates.StatusError || !strings.Contains(rs.Reason, "commit mismatch") {
			t.Fatalf("run-level: %+v", rs)
		}
	}
	jobs[1].SHA = sha2
	for _, rs := range results(RunStatus{SHA: sha1, Verdict: VerdictPassed}, nil, jobs...) {
		if rs.Status != gates.StatusError || !strings.Contains(rs.Reason, "commit mismatch") {
			t.Fatalf("job-level: %+v", rs)
		}
	}
	if r := results(RunStatus{Verdict: VerdictPassed}, nil, jobs[0])[0]; r.Status != gates.StatusError {
		t.Fatalf("unreadable run sha must not pass: %+v", r)
	}
}

// TestResultsCancelledOrTimedOutRunNeverPasses proves P2: a passed job in a cancelled or timed-out run is error.
func TestResultsCancelledOrTimedOutRunNeverPasses(t *testing.T) {
	for v, reason := range map[Verdict]string{VerdictCancelled: "run cancelled", VerdictTimedOut: "run timed out"} {
		r := results(RunStatus{SHA: sha1, Verdict: v}, nil, Job{Name: "a", SHA: sha1, Verdict: VerdictPassed})[0]
		if r.Status != gates.StatusError || r.Reason != reason {
			t.Fatalf("%s: %+v", v, r)
		}
	}
}

// TestResultsUnreadableStatusIsErrorRedacted proves P2 and P4: unreadable status is error and the cause is redacted.
func TestResultsUnreadableStatusIsErrorRedacted(t *testing.T) {
	rs := results(RunStatus{}, errors.New("boom "+secret))
	if len(rs) != 1 || rs[0].Status != gates.StatusError || strings.Contains(rs[0].Reason, secret) || !strings.Contains(rs[0].Reason, "status unreadable") {
		t.Fatalf("%+v", rs)
	}
	if r := results(RunStatus{SHA: sha1}, nil); r[0].Status != gates.StatusNotRun {
		t.Fatalf("no jobs must be not_run: %+v", r)
	}
}

// --- Log redaction, capping and retention tests (P4, P5) ---

// memStore keeps the retained log in memory.
type memStore struct{ data string }

// Put stores data and returns a fixed reference.
func (m *memStore) Put(_ context.Context, _ string, data []byte) (string, string, error) {
	m.data = string(data)
	return "log://ref", "sha256:x", nil
}

// TestFetchLogRedactsCapsAndRetains proves P1 and P4: the secret is gone from retained and returned text; the excerpt is capped; the full log is kept by reference.
func TestFetchLogRedactsCapsAndRetains(t *testing.T) {
	f := &fakeProvider{log: []byte("line1 " + secret + "\nline2 token=" + secret + "\nend")}
	st := &memStore{}
	res, err := FetchLog(context.Background(), f, fakeBroker{}, st, RunRef{ID: "1"}, "5", 10)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(st.data, secret) || strings.Contains(res.Excerpt, secret) || !strings.Contains(st.data, Redacted) {
		t.Fatalf("secret leaked: %q / %q", st.data, res.Excerpt)
	}
	if len(res.Excerpt) != 10 || !res.Truncated || res.Ref != "log://ref" || !strings.HasSuffix(st.data, "end") {
		t.Fatalf("res=%+v", res)
	}
}

// TestFetchLogOversizedKeepsNoSecretFragment proves P5: an oversized log is cut and flagged, and a secret straddling the cut leaves no prefix.
func TestFetchLogOversizedKeepsNoSecretFragment(t *testing.T) {
	raw := []byte(strings.Repeat("a", MaxRetainedLog+10))
	copy(raw[MaxRetainedLog-3:], secret) // straddles the hard cut
	st := &memStore{}
	res, err := FetchLog(context.Background(), &fakeProvider{log: raw}, fakeBroker{}, st, RunRef{ID: "1"}, "5", 64)
	if err != nil || !res.RetainedTruncated {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if strings.Contains(st.data, "ghp") || len(st.data) > MaxRetainedLog {
		t.Fatalf("fragment kept or too long (%d)", len(st.data))
	}
	if len(res.Excerpt) != 64 {
		t.Fatalf("excerpt %d", len(res.Excerpt))
	}
}

// TestRedactLongestFirst proves P4: a secret containing another known secret is removed whole.
func TestRedactLongestFirst(t *testing.T) {
	if got := Redact("x abcdef y", []string{"abc", "abcdef", ""}); got != "x "+Redacted+" y" {
		t.Fatalf("got %q", got)
	}
}
