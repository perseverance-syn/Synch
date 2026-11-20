package github

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"synch/internal/ci"
	"synch/internal/gates"
)

const (
	secret = "ghp_PLANTEDSECRET123"
	sha1   = "0123456789abcdef0123456789abcdef01234567"
	sha2   = "fedcba9876543210fedcba9876543210fedcba98"
)

// broker is a ci.TokenBroker with one known secret.
type broker struct{}

// Token returns the planted secret.
func (broker) Token(context.Context, string) (string, error) { return secret, nil }

// KnownValues returns the planted secret.
func (broker) KnownValues() []string { return []string{secret} }

// journal records external request calls.
type journal struct{ events []string }

// Persist records the persisted event.
func (j *journal) Persist(_ context.Context, r ci.Record) error {
	j.events = append(j.events, "persisted:"+r.Driver)
	return nil
}

// BindTask records the bound run.
func (j *journal) BindTask(_ context.Context, _, _, task string) error {
	j.events = append(j.events, "bound:"+task)
	return nil
}

// Resolve records the resolution.
func (j *journal) Resolve(_ context.Context, r ci.Resolution) error {
	j.events = append(j.events, "resolved:"+string(r.Outcome)+":"+string(r.Via))
	return nil
}

// store keeps the retained log.
type store struct{ data string }

// Put stores the log.
func (s *store) Put(_ context.Context, _ string, d []byte) (string, string, error) {
	s.data = string(d)
	return "log://1", "sha256:x", nil
}

// mock mimics the GitHub REST endpoints the provider uses and records every
// request URL so tests can prove the token never appears in one.
type mock struct {
	mu         sync.Mutex
	urls       []string
	dispatches int
	dropReply  bool // simulate a lost response to the dispatch
	hideRuns   bool // the dispatched run is not listed yet
	rejectCode int
	token      string // run-name of the dispatched run
	headSHA    string
	conclusion string
	jobSHA     string
	jobConc    string
	cancelled  bool
	log        string
	failBody   string // when set, every read fails with 500 echoing this body
}

// handler serves the mocked GitHub API.
func (m *mock) handler(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.urls = append(m.urls, r.URL.String())
	if r.Header.Get("Authorization") != "Bearer "+secret {
		http.Error(w, "bad credentials", http.StatusUnauthorized)
		return
	}
	p := r.URL.Path
	if r.Method == http.MethodGet && m.failBody != "" {
		http.Error(w, m.failBody, http.StatusInternalServerError)
		return
	}
	switch {
	case r.Method == http.MethodPost && p == "/repos/o/r/actions/workflows/gates.yml/dispatches":
		m.dispatches++
		var body struct {
			Ref    string            `json:"ref"`
			Inputs map[string]string `json:"inputs"`
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		if m.rejectCode != 0 {
			http.Error(w, "Workflow does not have workflow_dispatch trigger", m.rejectCode)
			return
		}
		m.token = body.Inputs["synch_token"]
		if m.dropReply {
			panic(http.ErrAbortHandler) // connection dropped after the run was created
		}
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet && p == "/repos/o/r/actions/runs":
		runs := []map[string]any{{"id": 1, "name": "other", "display_title": "other", "head_sha": sha1}}
		if m.token != "" && !m.hideRuns {
			runs = append(runs, map[string]any{"id": 42, "name": m.token, "display_title": m.token, "head_sha": m.headSHA})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"workflow_runs": runs})
	case r.Method == http.MethodGet && p == "/repos/o/r/actions/runs/42":
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 42, "head_sha": m.headSHA, "status": "completed", "conclusion": m.conclusion})
	case r.Method == http.MethodGet && p == "/repos/o/r/actions/runs/42/jobs":
		_ = json.NewEncoder(w).Encode(map[string]any{"jobs": []map[string]any{
			{"id": 500, "name": "unit-tests", "head_sha": m.jobSHA, "status": "completed", "conclusion": m.jobConc}}})
	case r.Method == http.MethodGet && p == "/repos/o/r/actions/jobs/500/logs":
		_, _ = io.WriteString(w, m.log)
	case r.Method == http.MethodPost && p == "/repos/o/r/actions/runs/42/cancel":
		m.cancelled = true
		w.WriteHeader(http.StatusAccepted)
	default:
		http.NotFound(w, r)
	}
}

// setup starts the mock and returns a provider, dispatcher and journal.
func setup(t *testing.T, m *mock) (*Provider, *ci.Dispatcher, *journal) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(m.handler))
	t.Cleanup(srv.Close)
	p := New(Config{BaseURL: srv.URL, Endpoint: "gh", Broker: broker{}, Client: srv.Client()})
	j := &journal{}
	return p, &ci.Dispatcher{Provider: p, Journal: j, Endpoint: "gh", NewID: func() string { return "01JABCDEFGHJKMNPQRSTVWXYZ0" }}, j
}

// req is the standard trigger request.
var req = ci.TriggerRequest{Project: "o/r", Ref: "main", SHA: sha1, Workflow: "gates.yml"}

// --- Operation tests (P1, P2) ---

// TestGitHubAllOperations proves P1 and P2: trigger, find, status, jobs, logs and cancel work against the mock and results bind to the commit.
func TestGitHubAllOperations(t *testing.T) {
	m := &mock{headSHA: sha1, conclusion: "success", jobSHA: sha1, jobConc: "success", log: "ok\n"}
	p, d, j := setup(t, m)
	res, err := d.Trigger(context.Background(), req)
	if err != nil || res.Outcome != ci.Succeeded || res.Run.ID != "42" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if strings.Join(j.events, ",") != "persisted:github-actions,bound:42,resolved:succeeded:response" {
		t.Fatalf("events=%v", j.events)
	}
	rs := ci.Results(context.Background(), p, broker{}, res.Run, sha1)
	if len(rs) != 1 || rs[0].Check != "unit-tests" || rs[0].Status != gates.StatusPassed || rs[0].Revision != sha1 {
		t.Fatalf("results=%+v", rs)
	}
	lr, err := ci.FetchLog(context.Background(), p, broker{}, &store{}, res.Run, "500", 100)
	if err != nil || lr.Excerpt != "ok\n" {
		t.Fatalf("log=%+v err=%v", lr, err)
	}
	if err := p.Cancel(context.Background(), res.Run); err != nil || !m.cancelled {
		t.Fatalf("cancel err=%v cancelled=%v", err, m.cancelled)
	}
}

// TestGitHubTokenOnlyInHeader proves P4: no request URL carries the token.
func TestGitHubTokenOnlyInHeader(t *testing.T) {
	m := &mock{headSHA: sha1, conclusion: "success", jobSHA: sha1, jobConc: "success"}
	p, d, _ := setup(t, m)
	res, _ := d.Trigger(context.Background(), req)
	ci.Results(context.Background(), p, broker{}, res.Run, sha1)
	for _, u := range m.urls {
		if strings.Contains(u, secret) {
			t.Fatalf("token in URL %q", u)
		}
	}
}

// --- Failure-path tests (P2, P3, P4, P5) ---

// TestGitHubLostResponseReconciles proves P3: the dispatch response is lost after the run exists; the token lookup binds it and dispatch ran exactly once.
func TestGitHubLostResponseReconciles(t *testing.T) {
	m := &mock{headSHA: sha1, dropReply: true}
	_, d, j := setup(t, m)
	res, err := d.Trigger(context.Background(), req)
	if err != nil || res.Outcome != ci.Succeeded || res.Via != ci.ViaResultingRun || res.Run.ID != "42" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if m.dispatches != 1 || !strings.HasSuffix(strings.Join(j.events, ","), "resolved:succeeded:resulting_resource") {
		t.Fatalf("dispatches=%d events=%v", m.dispatches, j.events)
	}
}

// TestGitHubLostResponseUnknown proves P3: a lost response with no visible run is reported unknown and not retried.
func TestGitHubLostResponseUnknown(t *testing.T) {
	m := &mock{dropReply: true, hideRuns: true}
	_, d, _ := setup(t, m)
	res, err := d.Trigger(context.Background(), req)
	if err != nil || res.Outcome != ci.Unknown || m.dispatches != 1 {
		t.Fatalf("res=%+v err=%v dispatches=%d", res, err, m.dispatches)
	}
}

// TestGitHubRejectedIsNotExecuted proves P3: a 422 from dispatch is not_executed.
func TestGitHubRejectedIsNotExecuted(t *testing.T) {
	m := &mock{rejectCode: http.StatusUnprocessableEntity}
	_, d, _ := setup(t, m)
	res, err := d.Trigger(context.Background(), req)
	if err != nil || res.Outcome != ci.NotExecuted {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

// TestGitHubCommitMismatch proves P2: a run whose head_sha is another commit yields error, not passed.
func TestGitHubCommitMismatch(t *testing.T) {
	m := &mock{headSHA: sha2, conclusion: "success", jobSHA: sha2, jobConc: "success"}
	p, d, _ := setup(t, m)
	res, _ := d.Trigger(context.Background(), req)
	rs := ci.Results(context.Background(), p, broker{}, res.Run, sha1)
	if rs[0].Status != gates.StatusError || !strings.Contains(rs[0].Reason, "commit mismatch") {
		t.Fatalf("%+v", rs)
	}
}

// TestGitHubCancelledAndTimedOut proves P2: cancelled and timed_out conclusions are error with a reason.
func TestGitHubCancelledAndTimedOut(t *testing.T) {
	for conc, reason := range map[string]string{"cancelled": "cancelled", "timed_out": "timed out"} {
		m := &mock{headSHA: sha1, conclusion: conc, jobSHA: sha1, jobConc: conc}
		p, d, _ := setup(t, m)
		res, _ := d.Trigger(context.Background(), req)
		r := ci.Results(context.Background(), p, broker{}, res.Run, sha1)[0]
		if r.Status != gates.StatusError || r.Reason != reason {
			t.Fatalf("%s: %+v", conc, r)
		}
	}
}

// TestGitHubUnreadableStatusRedacted proves P2 and P4: a failing status read is error and an echoed token is redacted.
func TestGitHubUnreadableStatusRedacted(t *testing.T) {
	m := &mock{headSHA: sha1}
	p, d, _ := setup(t, m)
	res, _ := d.Trigger(context.Background(), req)
	m.mu.Lock()
	m.failBody = "upstream said " + secret
	m.mu.Unlock()
	rs := ci.Results(context.Background(), p, broker{}, res.Run, sha1)
	if rs[0].Status != gates.StatusError || strings.Contains(rs[0].Reason, secret) {
		t.Fatalf("%+v", rs)
	}
	if _, err := p.Status(context.Background(), res.Run); err == nil || strings.Contains(err.Error(), secret) || !errors.Is(err, ci.ErrTransient) {
		t.Fatalf("err=%v", err)
	}
}

// TestGitHubLogRedactedAndCapped proves P4 and P5: a planted token in a large log is redacted before retention, and the excerpt is capped while the full redacted log is kept.
func TestGitHubLogRedactedAndCapped(t *testing.T) {
	m := &mock{headSHA: sha1, log: strings.Repeat("x", 5000) + " token " + secret + " tail"}
	p, d, _ := setup(t, m)
	res, _ := d.Trigger(context.Background(), req)
	st := &store{}
	lr, err := ci.FetchLog(context.Background(), p, broker{}, st, res.Run, "500", 100)
	if err != nil || strings.Contains(st.data, secret) || strings.Contains(lr.Excerpt, secret) {
		t.Fatalf("err=%v leaked", err)
	}
	if !lr.Truncated || len(lr.Excerpt) != 100 || len(st.data) < 5000 || lr.Ref != "log://1" {
		t.Fatalf("lr=%+v retained=%d", lr, len(st.data))
	}
}

// TestGitHubRejectsInjectedProject proves input validation: a project with extra path segments is refused before any request.
func TestGitHubRejectsInjectedProject(t *testing.T) {
	m := &mock{}
	p, _, _ := setup(t, m)
	_, err := p.Trigger(context.Background(), ci.TriggerRequest{Project: "o/r/../x", Workflow: "gates.yml"})
	if !errors.Is(err, ci.ErrInvalid) || len(m.urls) != 0 {
		t.Fatalf("err=%v urls=%v", err, m.urls)
	}
}
