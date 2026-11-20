package gitlab

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
	secret = "glpat-PLANTEDSECRET123"
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

// BindTask records the bound pipeline.
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

// mock mimics the GitLab REST v4 endpoints the provider uses and records every
// request URL so tests can prove the token never appears in one.
type mock struct {
	mu         sync.Mutex
	urls       []string
	creates    int
	dropReply  bool // lose the response to the create after the pipeline exists
	hidePipes  bool // the created pipeline is not listed yet
	rejectCode int
	token      string // SYNCH_TOKEN of the created pipeline
	pipeSHA    string
	pipeStatus string
	jobSHA     string
	jobStatus  string
	jobReason  string
	cancelled  bool
	trace      string
	failBody   string // when set, every read fails with 500 echoing this body
}

// handler serves the mocked GitLab API.
func (m *mock) handler(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.urls = append(m.urls, r.URL.String())
	if r.Header.Get("PRIVATE-TOKEN") != secret {
		http.Error(w, `{"message":"401 Unauthorized"}`, http.StatusUnauthorized)
		return
	}
	p := r.URL.Path
	if r.Method == http.MethodGet && m.failBody != "" {
		http.Error(w, m.failBody, http.StatusInternalServerError)
		return
	}
	switch {
	case r.Method == http.MethodPost && p == "/projects/g/p/pipeline":
		m.creates++
		var body struct {
			Ref       string `json:"ref"`
			Variables []struct{ Key, Value string }
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		if m.rejectCode != 0 {
			http.Error(w, `{"message":"Reference not found"}`, m.rejectCode)
			return
		}
		for _, v := range body.Variables {
			if v.Key == "SYNCH_TOKEN" {
				m.token = v.Value
			}
		}
		if m.dropReply {
			panic(http.ErrAbortHandler) // connection dropped after the pipeline was created
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 77, "sha": m.pipeSHA, "ref": body.Ref, "status": "created"})
	case r.Method == http.MethodGet && p == "/projects/g/p/pipelines":
		list := []map[string]any{{"id": 5, "sha": sha1, "status": "success"}}
		if m.token != "" && !m.hidePipes {
			list = append(list, map[string]any{"id": 77, "sha": m.pipeSHA, "status": m.pipeStatus})
		}
		_ = json.NewEncoder(w).Encode(list)
	case r.Method == http.MethodGet && p == "/projects/g/p/pipelines/5/variables":
		_ = json.NewEncoder(w).Encode([]map[string]string{{"key": "SYNCH_TOKEN", "value": "synch-other"}})
	case r.Method == http.MethodGet && p == "/projects/g/p/pipelines/77/variables":
		_ = json.NewEncoder(w).Encode([]map[string]string{{"key": "SYNCH_TOKEN", "value": m.token}})
	case r.Method == http.MethodGet && p == "/projects/g/p/pipelines/77":
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 77, "sha": m.pipeSHA, "status": m.pipeStatus})
	case r.Method == http.MethodGet && p == "/projects/g/p/pipelines/77/jobs":
		_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 900, "name": "unit-tests", "status": m.jobStatus,
			"failure_reason": m.jobReason, "pipeline": map[string]any{"id": 77, "sha": m.jobSHA}}})
	case r.Method == http.MethodGet && p == "/projects/g/p/jobs/900/trace":
		_, _ = io.WriteString(w, m.trace)
	case r.Method == http.MethodPost && p == "/projects/g/p/pipelines/77/cancel":
		m.cancelled = true
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 77, "status": "canceled"})
	default:
		http.NotFound(w, r)
	}
}

// setup starts the mock and returns a provider, dispatcher and journal.
func setup(t *testing.T, m *mock) (*Provider, *ci.Dispatcher, *journal) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(m.handler))
	t.Cleanup(srv.Close)
	p := New(Config{BaseURL: srv.URL, Endpoint: "gl", Broker: broker{}, Client: srv.Client()})
	j := &journal{}
	return p, &ci.Dispatcher{Provider: p, Journal: j, Endpoint: "gl", NewID: func() string { return "01JABCDEFGHJKMNPQRSTVWXYZ0" }}, j
}

// req is the standard trigger request (project path g/p is URL-encoded by the provider).
var req = ci.TriggerRequest{Project: "g/p", Ref: "main", SHA: sha1}

// ok returns a mock for a fully successful pipeline on sha1.
func ok() *mock {
	return &mock{pipeSHA: sha1, pipeStatus: "success", jobSHA: sha1, jobStatus: "success", trace: "ok\n"}
}

// --- Operation tests (P1, P2) ---

// TestGitLabAllOperations proves P1 and P2: create, status, jobs, trace and cancel work against the mock and results bind to the commit.
func TestGitLabAllOperations(t *testing.T) {
	m := ok()
	p, d, j := setup(t, m)
	res, err := d.Trigger(context.Background(), req)
	if err != nil || res.Outcome != ci.Succeeded || res.Run.ID != "77" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if strings.Join(j.events, ",") != "persisted:gitlab-ci,bound:77,resolved:succeeded:response" {
		t.Fatalf("events=%v", j.events)
	}
	rs := ci.Results(context.Background(), p, broker{}, res.Run, sha1)
	if len(rs) != 1 || rs[0].Check != "unit-tests" || rs[0].Status != gates.StatusPassed || rs[0].Revision != sha1 {
		t.Fatalf("results=%+v", rs)
	}
	lr, err := ci.FetchLog(context.Background(), p, broker{}, &store{}, res.Run, "900", 100)
	if err != nil || lr.Excerpt != "ok\n" {
		t.Fatalf("log=%+v err=%v", lr, err)
	}
	if err := p.Cancel(context.Background(), res.Run); err != nil || !m.cancelled {
		t.Fatalf("cancel err=%v cancelled=%v", err, m.cancelled)
	}
}

// TestGitLabTokenOnlyInHeader proves P4: no request URL carries the token.
func TestGitLabTokenOnlyInHeader(t *testing.T) {
	m := ok()
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

// TestGitLabLostResponseReconciles proves P3: the create response is lost after the pipeline exists; the variable lookup binds it and create ran exactly once.
func TestGitLabLostResponseReconciles(t *testing.T) {
	m := ok()
	m.dropReply = true
	_, d, j := setup(t, m)
	res, err := d.Trigger(context.Background(), req)
	if err != nil || res.Outcome != ci.Succeeded || res.Via != ci.ViaResultingRun || res.Run.ID != "77" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if m.creates != 1 || !strings.HasSuffix(strings.Join(j.events, ","), "resolved:succeeded:resulting_resource") {
		t.Fatalf("creates=%d events=%v", m.creates, j.events)
	}
}

// TestGitLabLostResponseUnknown proves P3: a lost response with no visible pipeline is reported unknown and not retried.
func TestGitLabLostResponseUnknown(t *testing.T) {
	m := ok()
	m.dropReply, m.hidePipes = true, true
	_, d, _ := setup(t, m)
	res, err := d.Trigger(context.Background(), req)
	if err != nil || res.Outcome != ci.Unknown || m.creates != 1 {
		t.Fatalf("res=%+v err=%v creates=%d", res, err, m.creates)
	}
}

// TestGitLabRejectedIsNotExecuted proves P3: a 400 from create is not_executed.
func TestGitLabRejectedIsNotExecuted(t *testing.T) {
	m := ok()
	m.rejectCode = http.StatusBadRequest
	_, d, _ := setup(t, m)
	res, err := d.Trigger(context.Background(), req)
	if err != nil || res.Outcome != ci.NotExecuted {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

// TestGitLabCommitMismatch proves P2: a pipeline on another commit yields error, not passed.
func TestGitLabCommitMismatch(t *testing.T) {
	m := ok()
	m.pipeSHA, m.jobSHA = sha2, sha2
	p, d, _ := setup(t, m)
	res, _ := d.Trigger(context.Background(), req)
	rs := ci.Results(context.Background(), p, broker{}, res.Run, sha1)
	if rs[0].Status != gates.StatusError || !strings.Contains(rs[0].Reason, "commit mismatch") {
		t.Fatalf("%+v", rs)
	}
}

// TestGitLabCancelledAndTimedOut proves P2: a canceled job and a timeout failure are error with a reason.
func TestGitLabCancelledAndTimedOut(t *testing.T) {
	cases := []struct{ status, reason, want string }{
		{"canceled", "", "cancelled"},
		{"failed", "job_execution_timeout", "timed out"},
		{"failed", "stuck_or_timeout_failure", "timed out"},
	}
	for _, c := range cases {
		m := ok()
		m.jobStatus, m.jobReason = c.status, c.reason
		p, d, _ := setup(t, m)
		res, _ := d.Trigger(context.Background(), req)
		r := ci.Results(context.Background(), p, broker{}, res.Run, sha1)[0]
		if r.Status != gates.StatusError || r.Reason != c.want {
			t.Fatalf("%+v: %+v", c, r)
		}
	}
	m := ok()
	m.pipeStatus = "canceled" // passed job inside a canceled pipeline must not pass
	p, d, _ := setup(t, m)
	res, _ := d.Trigger(context.Background(), req)
	if r := ci.Results(context.Background(), p, broker{}, res.Run, sha1)[0]; r.Status != gates.StatusError || r.Reason != "run cancelled" {
		t.Fatalf("%+v", r)
	}
}

// TestGitLabFailedAndNotRun proves P2: a failed job is failed, manual or running jobs are not_run, an unknown state is error.
func TestGitLabFailedAndNotRun(t *testing.T) {
	for status, want := range map[string]gates.Status{"failed": gates.StatusFailed, "manual": gates.StatusNotRun, "running": gates.StatusNotRun, "weird": gates.StatusError} {
		m := ok()
		m.jobStatus = status
		p, d, _ := setup(t, m)
		res, _ := d.Trigger(context.Background(), req)
		if r := ci.Results(context.Background(), p, broker{}, res.Run, sha1)[0]; r.Status != want || r.Reason == "" {
			t.Fatalf("%s: %+v", status, r)
		}
	}
}

// TestGitLabUnreadableStatusRedacted proves P2 and P4: a failing status read is error and an echoed token is redacted.
func TestGitLabUnreadableStatusRedacted(t *testing.T) {
	m := ok()
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

// TestGitLabLogRedactedAndCapped proves P4 and P5: a planted token in a large trace is redacted before retention, and the excerpt is capped while the full redacted log is kept.
func TestGitLabLogRedactedAndCapped(t *testing.T) {
	m := ok()
	m.trace = strings.Repeat("x", 5000) + " token " + secret + " tail"
	p, d, _ := setup(t, m)
	res, _ := d.Trigger(context.Background(), req)
	st := &store{}
	lr, err := ci.FetchLog(context.Background(), p, broker{}, st, res.Run, "900", 100)
	if err != nil || strings.Contains(st.data, secret) || strings.Contains(lr.Excerpt, secret) {
		t.Fatalf("err=%v leaked", err)
	}
	if !lr.Truncated || len(lr.Excerpt) != 100 || len(st.data) < 5000 || lr.Ref != "log://1" {
		t.Fatalf("lr=%+v retained=%d", lr, len(st.data))
	}
}

// TestGitLabRejectsNonNumericIDs proves input validation: a run or job id with path characters is refused before any request.
func TestGitLabRejectsNonNumericIDs(t *testing.T) {
	m := ok()
	p, _, _ := setup(t, m)
	if _, err := p.Status(context.Background(), ci.RunRef{Project: "g/p", ID: "77/../x"}); !errors.Is(err, ci.ErrInvalid) {
		t.Fatalf("err=%v", err)
	}
	if _, err := p.Logs(context.Background(), ci.RunRef{Project: "g/p", ID: "77"}, "9/x"); !errors.Is(err, ci.ErrInvalid) || len(m.urls) != 0 {
		t.Fatalf("err=%v urls=%v", err, m.urls)
	}
}
