package jenkins

import (
	"context"
	"errors"

	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"synch/internal/ci"
)

type journal struct {
	persisted atomic.Bool
	fail      bool
	bound     string
	result    ci.Resolution
}

// Persist records the durable boundary observed by the Jenkins mock.
func (j *journal) Persist(_ context.Context, r ci.Record) error {
	if j.fail || r.Driver != "jenkins" || r.Operation != ci.OperationTrigger || r.Resource != "gate@"+sha {
		return errors.New("persist failed")
	}
	j.persisted.Store(true)
	return nil
}

// BindTask records the recovered queue or build ID.
func (j *journal) BindTask(_ context.Context, _, _, id string) error {
	j.bound = id
	return nil
}

// Resolve captures the settled decision for assertions.
func (j *journal) Resolve(_ context.Context, r ci.Resolution) error {
	j.result = r
	return nil
}

// TestRecovery proves J3: persist first, single trigger, queue/build recovery,
// deduplication across the queue transition, and unknown on absent or conflicting evidence.
func TestRecovery(t *testing.T) {
	const params = `"actions":[{"parameters":[{"name":"SYNCH_TOKEN","value":"synch-request"}]}]`
	for _, tc := range []struct {
		name          string
		queue, builds string
		want          string
	}{
		{"queue", `{"id":2,"task":{"url":"/job/gate/"}},DUMMY`, "", "queue:2"},
		{"build", "", `{"number":3,"queueId":2,` + params + "}", "build:3"},
		{"transition", `{"id":2,"task":{"url":"/job/gate/"}},DUMMY`, `{"number":3,"queueId":2,` + params + "}", "queue:2"},
		{"missing", "", "", ""},
		{"multiple", "", `{"number":3,` + params + `},{"number":4,` + params + "}", ""},
		{"other-job", `{"id":2,"task":{"url":"/job/elsewhere/"}},DUMMY`, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			j := &journal{}
			var posts atomic.Int32
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/job/gate/buildWithParameters":
					posts.Add(1)
					if !j.persisted.Load() || r.FormValue("SYNCH_TOKEN") != "synch-request" {
						t.Error("dispatched without persistence or token")
					}
					// Jenkins committed the operation, but the client lost the response.
					conn, _, err := http.NewResponseController(w).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
				case "/queue/api/json":
					writeMock(t, w, `{"items":[%s]}`, strings.Replace(tc.queue, "},DUMMY", ","+params+"}", 1))
				case "/job/gate/api/json":
					writeMock(t, w, `{"builds":[%s]}`, tc.builds)
				default:
					t.Errorf("unexpected request to %v", r.URL)
					http.NotFound(w, r)
				}
			}))
			defer s.Close()
			p := New(Config{BaseURL: s.URL, Endpoint: "registered", Broker: broker{}})
			d := ci.Dispatcher{Provider: p, Journal: j, Endpoint: "registered", NewID: func() string { return "request" }}
			req := ci.TriggerRequest{Project: "gate", Ref: "main", SHA: sha}
			j.fail = true
			if _, err := d.Trigger(t.Context(), req); err == nil || posts.Load() != 0 {
				t.Fatalf("persist failure allowed dispatch: %v", err)
			}
			j.fail = false
			got, err := d.Trigger(t.Context(), req)
			want, via := ci.Unknown, ci.ViaNone
			if tc.want != "" {
				want, via = ci.Succeeded, ci.ViaResultingRun
			}
			if err != nil || posts.Load() != 1 || got.Outcome != want || got.Via != via || got.Run.ID != tc.want || j.bound != tc.want || j.result.Outcome != want {
				t.Fatalf("result %+v, err %v, posts %d, journal %+v", got, err, posts.Load(), j.result)
			}
		})
	}
}

// --- Check verdicts and fail-closed boundaries ---

// TestBuildVerdicts proves J2/J6: Jenkins results, in-flight state, and no
// passed stage evidence after an abort or timeout.
func TestBuildVerdicts(t *testing.T) {
	for _, tc := range []struct {
		result   string
		building bool
		want     ci.Verdict
	}{
		{"SUCCESS", false, ci.VerdictPassed},
		{"UNSTABLE", false, ci.VerdictFailed},
		{"FAILURE", false, ci.VerdictFailed},
		{"ABORTED", false, ci.VerdictCancelled},
		{"NOT_BUILT", false, ci.VerdictSkipped},
		{"TIMED_OUT", false, ci.VerdictTimedOut},
		{"", true, ci.VerdictPending},
		{"planted-token", false, ci.VerdictOther},
	} {
		t.Run(tc.result, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/job/gate/3/api/json":
					writeMock(t, w, `{"result":%q,"building":%t,"actions":[{"lastBuiltRevision":{"SHA1":%q}}]}`, tc.result, tc.building, sha)
				case "/job/gate/3/wfapi/describe":
					writeMock(t, w, `{"stages":[{"name":"lint","id":"7","status":"SUCCESS"}]}`)
				default:
					http.NotFound(w, r)
				}
			}))
			defer s.Close()
			p := New(Config{BaseURL: s.URL, Endpoint: "registered", Broker: broker{}})
			run := ci.RunRef{Project: "gate", ID: "build:3"}
			status, err := p.Status(t.Context(), run)
			if err != nil || status.Verdict != tc.want || strings.Contains(status.Raw, "planted-token") {
				t.Fatalf("status %+v, err %v", status, err)
			}
			if tc.want == ci.VerdictCancelled || tc.want == ci.VerdictTimedOut || tc.want == ci.VerdictOther || tc.want == ci.VerdictPending {
				results := ci.Results(t.Context(), p, broker{}, run, sha)
				if len(results) != 1 || results[0].Status == "passed" {
					t.Fatalf("unsafe results %+v", results)
				}
			}
		})
	}
}

// TestCrumbFailure proves J4/J6: no mutation follows a failed crumb fetch and
// a credential-bearing response or URL is never reflected in an error.
func TestCrumbFailure(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/crumbIssuer/api/json" {
			t.Errorf("unexpected dispatch: %v", r.URL)
		}
		http.Error(w, "planted-token crumb-secret", http.StatusForbidden)
	}))
	defer s.Close()
	p := New(Config{BaseURL: s.URL, Endpoint: "registered", Broker: broker{}, RequireCrumb: true})
	_, err := p.Trigger(t.Context(), ci.TriggerRequest{Project: "gate", Ref: "main", SHA: sha, Token: "synch-request"})
	if !errors.Is(err, ci.ErrRejected) || strings.Contains(err.Error(), "planted-token") || strings.Contains(err.Error(), "crumb-secret") {
		t.Fatalf("trigger error: %v", err)
	}
}

// TestOversizedLog proves J1/J6: retention and excerpt caps remove a secret
// crossing the source retention boundary and report the cut.
func TestOversizedLog(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/job/gate/3/consoleText" {
			t.Errorf("unexpected request %v", r.URL)
		}
		writeMock(t, w, "%s%s", strings.Repeat("x", ci.MaxRetainedLog-4), "planted-token")
	}))
	defer s.Close()
	p := New(Config{BaseURL: s.URL, Endpoint: "registered", Broker: broker{}})
	run := ci.RunRef{Project: "gate", ID: "build:3"}
	store := &logStore{}
	got, err := p.FetchLog(t.Context(), store, run, "console", 8)
	if err != nil || !got.RetainedTruncated || !got.Truncated || got.Excerpt != "xxxxxxxx" ||
		len(store.data) > ci.MaxRetainedLog || strings.Trim(store.data, "x") != "" {
		t.Fatalf("log %+v, retained length %d, error %v", got, len(store.data), err)
	}
}
