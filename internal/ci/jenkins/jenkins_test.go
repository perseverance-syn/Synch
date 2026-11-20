package jenkins

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"synch/internal/ci"
)

const sha = "0123456789abcdef0123456789abcdef01234567"

type broker struct{}

// Token supplies separate Jenkins username and token values.
func (broker) Token(_ context.Context, endpoint string) (string, error) {
	if endpoint == "registered/user" {
		return "build-user", nil
	}
	return "planted-token", nil
}

// KnownValues declares the broker credentials for shared log retention.
func (broker) KnownValues() []string { return []string{"build-user", "planted-token"} }

// TestOperations proves J1, J2, J4 and J6 against Jenkins HTTP shapes.
func TestOperations(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, token, _ := r.BasicAuth()
		if u != "build-user" || token != "planted-token" || strings.Contains(r.URL.String(), token) {
			t.Error("incorrect credential placement")
		}
		switch r.URL.Path {
		case "/jenkins/crumbIssuer/api/json":
			http.SetCookie(w, &http.Cookie{Name: "JSESSIONID", Value: "session-secret", Path: "/jenkins"})
			writeMock(t, w, `{"crumbRequestField":"Jenkins-Crumb","crumb":"crumb-secret"}`)
		case "/jenkins/job/folder/job/gates/buildWithParameters":
			if r.Method != http.MethodPost || r.FormValue("SYNCH_SHA") != sha || r.FormValue("SYNCH_TOKEN") != "synch-test" || r.FormValue("SYNCH_REF") != "main" {
				t.Error("incorrect trigger parameters")
			}
			cookie, err := r.Cookie("JSESSIONID")
			if r.Header.Get("Jenkins-Crumb") != "crumb-secret" || err != nil || cookie.Value != "session-secret" {
				t.Error("missing crumb session")
			}
			w.Header().Set("Location", "/jenkins/queue/item/2/")
			w.WriteHeader(http.StatusCreated)
		case "/jenkins/queue/item/2/api/json":
			writeMock(t, w, `{"id":2,"executable":{"number":3}}`)
		case "/jenkins/job/folder/job/gates/3/api/json":
			writeMock(t, w, `{"result":"SUCCESS","actions":[{"lastBuiltRevision":{"SHA1":%q}}]}`, sha)
		case "/jenkins/job/folder/job/gates/3/wfapi/describe":
			writeMock(t, w, `{"stages":[{"id":"7","name":"lint","status":"SUCCESS"}]}`)
		case "/jenkins/job/folder/job/gates/3/execution/node/7/wfapi/log":
			writeMock(t, w, `{"text":"build-user planted-token crumb-secret session-secret done","hasMore":false}`)
		case "/jenkins/job/folder/job/gates/3/stop":
			if r.Method != http.MethodPost || r.Header.Get("Jenkins-Crumb") != "crumb-secret" {
				t.Error("invalid stop")
			}
			w.WriteHeader(http.StatusFound)
		default:
			t.Errorf("unexpected request %v", r.URL)
			http.NotFound(w, r)
		}
	}))
	defer s.Close()
	p := New(Config{BaseURL: s.URL + "/jenkins", Endpoint: "registered", Broker: broker{}, RequireCrumb: true})
	req := ci.TriggerRequest{Project: "folder/gates", Ref: "main", SHA: sha, Token: "synch-test"}
	run, err := p.Trigger(t.Context(), req)
	if err != nil || run.ID != "queue:2" {
		t.Fatalf("trigger: %v %v", run, err)
	}
	results := ci.Results(t.Context(), p, broker{}, run, sha)
	if len(results) != 1 || results[0].Check != "lint" || results[0].Status != "passed" {
		t.Fatalf("results: %+v", results)
	}
	results = ci.Results(t.Context(), p, broker{}, run, strings.Repeat("a", 40))
	if len(results) != 1 || results[0].Status != "error" {
		t.Fatalf("mismatch: %+v", results)
	}
	raw, err := p.Logs(t.Context(), run, "7")
	if err != nil || strings.Count(string(raw), ci.Redacted) != 4 {
		t.Fatalf("direct log was not redacted: %v", err)
	}
	store := &logStore{}
	log, err := p.FetchLog(t.Context(), store, run, "7", 4)
	if err != nil || log.Ref != "stored" || log.Excerpt != "done" || !log.Truncated || strings.Count(store.data, ci.Redacted) != 4 {
		t.Fatalf("log: %+v, %v, stored %q", log, err, store.data)
	}
	if err := p.Cancel(t.Context(), run); err != nil {
		t.Fatal(err)
	}
}

type logStore struct{ data string }

// Put captures retained log data for assertions.
func (s *logStore) Put(_ context.Context, _ string, data []byte) (string, string, error) {
	s.data = string(data)
	return "stored", "digest", nil
}

// writeMock sends a fixture and reports an unexpected disconnected client.
func writeMock(t *testing.T, w http.ResponseWriter, format string, args ...any) {
	t.Helper()
	if _, err := fmt.Fprintf(w, format, args...); err != nil {
		t.Error(err)
	}
}
