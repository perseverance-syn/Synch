package jenkins

import (
	"errors"

	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"synch/internal/ci"
)

// TestQueueLifecycle proves J1/J3: cancel a pending item and resolve an evicted
// item by matching the build's queue ID.
func TestQueueLifecycle(t *testing.T) {
	var cancelled atomic.Bool
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/queue/item/2/api/json":
			if cancelled.Load() {
				http.NotFound(w, r)
				return
			}
			writeMock(t, w, "{\"id\":2}")
		case "/queue/cancelItem":
			if r.Method != http.MethodPost || r.FormValue("id") != "2" {
				t.Error("incorrect queue cancellation")
			}
			cancelled.Store(true)
			w.WriteHeader(http.StatusNoContent)
		case "/job/gate/api/json":
			writeMock(t, w, "{\"builds\":[{\"number\":3,\"queueId\":2}]}")
		case "/job/gate/3/api/json":
			writeMock(t, w, "{\"result\":\"SUCCESS\",\"actions\":[{\"lastBuiltRevision\":{\"SHA1\":%q}}]}", sha)
		default:
			t.Errorf("unexpected request %v", r.URL)
			http.NotFound(w, r)
		}
	}))
	defer s.Close()
	p := New(Config{BaseURL: s.URL, Endpoint: "registered", Broker: broker{}})
	run := ci.RunRef{Project: "gate", ID: "queue:2"}
	status, err := p.Status(t.Context(), run)
	if err != nil || status.Verdict != ci.VerdictPending {
		t.Fatalf("queue: %+v, %v", status, err)
	}
	if err := p.Cancel(t.Context(), run); err != nil || !cancelled.Load() {
		t.Fatalf("cancel: %v", err)
	}
	status, err = p.Status(t.Context(), run)
	if err != nil || status.Verdict != ci.VerdictPassed || status.SHA != sha {
		t.Fatalf("expired queue: %+v, %v", status, err)
	}
}

// TestUntrustedLocation proves J4: a foreign Location cannot capture credentials.
func TestUntrustedLocation(t *testing.T) {
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("followed an untrusted queue URL")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer foreign.Close()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", foreign.URL+"/queue/item/2/")
		w.WriteHeader(http.StatusCreated)
	}))
	defer s.Close()
	p := New(Config{BaseURL: s.URL, Endpoint: "registered", Broker: broker{}})
	run, err := p.Trigger(t.Context(), ci.TriggerRequest{Project: "gate", Ref: "main", SHA: sha, Token: "synch-test"})
	if !errors.Is(err, ci.ErrTransient) || run.ID != "" {
		t.Fatalf("trigger: %+v, %v", run, err)
	}
}

// TestUnverifiedCheckout proves J2: requested parameters cannot establish revision evidence.
func TestUnverifiedCheckout(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeMock(t, w, "{\"result\":\"SUCCESS\",\"actions\":[{\"parameters\":[{\"name\":\"SYNCH_SHA\",\"value\":%q}]}]}", sha)
	}))
	defer s.Close()
	p := New(Config{BaseURL: s.URL, Endpoint: "registered", Broker: broker{}})
	results := ci.Results(t.Context(), p, broker{}, ci.RunRef{Project: "gate", ID: "build:3"}, sha)
	if len(results) != 1 || results[0].Status != "error" {
		t.Fatalf("results: %+v", results)
	}
}
