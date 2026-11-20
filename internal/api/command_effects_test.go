package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"synch/internal/eventlog"
	"synch/internal/thread"
	"testing"
)

const testRevision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// appendThreadFact seeds an event already produced by another daemon component.
func appendThreadFact(t *testing.T, store *eventlog.Store, id, kind string, version int, payload map[string]string) {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(t.Context(), []eventlog.NewEvent{{
		ID: eventlog.NewID(), Stream: "thread:" + id, ThreadID: id, Type: kind, TypeVersion: version,
		Actor: eventlog.Actor{Kind: "daemon", ID: "fixture"}, Payload: raw,
	}}); err != nil {
		t.Fatal(err)
	}
}

// awaitingAcceptance runs the public machine operations for a verified revision.
func awaitingAcceptance(t *testing.T, s *Server, store *eventlog.Store) string {
	t.Helper()
	id := create(t, s, "create")
	allow := func(context.Context, *eventlog.Transaction, thread.State) (bool, error) { return true, nil }
	machine, err := thread.Open(t.Context(), store, thread.Hooks{Limits: allow, Policy: allow, Pair: allow})
	if err != nil {
		t.Fatal(err)
	}
	s.config.Threads = machine
	if err := machine.Update(t.Context(), id, func(facts *thread.Facts) error {
		facts.AuditSetting = "off"
		facts.Revision = testRevision
		facts.NeedsDispatch = true
		facts.RequiredChecks = []string{"ci"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	selection := request(t, s, "POST", "/v1/threads/"+id+"/commands", testToken, "select", `{"command":"select_harness","harness":"codex","model":"model-x"}`)
	if selection.Code != 202 {
		t.Fatalf("select: %d %s", selection.Code, selection.Body)
	}
	for _, command := range []thread.Command{
		{Transition: "T01"},
		{Transition: "T05", Session: "session", Assignment: "assignment"},
		{Transition: "T25"},
	} {
		if err := machine.Apply(t.Context(), id, command); err != nil {
			state, _ := machine.Status(t.Context(), id)
			t.Fatalf("%s from %s: %v", command.Transition, state.Phase, err)
		}
	}
	appendThreadFact(t, store, id, "check.result", 1, map[string]string{"revision": testRevision, "check": "ci", "status": "passed"})
	if err := machine.Apply(t.Context(), id, thread.Command{Transition: "T26"}); err != nil {
		t.Fatal(err)
	}
	state, err := machine.Status(t.Context(), id)
	if err != nil || state.Phase != "awaiting_acceptance" {
		t.Fatalf("awaiting acceptance: %s %v", state.Phase, err)
	}
	return id
}

// eventsOfType reads durable thread evidence and verifies its exact expected count.
func eventsOfType(t *testing.T, store *eventlog.Store, id, kind string, count int) []eventlog.Event {
	t.Helper()
	all, err := store.ReadStream("thread:"+id, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var matches []eventlog.Event
	for _, e := range all {
		if e.Type == kind {
			matches = append(matches, e)
		}
	}
	if len(matches) != count {
		t.Fatalf("%s: got %d, want %d", kind, len(matches), count)
	}
	return matches
}

// TestAcceptanceCommands proves AC3 and AC4 acceptance/rejection evidence reaches the thread exactly once.
func TestAcceptanceCommands(t *testing.T) {
	for _, tc := range []struct{ action, decision, phase string }{
		{"accept", "accepted", "completed"}, {"reject", "rejected", "queued"},
	} {
		t.Run(tc.action, func(t *testing.T) {
			s, store, _ := fixture(t)
			id := awaitingAcceptance(t, s, store)
			path := "/v1/threads/" + id + "/commands"
			if tc.action == "accept" {
				stale := request(t, s, "POST", path, testToken, "stale", `{"command":"accept","revision":"`+strings.Repeat("b", 40)+`"}`)
				if stale.Code != 409 || decode[problem](t, stale.Body.Bytes()).Code != "invalid_transition" || stale.Header().Get(receiptIDHeader) == "" {
					t.Fatalf("stale: %d %s", stale.Code, stale.Body)
				}
				eventsOfType(t, store, id, "acceptance.recorded", 0)
			}
			body := `{"command":"` + tc.action + `","revision":"` + testRevision + `","note":""}`
			applied := request(t, s, "POST", path, testToken, "decision", body)
			if applied.Code != 202 {
				t.Fatalf("%s: %d %s", tc.action, applied.Code, applied.Body)
			}
			replay := request(t, s, "POST", path, testToken, "decision", body)
			if replay.Code != 202 || replay.Body.String() != applied.Body.String() || replay.Header().Get(replayedHeader) != "true" {
				t.Fatalf("replay: %d %s", replay.Code, replay.Body)
			}
			event := eventsOfType(t, store, id, "acceptance.recorded", 1)[0]
			payload := decode[map[string]string](t, event.Payload)
			note, hasNote := payload["note"]
			if event.TypeVersion != 1 || event.RequestID != "decision" || payload["revision"] != testRevision ||
				payload["decision"] != tc.decision || payload["operator"] != testCaller || !hasNote || note != "" || len(payload) != 4 {
				t.Fatalf("evidence: %+v %s", event, event.Payload)
			}
			state, err := s.config.Threads.Status(t.Context(), id)
			if err != nil || state.Phase != tc.phase {
				t.Fatalf("phase: %s %v", state.Phase, err)
			}
			if tc.action == "accept" && state.AcceptedRevision != testRevision {
				t.Fatalf("accepted revision: %s", state.AcceptedRevision)
			}
		})
	}
}

// TestExternalResolution proves AC3 and AC4 resolve only a known thread request using stored operation and caller evidence.
func TestExternalResolution(t *testing.T) {
	s, store, _ := fixture(t)
	id := create(t, s, "create")
	externalID := eventlog.NewID()
	appendThreadFact(t, store, id, "external_request.persisted", 3, map[string]string{"external_request_id": externalID, "operation": "snapshot"})
	path := "/v1/threads/" + id + "/commands"
	otherThread := create(t, s, "create-other")
	otherRequest := eventlog.NewID()
	appendThreadFact(t, store, otherThread, "external_request.persisted", 3, map[string]string{"external_request_id": otherRequest, "operation": "stop"})
	missing := request(t, s, "POST", path, testToken, "missing", `{"command":"resolve_external_request","external_request_id":"`+otherRequest+`","resolution":"succeeded","note":"checked provider"}`)
	if missing.Code != 404 || decode[problem](t, missing.Body.Bytes()).Code != "not_found" || missing.Header().Get(receiptIDHeader) == "" {
		t.Fatalf("missing request: %d %s", missing.Code, missing.Body)
	}
	eventsOfType(t, store, id, "external_request.resolved", 0)
	body := `{"command":"resolve_external_request","external_request_id":"` + externalID + `","resolution":"succeeded","note":"checked provider"}`
	first := request(t, s, http.MethodPost, path, testToken, "resolve", body)
	if first.Code != 202 {
		t.Fatalf("resolution: %d %s", first.Code, first.Body)
	}
	replay := request(t, s, http.MethodPost, path, testToken, "resolve", body)
	if replay.Code != 202 || replay.Body.String() != first.Body.String() || replay.Header().Get(replayedHeader) != "true" {
		t.Fatalf("replay: %d %s", replay.Code, replay.Body)
	}
	event := eventsOfType(t, store, id, "external_request.resolved", 1)[0]
	p := decode[map[string]string](t, event.Payload)
	if event.TypeVersion != 4 || event.RequestID != "resolve" || p["external_request_id"] != externalID || p["operation"] != "snapshot" ||
		p["resolution"] != "succeeded" || p["via"] != "operator" || p["operator"] != testCaller || p["note"] != "checked provider" || len(p) != 6 {
		t.Fatalf("evidence: %+v %s", event, event.Payload)
	}
	st, err := s.config.Threads.Status(t.Context(), id)
	if err != nil || st.Requests[externalID] != "succeeded" {
		t.Fatalf("resolved state: %v %v", st.Requests, err)
	}
}
