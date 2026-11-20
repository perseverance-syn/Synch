package queue

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"synch/internal/eventlog"
	"synch/internal/thread"
)

// --- Fixtures: apply real transitions and store real decide receipts ---

// decisionFixture starts a dispatched thread on the real SQLite stores.
func decisionFixture(t *testing.T) (*eventlog.Store, *thread.Machine, string) {
	t.Helper()
	store := openStore(t, filepath.Join(t.TempDir(), "decisions.db"))
	t.Cleanup(func() { _ = store.Close() })
	allow := func(context.Context, *eventlog.Transaction, thread.State) (bool, error) { return true, nil }
	m, err := thread.Open(t.Context(), store, thread.Hooks{Limits: allow, Policy: allow, Pair: allow})
	if err != nil {
		t.Fatal(err)
	}
	id := eventlog.NewID()
	if err := m.Create(t.Context(), id, thread.Facts{}); err != nil {
		t.Fatal(err)
	}
	applyRow(t, m, id, thread.Command{Transition: "T01"})
	applyRow(t, m, id, thread.Command{Transition: "T05", Assignment: eventlog.NewID(), Session: "first"})
	return store, m, id
}

// applyRow applies a fixture transition through the durable state machine.
func applyRow(t *testing.T, m *thread.Machine, id string, cmd thread.Command) {
	t.Helper()
	if err := m.Apply(t.Context(), id, cmd); err != nil {
		t.Fatal(err)
	}
}

// decideRequest stores an accepted or rejected API decide receipt through the real command path.
func decideRequest(t *testing.T, s *eventlog.Store, m *thread.Machine, id, decisionID, requestID string, rejected bool) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"command": "decide", "decision_id": decisionID, "choice": "proceed"})
	if err != nil {
		t.Fatal(err)
	}
	r := Request{Caller: "operator", ID: requestID, Method: "POST", Route: "/v1/threads/{thread_id}/commands", PathParams: map[string]string{"thread_id": id}, Body: body}
	want := "accepted"
	if rejected {
		want = "rejected"
	}
	got, err := Execute(t.Context(), s, r, func(context.Context, *eventlog.Transaction, map[string]any) (Decision, error) {
		return Decision{ThreadID: id, HTTPStatus: 202, Commit: func(ctx context.Context, tx *eventlog.Transaction) (any, error) {
			if err := m.Control(ctx, tx, id, "decide", nil); err != nil {
				return nil, err
			}
			if rejected {
				return nil, &Rejection{Code: "policy_denied"}
			}
			return nil, nil
		}}, nil
	})
	if err != nil || got.Status != want {
		t.Fatal(got, err)
	}
}

// lastTransition returns the event ID used as the D-15 decision key.
func lastTransition(t *testing.T, s *eventlog.Store, id, want string) string {
	t.Helper()
	events, err := s.ReadStream("thread:"+id, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := len(events) - 1; i >= 0; i-- {
		e := events[i]
		if e.Type != "thread.state_changed" {
			continue
		}
		var p struct{ Transition string }
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			t.Fatal(err)
		}
		if p.Transition == want {
			return e.ID
		}
	}
	t.Fatalf("missing %s in %s", want, id)
	return ""
}

// requireDecisionBlocked checks that T11 has stayed put and reports the open decision.
func requireDecisionBlocked(t *testing.T, m *thread.Machine, id string) {
	t.Helper()
	s, err := m.Status(t.Context(), id)
	if err != nil || s.Phase != "awaiting_decision" || s.BlockedBy != "G.2" || s.Pending == nil || s.Pending.Transition != "T11" {
		t.Fatal(s, err)
	}
}

// --- Decision lifecycles: require the accepted receipt for each opening event ---

// TestOutOfScopeRequiresMatchingAcceptedDecide verifies D-15 event, decision ID, thread, and receipt status binding.
func TestOutOfScopeRequiresMatchingAcceptedDecide(t *testing.T) {
	store, m, id := decisionFixture(t)
	planID := eventlog.NewID()
	p, err := json.Marshal(map[string]any{"plan_revision": 2, "plan_digest": "sha256:" + strings.Repeat("a", 64), "brief_version": 1, "within_scope": false, "scope_changes": []string{"new_target"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Append(t.Context(), []eventlog.NewEvent{{ID: planID, ThreadID: id, Stream: "thread:" + id, Type: "plan.revised", TypeVersion: 2, Actor: eventlog.Actor{Kind: "daemon", ID: "local"}, Payload: p}})
	if err != nil {
		t.Fatal(err)
	}
	applyRow(t, m, id, thread.Command{Transition: "T10"})
	transitionID := lastTransition(t, store, id, "T10")
	applyRow(t, m, id, thread.Command{Transition: "T11"})
	requireDecisionBlocked(t, m, id)
	decideRequest(t, store, m, id, eventlog.NewID(), "different-id", false)
	requireDecisionBlocked(t, m, id)
	other := eventlog.NewID()
	if err := m.Create(t.Context(), other, thread.Facts{}); err != nil {
		t.Fatal(err)
	}
	decideRequest(t, store, m, other, planID, "different-thread", false)
	requireDecisionBlocked(t, m, id)
	decideRequest(t, store, m, id, planID, "denied", true)
	requireDecisionBlocked(t, m, id)
	decideRequest(t, store, m, id, transitionID, "transition", false)
	requireDecisionBlocked(t, m, id)
	decideRequest(t, store, m, id, planID, "plan", false)
	s, err := m.Status(t.Context(), id)
	if err != nil || s.Phase != "queued" || s.BlockedBy != "" || s.Pending != nil || s.Facts.PlanRevision != 2 {
		t.Fatal(s, err)
	}
	lastTransition(t, store, id, "T11")
}

// TestDisagreementDecisionsVerifyBothEvents verifies T17 and T34 each require their own accepted decide receipt.
func TestDisagreementDecisionsVerifyBothEvents(t *testing.T) {
	store, m, id := decisionFixture(t)
	applyRow(t, m, id, thread.Command{Transition: "T17"})
	disagreement := lastTransition(t, store, id, "T17")
	err := m.Update(t.Context(), id, func(f *thread.Facts) error { f.Material = true; f.ResolutionChecked = true; return nil })
	if err != nil {
		t.Fatal(err)
	}
	applyRow(t, m, id, thread.Command{Transition: "T34"})
	escalation := lastTransition(t, store, id, "T34")
	decideRequest(t, store, m, id, escalation, "escalation", false)
	requireDecisionBlocked(t, m, id)
	decideRequest(t, store, m, id, disagreement, "disagreement", false)
	s, err := m.Status(t.Context(), id)
	if err != nil || s.Phase != "queued" {
		t.Fatal(s, err)
	}
	if err := m.Apply(t.Context(), id, thread.Command{Transition: "T11"}); !errors.Is(err, thread.ErrTransition) {
		t.Fatal(err)
	}
}
