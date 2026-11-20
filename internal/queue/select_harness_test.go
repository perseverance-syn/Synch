package queue

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"synch/internal/eventlog"
	"synch/internal/thread"
)

// --- Fixtures: submit harness commands through the real receipt writer ---

// harnessFixture creates a running thread whose work can resume after a harness failure.
func harnessFixture(t *testing.T, path string) (*eventlog.Store, *thread.Machine, string) {
	t.Helper()
	s := openStore(t, path)
	h := thread.Hooks{Limits: allowHarness, Policy: allowHarness, Pair: allowHarness}
	m, err := thread.Open(t.Context(), s, h)
	if err != nil {
		t.Fatal(err)
	}
	id, assignment := eventlog.NewID(), eventlog.NewID()
	if err := m.Create(t.Context(), id, thread.Facts{}); err != nil {
		t.Fatal(err)
	}
	applyRow(t, m, id, thread.Command{Transition: "T01"})
	applyRow(t, m, id, thread.Command{Transition: "T05", Assignment: assignment, Session: "current"})
	raw, err := json.Marshal(map[string]string{"assignment_id": assignment, "execution_outcome": "exited", "work": "resumable"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Append(t.Context(), []eventlog.NewEvent{{ID: eventlog.NewID(), ThreadID: id, Stream: "thread:" + id, Type: "process.exited", TypeVersion: 1, Actor: eventlog.Actor{Kind: "daemon", ID: "local"}, Payload: raw}})
	if err != nil {
		t.Fatal(err)
	}
	return s, m, id
}

// allowHarness supplies a permissive dispatch predicate for the fixture.
func allowHarness(context.Context, *eventlog.Transaction, thread.State) (bool, error) {
	return true, nil
}

// selectHarness persists a selection via queue.Execute with no result supplied by the handler.
func selectHarness(t *testing.T, s *eventlog.Store, id, request, harness, model string, rejected bool) Response {
	t.Helper()
	raw, err := json.Marshal(map[string]string{"command": "select_harness", "harness": harness, "model": model})
	if err != nil {
		t.Fatal(err)
	}
	req := Request{Caller: "operator", ID: request, Method: "POST", Route: "/v1/threads/{thread_id}/commands", PathParams: map[string]string{"thread_id": id}, Body: raw}
	got, err := Execute(t.Context(), s, req, func(context.Context, *eventlog.Transaction, map[string]any) (Decision, error) {
		if rejected {
			return Decision{}, &Rejection{Code: "policy_denied"}
		}
		return Decision{ThreadID: id, HTTPStatus: 202}, nil
	})
	want := "accepted"
	if rejected {
		want = "rejected"
	}
	if err != nil || got.Status != want {
		t.Fatal(got, err)
	}
	return got
}

// --- D-16: scope, order, and replay the durable choice ---

// TestAcceptedSelectHarnessAllowsT21 reproduces D1 with a stored API receipt and resumable work.
func TestAcceptedSelectHarnessAllowsT21(t *testing.T) {
	s, m, id := harnessFixture(t, filepath.Join(t.TempDir(), "threads.db"))
	t.Cleanup(func() { _ = s.Close() })
	applyRow(t, m, id, thread.Command{Transition: "T15"})
	if err := m.Apply(t.Context(), id, thread.Command{Transition: "T21"}); !errors.Is(err, thread.ErrTransition) {
		t.Fatalf("resume without a selection: %v", err)
	}
	selectHarness(t, s, id, "selected", "codex", "gpt-6.2-sol", false)
	applyRow(t, m, id, thread.Command{Transition: "T21"})
	state, err := m.Status(t.Context(), id)
	if err != nil || state.Phase != "queued" || !state.Facts.SelectedPair {
		t.Fatal(state, err)
	}
	if got := state.SelectedHarness; got == nil || got.Harness != "codex" || got.Model != "gpt-6.2-sol" {
		t.Fatal(got)
	}
}

// TestSelectHarnessResumesAfterReopen verifies the durable selection drives T21 exactly once.
func TestSelectHarnessResumesAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "threads.db")
	s, m, id := harnessFixture(t, path)
	applyRow(t, m, id, thread.Command{Transition: "T15"})
	selectHarness(t, s, id, "selected", "claude-code", "sonnet", false)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	m, err := thread.Open(t.Context(), reopened, thread.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := m.Restart(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	events, err := reopened.ReadStream("thread:"+id, 0, 30)
	if err != nil {
		t.Fatal(err)
	}
	resumes := 0
	for _, event := range events {
		if event.Type != "thread.state_changed" {
			continue
		}
		var payload struct{ Transition string }
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Transition == "T21" {
			resumes++
		}
	}
	state, err := m.Status(t.Context(), id)
	if err != nil || state.Phase != "queued" || resumes != 1 {
		t.Fatal(state, resumes, err)
	}
	if got := state.SelectedHarness; got == nil || got.Harness != "claude-code" || got.Model != "sonnet" {
		t.Fatal(got)
	}
}

// TestHarnessSelectionUsesLatestAcceptedReceipt verifies replacement freshness, thread isolation, and replay ordering.
func TestHarnessSelectionUsesLatestAcceptedReceipt(t *testing.T) {
	s, m, id := harnessFixture(t, filepath.Join(t.TempDir(), "threads.db"))
	t.Cleanup(func() { _ = s.Close() })
	selectHarness(t, s, id, "old", "codex", "previous", false)
	applyRow(t, m, id, thread.Command{Transition: "T15"})
	other := eventlog.NewID()
	if err := m.Create(t.Context(), other, thread.Facts{}); err != nil {
		t.Fatal(err)
	}
	selectHarness(t, s, other, "other", "kimi-code", "different-thread", false)
	selectHarness(t, s, id, "denied", "kimi-code", "denied", true)
	if err := m.Update(t.Context(), id, func(f *thread.Facts) error { f.SelectedPair = true; return nil }); err != nil {
		t.Fatal(err)
	}
	state, err := m.Status(t.Context(), id)
	if err != nil || state.Phase != "paused" || state.Facts.SelectedPair || state.SelectedHarness == nil || state.SelectedHarness.Model != "previous" {
		t.Fatal(state, err)
	}

	selectHarness(t, s, id, "replacement", "claude-code", "sonnet", false)
	selectHarness(t, s, id, "latest", "codex", "gpt-6.2-sol", false)
	if got := selectHarness(t, s, id, "replacement", "claude-code", "sonnet", false); !got.Replayed {
		t.Fatal(got)
	}
	selectHarness(t, s, id, "new-denied", "kimi-code", "denied", true)
	// Card 105 signals after committing the receipt through this existing callback.
	if err := m.Update(t.Context(), id, func(*thread.Facts) error { return nil }); err != nil {
		t.Fatal(err)
	}
	state, err = m.Status(t.Context(), id)
	if err != nil || state.Phase != "queued" || !state.Facts.SelectedPair || state.SelectedHarness == nil ||
		state.SelectedHarness.Harness != "codex" || state.SelectedHarness.Model != "gpt-6.2-sol" {
		t.Fatal(state, err)
	}
}
