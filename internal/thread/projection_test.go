package thread

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"synch/internal/eventlog"
)

// --- Catalogue projection: state guards and persistence isolation (F1 and F3) ---

// TestCatalogueRunnerReplay verifies A2 and T21 using connected state and inclusive replay bounds.
func TestCatalogueRunnerReplay(t *testing.T) {
	f := setup(t)
	f.prepare("T21", "paused")
	f.seed(func(s *State) { s.Reason = "runner_disconnected"; s.Facts.RunnerID = "local" })
	f.emit("runner.connection", map[string]any{"runner": "local", "state": "connected"})
	if s := f.status(); !s.Facts.Connected || s.Facts.JournalReplayed {
		t.Fatalf("connected without replay: %+v", s.Facts)
	}
	if err := f.m.Apply(t.Context(), f.id, Command{Transition: "T21"}); !errors.Is(err, ErrTransition) {
		t.Fatalf("resume without replay: %v", err)
	}
	f.emit("runner.connection", map[string]any{"runner": "local", "state": "connected", "replayed_from_seq": 1, "replayed_to_seq": 8})
	f.apply(Command{Transition: "T21"})
	if s := f.status(); s.Phase != "queued" {
		t.Fatal(s)
	}
}

// TestProjectionCannotRejectWrites verifies A7 and persist-before-ack for a mistyped runner payload.
func TestProjectionCannotRejectWrites(t *testing.T) {
	t.Run("audit", func(t *testing.T) {
		f := setup(t)
		f.prepare("T26", "verifying")
		f.emit("audit.status", map[string]any{"audit_id": req, "level": 1, "status": "pass", "revision": "rev"})
		if s := f.status(); s.Audit != "pass" {
			t.Fatal(s)
		}
	})
	t.Run("malformed runner payload", func(t *testing.T) {
		f := setup(t)
		now := time.Now().UTC()
		// A mistyped revision is the intentional exception to catalogue-valid fixtures.
		bad := eventlog.RunnerRecord{RunnerEventVersion: 2, SourceSeq: 1, ID: eventlog.NewID(), Type: "check.result", TypeVersion: 1, Runner: "local", ThreadID: f.id, OccurredAt: now, Payload: json.RawMessage(`{"check":"unit","status":"passed","revision":1}`)}
		good := bad
		good.SourceSeq = 2
		good.ID = eventlog.NewID()
		good.Payload = json.RawMessage(`{"check":"unit","status":"failed","revision":"rev"}`)
		f.prepare("T26", "verifying")
		wrote, err := f.store.IngestRunner(t.Context(), "local", []eventlog.RunnerRecord{bad, good})
		if err != nil || len(wrote) != 2 {
			t.Fatal(wrote, err)
		}
		got, err := f.store.ReadStream("thread:"+f.id, 0, 10)
		if err != nil || len(got) != 2 || got[0].ID != bad.ID || got[1].ID != good.ID {
			t.Fatal(got, err)
		}
		if s := f.status(); s.Checks["unit"] != "failed" {
			t.Fatal(s)
		}
	})
}

// TestCatalogueGuardFacts exercises the external request and sandbox inputs with catalogue field names.
func TestCatalogueGuardFacts(t *testing.T) {
	f := setup(t)
	f.emit("external_request.persisted", map[string]any{"external_request_id": req, "driver": "vm", "endpoint": "endpoint", "operation": "start", "resource": "vm-1", "params_digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	f.apply(Command{Transition: "T01"})
	if s := f.status(); s.BlockedBy != "G.1" {
		t.Fatal(s)
	}
	f.resolve("succeeded")
	if s := f.status(); s.Phase != "queued" {
		t.Fatal(s)
	}
	f.seed(func(s *State) {
		s.Phase = "paused"
		s.Reason = "mode_switch"
		s.Facts.ActiveSettled = true
		s.Facts.SwitchToSandbox = true
	})
	f.emit("mode.changed", map[string]any{"target": "machine", "from": "full_control", "to": "sandbox", "phase": "active", "actor": "operator", "host_processes_stopped": true})
	f.apply(Command{Transition: "T21"})
	if s := f.status(); s.Phase != "queued" {
		t.Fatal(s)
	}
}

// --- Durable retry and recovery: use committed records across store lifetimes (F4 and F5) ---

// TestPendingResolutionFromAnotherStore verifies A6 at status and restart boundaries without duplicating T01.
func TestPendingResolutionFromAnotherStore(t *testing.T) {
	for _, boundary := range []string{"status", "restart"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := t.Context()
			path := filepath.Join(t.TempDir(), "threads.db")
			store, err := eventlog.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			machine, err := Open(ctx, store, Hooks{})
			if err != nil {
				t.Fatal(err)
			}
			id := eventlog.NewID()
			if err := machine.Create(ctx, id, Facts{}); err != nil {
				t.Fatal(err)
			}
			f := &fixture{t, machine, store, id}
			f.resolve("unknown")
			f.apply(Command{Transition: "T01"})
			if s := f.status(); s.BlockedBy != "G.1" {
				t.Fatal(s)
			}
			other, err := eventlog.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = other.Close() })
			(&fixture{t, nil, other, id}).resolve("succeeded")
			if boundary == "restart" {
				if err := machine.Restart(ctx); err != nil {
					t.Fatal(err)
				}
				// Inspect the stream before Status can perform its own catch-up.
				events, err := store.ReadStream("thread:"+id, 0, 10)
				if err != nil || len(events) != 3 || events[2].Type != "thread.state_changed" {
					t.Fatal(events, err)
				}
			}
			for range 2 {
				if s := f.status(); s.Phase != "queued" || s.BlockedBy != "" || s.Pending != nil {
					t.Fatal(s)
				}
			}
			events, err := store.ReadStream("thread:"+id, 0, 10)
			if err != nil || len(events) != 3 || events[2].Type != "thread.state_changed" {
				t.Fatal(events, err)
			}
		})
	}
}

// TestPendingVerificationRetrySurvivesRecovery verifies D-08 when T24 temporarily interrupts T27.
func TestPendingVerificationRetrySurvivesRecovery(t *testing.T) {
	f := setup(t)
	c := f.prepare("T27", "verifying")
	f.seed(func(s *State) { s.Facts.Assignment = req })
	f.emit("process.exited", map[string]string{"assignment_id": req, "execution_outcome": "exited", "work": "complete"})
	f.resolve("unknown")
	f.apply(c)
	f.resolve("succeeded")
	if err := f.m.Restart(t.Context()); err != nil {
		t.Fatal(err)
	}
	events, err := f.store.ReadStream("thread:"+f.id, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	var transitions []string
	for _, e := range events {
		if e.Type != "thread.state_changed" {
			continue
		}
		var p struct{ Transition string }
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			t.Fatal(err)
		}
		transitions = append(transitions, p.Transition)
	}
	if !slices.Equal(transitions, []string{"T24", "T38", "T27"}) {
		t.Fatal(transitions)
	}
	if s := f.status(); s.Phase != "queued" || s.Pending != nil || s.BlockedBy != "" {
		t.Fatal(s)
	}
}

// TestRestartJoinsRecoveryErrors verifies that failed supervisor calls do not skip later threads.
func TestRestartJoinsRecoveryErrors(t *testing.T) {
	f := setup(t)
	first, second := errors.New("first supervisor failure"), errors.New("second supervisor failure")
	threadIDs := []string{"01JD3Q8Z7Y6X5W4V3T2S1R0QP1", "01JD3Q8Z7Y6X5W4V3T2S1R0QP2", "01JD3Q8Z7Y6X5W4V3T2S1R0QP3"}
	var called []string
	f.m.hooks.ConfirmGone = func(_ context.Context, id string) (bool, error) {
		called = append(called, id)
		switch id {
		case threadIDs[0]:
			return false, first
		case threadIDs[1]:
			return false, second
		default:
			return true, nil
		}
	}
	for _, id := range threadIDs {
		if err := f.m.Create(t.Context(), id, Facts{}); err != nil {
			t.Fatal(err)
		}
		other := fixture{t, f.m, f.store, id}
		other.seed(func(s *State) { s.Phase = "running"; s.Facts.Assignment = req; s.Work[req] = "complete" })
	}
	err := f.m.Restart(t.Context())
	if !errors.Is(err, first) || !errors.Is(err, second) || !slices.Equal(called, threadIDs) {
		t.Fatal(called, err)
	}
	s, err := f.m.Status(t.Context(), threadIDs[2])
	if err != nil || s.Phase != "verifying" {
		t.Fatal(s, err)
	}
}
