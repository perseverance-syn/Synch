package thread

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"synch/internal/eventlog"
	"testing"
)

// --- Interrupted work: classify recovery and handle an existing harness pause (AC4, D-09) ---

// TestRecoveryClassification verifies AC4 by checking the final row selected for each interrupted work classification.
func TestRecoveryClassification(t *testing.T) {
	cases := []struct {
		start, work string
		left        bool
		exit        string
	}{{"running", "complete", true, "T38"}, {"verifying", "complete", false, "T38"}, {"rollover", "complete", false, "T22"}, {"rollover", "resumable", true, "T22"}, {"running", "lost", true, "T23"}, {"verifying", "lost", false, "T36"}}
	for _, tc := range cases {
		t.Run(tc.start+"/"+tc.work+"/"+tc.exit, func(t *testing.T) {
			f := setup(t)
			f.seed(func(s *State) {
				s.Phase = tc.start
				s.Facts.Assignment = "a"
				s.Facts.AttemptsLeft = tc.left
				s.Work["a"] = tc.work
				if tc.start == "rollover" {
					s.Facts.CheckpointID = "cp"
				}
			})
			if err := f.m.Restart(t.Context()); err != nil {
				t.Fatal(err)
			}
			ev, err := f.store.ReadStream("thread:"+f.id, 0, 10)
			if err != nil || len(ev) != 2 {
				t.Fatal(ev, err)
			}
			for i, want := range []string{"T24", tc.exit} {
				var p map[string]string
				if err := json.Unmarshal(ev[i].Payload, &p); err != nil {
					t.Fatal(err)
				}
				if p["transition"] != want || i == 0 && p["interrupted_state"] != tc.start {
					t.Fatal(p)
				}
			}
		})
	}
}

// TestAlreadyPausedHarnessWithLostWork verifies D-09 by covering the existing harness pause, explicit restart, and exhausted attempts.
func TestAlreadyPausedHarnessWithLostWork(t *testing.T) {
	for _, left := range []bool{true, false} {
		f := setup(t)
		f.seed(func(s *State) {
			s.Phase = "paused"
			s.Reason = "harness_unavailable"
			s.Facts.Assignment = req
			s.Facts.AttemptsLeft = left
			s.Checkpoint = Reference{"file:///old", "digest"}
			s.Facts.CheckpointID = "old"
		})
		f.emit("process.exited", map[string]string{"assignment_id": req, "work": "lost", "execution_outcome": "lost_on_reboot"})
		if !left {
			if s := f.status(); s.Phase != "failed" {
				t.Fatal(s)
			}
			continue
		}
		if s := f.status(); s.Reason != "harness_unavailable" {
			t.Fatal(s)
		}
		ev, err := f.store.ReadStream("thread:"+f.id, 0, 10)
		if err != nil || len(ev) != 1 {
			t.Fatal(ev, err)
		}
		f.seedHarnessReceipt()
		if err := f.m.Update(t.Context(), f.id, func(p *Facts) error { p.RestartFromCheckpoint = true; return nil }); err != nil {
			t.Fatal(err)
		}
		if s := f.status(); s.Phase != "queued" {
			t.Fatal(s)
		}
	}
}

// --- Pause behavior: preserve live work and admit controls (AC2?AC3) ---

// TestPreDispatchAndRunningPressure verifies AC2 and AC3 by exercising G.4 before first dispatch and disk pressure during live work.
func TestPreDispatchAndRunningPressure(t *testing.T) {
	f := setup(t)
	f.apply(Command{Transition: "T01"})
	pressure := func(level string) {
		f.emit("resource.pressure", map[string]string{"resource": "disk", "level": level})
	}
	pressure("pressure")
	f.apply(Command{Transition: "T07"})
	pressure("ok")
	f.apply(Command{Transition: "T21"})
	f.apply(Command{Transition: "T05", Assignment: "a", Session: "first"})
	pressure("pressure")
	if s := f.status(); s.Phase != "running" {
		t.Fatal(s)
	}
	if err := f.m.Apply(t.Context(), f.id, Command{Transition: "T33"}); !errors.Is(err, ErrTransition) {
		t.Fatal(err)
	}
	if err := f.m.Update(t.Context(), f.id, func(p *Facts) error { p.NeedsDispatch = true; return nil }); err != nil {
		t.Fatal(err)
	}
	f.apply(Command{Transition: "T33"})
}

// TestControlAdmissionAllStates verifies AC3 by admitting each control and recovery command in every state.
func TestControlAdmissionAllStates(t *testing.T) {
	f := setup(t)
	err := f.store.Write(t.Context(), func(tx *eventlog.Transaction) error {
		for _, phase := range strings.Fields("draft plan_review queued running waiting_children awaiting_decision paused rollover verifying awaiting_acceptance completed failed cancelled") {
			for _, action := range strings.Fields("pause cancel resume revoke export decide resolve_external_request") {
				s, err := load(t.Context(), tx, f.id)
				if err != nil {
					return err
				}
				s.Phase = phase
				s.Reason = ""
				if phase == "paused" {
					s.Reason = "operator"
				}
				if err := save(t.Context(), tx, &s); err != nil {
					return err
				}
				called := false
				if err := f.m.Control(t.Context(), tx, f.id, action, func() error { called = true; return nil }); err != nil {
					return err
				}
				if !called {
					t.Fatalf("%s in %s", action, phase)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// --- Durability: preserve checkpoint and recovery obligations across restarts (AC4?AC5) ---

// TestUncertainRollover verifies AC5 by checking the checkpoint fields and T35 when an external request is unresolved.
func TestUncertainRollover(t *testing.T) {
	f := setup(t)
	f.prepare("T13", "running")
	f.resolve("unknown")
	dir := t.TempDir()
	_, err := f.m.Rollover(t.Context(), dir, Checkpoint{ThreadID: f.id}, "operator")
	if err != nil {
		t.Fatal(err)
	}
	if s := f.status(); s.Reason != "reconciliation" {
		t.Fatal(s)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatal(entries, err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	keys := []string{"checkpoint_version", "thread_id", "brief_version", "plan", "revision", "pending_edits", "failed_attempts", "open_decisions", "uncertain_external_requests", "skills", "handoff_files"}
	if len(fields) != len(keys) {
		t.Fatal(fields)
	}
	for _, k := range keys {
		if fields[k] == nil {
			t.Fatal(k)
		}
	}
	var requests []string
	if err := json.Unmarshal(fields["uncertain_external_requests"], &requests); err != nil || !slices.Equal(requests, []string{req}) {
		t.Fatal(requests, err)
	}
}

// TestRecoveryAfterStoreReopen verifies AC4 by reopening SQLite twice while interrupted work still owes classification.
func TestRecoveryAfterStoreReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.db")
	id := eventlog.NewID()
	for i := range 3 {
		s, err := eventlog.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		m, err := Open(t.Context(), s, Hooks{ConfirmGone: func(context.Context, string) (bool, error) { return true, nil }})
		if err != nil {
			t.Fatal(err)
		}
		f := fixture{t, m, s, id}
		if i == 0 {
			if err = m.Create(t.Context(), id, Facts{}); err != nil {
				t.Fatal(err)
			}
			f.seed(func(s *State) {
				s.Phase = "running"
				s.Facts.Assignment = "a"
				s.Work["a"] = "lost"
				s.Facts.AttemptsLeft = false
			})
			f.resolve("unknown")
		}
		if err = m.Restart(t.Context()); err != nil {
			t.Fatal(err)
		}
		if state := f.status(); state.Reason != recovery || state.Interrupted != "running" {
			t.Fatal(state)
		}
		if i == 2 {
			f.resolve("failed")
			if state := f.status(); state.Phase != "failed" {
				t.Fatal(state)
			}
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
