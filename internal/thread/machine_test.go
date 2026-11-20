package thread

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"synch/internal/eventlog"
	"testing"
)

// --- Fixtures: persist inputs and apply commands through the writer ---

type fixture struct {
	t     *testing.T
	m     *Machine
	store *eventlog.Store
	id    string
}

// allow supplies a permissive injected dispatch predicate for tests.
func allow(context.Context, *eventlog.Transaction, State) (bool, error) { return true, nil }

// setup creates an isolated machine, event store, and draft thread.
func setup(t *testing.T) *fixture {
	s, err := eventlog.Open(filepath.Join(t.TempDir(), "threads.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	m, err := Open(t.Context(), s, Hooks{Limits: allow, Policy: allow, Pair: allow, ConfirmGone: func(context.Context, string) (bool, error) { return true, nil }})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t, m, s, eventlog.NewID()}
	if err := m.Create(t.Context(), f.id, Facts{}); err != nil {
		t.Fatal(err)
	}
	return f
}

// seed sets durable state for a test without replaying unrelated transitions.
func (f *fixture) seed(edit func(*State)) {
	f.t.Helper()
	err := f.store.Write(f.t.Context(), func(tx *eventlog.Transaction) error {
		s, err := load(f.t.Context(), tx, f.id)
		if err != nil {
			return err
		}
		edit(&s)
		return save(f.t.Context(), tx, &s)
	})
	if err != nil {
		f.t.Fatal(err)
	}
}

// status loads the fixture's committed thread state or fails the test.
func (f *fixture) status() State {
	f.t.Helper()
	s, err := f.m.Status(f.t.Context(), f.id)
	if err != nil {
		f.t.Fatal(err)
	}
	return s
}

// apply requests a transition through the machine or fails the test.
func (f *fixture) apply(c Command) {
	f.t.Helper()
	if err := f.m.Apply(f.t.Context(), f.id, c); err != nil {
		f.t.Fatal(c, err)
	}
}

// emit appends a catalogue event and returns its durable event ID and sequence.
func (f *fixture) emit(typ string, p any) eventlog.Event {
	f.t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		f.t.Fatal(err)
	}
	v := 1
	switch typ {
	case "external_request.resolved":
		v = 4
	case "external_request.persisted":
		v = 3
	case "plan.revised", "mode.changed":
		v = 2
	}
	stream, id := "thread:"+f.id, f.id
	if typ == "runner.connection" || typ == "resource.pressure" {
		stream, id = "system", ""
	}
	events, err := f.store.Append(f.t.Context(), []eventlog.NewEvent{{ID: eventlog.NewID(), ThreadID: id, Stream: stream, Type: typ, TypeVersion: v, Actor: eventlog.Actor{Kind: "daemon", ID: "local"}, Payload: raw}})
	if err != nil {
		f.t.Fatal(err)
	}
	return events[0]
}

const req = "01JD3Q8Z7Y6X5W4V3T2S1R0QPN"

// resolve records the latest outcome for the fixture's external request.
func (f *fixture) resolve(status string) {
	via := "response"
	if status == "unknown" {
		via = "none"
	}
	f.emit("external_request.resolved", map[string]string{"external_request_id": req, "operation": "start", "resolution": status, "via": via})
}

// seedHarnessReceipt installs the accepted receipt record for synthetic pause fixtures.
func (f *fixture) seedHarnessReceipt() {
	f.t.Helper()
	record, err := json.Marshal(map[string]any{"thread_id": f.id, "command": "select_harness", "event_seq": 10,
		"result": map[string]string{"harness": "codex", "model": "gpt-6.2-sol"}})
	if err != nil {
		f.t.Fatal(err)
	}
	err = f.store.Write(f.t.Context(), func(tx *eventlog.Transaction) error {
		_, err := tx.ExecContext(f.t.Context(), `INSERT INTO receipts
			(caller_identity, request_id, receipt_id, payload_digest, status, http_status, content_type, body, record)
			VALUES('local','select-fixture',?,'fixture','accepted',202,'application/json',?,?)`, eventlog.NewID(), record, record)
		return err
	})
	if err != nil {
		f.t.Fatal(err)
	}
}

// prepare arranges the row-specific facts and returns a catalogue test command.
func (f *fixture) prepare(row, from string) Command {
	f.seed(func(s *State) {
		s.Phase = from
		s.Reason = ""
		s.Facts = Facts{PlanDigest: "p", Revision: "rev", EvaluatedRevision: "rev", AuditSetting: "off", AttemptsLeft: true}
		s.Work = map[string]string{}
		s.Requests = map[string]string{}
		s.Decisions = map[string]bool{}
		s.Checks = map[string]string{}
		s.Pending = nil
		s.BlockedBy = ""
		s.OutOfScope = ""
		if from == "paused" {
			s.Reason = "operator"
		}
		switch row {
		case "T02":
			s.Facts.ReviewPlanFirst = true
		case "T06":
			f.m.hooks.Pair = func(context.Context, *eventlog.Transaction, State) (bool, error) { return false, nil }
		case "T07":
			s.Facts.DiskPressure = true
		case "T08":
			s.Facts.Awaited = []string{"child"}
		case "T10":
			s.Facts.Material = true
		case "T14":
			s.Facts.CheckpointID = "cp"
		case "T16", "T35":
			s.Requests[req] = "unknown"
			if row == "T35" {
				s.Facts.CheckpointID = "cp"
			}
		case "T19":
			s.Facts.NeedsDispatch = true
		case "T22", "T38":
			s.Reason = recovery
			s.RecoveryReady = true
			s.Interrupted = "running"
			s.Facts.Assignment = "a"
			s.Work["a"] = "resumable"
			if row == "T38" {
				s.Work["a"] = "complete"
			}
		case "T23", "T36":
			s.Facts.Assignment = "a"
			s.Work["a"] = "lost"
			s.Facts.AttemptsLeft = row == "T23"
		case "T26", "T27":
			s.Facts.RequiredChecks = []string{"test"}
			s.Checks["test"] = "failed"
		case "T28":
			s.AcceptedRevision = "rev"
		case "T31":
			s.Facts.Unrecoverable = true
		case "T33":
			s.Facts.NeedsDispatch = true
			s.Facts.DiskPressure = true
		case "T34":
			s.Reason = "disagreement"
			s.Facts.Material = true
			s.Facts.ResolutionChecked = true
		}
	})
	return Command{Transition: row, PlanDigest: "p", Revision: "rev", Assignment: "a", Session: "session", BriefVersion: 1}
}

// --- Transition catalogue: cover legal rows and reject illegal transitions (AC1) ---

type schemaValue struct {
	Const string
	Enum  []string
}

// TestTransitionCatalogue verifies AC1 with every catalogue row, illegal source/destination pairs, and retired T37.
func TestTransitionCatalogue(t *testing.T) {
	raw, err := os.ReadFile("../../contracts/events/catalogue.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Defs map[string]json.RawMessage `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	var d struct {
		Properties struct {
			Payload struct {
				OneOf []struct{ Properties map[string]schemaValue }
			}
		}
	}
	if err := json.Unmarshal(schema.Defs["e_thread_state_changed"], &d); err != nil {
		t.Fatal(err)
	}
	states := strings.Fields("draft plan_review queued running waiting_children awaiting_decision paused rollover verifying awaiting_acceptance completed failed cancelled")
	allowed := map[string]bool{}
	for _, r := range d.Properties.Payload.OneOf {
		p := r.Properties
		row, to := p["transition"].Const, p["to"].Const
		from := p["from"].Enum
		if len(from) == 0 {
			from = []string{p["from"].Const}
		}
		if row == "T30" {
			from = states[:10]
		}
		for _, src := range from {
			allowed[src+"/"+to] = true
			t.Run(row+"/"+src, func(t *testing.T) {
				f := setup(t)
				c := f.prepare(row, src)
				f.apply(c)
				s := f.status()
				if s.Phase != to || s.Reason != p["pause_reason"].Const {
					t.Fatal(s)
				}
				ev, err := f.store.ReadStream("thread:"+f.id, 0, 10)
				if err != nil || len(ev) != 1 || ev[0].Type != "thread.state_changed" || ev[0].TypeVersion != 4 {
					t.Fatal(ev, err)
				}
				var payload map[string]string
				if err := json.Unmarshal(ev[0].Payload, &payload); err != nil {
					t.Fatal(err)
				}
				if payload["transition"] != row || payload["from"] != src || payload["to"] != to {
					t.Fatal(payload)
				}
			})
		}
	}
	f := setup(t)
	for _, from := range states {
		f.seed(func(s *State) { s.Phase = from })
		for _, to := range states {
			if allowed[from+"/"+to] {
				continue
			}
			for row, r := range rules {
				if r.to != to {
					continue
				}
				c := Command{Transition: row}
				if err := f.m.Apply(t.Context(), f.id, c); !errors.Is(err, ErrTransition) {
					t.Fatalf("illegal %s -> %s (%s): %v", from, to, row, err)
				}
			}
		}
	}
	if err := f.m.Apply(t.Context(), f.id, Command{Transition: "T37"}); !errors.Is(err, ErrTransition) {
		t.Fatal(err)
	}
}

// --- Dispatch and pauses: exercise guards, reasons, and D-08 retries (AC2?AC3) ---

// TestGuardRetryAfterResolution verifies D-08 for each affected row and checks that a later unknown re-blocks G.1.
func TestGuardRetryAfterResolution(t *testing.T) {
	for _, tc := range []struct{ row, from string }{{"T01", "draft"}, {"T03", "plan_review"}, {"T09", "waiting_children"}, {"T11", "awaiting_decision"}, {"T27", "verifying"}, {"T29", "awaiting_acceptance"}} {
		t.Run(tc.row, func(t *testing.T) {
			f := setup(t)
			c := f.prepare(tc.row, tc.from)
			f.resolve("succeeded")
			f.resolve("unknown")
			f.apply(c)
			s := f.status()
			if s.Phase != tc.from || s.BlockedBy != "G.1" {
				t.Fatal(s)
			}
			f.resolve("not_executed")
			s = f.status()
			if s.Phase != "queued" || s.Pending != nil {
				t.Fatal(s)
			}
		})
	}
}

// TestGuardClauses verifies AC2 by forcing each dispatch guard clause to block.
func TestGuardClauses(t *testing.T) {
	for _, clause := range []string{"G.1", "G.2", "G.3", "G.4", "G.5/limits", "G.5/policy", "G.5/pair", "G.5/disk"} {
		t.Run(clause, func(t *testing.T) {
			f := setup(t)
			c := f.prepare("T05", "queued")
			deny := func(context.Context, *eventlog.Transaction, State) (bool, error) { return false, nil }
			f.seed(func(s *State) {
				switch clause {
				case "G.1":
					s.Requests["req"] = "unknown"
				case "G.2":
					s.Decisions["d"] = true
				case "G.3":
					s.OutOfScope = "missing"
				case "G.4":
					s.Phase = "paused"
					s.Reason = "operator"
					s.Facts.Assignment = "a"
					c.Transition = "T21"
				case "G.5/limits":
					f.m.hooks.Limits = deny
				case "G.5/policy":
					f.m.hooks.Policy = deny
				case "G.5/pair":
					f.m.hooks.Pair = deny
				case "G.5/disk":
					s.Facts.DiskPressure = true
				}
			})
			f.apply(c)
			if s := f.status(); s.BlockedBy != strings.Split(clause, "/")[0] {
				t.Fatal(s)
			}
		})
	}
}

// TestPauseExitGuards verifies AC3 by exercising the exit prerequisite for every pause reason.
func TestPauseExitGuards(t *testing.T) {
	for _, reason := range strings.Fields("operator harness_unavailable no_eligible_pair disk_pressure reconciliation disagreement mode_switch runner_disconnected daemon_recovery") {
		t.Run(reason, func(t *testing.T) {
			f := setup(t)
			c := f.prepare("T21", "paused")
			f.seed(func(s *State) {
				s.Reason = reason
				s.Facts.DiskPressure = true
				s.Facts.SwitchToSandbox = true
				if reason == "reconciliation" {
					s.Requests["req"] = "unknown"
				}
			})
			f.m.hooks.Pair = func(context.Context, *eventlog.Transaction, State) (bool, error) { return false, nil }
			if reason != "operator" {
				if err := f.m.Apply(t.Context(), f.id, c); !errors.Is(err, ErrTransition) {
					t.Fatal(err)
				}
			}
			if reason == recovery {
				return
			}
			f.seed(func(s *State) {
				s.Facts.DiskPressure = false
				s.Facts.Settled = true
				s.Facts.ActiveSettled = true
				s.Facts.SandboxActive = true
				s.Facts.Connected = true
				s.Facts.JournalReplayed = true
				s.Requests = map[string]string{}
			})
			f.m.hooks.Pair = allow
			if reason == "harness_unavailable" {
				f.seedHarnessReceipt()
			}
			f.apply(c)
			if s := f.status(); s.Phase != "queued" {
				t.Fatal(s)
			}
		})
	}
}

// --- Lifecycle and outcomes: verify restart, rollover, cancellation, and acceptance (AC4?AC7) ---

// TestRepeatedRecovery verifies AC4 by repeating recovery while an unknown request remains outstanding.
func TestRepeatedRecovery(t *testing.T) {
	f := setup(t)
	f.prepare("T25", "running")
	f.seed(func(s *State) { s.Facts.Assignment = "a"; s.Work["a"] = "complete"; s.Requests[req] = "unknown" })
	var reconciled int
	f.m.hooks.Reconcile = func(context.Context, string, string) error { reconciled++; return nil }
	for range 3 {
		if err := f.m.Restart(t.Context()); err != nil {
			t.Fatal(err)
		}
		s := f.status()
		if s.Reason != recovery || s.Interrupted != "running" {
			t.Fatal(s)
		}
	}
	if reconciled != 3 {
		t.Fatal(reconciled)
	}
	f.resolve("succeeded")
	if s := f.status(); s.Phase != "verifying" {
		t.Fatal(s)
	}
	ev, err := f.store.ReadStream("thread:"+f.id, 0, 10)
	if err != nil || len(ev) != 3 {
		t.Fatal(ev, err)
	}
}

// TestRolloverNewSession verifies AC5 by supplying the persisted checkpoint to a different session after T05 commits.
func TestRolloverNewSession(t *testing.T) {
	f := setup(t)
	f.apply(Command{Transition: "T01"})
	f.apply(Command{Transition: "T05", Assignment: "a", Session: "first"})
	if err := f.m.Update(t.Context(), f.id, func(p *Facts) error { p.Revision = "input"; return nil }); err != nil {
		t.Fatal(err)
	}
	cp := Checkpoint{ThreadID: f.id, Revision: "input", Handoff: []string{"STATUS.md"}}
	ref, err := f.m.Rollover(t.Context(), t.TempDir(), cp, "context_limit")
	if err != nil {
		t.Fatal(err)
	}
	if f.status().Checkpoint != ref {
		t.Fatal(ref)
	}
	if err := f.m.Dispatch(t.Context(), f.id, "b", "first", nil); !errors.Is(err, ErrTransition) {
		t.Fatal(err)
	}
	started := false
	err = f.m.Dispatch(t.Context(), f.id, "b", "second", func(_ context.Context, restored *Checkpoint) error {
		started = true
		if s := f.status(); s.Facts.Session != "second" || s.Phase != "running" || restored.Revision != "input" || !slices.Equal(restored.Handoff, cp.Handoff) {
			return fmt.Errorf("invalid handoff %v %v", s, restored)
		}
		return nil
	})
	if err != nil || !started {
		t.Fatal(started, err)
	}
}

// TestCancelManagedChildren verifies AC6 by requiring process-tree cleanup before child and parent T30 commit.
func TestCancelManagedChildren(t *testing.T) {
	f := setup(t)
	child := eventlog.NewID()
	if err := f.m.Create(t.Context(), child, Facts{ManagedParent: f.id}); err != nil {
		t.Fatal(err)
	}
	gone := false
	f.m.hooks.ConfirmGone = func(_ context.Context, id string) (bool, error) { return id != child || gone, nil }
	c := Command{Transition: "T30"}
	if err := f.m.Apply(t.Context(), f.id, c); err == nil {
		t.Fatal("cancelled a live child")
	}
	if s := f.status(); s.Phase == "cancelled" {
		t.Fatal(s)
	}
	gone = true
	f.apply(c)
	events, err := f.store.ReadGlobal(0, 10)
	if err != nil || len(events) != 2 || events[0].ThreadID != child || events[1].ThreadID != f.id {
		t.Fatal(events, err)
	}
}

// TestIndependentOutcomes verifies AC7 by allowing failed checks at T26 and requiring recorded human acceptance at T28.
func TestIndependentOutcomes(t *testing.T) {
	f := setup(t)
	c := f.prepare("T26", "verifying")
	f.seed(func(s *State) { delete(s.Checks, "test") })
	if err := f.m.Apply(t.Context(), f.id, c); !errors.Is(err, ErrTransition) {
		t.Fatal(err)
	}
	f.emit("check.result", map[string]string{"check": "test", "revision": "rev", "status": "failed"})
	f.emit("check.result", map[string]string{"check": "unit", "revision": "rev", "status": "passed"})
	if s := f.status(); s.Checks["unit"] != "passed" || s.Checks["test"] != "failed" {
		t.Fatal(s.Checks)
	}
	f.apply(c)
	accept := Command{Transition: "T28", Revision: "rev"}
	if err := f.m.Apply(t.Context(), f.id, accept); !errors.Is(err, ErrTransition) {
		t.Fatal(err)
	}
	f.emit("acceptance.recorded", map[string]string{"revision": "rev", "decision": "accepted", "operator": "local"})
	if s := f.status(); s.AcceptedRevision != "rev" {
		t.Fatal(s.AcceptedRevision)
	}
	f.apply(accept)
}
