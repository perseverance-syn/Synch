package thread

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"synch/internal/eventlog"
)

// --- Machine and state: store durable facts and blocked transitions ---

const recovery = "daemon_recovery"

var ErrTransition = errors.New("invalid_transition")
var ErrMissing = errors.New("thread not found")

// Facts are durable coordinator records used alongside the numbered event stream.
type Facts struct {
	ReviewPlanFirst                                 bool
	BriefVersion                                    int
	PlanRevision                                    int
	PlanDigest, Revision, EvaluatedRevision         string
	ManagedParent                                   string
	Awaited                                         []string
	RequiredChecks                                  []string
	AuditSetting                                    string
	Session, Assignment                             string
	DiskPressure, NeedsDispatch                     bool
	SelectedPair                                    bool // Derived from receipts, including their order relative to the current pause.
	RestartFromCheckpoint                           bool
	Settled, Material, ResolutionChecked            bool
	SwitchToSandbox, ActiveSettled, SandboxActive   bool
	Connected, JournalReplayed, FreshApprovalNeeded bool
	RunnerID                                        string
	AttemptsLeft                                    bool
	Unrecoverable                                   bool
	CheckpointID                                    string
}
type State struct {
	Projected                      int64
	ID, Phase, Reason, Interrupted string
	Facts                          Facts
	SelectedHarness                *HarnessSelection
	Checkpoint                     Reference
	FailedAssignments              []string
	Work                           map[string]string
	Requests                       map[string]string
	Decisions                      map[string]bool
	CatalogueDecisions             bool
	OutOfScope                     string
	Checks                         map[string]string
	Audit, AcceptedRevision        string
	Slot                           bool
	RecoveryReady                  bool
	Pending                        *Command
	BlockedBy                      string
}
type Command struct {
	Transition                                string
	RequestID                                 string
	PlanDigest, Revision, Assignment, Session string
	BriefVersion                              int
	PlanRevision                              int
}
type Hooks struct {
	Limits, Policy, Pair func(context.Context, *eventlog.Transaction, State) (bool, error)
	ConfirmGone          func(context.Context, string) (bool, error)
	Reconcile            func(context.Context, string, string) error
}
type Machine struct {
	store *eventlog.Store
	hooks Hooks
}

// --- State storage: create and load snapshots within the event writer ---

// Open initializes durable thread state storage for post-commit catch-up.
func Open(ctx context.Context, store *eventlog.Store, h Hooks) (*Machine, error) {
	m := &Machine{store, h}
	err := store.Write(ctx, func(tx *eventlog.Transaction) error {
		_, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS thread_state(id TEXT PRIMARY KEY, state BLOB NOT NULL)")
		return err
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

// Create persists a draft thread with its initial coordinator records.
func (m *Machine) Create(ctx context.Context, id string, f Facts) error {
	return m.store.Write(ctx, func(tx *eventlog.Transaction) error { return m.CreateTx(ctx, tx, id, f) })
}

// CreateTx persists a draft inside the caller's transaction so creation and its receipt can commit together.
func (m *Machine) CreateTx(ctx context.Context, tx *eventlog.Transaction, id string, f Facts) error {
	if f.ManagedParent != "" {
		if _, err := load(ctx, tx, f.ManagedParent); err != nil {
			return err
		}
	}
	s := State{ID: id, Phase: "draft", Facts: f, Work: map[string]string{}, Requests: map[string]string{}, Decisions: map[string]bool{}, CatalogueDecisions: true, Checks: map[string]string{}}
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO thread_state VALUES(?,?)", id, b)
	return err
}

// load loads a durable thread and applies any event records its snapshot has not seen.
func load(ctx context.Context, tx *eventlog.Transaction, id string) (State, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, "SELECT state FROM thread_state WHERE id=?", id).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return State{}, ErrMissing
	}
	if err != nil {
		return State{}, err
	}
	var s State
	if err := json.Unmarshal(raw, &s); err != nil {
		return State{}, err
	}
	err = catchUp(ctx, tx, &s)
	return s, err
}

// save persists the updated state and projection cursor within the current transaction.
func save(ctx context.Context, tx *eventlog.Transaction, s *State) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "UPDATE thread_state SET state=? WHERE id=?", b, s.ID)
	return err
}

// Status returns the current phase, recovery obligation, and any blocking guard clause.
func (m *Machine) Status(ctx context.Context, id string) (s State, err error) {
	err = m.store.Write(ctx, func(tx *eventlog.Transaction) error {
		var err error
		s, err = load(ctx, tx, id)
		if err != nil {
			return err
		}
		return m.refresh(ctx, tx, &s)
	})
	return
}

// terminal reports whether the transition table permits any further state change.
func terminal(s string) bool { return s == "cancelled" || s == "completed" || s == "failed" }

// --- Transition catalogue: encode legal sources and destinations (?2.2) ---

type rule struct{ from, to, reason string }

var rules = map[string]rule{
	"T01": {"draft", "queued", ""},
	"T02": {"draft", "plan_review", ""},
	"T03": {"plan_review", "queued", ""},
	"T04": {"plan_review", "draft", ""},
	"T05": {"queued", "running", ""},
	"T06": {"queued", "paused", "no_eligible_pair"},
	"T07": {"queued", "paused", "disk_pressure"},
	"T08": {"running", "waiting_children", ""},
	"T09": {"waiting_children", "queued", ""},
	"T10": {"running", "awaiting_decision", ""},
	"T11": {"awaiting_decision", "queued", ""},
	"T12": {"awaiting_decision", "failed", ""},
	"T13": {"running", "rollover", ""},
	"T14": {"rollover", "queued", ""},
	"T15": {"running", "paused", "harness_unavailable"},
	"T16": {"running", "paused", "reconciliation"},
	"T17": {"running", "paused", "disagreement"},
	"T18": {"running", "paused", "mode_switch"},
	"T19": {"running", "paused", "runner_disconnected"},
	"T20": {"running", "paused", "operator"},
	"T21": {"paused", "queued", ""},
	"T22": {"paused", "queued", ""},
	"T23": {"paused", "paused", "harness_unavailable"},
	"T24": {"running rollover verifying", "paused", recovery},
	"T25": {"running", "verifying", ""},
	"T26": {"verifying", "awaiting_acceptance", ""},
	"T27": {"verifying", "queued", ""},
	"T28": {"awaiting_acceptance", "completed", ""},
	"T29": {"awaiting_acceptance", "queued", ""},
	"T30": {"*", "cancelled", ""},
	"T31": {"running verifying", "failed", ""},
	"T32": {"paused", "failed", ""},
	"T33": {"running", "paused", "disk_pressure"},
	"T34": {"paused", "awaiting_decision", ""},
	"T35": {"rollover", "paused", "reconciliation"},
	"T36": {"paused", "failed", ""},
	"T38": {"paused", "verifying", ""},
}

// --- Dispatch safety: evaluate the persisted guard facts (?2.1) ---

// unresolved reports whether any persisted external request lacks a definitive latest resolution.
func unresolved(s *State) bool {
	for _, v := range s.Requests {
		if v != "succeeded" && v != "failed" && v != "not_executed" {
			return true
		}
	}
	return false
}

// openDecision checks whether an operator decision remains open for G.2.
func openDecision(s *State) bool {
	for _, open := range s.Decisions {
		if open {
			return true
		}
	}
	return false
}

// work looks up the execution classification for the current assignment.
func (s *State) work() string { return s.Work[s.Facts.Assignment] }

// guard evaluates G.1?G.5 against the records visible within the writer transaction.
func (m *Machine) guard(ctx context.Context, tx *eventlog.Transaction, s *State, id string) (string, error) {
	switch {
	case unresolved(s):
		return "G.1", nil
	case openDecision(s):
		return "G.2", nil
	case s.OutOfScope != "":
		if open, ok := s.Decisions[s.OutOfScope]; !ok || open {
			return "G.3", nil
		}
	}
	if s.Phase == "paused" && s.Facts.Assignment != "" && s.work() != "complete" && s.work() != "resumable" {
		restart := s.Reason == "harness_unavailable" && s.work() == "lost" && s.Facts.AttemptsLeft && s.Facts.RestartFromCheckpoint && s.Facts.SelectedPair && s.Facts.CheckpointID != ""
		if !restart {
			return "G.4", nil
		}
	}
	if id == "T05" {
		if s.Facts.DiskPressure {
			return "G.5", nil
		}
		for _, p := range []func(context.Context, *eventlog.Transaction, State) (bool, error){m.hooks.Limits, m.hooks.Policy, m.hooks.Pair} {
			if p == nil {
				return "G.5", nil
			}
			ok, err := p(ctx, tx, *s)
			if err != nil {
				return "", err
			}
			if !ok {
				return "G.5", nil
			}
		}
	}
	return "", nil
}

// --- Transition execution: commit state and event together (?2.2) ---

// Apply evaluates a transition request and commits its event, state, or blocked status.
func (m *Machine) Apply(ctx context.Context, id string, c Command) error {
	return m.store.Write(ctx, func(tx *eventlog.Transaction) error { return m.ApplyTx(ctx, tx, id, c) })
}

// ApplyTx applies a transition inside the caller's transaction for atomic command receipts.
func (m *Machine) ApplyTx(ctx context.Context, tx *eventlog.Transaction, id string, c Command) error {
	s, err := load(ctx, tx, id)
	if err != nil {
		return err
	}
	if s.Pending != nil && s.Pending.Transition != c.Transition && c.Transition != "T30" {
		if err := m.refresh(ctx, tx, &s); err != nil {
			return err
		}
	}
	return m.apply(ctx, tx, &s, c)
}

// apply validates a row, evaluates its trigger and guards, and records the resulting transition or blocker.
func (m *Machine) apply(ctx context.Context, tx *eventlog.Transaction, s *State, c Command) error {
	if c.RequestID == "" {
		c.RequestID = tx.RequestID()
	}
	r, ok := rules[c.Transition]
	if !ok || terminal(s.Phase) || r.from != "*" && !slices.Contains(strings.Fields(r.from), s.Phase) {
		return ErrTransition
	}
	valid, err := m.trigger(ctx, tx, s, c)
	if err != nil {
		return err
	}
	if !valid {
		return ErrTransition
	}
	// Commit the operator's new versions or invalidate evidence once, even if G blocks entry to queued.
	if s.Pending == nil || s.Pending.Transition != c.Transition {
		switch c.Transition {
		case "T11":
			if c.BriefVersion > 0 {
				s.Facts.BriefVersion = c.BriefVersion
			}
			if c.PlanDigest != "" {
				s.Facts.PlanDigest = c.PlanDigest
				s.Facts.PlanRevision = c.PlanRevision
			}
		case "T29":
			s.Checks = map[string]string{}
			s.Audit = ""
			s.AcceptedRevision = ""
			s.Facts.EvaluatedRevision = ""
		}
	}
	if r.to == "queued" || r.to == "running" {
		blocked, err := m.guard(ctx, tx, s, c.Transition)
		if err != nil {
			return err
		}
		if blocked != "" {
			if c.Transition == "T14" && blocked == "G.1" {
				return m.apply(ctx, tx, s, Command{Transition: "T35"})
			}
			if blocked == "G.4" && s.work() == "lost" {
				if !s.Facts.AttemptsLeft {
					return m.apply(ctx, tx, s, Command{Transition: "T36"})
				}
				if s.Reason != "harness_unavailable" {
					return m.apply(ctx, tx, s, Command{Transition: "T23"})
				}
			}
			s.BlockedBy = blocked
			// T05 needs an explicit dispatch call so a guard retry cannot launch an unowned session.
			if c.Transition != "T05" {
				s.Pending = &c
			}
			return save(ctx, tx, s)
		}
	}
	if c.Transition == "T30" {
		if err := m.cancelChildren(ctx, tx, s); err != nil {
			return err
		}
	}
	return transition(ctx, tx, s, c, r)
}

// transition appends a catalogue v4 event and updates the thread snapshot atomically.
func transition(ctx context.Context, tx *eventlog.Transaction, s *State, c Command, r rule) error {
	p := map[string]string{"transition": c.Transition, "from": s.Phase, "to": r.to}
	if s.Reason != "" {
		p["from_pause_reason"] = s.Reason
	}
	if r.reason != "" {
		p["pause_reason"] = r.reason
	}
	if c.Transition == "T24" {
		s.Interrupted = s.Phase
		p["interrupted_state"] = s.Phase
	} else {
		s.Interrupted = ""
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	event, err := tx.Append(ctx, eventlog.NewEvent{ThreadID: s.ID, Stream: "thread:" + s.ID, Type: "thread.state_changed", TypeVersion: 4, RequestID: c.RequestID, Actor: eventlog.Actor{Kind: "daemon", ID: "local"}, Payload: raw})
	if err != nil {
		return err
	}
	switch c.Transition {
	case "T10", "T17", "T34":
		openDecisionFor(s, event.ID)
	}
	previous := s.Pending
	s.Pending = nil
	// T24 interrupts the phase. A T27 retry still belongs to it if T38 restores verification.
	if c.Transition == "T24" || c.Transition == "T38" && previous != nil && previous.Transition == "T27" {
		s.Pending = previous
	}
	s.BlockedBy = ""
	s.Phase = r.to
	s.Reason = r.reason
	s.RecoveryReady = false
	s.Slot = r.to == "running"
	switch c.Transition {
	case "T04":
		s.Facts.BriefVersion = c.BriefVersion
	case "T05":
		s.Facts.Session = c.Session
		s.Facts.Assignment = c.Assignment
	case "T13":
		s.Facts.CheckpointID = ""
	case "T17":
		s.Facts.Settled = false
		s.Facts.ResolutionChecked = false
	case "T18":
		s.Facts.ActiveSettled = false
		s.Facts.SandboxActive = false
	case "T19":
		s.Facts.JournalReplayed = false
	case "T15", "T23":
		s.Facts.SelectedPair = false
		s.Facts.RestartFromCheckpoint = false
	case "T26":
		s.Facts.EvaluatedRevision = s.Facts.Revision
	case "T27", "T31":
		s.FailedAssignments = append(s.FailedAssignments, s.Facts.Assignment)
	}
	if r.to == "queued" {
		s.Facts.Assignment = ""
	}
	return save(ctx, tx, s)
}

// --- Guard updates: re-evaluate pending transitions (D-08) ---

// Update records coordinator facts and rechecks any newly eligible or required transition.
func (m *Machine) Update(ctx context.Context, id string, update func(*Facts) error) error {
	return m.store.Write(ctx, func(tx *eventlog.Transaction) error {
		s, err := load(ctx, tx, id)
		if err != nil {
			return err
		}
		oldRevision, parent := s.Facts.Revision, s.Facts.ManagedParent
		selected := s.Facts.SelectedPair
		if err := update(&s.Facts); err != nil {
			return err
		}
		// D-16 permits a callback to signal a new receipt, but only the receipt can choose a pair.
		s.Facts.SelectedPair = selected
		if parent != s.Facts.ManagedParent {
			return ErrTransition
		}
		if oldRevision != s.Facts.Revision {
			s.Checks = map[string]string{}
			s.Audit = ""
			s.AcceptedRevision = ""
			if s.Phase == "awaiting_acceptance" {
				return m.apply(ctx, tx, &s, Command{Transition: "T29"})
			}
		}
		return m.recheck(ctx, tx, &s)
	})
}

// recheck classifies interrupted work or retries a pending transition after a guard input changes.
func (m *Machine) recheck(ctx context.Context, tx *eventlog.Transaction, s *State) error {
	if s.Phase == "paused" && !unresolved(s) && (s.Reason != recovery || s.RecoveryReady) {
		for _, id := range []string{"T22", "T23", "T36", "T38"} {
			c := Command{Transition: id}
			ok, err := m.trigger(ctx, tx, s, c)
			if err != nil {
				return err
			}
			if ok {
				if err := m.apply(ctx, tx, s, c); err != nil {
					return err
				}
				if id == "T38" && s.Pending != nil {
					return m.recheck(ctx, tx, s)
				}
				return nil
			}
		}
	}
	if s.Pending != nil && s.Reason != recovery {
		err := m.apply(ctx, tx, s, *s.Pending)
		if !errors.Is(err, ErrTransition) {
			return err
		}
		// A pause prerequisite can stop holding while G is blocked. Keep the request, but clear the stale G result.
		s.BlockedBy = ""
	}
	if s.Pending == nil && s.Phase == "paused" && s.Reason == "harness_unavailable" && s.Facts.SelectedPair {
		return m.apply(ctx, tx, s, Command{Transition: "T21"})
	}
	return save(ctx, tx, s)
}

// refresh applies deferred transitions at a machine read boundary and updates dispatch status.
func (m *Machine) refresh(ctx context.Context, tx *eventlog.Transaction, s *State) error {
	if s.Pending != nil || s.Phase == "paused" {
		return m.recheck(ctx, tx, s)
	}
	if s.Phase == "queued" && s.BlockedBy != "" {
		blocked, err := m.guard(ctx, tx, s, "T05")
		if err != nil {
			return err
		}
		s.BlockedBy = blocked
	}
	return save(ctx, tx, s)
}
