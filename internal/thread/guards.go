package thread

import (
	"context"
	"errors"
	"fmt"
	"synch/internal/eventlog"
)

// --- Transition triggers: check row prerequisites and pause exits (??1?2.2) ---

// trigger checks the row-specific prerequisites and pause exit conditions in ??1?2.2.
func (m *Machine) trigger(ctx context.Context, tx *eventlog.Transaction, s *State, c Command) (bool, error) {
	f := s.Facts
	if s.Reason == recovery && !s.RecoveryReady {
		switch c.Transition {
		case "T22", "T23", "T36", "T38":
			return false, nil
		}
	}
	switch c.Transition {
	case "T01":
		return !f.ReviewPlanFirst, nil
	case "T02":
		return f.ReviewPlanFirst, nil
	case "T03":
		return c.PlanDigest != "" && c.PlanDigest == f.PlanDigest, nil
	case "T04":
		return c.BriefVersion > f.BriefVersion, nil
	case "T05":
		return c.Session != "" && c.Assignment != "" && (f.CheckpointID == "" || c.Session != f.Session), nil
	case "T06":
		if m.hooks.Pair == nil {
			return false, nil
		}
		ok, err := m.hooks.Pair(ctx, tx, *s)
		return !ok, err
	case "T07":
		return f.DiskPressure, nil
	case "T08":
		return len(f.Awaited) > 0, nil
	case "T09":
		for _, id := range f.Awaited {
			child, err := load(ctx, tx, id)
			if err != nil {
				return false, err
			}
			if !terminal(child.Phase) {
				return false, nil
			}
		}
		return true, nil
	case "T10":
		return f.Material || s.OutOfScope != "", nil
	case "T14":
		return f.CheckpointID != "", nil
	case "T35":
		return f.CheckpointID != "" && unresolved(s), nil
	case "T16":
		return unresolved(s), nil
	case "T19":
		return !f.Connected && (f.NeedsDispatch || f.FreshApprovalNeeded), nil
	case "T21":
		switch s.Reason {
		case "operator":
			return true, nil
		case "harness_unavailable":
			return f.SelectedPair, nil
		case "no_eligible_pair":
			if m.hooks.Pair == nil {
				return false, nil
			}
			return m.hooks.Pair(ctx, tx, *s)
		case "disk_pressure":
			return !f.DiskPressure, nil
		case "reconciliation":
			return !unresolved(s), nil
		case "disagreement":
			return f.Settled, nil
		case "mode_switch":
			return f.ActiveSettled && (!f.SwitchToSandbox || f.SandboxActive), nil
		case "runner_disconnected":
			return f.Connected && f.JournalReplayed, nil
		}
		return false, nil
	case "T22":
		return s.Reason == recovery && !unresolved(s) && (s.work() == "resumable" || s.work() == "complete" && s.Interrupted == "rollover" && f.CheckpointID != ""), nil
	case "T23":
		return s.Reason != "harness_unavailable" && !unresolved(s) && s.work() == "lost" && f.AttemptsLeft, nil
	case "T26":
		if len(s.Checks) == 0 {
			return false, nil
		}
		for _, name := range f.RequiredChecks {
			if s.Checks[name] == "" {
				return false, nil
			}
		}
		return f.AuditSetting == "off" || s.Audit == "pass" || s.Audit == "corrections_requested" || s.Audit == "error", nil
	case "T27":
		for _, status := range s.Checks {
			if status == "failed" {
				return f.AttemptsLeft, nil
			}
		}
		return false, nil
	case "T28":
		return c.Revision != "" && c.Revision == f.EvaluatedRevision && c.Revision == f.Revision && s.AcceptedRevision == c.Revision, nil
	case "T31":
		return !f.AttemptsLeft || f.Unrecoverable, nil
	case "T33":
		return f.DiskPressure && f.NeedsDispatch, nil
	case "T34":
		return s.Reason == "disagreement" && f.ResolutionChecked && !f.Settled && f.Material, nil
	case "T36":
		return s.work() == "lost" && !f.AttemptsLeft && !unresolved(s), nil
	case "T38":
		return s.Reason == recovery && !unresolved(s) && s.work() == "complete" && (s.Interrupted == "running" || s.Interrupted == "verifying"), nil
	default:
		return true, nil
	}
}

// --- Cancellation: confirm cleanup and record descendants before the parent (T30) ---

// ids lists persisted thread IDs in deterministic order for startup recovery or system events.
func ids(ctx context.Context, tx *eventlog.Transaction) ([]string, error) {
	return queryIDs(ctx, tx, "SELECT id FROM thread_state ORDER BY id")
}

// queryIDs collects IDs before callers perform nested state reads or writes.
func queryIDs(ctx context.Context, tx *eventlog.Transaction, query string, args ...any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var list []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		list = append(list, id)
	}
	return list, rows.Err()
}

// cancelChildren cancels managed descendants depth-first and requires process-tree confirmation before T30.
func (m *Machine) cancelChildren(ctx context.Context, tx *eventlog.Transaction, s *State) error {
	list, err := queryIDs(ctx, tx, "SELECT id FROM thread_state WHERE json_extract(state,'$.Facts.ManagedParent')=? ORDER BY id", s.ID)
	if err != nil {
		return err
	}
	for _, id := range list {
		child, err := load(ctx, tx, id)
		if err != nil {
			return err
		}
		if terminal(child.Phase) {
			err = m.cancelChildren(ctx, tx, &child)
		} else {
			err = m.apply(ctx, tx, &child, Command{Transition: "T30"})
		}
		if err != nil {
			return err
		}
	}
	if m.hooks.ConfirmGone == nil {
		return errors.New("process cleanup confirmation required")
	}
	gone, err := m.hooks.ConfirmGone(ctx, s.ID)
	if err != nil {
		return err
	}
	if !gone {
		return fmt.Errorf("process tree still present for %s", s.ID)
	}
	return nil
}

// --- Restart recovery: reconcile and classify interrupted assignments (?3) ---

// Restart persists interruptions, retries pending transitions and joins individual recovery failures.
func (m *Machine) Restart(ctx context.Context) error {
	var list []string
	if err := m.store.Write(ctx, func(tx *eventlog.Transaction) error {
		var err error
		list, err = ids(ctx, tx)
		return err
	}); err != nil {
		return err
	}
	var recovering []string
	var errs []error
	for _, id := range list {
		var interrupted bool
		err := m.store.Write(ctx, func(tx *eventlog.Transaction) error {
			s, err := load(ctx, tx, id)
			if err != nil {
				return err
			}
			switch s.Phase {
			case "running", "rollover", "verifying":
				if err := m.apply(ctx, tx, &s, Command{Transition: "T24"}); err != nil {
					return err
				}
			}
			if s.Reason == recovery {
				// A new supervisor must confirm cleanup and reconcile before classifying work.
				s.RecoveryReady = false
				interrupted = true
				return save(ctx, tx, &s)
			}
			return m.refresh(ctx, tx, &s)
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("prepare restart for %s: %w", id, err))
			continue
		}
		if interrupted {
			recovering = append(recovering, id)
		}
	}
	for _, id := range recovering {
		if err := m.Recover(ctx, id); err != nil {
			errs = append(errs, fmt.Errorf("recover %s: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

// Recover confirms process cleanup, reconciles external requests, and classifies the interrupted work.
func (m *Machine) Recover(ctx context.Context, id string) error {
	var s State
	err := m.store.Write(ctx, func(tx *eventlog.Transaction) error {
		var err error
		s, err = load(ctx, tx, id)
		return err
	})
	if err != nil || s.Reason != recovery {
		return err
	}
	if m.hooks.ConfirmGone == nil {
		return errors.New("process cleanup confirmation required")
	}
	ok, err := m.hooks.ConfirmGone(ctx, id)
	if err != nil || !ok {
		return err
	}
	for req, status := range s.Requests {
		if status == "unknown" || status == "" {
			if m.hooks.Reconcile != nil {
				if err := m.hooks.Reconcile(ctx, id, req); err != nil {
					return err
				}
			}
		}
	}
	return m.store.Write(ctx, func(tx *eventlog.Transaction) error {
		s, err := load(ctx, tx, id)
		if err != nil || s.Reason != recovery {
			return err
		}
		s.RecoveryReady = true
		return m.recheck(ctx, tx, &s)
	})
}
