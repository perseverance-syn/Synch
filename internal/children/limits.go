package children

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"synch/internal/eventlog"
	"synch/internal/thread"
)

// Limits configures Q-27 caps. Zero selects the default for that field.
type Limits struct {
	MaxDepth, MaxLocal, MaxAttempts int
}

// defaults fills in the proposed Q-27 values and rejects negative overrides.
func (l Limits) defaults() (Limits, error) {
	if l.MaxDepth < 0 || l.MaxLocal < 0 || l.MaxAttempts < 0 {
		return Limits{}, errors.New("child limits cannot be negative")
	}
	if l.MaxDepth == 0 {
		l.MaxDepth = 3
	}
	if l.MaxLocal == 0 {
		l.MaxLocal = 4
	}
	if l.MaxAttempts == 0 {
		l.MaxAttempts = 3
	}
	return l, nil
}

// DepthError reports a refused child depth and the operator's cap.
type DepthError struct{ Depth, Max int }

// Error describes the depth refusal for operators.
func (e *DepthError) Error() string {
	return fmt.Sprintf("child depth %d exceeds configured maximum %d", e.Depth, e.Max)
}

// slotProjection tracks committed occupancy from the machine's transition events.
type slotProjection struct {
	cursor  int64
	running map[string]bool
}

// catchUp consumes committed transitions under the serialized writer lock.
// Advancing only on committed data keeps the projection valid after a rollback.
func (p *slotProjection) catchUp(ctx context.Context, store *eventlog.Store) error {
	const page = 256
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		events, err := store.ReadGlobal(p.cursor, page)
		if err != nil {
			return fmt.Errorf("read slot transitions: %w", err)
		}
		for _, e := range events {
			if e.Type == "thread.state_changed" && (e.TypeVersion == 3 || e.TypeVersion == 4) {
				var change struct{ To string }
				if err := json.Unmarshal(e.Payload, &change); err != nil {
					return fmt.Errorf("decode slot transition %s: %w", e.ID, err)
				}
				if change.To == "running" {
					p.running[e.ThreadID] = true
				} else if change.To != "" {
					delete(p.running, e.ThreadID)
				}
			}
			p.cursor = e.GlobalSeq
		}
		if len(events) < page {
			return nil
		}
	}
}

// admit is the injected G.5 predicate for depth, local capacity, retry budget,
// and sibling dependencies. The machine owns guard ordering and queued status.
func (c *Coordinator) admit(ctx context.Context, tx *eventlog.Transaction, s thread.State) (bool, error) {
	if failedAttempts(s) >= c.limits.MaxAttempts {
		return false, nil
	}
	if s.Facts.ManagedParent != "" {
		ok, err := c.childReady(ctx, s.ID)
		if !ok || err != nil {
			return ok, err
		}
	}
	remote, err := remotePlacement(ctx, tx, s.ID)
	if err != nil {
		return false, err
	}
	// Routing can change after dispatch preparation. Keep the thread queued
	// until a dispatch request records the new placement.
	if remote != (s.Facts.RunnerID != "") {
		return false, nil
	}
	if remote {
		return true, nil
	}
	if err := c.slots.catchUp(ctx, c.store); err != nil {
		return false, err
	}
	local := 0
	for id := range c.slots.running {
		remote, err := remotePlacement(ctx, tx, id)
		if err != nil {
			return false, err
		}
		if !remote {
			local++
			if local >= c.limits.MaxLocal {
				return false, nil
			}
		}
	}
	return true, nil
}

// remotePlacement reads the recorded placement; older threads are counted
// against local capacity until their next coordinator dispatch.
func remotePlacement(ctx context.Context, tx *eventlog.Transaction, id string) (bool, error) {
	var remote bool
	err := tx.QueryRowContext(ctx, "SELECT remote FROM children_placement WHERE thread_id=?", id).Scan(&remote)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("read thread placement: %w", err)
	}
	return remote, nil
}

// --- Child admission and retries: combine plan evidence with the machine's failure history ---

// DependencyError reports a child that cannot start because a sibling it
// depends on did not complete.
type DependencyError struct {
	Child, Dependency string // Dependency is the sibling's assignment ID.
	Reason            string
}

// Error names the blocking dependency and why it blocks.
func (e *DependencyError) Error() string {
	return fmt.Sprintf("child %s blocked by dependency %s: %s", e.Child, e.Dependency, e.Reason)
}

// dependencyState reports whether every dependency has completed (true, nil), is still
// pending (false, nil) or blocks the child (error). With checkEnded, a dependency that
// ended terminal without a result also blocks; that needs the machine, so the
// writer-side gate leaves it to the Dispatch pre-check.
func (c *Coordinator) dependencyState(ctx context.Context, id string, checkEnded bool) (bool, error) {
	a, err := c.recorder.Assignment(ctx, id)
	if err != nil {
		return false, fmt.Errorf("read assignment for %s: %w", id, err)
	}
	ready := true
	for _, dep := range a.Dependencies {
		sibling, err := c.recorder.AssignmentByID(ctx, dep)
		if err != nil {
			return false, fmt.Errorf("read sibling assignment %s: %w", dep, err)
		}
		r, err := c.recorder.Result(ctx, sibling.ChildThreadID)
		switch {
		case err == nil && r.Outcome != "completed":
			return false, &DependencyError{Child: id, Dependency: dep, Reason: "outcome " + r.Outcome}
		case err == nil:
		case !errors.Is(err, ErrRecordNotFound):
			return false, fmt.Errorf("read sibling result %s: %w", dep, err)
		case checkEnded:
			s, err := c.machine.Status(ctx, sibling.ChildThreadID)
			if err != nil {
				return false, err
			}
			if terminal(s.Phase) {
				return false, &DependencyError{Child: id, Dependency: dep, Reason: "ended " + s.Phase + " without a recorded result"}
			}
			ready = false
		default:
			ready = false
		}
	}
	return ready, nil
}

// childReady checks recorded depth and waits for all promised sibling results.
func (c *Coordinator) childReady(ctx context.Context, id string) (bool, error) {
	created, err := c.creation(ctx, id)
	if err != nil {
		return false, err
	}
	if created.Depth > c.limits.MaxDepth {
		return false, &DepthError{Depth: created.Depth, Max: c.limits.MaxDepth}
	}
	return c.dependencyState(ctx, id, false)
}

// failedAttempts counts distinct failed attempt IDs because the machine can
// see the same attempt both as a lost process and as a failed transition.
func failedAttempts(s thread.State) int {
	ids := slices.Clone(s.FailedAssignments)
	slices.Sort(ids)
	return len(slices.Compact(ids))
}

// containsFailedAttempt rejects reuse of an ID whose attempt has already failed.
func containsFailedAttempt(s thread.State, id string) bool {
	return slices.Contains(s.FailedAssignments, id)
}

// retryAfterCurrent uses the failure budget after charging the current or
// about-to-start attempt. An already classified loss is charged only once.
func retryAfterCurrent(s thread.State, maxAttempts int) bool {
	used := failedAttempts(s)
	if s.Facts.Assignment == "" || !containsFailedAttempt(s, s.Facts.Assignment) {
		used++
	}
	return used < maxAttempts
}

// setAttemptsLeft feeds the retry limit into T27, T31 and restart recovery.
func (c *Coordinator) setAttemptsLeft(ctx context.Context, s thread.State) error {
	left := retryAfterCurrent(s, c.limits.MaxAttempts)
	if s.Facts.AttemptsLeft == left {
		return nil
	}
	return c.machine.Update(ctx, s.ID, func(f *thread.Facts) error {
		f.AttemptsLeft = left
		return nil
	})
}

// rememberPlacement records where a queued thread will execute before T05.
// A blocked dispatch still owns no slot. Writing here also handles routing
// changes between attempts and keeps the G.5 predicate read-only.
func (c *Coordinator) rememberPlacement(ctx context.Context, s thread.State) error {
	return c.store.Write(ctx, func(tx *eventlog.Transaction) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO children_placement(thread_id,remote) VALUES(?,?)
			ON CONFLICT(thread_id) DO UPDATE SET remote=excluded.remote`, s.ID, s.Facts.RunnerID != "")
		if err != nil {
			return fmt.Errorf("record execution placement: %w", err)
		}
		return nil
	})
}
