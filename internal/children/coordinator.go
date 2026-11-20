package children

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"synch/internal/eventlog"
	"synch/internal/thread"
)

// Coordinator installs G.5 limits and coordinates managed threads on the daemon's writer.
type Coordinator struct {
	mu       sync.Mutex
	store    *eventlog.Store
	machine  *thread.Machine
	limits   Limits
	recorder Recorder
	slots    slotProjection
}

// Open wires the child limits into the thread machine while preserving the
// caller's policy, routing and process-cleanup hooks.
func Open(ctx context.Context, store *eventlog.Store, limits Limits, hooks thread.Hooks, recorder Recorder) (*Coordinator, error) {
	if store == nil || recorder == nil || hooks.ConfirmGone == nil {
		return nil, errors.New("children requires an event store, recorder and process-tree confirmation")
	}
	limits, err := limits.defaults()
	if err != nil {
		return nil, err
	}
	c := &Coordinator{store: store, limits: limits, recorder: recorder, slots: slotProjection{running: make(map[string]bool)}}
	err = store.Write(ctx, func(tx *eventlog.Transaction) error {
		_, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS children_placement(thread_id TEXT PRIMARY KEY, remote BOOL NOT NULL)")
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("initialize children placement: %w", err)
	}
	hooks.Limits = c.admit
	c.machine, err = thread.Open(ctx, store, hooks)
	if err != nil {
		return nil, fmt.Errorf("open child thread machine: %w", err)
	}
	return c, nil
}

// Threads returns the machine for normal lifecycle transitions. T05 dispatch
// and managed T30 cancellation use the coordinator's serialized entry points.
func (c *Coordinator) Threads() *thread.Machine { return c.machine }

type createdThread struct {
	ID      string `json:"thread_id"`
	Project string `json:"project_id"`
	Parent  string `json:"parent_thread_id,omitempty"`
	Role    string `json:"role"`
	Review  bool   `json:"review_plan_first"`
	Depth   int    `json:"depth"`
}

// --- Creation: persist catalogue ancestry and refuse over-depth assignments ---

// CreateRoot registers a root at depth zero.
func (c *Coordinator) CreateRoot(ctx context.Context, id, project string, facts thread.Facts) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if blank(id) || blank(project) || facts.ManagedParent != "" {
		return errors.New("root requires a thread and project ID and no managed parent")
	}
	return c.create(ctx, createdThread{ID: id, Project: project, Role: "coordinator"}, facts)
}

// CreateChild validates sibling dependencies and saves the assignment before
// making the child available. A retry after a failed thread write reuses the record.
func (c *Coordinator) CreateChild(ctx context.Context, a Assignment, facts thread.Facts) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := a.Validate(); err != nil {
		return err
	}
	if facts.ManagedParent != "" && facts.ManagedParent != a.ParentThreadID {
		return errors.New("child facts disagree with the assignment parent")
	}
	parent, err := c.creation(ctx, a.ParentThreadID)
	if err != nil {
		return err
	}
	depth := parent.Depth + 1
	if depth > c.limits.MaxDepth {
		return &DepthError{Depth: depth, Max: c.limits.MaxDepth}
	}
	s, err := c.machine.Status(ctx, a.ParentThreadID)
	if err != nil {
		return err
	}
	if terminal(s.Phase) {
		return errors.New("cannot add a child to a terminal parent")
	}
	if _, err := c.machine.Status(ctx, a.ChildThreadID); !errors.Is(err, thread.ErrMissing) {
		if err != nil {
			return err
		}
		return fmt.Errorf("child thread %s already exists", a.ChildThreadID)
	}
	if err := c.checkDependencies(ctx, a); err != nil {
		return err
	}
	if err := c.recorder.RecordAssignment(ctx, a); err != nil {
		return fmt.Errorf("record child assignment: %w", err)
	}
	facts.ManagedParent = a.ParentThreadID
	return c.create(ctx, createdThread{ID: a.ChildThreadID, Project: parent.Project, Parent: parent.ID, Role: a.Role, Depth: depth}, facts)
}

// create commits the thread and its existing catalogue creation event together.
func (c *Coordinator) create(ctx context.Context, p createdThread, facts thread.Facts) error {
	p.Review = facts.ReviewPlanFirst
	raw, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("encode thread creation: %w", err)
	}
	return c.store.Write(ctx, func(tx *eventlog.Transaction) error {
		if err := c.machine.CreateTx(ctx, tx, p.ID, facts); err != nil {
			return fmt.Errorf("create thread: %w", err)
		}
		_, err := tx.Append(ctx, eventlog.NewEvent{
			ThreadID: p.ID, Stream: "thread:" + p.ID, Type: "thread.created", TypeVersion: 1,
			Actor: eventlog.Actor{Kind: "daemon", ID: "local"}, Payload: raw,
		})
		return err
	})
}

// creation reads immutable catalogue metadata from a thread stream.
func (c *Coordinator) creation(ctx context.Context, id string) (createdThread, error) {
	const page = 32
	var cursor int64
	for {
		if err := ctx.Err(); err != nil {
			return createdThread{}, err
		}
		events, err := c.store.ReadStream("thread:"+id, cursor, page)
		if err != nil {
			return createdThread{}, fmt.Errorf("read creation for %s: %w", id, err)
		}
		for _, e := range events {
			cursor = e.Seq
			if e.Type == "thread.created" && e.TypeVersion == 1 {
				var p createdThread
				if err := json.Unmarshal(e.Payload, &p); err != nil {
					return p, fmt.Errorf("decode creation for %s: %w", id, err)
				}
				if p.ID != id || blank(p.Project) || p.Depth < 0 {
					return p, fmt.Errorf("invalid creation metadata for %s", id)
				}
				return p, nil
			}
		}
		if len(events) < page {
			return createdThread{}, fmt.Errorf("missing thread.created for %s: %w", id, thread.ErrMissing)
		}
	}
}

// --- Lifecycle: route dispatch, wait, retry and cancellation through the machine ---

// Dispatch requests T05. The starter launches execution and returns promptly
// so other threads can start. It must not reenter the coordinator.
func (c *Coordinator) Dispatch(ctx context.Context, id, attempt, session string, start func(context.Context, *thread.Checkpoint) error) error {
	// Keep T30 out until the committed dispatch has registered its process.
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.machine.Status(ctx, id)
	if err != nil {
		return err
	}
	if start == nil {
		return errors.New("missing execution starter")
	}
	if s.Phase != "queued" || containsFailedAttempt(s, attempt) {
		return thread.ErrTransition
	}
	if s.Facts.ManagedParent != "" {
		if _, err := c.dependencyState(ctx, id, true); err != nil {
			return c.failDependent(ctx, id, err)
		}
	}
	if err := c.setAttemptsLeft(ctx, s); err != nil {
		return err
	}
	if err := c.rememberPlacement(ctx, s); err != nil {
		return err
	}
	return c.machine.Dispatch(ctx, id, attempt, session, start)
}

// WaitForChildren applies T08 after the caller settles its running parent session.
// The machine releases the execution slot in the same commit as the transition.
func (c *Coordinator) WaitForChildren(ctx context.Context, parent string, children []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(children) == 0 {
		return errors.New("waiting requires at least one child")
	}
	s, err := c.machine.Status(ctx, parent)
	if err != nil {
		return err
	}
	if s.Phase != "running" {
		return thread.ErrTransition
	}
	if err := c.checkChildren(ctx, parent, children); err != nil {
		return err
	}
	if err := c.machine.Update(ctx, parent, func(f *thread.Facts) error {
		f.Awaited = children
		return nil
	}); err != nil {
		return err
	}
	return c.machine.Apply(ctx, parent, thread.Command{Transition: "T08"})
}

// RequeueParent reconciles T09 after child completion. A guarded or premature
// transition leaves the parent waiting. Only subsequent T05 can assign a slot.
func (c *Coordinator) RequeueParent(ctx context.Context, parent string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.machine.Apply(ctx, parent, thread.Command{Transition: "T09"})
	if errors.Is(err, thread.ErrTransition) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	s, err := c.machine.Status(ctx, parent)
	return s.Phase == "queued", err
}

// FailAttempt applies T27 for a failed check when a retry remains, otherwise
// T31. The machine keeps the attempt ID and checks the transition prerequisites.
func (c *Coordinator) FailAttempt(ctx context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.machine.Status(ctx, id)
	if err != nil {
		return err
	}
	if s.Phase != "running" && s.Phase != "verifying" {
		return thread.ErrTransition
	}
	if err := c.setAttemptsLeft(ctx, s); err != nil {
		return err
	}
	row := "T27"
	if s.Facts.Unrecoverable || !retryAfterCurrent(s, c.limits.MaxAttempts) {
		row = "T31"
	}
	return c.machine.Apply(ctx, id, thread.Command{Transition: row})
}

// Cancel delegates depth-first cancellation to T30. The injected cleanup
// callback runs inside the writer and must not wait on writes to that store.
func (c *Coordinator) Cancel(ctx context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.machine.Apply(ctx, id, thread.Command{Transition: "T30"})
}

// --- Evidence: validate and forward records through the injected Recorder ---

// RecordResult saves the managed child's evidence and unblocks its dependents.
// T09 independently waits for terminal thread states.
func (c *Coordinator) RecordResult(ctx context.Context, r Result) error {
	if err := r.Validate(); err != nil {
		return err
	}
	a, err := c.recorder.Assignment(ctx, r.ChildThreadID)
	if err != nil {
		return fmt.Errorf("read result's assignment: %w", err)
	}
	if a.AssignmentID != r.AssignmentID {
		return errors.New("result does not match the child assignment")
	}
	if _, err := c.machine.Status(ctx, r.ChildThreadID); err != nil {
		return err
	}
	if err := c.recorder.RecordResult(ctx, r); err != nil {
		return fmt.Errorf("record child result: %w", err)
	}
	return nil
}

// RecordAuditSetting saves the selected audit setting and its rationale.
func (c *Coordinator) RecordAuditSetting(ctx context.Context, a AuditSetting) error {
	if err := a.Validate(); err != nil {
		return err
	}
	if _, err := c.machine.Status(ctx, a.ThreadID); err != nil {
		return err
	}
	if err := c.recorder.RecordAuditSetting(ctx, a); err != nil {
		return fmt.Errorf("record audit setting: %w", err)
	}
	return nil
}

// failDependent records a failed result with an open requirement for a child
// blocked by a dependency, then returns the typed error so it never starts.
func (c *Coordinator) failDependent(ctx context.Context, id string, cause error) error {
	var dep *DependencyError
	if !errors.As(cause, &dep) {
		return cause
	}
	a, err := c.recorder.Assignment(ctx, id)
	if err != nil {
		return errors.Join(cause, err)
	}
	revision := a.InputRevision
	if blank(revision) {
		revision = a.Target
	}
	r := Result{ChildThreadID: id, AssignmentID: a.AssignmentID, Outcome: "failed", Revision: revision,
		Checks: map[string][]Check{"passed": nil, "failed": nil, "error": nil, "partial": nil, "not_run": nil}, OpenRequirements: []string{dep.Error()}}
	if err := c.RecordResult(ctx, r); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

// checkDependencies verifies each dependency is an already-recorded sibling
// assignment of the same parent.
func (c *Coordinator) checkDependencies(ctx context.Context, a Assignment) error {
	for _, dep := range a.Dependencies {
		sibling, err := c.recorder.AssignmentByID(ctx, dep)
		if err != nil {
			return fmt.Errorf("dependency %s: %w", dep, err)
		}
		if sibling.ParentThreadID != a.ParentThreadID {
			return fmt.Errorf("dependency %s is not a sibling of %s", dep, a.ChildThreadID)
		}
	}
	return nil
}

// checkChildren verifies all referenced threads are managed by the given parent.
func (c *Coordinator) checkChildren(ctx context.Context, parent string, children []string) error {
	for _, id := range children {
		s, err := c.machine.Status(ctx, id)
		if err != nil {
			return err
		}
		if s.Facts.ManagedParent != parent {
			return fmt.Errorf("thread %s is not a managed child of %s", id, parent)
		}
	}
	return nil
}

// terminal reports whether the thread has reached a final state.
func terminal(phase string) bool {
	return phase == "completed" || phase == "failed" || phase == "cancelled"
}
