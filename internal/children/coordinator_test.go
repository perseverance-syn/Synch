package children

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"synch/internal/eventlog"
	"synch/internal/thread"
)

// TestDepthLimit proves C1 applies the configured depth at child creation.
func TestDepthLimit(t *testing.T) {
	c, store, _ := fixture(t, Limits{MaxDepth: 1}, nil)
	root := rootThread(t, c)
	child := childThread(t, c, root)
	events, err := store.ReadStream("thread:"+child, 0, 1)
	must(t, err)
	if len(events) != 1 || events[0].Type != "thread.created" || events[0].TypeVersion != 1 {
		t.Fatalf("missing child creation event: %v", events)
	}
	var p createdThread
	must(t, json.Unmarshal(events[0].Payload, &p))
	if p.ID != child || p.Parent != root || p.Depth != 1 || p.Role != "builder" {
		t.Fatalf("child creation payload = %+v", p)
	}
	err = c.CreateChild(t.Context(), assignment(child), thread.Facts{})
	var depth *DepthError
	if !errors.As(err, &depth) || depth.Depth != 2 || depth.Max != 1 {
		t.Fatalf("depth refusal = %v", err)
	}
}

// TestQ27Defaults proves C1 applies the specified defaults and preserves operator overrides.
func TestQ27Defaults(t *testing.T) {
	got, err := (Limits{MaxLocal: 8}).defaults()
	must(t, err)
	if want := (Limits{MaxDepth: 3, MaxLocal: 8, MaxAttempts: 3}); got != want {
		t.Fatalf("limits = %+v, want %+v", got, want)
	}
	got, err = (Limits{}).defaults()
	must(t, err)
	if got.MaxLocal != 4 {
		t.Fatalf("default local cap = %d, want 4", got.MaxLocal)
	}
}

// TestWaitingParentAtLimitOne proves C2's T08, T09 and T05 sequence makes progress.
func TestWaitingParentAtLimitOne(t *testing.T) {
	c, _, _ := fixture(t, Limits{MaxLocal: 1}, nil)
	root := rootThread(t, c)
	children := []string{childThread(t, c, root), childThread(t, c, root)}
	dispatch(t, c, root, true)
	dispatch(t, c, children[0], false)
	must(t, c.WaitForChildren(t.Context(), root, children))
	assertPhase(t, c, root, "waiting_children", false)
	for i, child := range children {
		dispatch(t, c, child, true)
		if i == 0 {
			dispatch(t, c, children[1], false)
		}
		requeued, err := c.RequeueParent(t.Context(), root)
		must(t, err)
		if requeued {
			t.Fatal("parent requeued while a child was active")
		}
		must(t, c.Cancel(t.Context(), child))
	}
	requeued, err := c.RequeueParent(t.Context(), root)
	must(t, err)
	if !requeued {
		t.Fatal("parent did not requeue")
	}
	assertPhase(t, c, root, "queued", false)
	competitor := rootThread(t, c)
	dispatch(t, c, competitor, true)
	dispatch(t, c, root, false)
	assertPhase(t, c, root, "queued", false)
	must(t, c.Cancel(t.Context(), competitor))
	dispatch(t, c, root, true)
	assertPhase(t, c, root, "running", true)
}

// TestSimultaneousDispatchesKeepExcessQueued proves C1 and C2 cap concurrent parent and child launches.
func TestSimultaneousDispatchesKeepExcessQueued(t *testing.T) {
	c, _, _ := fixture(t, Limits{MaxLocal: 2}, nil)
	root := rootThread(t, c)
	ids := []string{root, childThread(t, c, root), childThread(t, c, root), childThread(t, c, root)}
	var started atomic.Int32
	var callers sync.WaitGroup
	ready := make(chan struct{})
	for _, id := range ids {
		callers.Go(func() {
			<-ready
			err := c.Dispatch(t.Context(), id, eventlog.NewID(), eventlog.NewID(), func(context.Context, *thread.Checkpoint) error {
				started.Add(1)
				return nil
			})
			if err != nil {
				t.Errorf("dispatch %s: %v", id, err)
			}
		})
	}
	close(ready)
	callers.Wait()
	if got := started.Load(); got != 2 {
		t.Fatalf("launched %d threads, want 2", got)
	}
	var queued, running []string
	for _, id := range ids {
		state, err := c.Threads().Status(t.Context(), id)
		must(t, err)
		switch state.Phase {
		case "running":
			running = append(running, id)
		case "queued":
			queued = append(queued, id)
		default:
			t.Fatalf("unexpected phase %s for %s", state.Phase, id)
		}
	}
	if len(queued) != 2 || len(running) != 2 {
		t.Fatalf("queued %v, running %v", queued, running)
	}
}

// TestRemoteWorkDoesNotConsumeLocalSlot proves C1 caps only daemon-local execution.
func TestRemoteWorkDoesNotConsumeLocalSlot(t *testing.T) {
	c, _, _ := fixture(t, Limits{MaxLocal: 1}, nil)
	root := rootThread(t, c)
	remote := assignment(root)
	must(t, c.CreateChild(t.Context(), remote, thread.Facts{}))
	queue(t, c, remote.ChildThreadID)
	must(t, c.Threads().Update(t.Context(), remote.ChildThreadID, func(f *thread.Facts) error {
		f.RunnerID = "runner"
		return nil
	}))
	local := childThread(t, c, root)
	dispatch(t, c, remote.ChildThreadID, true)
	dispatch(t, c, root, true)
	dispatch(t, c, local, false)
}

// TestChangedPlacementKeepsThreadQueued proves C1 cannot admit stale dispatch placement.
func TestChangedPlacementKeepsThreadQueued(t *testing.T) {
	c, _, _ := fixture(t, Limits{MaxLocal: 1}, nil)
	root := rootThread(t, c)
	child := childThread(t, c, root)
	state := assertPhase(t, c, child, "queued", false)
	state.Facts.RunnerID = "stale remote selection"
	must(t, c.rememberPlacement(t.Context(), state))
	must(t, c.Threads().Dispatch(t.Context(), child, eventlog.NewID(), eventlog.NewID(), func(context.Context, *thread.Checkpoint) error {
		t.Error("started with stale remote placement")
		return nil
	}))
	assertPhase(t, c, child, "queued", false)
	dispatch(t, c, child, true)
	dispatch(t, c, root, false)
	must(t, c.Cancel(t.Context(), root))
	assertPhase(t, c, child, "cancelled", false)
}

// --- Managed outcomes: cancel trees, gate sibling work and retain failed attempts ---

// TestCancelTree proves C3 rolls back T30 until process confirmation and records leaf first.
func TestCancelTree(t *testing.T) {
	var calls []string
	blocked := ""
	c, store, _ := fixture(t, Limits{}, func(_ context.Context, id string) (bool, error) {
		calls = append(calls, id)
		return id != blocked, nil
	})
	root := rootThread(t, c)
	child := childThread(t, c, root)
	leaf := childThread(t, c, child)
	for _, id := range []string{root, child, leaf} {
		dispatch(t, c, id, true)
	}
	blocked = child
	if err := c.Cancel(t.Context(), root); err == nil {
		t.Fatal("cancelled before confirmation")
	}
	if !slices.Equal(calls, []string{leaf, child}) {
		t.Fatalf("confirmation order = %v", calls)
	}
	assertPhase(t, c, leaf, "running", true)
	blocked = ""
	calls = nil
	must(t, c.Cancel(t.Context(), root))
	events, err := store.ReadGlobal(0, 100)
	must(t, err)
	var cancelled []string
	for _, e := range events {
		var p struct{ Transition string }
		must(t, json.Unmarshal(e.Payload, &p))
		if p.Transition == "T30" {
			cancelled = append(cancelled, e.ThreadID)
		}
	}
	want := []string{leaf, child, root}
	if !slices.Equal(calls, want) || !slices.Equal(cancelled, want) {
		t.Fatalf("confirmation = %v, persisted order = %v", calls, cancelled)
	}
}

// TestIndependentAndDependentChildren proves C5 allows overlapping runs and waits for a recorded result.
func TestIndependentAndDependentChildren(t *testing.T) {
	c, _, recorder := fixture(t, Limits{MaxLocal: 2}, nil)
	root := rootThread(t, c)
	first, independent := childThread(t, c, root), childThread(t, c, root)
	record, err := recorder.Assignment(t.Context(), first)
	must(t, err)
	dependent := assignment(root)
	dependent.Dependencies = []string{record.AssignmentID}
	must(t, c.CreateChild(t.Context(), dependent, thread.Facts{}))
	queue(t, c, dependent.ChildThreadID)
	dispatch(t, c, first, true)
	dispatch(t, c, independent, true)
	assertPhase(t, c, first, "running", true)
	assertPhase(t, c, independent, "running", true)
	must(t, c.Cancel(t.Context(), independent))
	dispatch(t, c, dependent.ChildThreadID, false)
	must(t, c.RecordResult(t.Context(), resultFor(record, "completed")))
	dispatch(t, c, dependent.ChildThreadID, true)
}

// TestAttemptExhaustion proves C1 retains failed attempts and fails on the configured last one.
func TestAttemptExhaustion(t *testing.T) {
	c, store, _ := fixture(t, Limits{MaxAttempts: 2}, nil)
	child := childThread(t, c, rootThread(t, c))
	must(t, c.Threads().Update(t.Context(), child, func(f *thread.Facts) error {
		f.Revision = "rev"
		return nil
	}))
	var attempts []string
	for _, want := range []string{"queued", "failed"} {
		attempts = append(attempts, dispatch(t, c, child, true))
		must(t, c.Threads().Apply(t.Context(), child, thread.Command{Transition: "T25"}))
		payload := json.RawMessage(`{"check":"build","status":"failed","revision":"rev","config_digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","tool_versions":{}}`)
		_, err := store.Append(t.Context(), []eventlog.NewEvent{{
			ID: eventlog.NewID(), Stream: "thread:" + child, ThreadID: child, Type: "check.result", TypeVersion: 1,
			Actor: eventlog.Actor{Kind: "daemon", ID: "test"}, Payload: payload,
		}})
		must(t, err)
		must(t, c.FailAttempt(t.Context(), child))
		state := assertPhase(t, c, child, want, false)
		if !slices.Equal(state.FailedAssignments, attempts) {
			t.Fatalf("retained attempts = %v, want %v", state.FailedAssignments, attempts)
		}
	}
}

// TestLostFinalAttemptFailsOnRecovery proves C1 supplies the retry budget to T36.
func TestLostFinalAttemptFailsOnRecovery(t *testing.T) {
	c, store, _ := fixture(t, Limits{MaxAttempts: 1}, nil)
	child := childThread(t, c, rootThread(t, c))
	attempt := dispatch(t, c, child, true)
	must(t, c.Threads().Apply(t.Context(), child, thread.Command{Transition: "T15"}))
	payload, err := json.Marshal(map[string]any{
		"assignment_id": attempt, "execution_outcome": "killed", "work": "lost", "descendants_cleaned": true,
	})
	must(t, err)
	_, err = store.Append(t.Context(), []eventlog.NewEvent{{
		ID: eventlog.NewID(), Stream: "thread:" + child, ThreadID: child, Type: "process.exited", TypeVersion: 1,
		Actor: eventlog.Actor{Kind: "daemon", ID: "test"}, Payload: payload,
	}})
	must(t, err)
	state := assertPhase(t, c, child, "failed", false)
	if !slices.Equal(state.FailedAssignments, []string{attempt}) {
		t.Fatalf("retained attempts = %v, want %s", state.FailedAssignments, attempt)
	}
}

// --- Fixtures: use the real thread machine and event writer for transitions ---

// fixture opens a coordinator with an isolated event log and recorder.
func fixture(t *testing.T, limits Limits, confirm func(context.Context, string) (bool, error)) (*Coordinator, *eventlog.Store, *MemoryRecorder) {
	t.Helper()
	store, err := eventlog.Open(filepath.Join(t.TempDir(), "events.db"))
	must(t, err)
	t.Cleanup(func() { must(t, store.Close()) })
	if confirm == nil {
		confirm = func(context.Context, string) (bool, error) { return true, nil }
	}
	recorder := new(MemoryRecorder)
	allow := func(context.Context, *eventlog.Transaction, thread.State) (bool, error) { return true, nil }
	c, err := Open(t.Context(), store, limits, thread.Hooks{Policy: allow, Pair: allow, ConfirmGone: confirm}, recorder)
	must(t, err)
	return c, store, recorder
}

// assignment creates a valid sibling assignment with stable IDs for retries.
func assignment(parent string) Assignment {
	return Assignment{ParentThreadID: parent, ChildThreadID: eventlog.NewID(), AssignmentID: eventlog.NewID(),
		Purpose: "Implement the step", Role: "builder", RoleReason: "The step changes code", InputRevision: "revision",
		ExpectedEvidence: []string{"build output"}}
}

// rootThread creates and queues a local root.
func rootThread(t *testing.T, c *Coordinator) string {
	t.Helper()
	id := eventlog.NewID()
	must(t, c.CreateRoot(t.Context(), id, "project", thread.Facts{}))
	queue(t, c, id)
	return id
}

// childThread creates and queues a local managed child.
func childThread(t *testing.T, c *Coordinator, parent string) string {
	t.Helper()
	a := assignment(parent)
	must(t, c.CreateChild(t.Context(), a, thread.Facts{}))
	queue(t, c, a.ChildThreadID)
	return a.ChildThreadID
}

// queue applies T01 to an accepted test brief.
func queue(t *testing.T, c *Coordinator, id string) {
	t.Helper()
	must(t, c.Threads().Apply(t.Context(), id, thread.Command{Transition: "T01"}))
}

// dispatch requests a fresh execution attempt and checks whether its launcher ran.
func dispatch(t *testing.T, c *Coordinator, id string, want bool) string {
	t.Helper()
	attempt := eventlog.NewID()
	started := false
	must(t, c.Dispatch(t.Context(), id, attempt, eventlog.NewID(), func(context.Context, *thread.Checkpoint) error {
		started = true
		return nil
	}))
	if started != want {
		t.Fatalf("dispatch %s started = %v, want %v", id, started, want)
	}
	return attempt
}

// assertPhase checks the public machine state and slot in one observation.
func assertPhase(t *testing.T, c *Coordinator, id, phase string, slot bool) thread.State {
	t.Helper()
	state, err := c.Threads().Status(t.Context(), id)
	must(t, err)
	if state.Phase != phase || state.Slot != slot {
		t.Fatalf("thread %s has phase %q and slot %v, want %q and %v", id, state.Phase, state.Slot, phase, slot)
	}
	return state
}

// must stops the test at an unexpected error.
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
