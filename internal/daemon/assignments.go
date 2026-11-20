package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"synch/internal/engine"
	"synch/internal/eventlog"
	"synch/internal/policy"
	"synch/internal/thread"
)

type assignment struct {
	cancel         context.CancelFunc
	done           chan struct{}
	harness, model string // the routed pair, kept for the failure checkpoint
	watch          watchLimits
}

// --- Assignment lifecycle: dispatch, settle, and continue from checkpoints ---

// StartAssignment promotes a managed child's draft, routes it and starts the
// chosen engine independently of the invoking client. A top-level thread is a
// coordinator and is refused (ErrCoordinatorOnly). A routing miss pauses the
// thread (T06) and returns ErrNoEligiblePair; nothing is dispatched.
func (d *Daemon) StartAssignment(ctx context.Context, req engine.StartRequest) error {
	req.AssignmentID = eventlog.NewID()
	state, err := d.threads.Status(ctx, req.ThreadID)
	if err != nil {
		return err
	}
	if state.Facts.ManagedParent == "" {
		return ErrCoordinatorOnly
	}
	if state.Phase == "draft" {
		if state, err = d.promote(ctx, state); err != nil || state.Phase != "queued" {
			return err
		}
	}
	route, err := d.routeThread(ctx, state)
	if err != nil {
		return err
	}
	if !route.Eligible() {
		if err := d.threads.Apply(ctx, req.ThreadID, thread.Command{Transition: "T06"}); err != nil {
			return err
		}
		return fmt.Errorf("%w: %s", ErrNoEligiblePair, route.RoutingReason)
	}
	if req.Workdir == "" {
		return errors.New("assignment has no working directory; configure the thread's project")
	}
	h := d.find(route.Pair.Harness)
	req.Model = route.Pair.Model
	capabilities, err := h.adapter.Capabilities(ctx)
	if err != nil {
		return err
	}
	for _, operation := range []string{engine.StartOp, engine.StreamOp} {
		if err := capabilities.Require(engineSink{daemon: d}, req.AssignmentID, req.ThreadID, operation); err != nil {
			return err
		}
	}
	host := d.driver.Describe().Host
	d.decisionMu.Lock()
	decision, err := d.policy.Decide(ctx, policy.Request{Thread: req.ThreadID, Action: "assign.dispatch", Target: capabilities.Adapter, Machine: host})
	d.decisionMu.Unlock()
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return fmt.Errorf("authorize assignment: %w: %s", ErrPolicyDenied, decision.Reason)
	}
	var project string
	if err := d.store.Write(ctx, func(tx *eventlog.Transaction) (err error) {
		_, project, err = readPin(ctx, tx, req.ThreadID)
		return err
	}); err != nil {
		return err
	}
	watch := d.watchFor(project)
	d.mu.Lock()
	if d.closed || d.sessions[req.ThreadID] != nil {
		d.mu.Unlock()
		return errors.New("daemon is closing or the thread already has a session")
	}
	workCtx, cancel := context.WithCancel(d.lifetime)
	active := &assignment{cancel: cancel, done: make(chan struct{}), harness: route.Pair.Harness, model: route.Pair.Model, watch: watch}
	d.sessions[req.ThreadID] = active
	d.workers.Add(1)
	d.mu.Unlock()

	started := false
	defer func() {
		if !started {
			d.releaseAssignment(req.ThreadID, active)
		}
	}()
	remoteBound := d.remote != nil && state.Facts.RunnerID == d.remoteID
	return d.children.Dispatch(ctx, req.ThreadID, req.AssignmentID, eventlog.NewID(), func(_ context.Context, cp *thread.Checkpoint) error {
		if remoteBound {
			return d.startRemote(ctx, req)
		}
		if cp != nil {
			data, err := json.Marshal(cp)
			if err != nil {
				return err
			}
			req.Instructions = append(req.Instructions, engine.Instruction{Text: "Continue from the verified checkpoint:\n" + string(data)})
		}
		revision := headRevision(ctx, req.Workdir)
		if revision == "" {
			revision = state.Facts.Revision
		}
		if revision == "" {
			revision = "unversioned"
		}
		// The dispatch fact precedes every output event of the session.
		if err := d.record(req.ThreadID, "assignment.dispatched", map[string]any{
			"assignment_id": req.AssignmentID, "harness": route.Pair.Harness, "model": route.Pair.Model,
			"adapter_capabilities_version": capabilities.Version, "pinned": route.Pinned,
			"routing_reason": route.RoutingReason, "input_revision": revision,
			"execution": d.driver.Kind(), "target": host, "skills": []any{},
		}, ""); err != nil {
			return err
		}
		session, err := h.adapter.Start(workCtx, req)
		if errors.Is(err, ErrPolicyDenied) {
			// Policy refused the launch: nothing ran. The caller ends the child failed.
			return fmt.Errorf("launch refused: %w", err)
		}
		if err != nil {
			return d.startFailed(workCtx, req, err)
		}
		events, err := session.Events()
		if err != nil {
			return errors.Join(err, stopSession(session))
		}
		started = true
		go d.watchAssignment(workCtx, req, active, session, events)
		return nil
	})
}

// startFailed maps a harness that could not start to T15. No work was produced,
// so no loss is recorded: restarting after the operator picks a replacement
// belongs to the thread machine's attempt policy. Shutdown is not a failure.
func (d *Daemon) startFailed(ctx context.Context, req engine.StartRequest, cause error) error {
	err := fmt.Errorf("start assignment session: %w", cause)
	if ctx.Err() != nil {
		return err
	}
	return errors.Join(err, d.threads.Apply(context.WithoutCancel(ctx), req.ThreadID, thread.Command{Transition: "T15"}))
}

// releaseAssignment drops admission for a finished assignment and wakes shutdown.
func (d *Daemon) releaseAssignment(id string, active *assignment) {
	active.cancel()
	d.mu.Lock()
	delete(d.sessions, id)
	d.mu.Unlock()
	close(active.done)
	d.workers.Done()
}

// stopSession bounds settlement of an adapter session.
func stopSession(session engine.Session) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return session.Stop(ctx)
}

// loopDetector (a zero limit turns it off) counts consecutive identical tool_call payloads. Any other
// output between them starts the count again.
type loopDetector struct {
	limit, count int
	last         string
}

// observe feeds one event and returns the run length once it reaches the limit, else 0.
func (l *loopDetector) observe(event engine.EngineEvent) int {
	if event.Channel != "tool_call" {
		l.count, l.last = 0, ""
		return 0
	}
	if l.count > 0 && event.Chunk == l.last {
		l.count++
	} else {
		l.count, l.last = 1, event.Chunk
	}
	if l.limit > 0 && l.count >= l.limit {
		return l.count
	}
	return 0
}

// watchAssignment consumes the persisted stream until exit or daemon settlement.
// It is also the child watchdog (D-42): the assignment's runtime budget and a run of
// identical tool calls stop the session and send the child to T15.
func (d *Daemon) watchAssignment(ctx context.Context, req engine.StartRequest, active *assignment, session engine.Session, events <-chan engine.EngineEvent) {
	defer d.releaseAssignment(req.ThreadID, active)
	var expired <-chan time.Time // a nil channel never fires: the budget is off
	if active.watch.runtime > 0 {
		budget := time.NewTimer(active.watch.runtime)
		defer budget.Stop()
		expired = budget.C
	}
	loop := loopDetector{limit: active.watch.repeats}
	for {
		select {
		case <-ctx.Done():
			if err := stopSession(session); err != nil {
				slog.Error("stop assignment session", "thread_id", req.ThreadID, "error", err)
			}
			return
		case <-expired:
			d.haltAssignment(ctx, req, active, session, fmt.Sprintf("runtime budget of %s exceeded", active.watch.runtime))
			return
		case event, ok := <-events:
			if !ok {
				if ctx.Err() == nil {
					d.finishAssignment(ctx, req, active, false, "the engine's event stream ended without an exit", "exited")
				}
				return
			}
			if event.Channel == "exit" {
				if ctx.Err() == nil {
					d.exited(ctx, req, active, event)
				}
				return
			}
			if n := loop.observe(event); n > 0 {
				d.haltAssignment(ctx, req, active, session, fmt.Sprintf("loop detected: %d identical tool calls with no other output", n))
				return
			}
		}
	}
}

// exited maps the engine's exit event to a finished or failed assignment. A
// supervisor stall arrives as an exit that carries an error.
func (d *Daemon) exited(ctx context.Context, req engine.StartRequest, active *assignment, event engine.EngineEvent) {
	if event.Err == nil && event.ExitCode == 0 {
		d.finishAssignment(ctx, req, active, true, "", "exited")
		return
	}
	reason, outcome := fmt.Sprintf("the engine exited with code %d", event.ExitCode), "exited"
	if event.Err != nil {
		reason = "the engine failed: " + event.Err.Error()
		if strings.Contains(event.Err.Error(), "stalled") {
			outcome = "stalled"
		}
	}
	d.finishAssignment(ctx, req, active, false, reason, outcome)
}

// haltAssignment stops a session the watchdog gave up on and records why.
func (d *Daemon) haltAssignment(ctx context.Context, req engine.StartRequest, active *assignment, session engine.Session, reason string) {
	if err := stopSession(session); err != nil {
		slog.Error("stop assignment session", "thread_id", req.ThreadID, "error", err)
	}
	d.finishAssignment(ctx, req, active, false, "watchdog: "+reason, "killed")
}

// finishAssignment records the disposition and advances a still-running thread.
// Finished work is T25 and then the gates. Anything else is lost work and a
// harness failure (T15, decision 07): the reason is recorded on the stream and in the
// failure checkpoint, and the coordinator or operator picks retry, another harness or cancel.
func (d *Daemon) finishAssignment(ctx context.Context, req engine.StartRequest, active *assignment, complete bool, reason, outcome string) {
	work := "lost"
	if complete {
		work = "complete"
	}
	err := d.record(req.ThreadID, "process.exited", map[string]any{
		"assignment_id": req.AssignmentID, "work": work,
		"execution_outcome": outcome, "descendants_cleaned": true,
	}, "")
	if err == nil && !complete {
		err = d.record(req.ThreadID, "engine.output", map[string]any{
			"assignment_id": req.AssignmentID, "channel": "stderr", "chunk": reason,
		}, "")
	}
	if err == nil {
		var state thread.State
		state, err = d.threads.Status(ctx, req.ThreadID)
		if err == nil && state.Phase == "running" && state.Facts.Assignment == req.AssignmentID {
			if complete {
				err = d.threads.Apply(ctx, req.ThreadID, thread.Command{Transition: "T25"})
				if err == nil {
					// T25 committed: the gate runner now checks the result (T26 follows).
					err = d.verify(ctx, req)
				}
			} else {
				err = d.harnessFailed(ctx, req, active, state, reason)
			}
		}
	}
	if err != nil {
		slog.Error("finish assignment", "thread_id", req.ThreadID, "error", err)
	}
}

// harnessFailed takes T15 and writes the handoff checkpoint, naming the reason as the attempt's failure.
func (d *Daemon) harnessFailed(ctx context.Context, req engine.StartRequest, active *assignment, state thread.State, reason string) error {
	revision := headRevision(ctx, req.Workdir)
	if revision == "" {
		revision = state.Facts.Revision
	}
	if revision == "" {
		revision = "unversioned"
	}
	dir := filepath.Join(d.dataDir, "checkpoints")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	cp := thread.Checkpoint{ThreadID: req.ThreadID, Revision: revision, FailedAttempts: []thread.FailedAttempt{{
		Assignment: req.AssignmentID, Harness: active.harness, Model: active.model, Reason: reason}}}
	return d.threads.HarnessFailure(ctx, dir, cp)
}

// Rollover commits the checkpoint, settles the old session, and dispatches
// a fresh session using the verified handoff.
func (d *Daemon) Rollover(ctx context.Context, cp thread.Checkpoint, req engine.StartRequest) error {
	if cp.ThreadID != req.ThreadID {
		return errors.New("rollover request belongs to a different thread")
	}
	dir := filepath.Join(d.dataDir, "checkpoints")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if _, err := d.threads.Rollover(ctx, dir, cp, "operator"); err != nil {
		return err
	}
	d.mu.Lock()
	prior := d.sessions[cp.ThreadID]
	d.mu.Unlock()
	if prior != nil {
		prior.cancel()
		select {
		case <-prior.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return d.StartAssignment(ctx, req)
}
