package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"synch/internal/engine"
	"synch/internal/engine/codex"
	"synch/internal/eventlog"
	"synch/internal/policy"
	"synch/internal/thread"
)

type assignment struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// deferredLauncher resolves Codex only after the adapter admits an operation.
type deferredLauncher struct {
	open func() (*codex.DirectLauncher, error)
}

// Version delegates the admitted version check to the Direct launcher.
func (l deferredLauncher) Version(ctx context.Context, req engine.StartRequest) (string, error) {
	launcher, err := l.open()
	if err != nil {
		return "", err
	}
	return launcher.Version(ctx, req)
}

// Launch delegates the admitted server launch to the Direct launcher.
func (l deferredLauncher) Launch(ctx context.Context, req engine.StartRequest) (codex.Transport, error) {
	launcher, err := l.open()
	if err != nil {
		return nil, err
	}
	return launcher.Launch(ctx, req)
}

// --- Assignment lifecycle: dispatch, settle, and continue from checkpoints ---

// StartAssignment dispatches queued work independently of the invoking client.
func (d *Daemon) StartAssignment(ctx context.Context, req engine.StartRequest) error {
	req.AssignmentID = eventlog.NewID()
	capabilities, err := d.adapter.Capabilities(ctx)
	if err != nil {
		return err
	}
	for _, operation := range []string{engine.StartOp, engine.StreamOp} {
		if err := capabilities.Require(engineSink{daemon: d}, req.AssignmentID, req.ThreadID, operation); err != nil {
			return err
		}
	}
	state, err := d.threads.Status(ctx, req.ThreadID)
	if err != nil {
		return err
	}
	host := d.driver.Describe().Host
	d.decisionMu.Lock()
	decision, err := d.policy.Decide(ctx, policy.Request{Thread: req.ThreadID, Action: "assign.dispatch", Target: capabilities.Adapter, Machine: host})
	d.decisionMu.Unlock()
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return fmt.Errorf("authorize assignment: %s", decision.Reason)
	}
	if state.Phase == "draft" {
		transition := "T01"
		if state.Facts.ReviewPlanFirst {
			transition = "T02"
		}
		if err := d.threads.Apply(ctx, req.ThreadID, thread.Command{Transition: transition}); err != nil {
			return err
		}
		if transition == "T02" {
			return nil
		}
	}
	d.mu.Lock()
	if d.closed || d.sessions[req.ThreadID] != nil {
		d.mu.Unlock()
		return errors.New("daemon is closing or the thread already has a session")
	}
	workCtx, cancel := context.WithCancel(d.lifetime)
	active := &assignment{cancel: cancel, done: make(chan struct{})}
	d.sessions[req.ThreadID] = active
	d.workers.Add(1)
	d.mu.Unlock()

	started := false
	defer func() {
		if !started {
			d.releaseAssignment(req.ThreadID, active)
		}
	}()
	return d.threads.Dispatch(ctx, req.ThreadID, req.AssignmentID, eventlog.NewID(), func(_ context.Context, cp *thread.Checkpoint) error {
		if cp != nil {
			data, err := json.Marshal(cp)
			if err != nil {
				return err
			}
			req.Instructions = append(req.Instructions, engine.Instruction{Text: "Continue from the verified checkpoint:\n" + string(data)})
		}
		session, err := d.adapter.Start(workCtx, req)
		if err != nil {
			return fmt.Errorf("start assignment session: %w", err)
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

// watchAssignment consumes the persisted stream until exit or daemon settlement.
func (d *Daemon) watchAssignment(ctx context.Context, req engine.StartRequest, active *assignment, session engine.Session, events <-chan engine.EngineEvent) {
	defer d.releaseAssignment(req.ThreadID, active)
	for {
		select {
		case <-ctx.Done():
			if err := stopSession(session); err != nil {
				slog.Error("stop assignment session", "thread_id", req.ThreadID, "error", err)
			}
			return
		case event, ok := <-events:
			if !ok {
				if ctx.Err() == nil {
					d.finishAssignment(ctx, req, false)
				}
				return
			}
			if event.Channel == "exit" {
				if ctx.Err() == nil {
					d.finishAssignment(ctx, req, event.Err == nil && event.ExitCode == 0)
				}
				return
			}
		}
	}
}

// finishAssignment records the disposition and advances a still-running thread.
func (d *Daemon) finishAssignment(ctx context.Context, req engine.StartRequest, complete bool) {
	work := "lost"
	transition := "T31"
	if complete {
		work = "complete"
		transition = "T25"
	}
	err := d.record(req.ThreadID, "process.exited", map[string]any{
		"assignment_id": req.AssignmentID, "work": work,
		"execution_outcome": work, "descendants_cleaned": true,
	}, "")
	if err == nil {
		var state thread.State
		state, err = d.threads.Status(ctx, req.ThreadID)
		if err == nil && state.Phase == "running" && state.Facts.Assignment == req.AssignmentID {
			err = d.threads.Apply(ctx, req.ThreadID, thread.Command{Transition: transition})
		}
	}
	if err != nil {
		slog.Error("finish assignment", "thread_id", req.ThreadID, "error", err)
	}
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
