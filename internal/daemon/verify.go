package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"synch/internal/engine"
	"synch/internal/eventlog"
	"synch/internal/gates"
	"synch/internal/thread"
)

// defaultGateFile is the gate file location relative to a checkout.
var defaultGateFile = filepath.Join(".synch", "gates.json")

// verify runs the project's gate file on the thread's worktree while it is in
// verifying (T25), records every check.result, and then asks for T26 (or, for a
// managed child with a failed check, T27 or T31).
// Checks that could not run are recorded not_run or error with a reason; a
// missing gate file can never produce a pass.
func (d *Daemon) verify(ctx context.Context, req engine.StartRequest) error {
	state, err := d.threads.Status(ctx, req.ThreadID)
	if err != nil {
		return err
	}
	// settleVerifying must not advance the thread while its gates are still running.
	d.mu.Lock()
	d.verifying[req.ThreadID] = true
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.verifying, req.ThreadID)
		d.mu.Unlock()
	}()
	revision := headRevision(ctx, req.Workdir)
	if revision == "" {
		revision = state.Facts.Revision
	}
	if revision == "" {
		revision = "unversioned"
	}
	// Results project onto the thread only for its current revision.
	if revision != state.Facts.Revision {
		if err := d.threads.Update(ctx, req.ThreadID, func(f *thread.Facts) error { f.Revision = revision; return nil }); err != nil {
			return err
		}
	}
	var project string
	if err := d.store.Write(ctx, func(tx *eventlog.Transaction) (err error) {
		_, project, err = readPin(ctx, tx, req.ThreadID)
		return err
	}); err != nil {
		return err
	}
	results := d.runGates(ctx, req.ThreadID, req.Workdir, d.projects[project], state.Facts.RequiredChecks, revision)
	for _, result := range results {
		if err := d.record(req.ThreadID, "check.result", result, ""); err != nil {
			return err
		}
	}
	if ctx.Err() != nil {
		// Shutdown mid-run: stay in verifying so recovery classifies the work.
		return nil
	}
	return d.advanceVerified(ctx, req.ThreadID)
}

// runGates executes the gate file through policy and the supervisor (gateExecutor),
// or explains why no check could run.
func (d *Daemon) runGates(ctx context.Context, threadID, dir string, project ProjectConfig, required []string, revision string) []gates.Result {
	path := project.GateFile
	if path == "" {
		path = defaultGateFile
	}
	if dir == "" && !filepath.IsAbs(path) {
		return unrun(required, revision, gates.StatusNotRun, "no gate file: the thread has no configured worktree")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(dir, path)
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return unrun(required, revision, gates.StatusNotRun, fmt.Sprintf("gate file %s is missing", path))
	}
	if err != nil {
		return unrun(required, revision, gates.StatusError, "gate file unreadable: "+err.Error())
	}
	results, err := gates.NewRunner(gateExecutor{daemon: d, thread: threadID, dir: dir}).Run(ctx, data, gates.RunInput{Dir: dir})
	if err != nil {
		return unrun(required, revision, gates.StatusError, "gate run failed: "+err.Error())
	}
	return results
}

// unrun records every required check (or one "gates" check) with a reason and no pass.
func unrun(required []string, revision string, status gates.Status, reason string) []gates.Result {
	if len(required) == 0 {
		required = []string{"gates"}
	}
	results := make([]gates.Result, 0, len(required))
	for _, name := range required {
		results = append(results, gates.Result{Check: name, Status: status, Reason: reason, Revision: revision})
	}
	return results
}
