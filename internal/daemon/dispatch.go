package daemon

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"synch/internal/engine"
	"synch/internal/eventlog"
	"synch/internal/routing"
	"synch/internal/thread"
)

// ErrNoEligiblePair reports that routing found no qualified, permitted pair and
// the thread was paused with no_eligible_pair (T06). It is an outcome, not a fault.
var ErrNoEligiblePair = errors.New("no eligible harness and model pair")

// ProjectConfig names a project's checkout and its gate file. GateFile is
// relative to the checkout unless absolute; it defaults to .synch/gates.json.
type ProjectConfig struct {
	Dir      string `json:"dir"`
	GateFile string `json:"gate_file,omitempty"`
	// MaxRuntime and LoopRepeats override the daemon's child watchdog defaults for this
	// project (same formats as Config.ChildMaxRuntime and ChildLoopRepeats).
	MaxRuntime  Setting `json:"max_runtime,omitempty"`
	LoopRepeats Setting `json:"loop_repeats,omitempty"`
}

// --- Dispatch loop: promote, route, and start every dispatchable thread ---

// maxRetryDelay caps the backoff between attempts to dispatch a failing thread.
const maxRetryDelay = time.Minute

// ErrPolicyDenied marks a dispatch the policy engine refused; it is not retried.
var ErrPolicyDenied = errors.New("policy denied")

// dispatchFailure is the retry state of a thread whose last start attempt failed.
type dispatchFailure struct {
	attempts int
	next     time.Time
	denied   bool
}

// dispatchLoop polls for dispatchable threads until the daemon closes.
// ponytail: polling plus sequential starts; an event-driven kick is the upgrade if latency matters.
func (d *Daemon) dispatchLoop() {
	defer d.workers.Done()
	tick := time.NewTicker(d.poll)
	defer tick.Stop()
	for {
		d.settleChildren()
		d.dispatchPending()
		d.settleVerifying()
		select {
		case <-d.lifetime.Done():
			return
		case <-tick.C:
		}
	}
}

// dispatchPending acts on each draft or queued thread once per attempt: a
// top-level thread coordinates (creates its Builder child and waits, or reports
// the finished children) and a managed child starts its engine session. A routing pause is an outcome and a policy denial is final until
// restart, so neither floods the event log; any other failure is retried with
// exponential backoff (card 111 H5).
func (d *Daemon) dispatchPending() {
	ids, err := d.threadsIn(d.lifetime, "any", "draft", "queued")
	if err != nil {
		if d.lifetime.Err() == nil {
			slog.Error("list dispatchable threads", "error", err)
		}
		return
	}
	for _, id := range ids {
		if d.lifetime.Err() != nil {
			return
		}
		d.mu.Lock()
		failure := d.failed[id]
		busy := d.sessions[id] != nil
		d.mu.Unlock()
		if busy || failure != nil && (failure.denied || time.Now().Before(failure.next)) {
			continue
		}
		state, err := d.threads.Status(d.lifetime, id)
		if err != nil || state.BlockedBy != "" {
			continue
		}
		if state.Facts.ManagedParent == "" {
			d.noteDispatch(id, d.coordinate(d.lifetime, id))
			continue
		}
		if _, err := d.recorder.Assignment(d.lifetime, id); err != nil {
			continue // not a coordinated child, so nothing assigns it
		}
		req, err := d.requestFor(d.lifetime, id)
		if err == nil {
			err = d.StartAssignment(d.lifetime, req)
		}
		if err != nil && d.childDispatchFailed(d.lifetime, id, err) {
			err = nil
		}
		d.noteDispatch(id, err)
	}
}

// noteDispatch records the outcome of one start attempt for retry scheduling.
func (d *Daemon) noteDispatch(id string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err == nil || errors.Is(err, ErrNoEligiblePair) || d.lifetime.Err() != nil {
		delete(d.failed, id)
		return
	}
	failure := d.failed[id]
	if failure == nil {
		failure = &dispatchFailure{}
		d.failed[id] = failure
	}
	failure.denied = errors.Is(err, ErrPolicyDenied)
	failure.attempts++
	delay := min(d.retryBase<<min(failure.attempts-1, 10), maxRetryDelay)
	failure.next = time.Now().Add(delay)
	slog.Warn("dispatch thread", "thread_id", id, "attempts", failure.attempts, "denied", failure.denied, "error", err)
}

// settleVerifying retries T26 for threads parked in verifying. A thread waits
// there when its checks were recorded before the audit settled (dec. 08), so the
// later audit.status needs no restart to move it on. Threads whose gates are
// still running, or that recorded no check at all, are left alone.
func (d *Daemon) settleVerifying() {
	ids, err := d.threadsIn(d.lifetime, "any", "verifying")
	if err != nil {
		return
	}
	for _, id := range ids {
		d.mu.Lock()
		running := d.verifying[id]
		d.mu.Unlock()
		state, err := d.threads.Status(d.lifetime, id)
		if running || err != nil || len(state.Checks) == 0 {
			continue
		}
		err = d.advanceVerified(d.lifetime, id)
		if err != nil && !errors.Is(err, thread.ErrTransition) && d.lifetime.Err() == nil {
			slog.Warn("settle verifying thread", "thread_id", id, "error", err)
		}
	}
}

// threadsIn lists threads in the given phases. Scope "top" keeps coordinators,
// "managed" keeps children and "any" keeps both.
func (d *Daemon) threadsIn(ctx context.Context, scope string, phases ...string) (ids []string, err error) {
	err = d.store.Write(ctx, func(tx *eventlog.Transaction) error {
		marks := strings.TrimSuffix(strings.Repeat("?,", len(phases)), ",")
		args := make([]any, len(phases))
		for i, phase := range phases {
			args[i] = phase
		}
		filter := map[string]string{"any": "", "top": " AND COALESCE(json_extract(state,'$.Facts.ManagedParent'),'')=''",
			"managed": " AND COALESCE(json_extract(state,'$.Facts.ManagedParent'),'')<>''"}[scope]
		rows, err := tx.QueryContext(ctx, `SELECT id FROM thread_state
 WHERE json_extract(state,'$.Phase') IN (`+marks+`)`+filter+` ORDER BY id`, args...)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return rows.Err()
	})
	return ids, err
}

// requestFor builds the start request from the thread's submission and project.
func (d *Daemon) requestFor(ctx context.Context, id string) (engine.StartRequest, error) {
	var project, brief string
	err := d.store.Write(ctx, func(tx *eventlog.Transaction) error {
		_, project, _ = readPin(ctx, tx, id)
		err := tx.QueryRowContext(ctx, `SELECT COALESCE(json_extract(record,'$.result.submission.brief'),'') FROM receipts
 WHERE status='accepted' AND json_extract(record,'$.thread_id')=? AND json_extract(record,'$.result.submission') IS NOT NULL LIMIT 1`, id).Scan(&brief)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	})
	if err != nil {
		return engine.StartRequest{}, err
	}
	if a, err := d.recorder.Assignment(ctx, id); err == nil {
		brief = a.Purpose // a child works on the purpose its coordinator assigned
	}
	req := engine.StartRequest{ThreadID: id, Workdir: d.workdir(id, project)}
	if brief != "" {
		req.Instructions = []engine.Instruction{{Text: brief}}
	}
	return req, nil
}

// workdir finds the thread's own worktree, else its project's checkout.
func (d *Daemon) workdir(id, project string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if dir := d.worktrees[id]; dir != "" {
		return dir
	}
	return d.projects[project].Dir
}

// --- Routing: decisions 03, 07 and 11 over the configured engines ---

// readPin reads the operator pin and project recorded when the API created the
// thread. A managed child has no row of its own and inherits its parent's.
func readPin(ctx context.Context, tx *eventlog.Transaction, id string) (pin *routing.Pair, project string, err error) {
	for range 8 {
		var harness, model string
		err = tx.QueryRowContext(ctx, "SELECT project_id,harness,model FROM api_threads WHERE id=?", id).Scan(&project, &harness, &model)
		if err == nil {
			if harness != "" && model != "" {
				pin = &routing.Pair{Harness: harness, Model: model}
			}
			return pin, project, nil
		}
		if !errors.Is(err, sql.ErrNoRows) && !strings.Contains(err.Error(), "no such table") {
			return nil, "", err
		}
		var parent string
		perr := tx.QueryRowContext(ctx, "SELECT COALESCE(json_extract(state,'$.Facts.ManagedParent'),'') FROM thread_state WHERE id=?", id).Scan(&parent)
		if perr != nil || parent == "" {
			return nil, "", nil
		}
		id = parent
	}
	return nil, "", nil
}

// route selects a pair for the thread from the offered engines' declarations.
// An adapter's start-time version probe is the final authority on the installed
// release, so routing sees the declared range's lower bound unless configured.
func (d *Daemon) route(ctx context.Context, s thread.State, pin *routing.Pair) routing.Result {
	req := routing.Request{
		HostOS: runtime.GOOS, TaskKind: d.taskKind, Default: d.defaults, Pin: pin,
		SelectedHarness: s.SelectedHarness, Evidence: d.routeData,
		// Policy authorizes the dispatch itself in StartAssignment; routing only
		// requires that the harness is offered.
		Permitted: func(p routing.Pair) bool { return d.find(p.Harness) != nil },
	}
	for _, h := range d.harnesses {
		caps, err := h.adapter.Capabilities(ctx)
		if err != nil {
			continue
		}
		for _, model := range caps.Models {
			req.Candidates = append(req.Candidates, routing.Candidate{
				Pair: routing.Pair{Harness: h.name, Model: model}, HarnessVersion: h.version, Capabilities: caps,
			})
		}
	}
	return routing.Select(req)
}

// pairHook answers the thread machine's T05, T06 and T21 pair guard with the same routing the dispatcher uses.
func (d *Daemon) pairHook(ctx context.Context, tx *eventlog.Transaction, s thread.State) (bool, error) {
	d.mu.Lock()
	refused := d.unrunnable[s.ID]
	d.mu.Unlock()
	if refused {
		return false, nil // a child being abandoned has no pair it may run on
	}
	pin, _, err := readPin(ctx, tx, s.ID)
	if err != nil {
		return false, err
	}
	return d.route(ctx, s, pin).Eligible(), nil
}

// routeThread reads the pin and routes a thread outside any transaction.
func (d *Daemon) routeThread(ctx context.Context, s thread.State) (routing.Result, error) {
	var pin *routing.Pair
	err := d.store.Write(ctx, func(tx *eventlog.Transaction) (err error) {
		pin, _, err = readPin(ctx, tx, s.ID)
		return err
	})
	if err != nil {
		return routing.Result{}, err
	}
	return d.route(ctx, s, pin), nil
}

// --- Evidence helpers shared by dispatch and verification ---

// headRevision reads the checked-out commit of a worktree without changing it.
func headRevision(ctx context.Context, dir string) string {
	if dir == "" {
		return ""
	}
	if _, err := os.Stat(dir); err != nil {
		return ""
	}
	cmd := exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	cmd.Dir = filepath.Clean(dir)
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
