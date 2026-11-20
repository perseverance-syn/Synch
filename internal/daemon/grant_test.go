package daemon

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"synch/internal/eventlog"
	"synch/internal/policy"
	"synch/internal/thread"
)

// TestNoGrantStaysQueued proves G1 (D-43) and G2's API half: a coordinator with no
// active Full Control grant is not delegated. It stays queued, no child exists, no
// session starts and its status says what to run. Once the operator grants, the
// next dispatch tick delegates and the Builder child inherits the grant and runs.
func TestNoGrantStaysQueued(t *testing.T) {
	worktree := passingWorktree(t)
	cfg, starts := loopConfig(t, worktree)
	d, base := serveGate(t, cfg)
	parent := gateThread(t, base, "no-grant")
	waitPhase(t, d, parent, "queued")
	time.Sleep(300 * time.Millisecond) // many dispatch ticks (poll is 20ms)
	state := waitPhase(t, d, parent, "queued")
	if ids, err := d.threadsIn(t.Context(), "managed", "draft", "queued", "running", "waiting_children"); err != nil || len(ids) != 0 || len(starts) != 0 || len(state.Facts.Awaited) != 0 {
		t.Fatalf("delegated without a grant: children %v, starts %d, awaited %v, err %v", ids, len(starts), state.Facts.Awaited, err)
	}
	info := d.driver.Describe()
	want := "synch grants create " + parent + " --host " + info.Host + " --identity " + info.Identity
	var view struct {
		WaitingFor string `json:"waiting_for"`
	}
	if err := json.Unmarshal([]byte(readBody(t, gateHTTP(t, "GET", base+"/v1/threads/"+parent, "", "", ""))), &view); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(view.WaitingFor, "waiting for an operator grant") || !strings.Contains(view.WaitingFor, want) {
		t.Fatalf("thread status does not name the grant command %q: %q", want, view.WaitingFor)
	}

	grantWorker(t, d, parent)
	session := <-starts
	child := session.req.ThreadID
	waitPhase(t, d, parent, "waiting_children")
	dec, err := d.policy.Decide(t.Context(), policy.Request{Thread: child, Action: "host.exec", Machine: info.Host,
		Identity: info.Identity, Target: info.Host, Paths: []string{worktree}})
	if err != nil || !dec.Allowed {
		t.Fatalf("child did not inherit the grant: %+v, %v", dec, err)
	}
	if body := readBody(t, gateHTTP(t, "GET", base+"/v1/threads/"+parent, "", "", "")); strings.Contains(body, "waiting_for") {
		t.Fatalf("status still waits after the grant: %s", body)
	}
}

// TestDispatchCrashBeforeChildRecord proves G3 for a crash after the parent's T05
// but before any child was recorded: recovery returns the parent to queued (T22)
// without an operator and the next dispatch delegates again, once.
func TestDispatchCrashBeforeChildRecord(t *testing.T) {
	worktree := passingWorktree(t)
	cfg, starts := loopConfig(t, worktree)
	cfg.noLoop = true
	d, base := serveGate(t, cfg)
	parent := gateThread(t, base, "crash-early")
	grantWorker(t, d, parent)
	dispatchNoSession(t, d, parent)
	if s := waitPhase(t, d, parent, "running"); len(s.Facts.Awaited) != 0 {
		t.Fatalf("setup: %+v", s)
	}
	reopened := restart(t, d, cfg)
	if s, err := reopened.threads.Status(t.Context(), parent); err != nil || s.Phase != "queued" {
		t.Fatalf("parent after recovery: %+v, %v", s, err)
	}
	cfg.noLoop = false
	reopened = restart(t, reopened, cfg) // the dispatch loop delegates it again
	session := <-starts
	waitPhase(t, reopened, parent, "waiting_children")
	if child := onlyChild(t, reopened, parent); child != session.req.ThreadID {
		t.Fatalf("child %s, session on %s", child, session.req.ThreadID)
	}
}

// TestDispatchCrashAfterChildRecord proves G3 for a crash after the child was
// recorded but before T08: recovery requeues the parent, which then waits on that
// same child. No duplicate child is created for the dispatch.
func TestDispatchCrashAfterChildRecord(t *testing.T) {
	worktree := passingWorktree(t)
	cfg, starts := loopConfig(t, worktree)
	cfg.noLoop = true
	d, base := serveGate(t, cfg)
	parent := gateThread(t, base, "crash-late")
	grantWorker(t, d, parent)
	child := childOf(t, d, parent)
	dispatchNoSession(t, d, parent)
	waitPhase(t, d, parent, "running")
	reopened := restart(t, d, cfg)
	cfg.noLoop = false
	reopened = restart(t, reopened, cfg)
	session := <-starts
	state := waitPhase(t, reopened, parent, "waiting_children")
	if len(state.Facts.Awaited) != 1 || state.Facts.Awaited[0] != child || session.req.ThreadID != child {
		t.Fatalf("parent awaits %v, session on %s, want %s", state.Facts.Awaited, session.req.ThreadID, child)
	}
	if got := onlyChild(t, reopened, parent); got != child {
		t.Fatalf("a second child %s exists next to %s", got, child)
	}
}

// dispatchNoSession takes a promoted parent through T05 without any session, the
// state a coordinator is in between T05 and T08.
func dispatchNoSession(t *testing.T, d *Daemon, id string) {
	t.Helper()
	state, err := d.threads.Status(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.promote(t.Context(), state); err != nil {
		t.Fatal(err)
	}
	noSession := func(_ context.Context, _ *thread.Checkpoint) error { return nil }
	if err := d.children.Dispatch(t.Context(), id, eventlog.NewID(), eventlog.NewID(), noSession); err != nil {
		t.Fatal(err)
	}
}

// readBody returns a response body as text.
func readBody(t *testing.T, res *http.Response) string {
	t.Helper()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// restart closes a daemon and opens a new one on the same data directory, the
// way a crash and restart do. The loop only runs if cfg.noLoop is false.
func restart(t *testing.T, d *Daemon, cfg Config) *Daemon {
	t.Helper()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _ := serveGate(t, cfg)
	return reopened
}
