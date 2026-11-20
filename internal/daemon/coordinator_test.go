package daemon

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"synch/internal/children"
	"synch/internal/engine"
	"synch/internal/eventlog"
	"synch/internal/execdriver"
	"synch/internal/policy"
	"synch/internal/thread"
)

// launchAdapter is the test fake engine whose Start first launches a process
// through the policy-checked driver, as every shipped adapter does.
type launchAdapter struct {
	fakeAdapter
	daemon *atomic.Pointer[Daemon]
}

// Start launches git through policy; a denial means no session starts.
func (l *launchAdapter) Start(ctx context.Context, req engine.StartRequest) (engine.Session, error) {
	d := l.daemon.Load()
	_, err := policyDriver{ExecutionDriver: d.driver, daemon: d}.Spawn(ctx, execdriver.ProcSpec{
		ID: eventlog.NewID(), ThreadID: req.ThreadID, Path: "git", Args: []string{"init", "marker"}, Dir: req.Workdir,
	})
	if err != nil {
		return nil, err
	}
	return l.fakeAdapter.Start(ctx, req)
}

// TestCoordinatorNeverRunsCommands proves C1: a top-level thread is a coordinator
// even with an assignment and an operator grant, so policy denies host.exec,
// and no engine session can start on it. Its managed child is a builder.
func TestCoordinatorNeverRunsCommands(t *testing.T) {
	cfg, _ := gateConfig(t)
	allowExec(t, &cfg)
	d, base := serveGate(t, cfg)
	parent := gateThread(t, base, "coordinator")
	if err := d.threads.Update(t.Context(), parent, func(f *thread.Facts) error { f.Assignment = "assigned"; return nil }); err != nil {
		t.Fatal(err)
	}
	grantWorker(t, d, parent)
	child := childOf(t, d, parent)
	host := d.driver.Describe()
	decide := func(id string) policy.Decision {
		dec, err := d.policy.Decide(t.Context(), policy.Request{Thread: id, Action: "host.exec", Machine: host.Host,
			Identity: host.Identity, Target: host.Host, Paths: []string{t.TempDir()}})
		if err != nil {
			t.Fatal(err)
		}
		return dec
	}
	if dec := decide(parent); dec.Allowed || !strings.Contains(dec.Reason, "coordinator may only plan") {
		t.Fatalf("coordinator host.exec: %+v", dec)
	}
	if dec := decide(child); !dec.Allowed {
		t.Fatalf("child did not inherit the grant: %+v", dec)
	}
	if err := d.StartAssignment(t.Context(), engine.StartRequest{ThreadID: parent, Workdir: t.TempDir()}); !errors.Is(err, ErrCoordinatorOnly) {
		t.Fatalf("engine on a coordinator: %v", err)
	}
}

// TestCoordinatorDispatchesBuilderChild proves C2, C3 and the first C5 test:
// create -> coordinator dispatch -> Builder child runs (SSE) -> gates -> child
// result -> parent awaiting_acceptance with the child's checks.
func TestCoordinatorDispatchesBuilderChild(t *testing.T) {
	worktree := passingWorktree(t)
	cfg, starts := loopConfig(t, worktree)
	d, base := serveGate(t, cfg)
	parent := grantedThread(t, d, base, "e2e", worktree)
	session := <-starts
	child := session.req.ThreadID
	if child == parent || session.req.Model != "m1" || session.req.Workdir != worktree || len(session.req.Instructions) != 1 ||
		session.req.Instructions[0].Text != "Gate scenario" {
		t.Fatalf("session request %+v", session.req)
	}
	waitPhase(t, d, child, "running")
	state := waitPhase(t, d, parent, "waiting_children")
	if len(state.Facts.Awaited) != 1 || state.Facts.Awaited[0] != child || state.Slot {
		t.Fatalf("parent %+v", state)
	}
	if len(eventsOf(t, d, parent, "assignment.dispatched")) != 0 || len(eventsOf(t, d, parent, "engine.output")) != 0 {
		t.Fatal("the coordinator ran work itself")
	}
	assigned := eventsOf(t, d, child, "child.assignment_recorded")
	if len(assigned) != 1 || assigned[0]["role"] != "builder" || assigned[0]["parent_thread_id"] != parent ||
		assigned[0]["purpose"] != "Gate scenario" || assigned[0]["role_reason"] == "" ||
		assigned[0]["input_revision"] != headRevision(t.Context(), worktree) ||
		len(assigned[0]["allowed_capabilities"].([]any)) == 0 || len(assigned[0]["expected_evidence"].([]any)) == 0 {
		t.Fatalf("assignment record %v", assigned)
	}
	if len(eventsOf(t, d, child, "audit.setting_recorded")) != 1 {
		t.Fatal("child audit setting not recorded")
	}
	if got, err := d.recorder.AssignmentByID(t.Context(), assigned[0]["assignment_id"].(string)); err != nil || got.ChildThreadID != child {
		t.Fatalf("assignment by ID: %+v, %v", got, err)
	}

	session.output(t, "hello over SSE")
	res := gateHTTP(t, "GET", base+"/v1/streams/thread:"+child+"/events", "", "", "")
	in := bufio.NewScanner(res.Body)
	for {
		kind, _, raw := readGateFrame(t, in)
		if kind == "engine.output" && strings.Contains(string(raw), "hello over SSE") {
			break
		}
	}

	session.events <- engine.EngineEvent{Channel: "exit"}
	state = waitPhase(t, d, parent, "awaiting_acceptance")
	if state.Checks["pass"] != "passed" || len(state.Checks) != 1 || state.Facts.Revision != headRevision(t.Context(), worktree) {
		t.Fatalf("parent checks %v at %q", state.Checks, state.Facts.Revision)
	}
	if s, err := d.threads.Status(t.Context(), child); err != nil || s.Phase != "completed" || s.Checks["pass"] != "passed" {
		t.Fatalf("child %+v, %v", s, err)
	}
	results := eventsOf(t, d, child, "child.result_recorded")
	if len(results) != 1 || results[0]["outcome"] != "completed" || results[0]["revision"] != state.Facts.Revision ||
		len(results[0]["checks"].(map[string]any)["passed"].([]any)) != 1 {
		t.Fatalf("result record %v", results)
	}
	// Card 401 C4: a child that ran under Direct shows that native actions can exceed journal coverage.
	if lim, _ := results[0]["limitations"].([]any); len(lim) != 1 || lim[0] != DirectLimitation {
		t.Fatalf("result limitations %v", results[0]["limitations"])
	}
}

// TestChildLaunchDeniedWithoutHostExecRule proves C5 test two: with the operator's grant but
// no host.exec rule policy denies the child's launch, nothing runs, the child ends
// failed and the parent shows the failure instead of success.
func TestChildLaunchDeniedWithoutHostExecRule(t *testing.T) {
	worktree := passingWorktree(t)
	cfg, _ := loopConfig(t, worktree)
	starts := make(chan *fakeSession, 8)
	writeGateFile(t, cfg.DataDir, "policy.json", `{"policy_version":3,"source":"operator","rules":[
		{"id":"dispatch","effect":"allow","actions":["assign.dispatch"]}]}`)
	var daemon atomic.Pointer[Daemon]
	cfg.Adapters = []func(engine.Sink) (engine.EngineAdapter, error){func(sink engine.Sink) (engine.EngineAdapter, error) {
		return &launchAdapter{fakeAdapter: fakeAdapter{sink: sink, next: starts}, daemon: &daemon}, nil
	}}
	d, base := serveGate(t, cfg)
	daemon.Store(d)
	parent := grantedThread(t, d, base, "denied", worktree)
	state := waitPhase(t, d, parent, "awaiting_acceptance")
	child := onlyChild(t, d, parent)
	if state.Checks["child:"+child] != "failed" || len(starts) != 0 {
		t.Fatalf("parent checks %v, starts %d", state.Checks, len(starts))
	}
	if s, err := d.threads.Status(t.Context(), child); err != nil || s.Phase != "failed" {
		t.Fatalf("child %+v, %v", s, err)
	}
	results := eventsOf(t, d, child, "child.result_recorded")
	if len(results) != 1 || results[0]["outcome"] != "failed" || !strings.Contains(results[0]["open_requirements"].([]any)[0].(string), "authorize Host access") {
		t.Fatalf("result record %v", results)
	}
	if _, err := os.Stat(filepath.Join(worktree, "marker")); err == nil || len(eventsOf(t, d, child, "process.exited")) != 0 {
		t.Fatal("a denied launch ran")
	}
}

// TestCancelParentCancelsChildFirst proves C4 and C5 test three: cancelling the
// parent mid-run stops the child's session and cancels the child (T30) before
// the parent, and the child's cancelled result is recorded.
func TestCancelParentCancelsChildFirst(t *testing.T) {
	worktree := passingWorktree(t)
	cfg, starts := loopConfig(t, worktree)
	d, base := serveGate(t, cfg)
	parent := grantedThread(t, d, base, "cancel", worktree)
	session := <-starts
	child := session.req.ThreadID
	waitPhase(t, d, child, "running")
	waitPhase(t, d, parent, "waiting_children")
	gateMutation(t, base+"/v1/threads/"+parent+"/commands", "cancel-parent", `{"command":"cancel"}`, 202)
	select {
	case <-session.stopped:
	default:
		t.Fatal("the child's process was not confirmed gone before the parent was cancelled")
	}
	waitPhase(t, d, child, "cancelled")
	waitPhase(t, d, parent, "cancelled")
	cancelSeq := func(id string) int64 {
		for _, e := range gateHistory(t, d, id) {
			if e.Type == "thread.state_changed" && strings.Contains(string(e.Payload), `"transition":"T30"`) {
				return e.GlobalSeq
			}
		}
		t.Fatalf("no T30 on %s", id)
		return 0
	}
	if cancelSeq(child) >= cancelSeq(parent) {
		t.Fatal("the parent was cancelled before its child")
	}
	waitGate(t, func() bool { return len(eventsOf(t, d, child, "child.result_recorded")) == 1 })
	if r := eventsOf(t, d, child, "child.result_recorded")[0]; r["outcome"] != "cancelled" {
		t.Fatalf("result %v", r)
	}
}

// TestFailedDependencyEndsChildFailed proves C4: a child whose sibling dependency
// did not complete ends failed with a recorded result instead of staying queued.
func TestFailedDependencyEndsChildFailed(t *testing.T) {
	cfg, _ := gateConfig(t)
	cfg.Projects = map[string]ProjectConfig{"local": {Dir: t.TempDir()}}
	d, base := serveGate(t, cfg)
	parent := gateThread(t, base, "dependency")
	first := children.Assignment{ParentThreadID: parent, ChildThreadID: eventlog.NewID(), AssignmentID: eventlog.NewID(),
		Purpose: "first", Role: "builder", RoleReason: "test", InputRevision: "r1", ExpectedEvidence: []string{"gates"}}
	second := first
	second.ChildThreadID, second.AssignmentID, second.Purpose = eventlog.NewID(), eventlog.NewID(), "second"
	second.Dependencies = []string{first.AssignmentID}
	for _, a := range []children.Assignment{first, second} {
		if err := d.children.CreateChild(t.Context(), a, thread.Facts{BriefVersion: 1, AuditSetting: "off"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.children.Cancel(t.Context(), first.ChildThreadID); err != nil {
		t.Fatal(err)
	}
	d.settleChildren() // records the cancelled result the dependency gate reads
	req, err := d.requestFor(t.Context(), second.ChildThreadID)
	if err != nil {
		t.Fatal(err)
	}
	err = d.StartAssignment(t.Context(), req)
	if !d.childDispatchFailed(t.Context(), second.ChildThreadID, err) {
		t.Fatalf("dispatch error %v", err)
	}
	if s, err := d.threads.Status(t.Context(), second.ChildThreadID); err != nil || s.Phase != "failed" {
		t.Fatalf("dependent child %+v, %v", s, err)
	}
	results := eventsOf(t, d, second.ChildThreadID, "child.result_recorded")
	if len(results) != 1 || results[0]["outcome"] != "failed" || !strings.Contains(results[0]["open_requirements"].([]any)[0].(string), "blocked by dependency") {
		t.Fatalf("result record %v", results)
	}
}

// TestChildDepthLimit proves C4: the configured Q-27 depth limit reaches the
// children package, so a child at the limit cannot delegate again.
func TestChildDepthLimit(t *testing.T) {
	cfg, _ := gateConfig(t)
	cfg.ChildLimits = children.Limits{MaxDepth: 1}
	d, base := serveGate(t, cfg)
	child := childOf(t, d, gateThread(t, base, "depth"))
	state, err := d.threads.Status(t.Context(), child)
	if err != nil {
		t.Fatal(err)
	}
	var depth *children.DepthError
	if _, err := d.delegate(t.Context(), state, "grandchild", "unversioned"); !errors.As(err, &depth) || depth.Depth != 2 {
		t.Fatalf("grandchild error %v", err)
	}
}

// grantedThread creates a thread that the dispatch loop cannot delegate yet (its
// project has no checkout), issues the operator's Full Control grant on it and
// then releases the checkout. A child inherits only grants issued before it exists.
func grantedThread(t *testing.T, d *Daemon, base, id, worktree string) string {
	t.Helper()
	d.mu.Lock()
	d.projects = nil
	d.mu.Unlock()
	parent := gateThread(t, base, id)
	grantWorker(t, d, parent)
	d.mu.Lock()
	d.projects = map[string]ProjectConfig{"local": {Dir: worktree}}
	d.mu.Unlock()
	return parent
}
