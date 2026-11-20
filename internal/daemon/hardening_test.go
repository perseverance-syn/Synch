package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"synch/internal/engine"
	"synch/internal/engine/kimi"
	"synch/internal/gates"
	"synch/internal/routing"
)

// hardenedGates writes a gate file into the worktree and commits it.
func hardenedGates(t *testing.T, dir, checks string) {
	t.Helper()
	writeGateFile(t, filepath.Join(dir, ".synch"), "gates.json", `{"version":1,"checks":[`+checks+`]}`)
	fixtureGit(t, dir, "add", ".")
	fixtureGit(t, dir, "commit", "-m", "gates")
}

// allowExec lets the operator policy authorise gate processes.
func allowExec(t *testing.T, cfg *Config) {
	t.Helper()
	writeGateFile(t, cfg.DataDir, "policy.json", `{"policy_version":3,"source":"operator","rules":[
		{"id":"run","effect":"allow","actions":["assign.dispatch","host.exec"]}]}`)
}

// grantWorker issues the operator's Full Control grant on this host for a thread,
// the state in which policy lets its engine and gate commands launch. The grant
// goes on a coordinator and its managed children inherit it.
func grantWorker(t *testing.T, d *Daemon, id string) {
	t.Helper()
	info := d.driver.Describe()
	if _, err := d.policy.IssueGrant(t.Context(), id, info.Host, info.Identity, "operator:gate"); err != nil {
		t.Fatal(err)
	}
}

// TestGatesDeniedByPolicyRunNothing proves H1: a host.exec denial makes the check
// `error` with the policy reason and the command never starts.
func TestGatesDeniedByPolicyRunNothing(t *testing.T) {
	worktree := gateWorktree(t)
	hardenedGates(t, worktree, `{"id":"mark","command":["git","init","marker"],"required":true}`)
	cfg, _ := loopConfig(t, worktree)
	cfg.noLoop = true
	writeGateFile(t, cfg.DataDir, "policy.json", `{"policy_version":3,"source":"operator","rules":[
		{"id":"dispatch","effect":"allow","actions":["assign.dispatch"]}]}`) // no host.exec rule
	d, base := serveGate(t, cfg)
	parent := gateThread(t, base, "deny")
	grantWorker(t, d, parent)
	id := childOf(t, d, parent)
	results := d.runGates(t.Context(), id, worktree, ProjectConfig{Dir: worktree}, nil, "rev")
	if len(results) != 1 || results[0].Status != gates.StatusError || !strings.Contains(results[0].Reason, "authorize Host access") {
		t.Fatalf("results %+v", results)
	}
	if _, err := os.Stat(filepath.Join(worktree, "marker")); err == nil {
		t.Fatal("denied gate command executed")
	}
	if len(eventsOf(t, d, id, "process.exited")) != 0 {
		t.Fatal("a process was supervised for a denied gate")
	}
}

// TestGatesRunUnderSupervisor proves H1: an allowed gate is a supervised process
// on the thread whose stdout and exit are persisted.
func TestGatesRunUnderSupervisor(t *testing.T) {
	worktree := gateWorktree(t)
	hardenedGates(t, worktree, `{"id":"ver","command":["go","version"],"required":true},
 {"id":"mark","command":["git","init","marker"],"required":true}`)
	cfg, _ := loopConfig(t, worktree)
	cfg.noLoop = true
	d, base := serveGate(t, cfg)
	parent := gateThread(t, base, "allow")
	grantWorker(t, d, parent)
	id := childOf(t, d, parent)
	results := d.runGates(t.Context(), id, worktree, ProjectConfig{Dir: worktree}, nil, "rev")
	if len(results) != 2 || results[0].Status != gates.StatusPassed || results[1].Status != gates.StatusPassed {
		t.Fatalf("results %+v", results)
	}
	if _, err := os.Stat(filepath.Join(worktree, "marker")); err != nil {
		t.Fatal("allowed gate command did not run")
	}
	exits := eventsOf(t, d, id, "process.exited")
	if len(exits) != 2 || exits[0]["execution_outcome"] != "exited" || exits[0]["descendants_cleaned"] != true {
		t.Fatalf("supervised exits %v", exits)
	}
	found := false
	for _, out := range eventsOf(t, d, id, "engine.output") {
		found = found || out["channel"] == "stdout" && strings.Contains(fmt.Sprint(out["chunk"]), "go version")
	}
	if !found {
		t.Fatal("gate stdout was not persisted")
	}
}

// TestGateTimeoutKillsSupervisedTree proves H1: a gate past its timeout is an
// `error` result and its supervised process is cancelled with the tree cleaned.
func TestGateTimeoutKillsSupervisedTree(t *testing.T) {
	t.Setenv("SYNCH_GATE_SLEEP", "1")
	worktree := gateWorktree(t)
	hardenedGates(t, worktree, fmt.Sprintf(`{"id":"sleep","command":[%q],"required":true,"timeout":1}`, filepath.ToSlash(os.Args[0])))
	cfg, _ := loopConfig(t, worktree)
	cfg.noLoop = true
	d, base := serveGate(t, cfg)
	parent := gateThread(t, base, "timeout")
	grantWorker(t, d, parent)
	id := childOf(t, d, parent)
	start := time.Now()
	results := d.runGates(t.Context(), id, worktree, ProjectConfig{Dir: worktree}, nil, "rev")
	if len(results) != 1 || results[0].Status != gates.StatusError || !strings.Contains(results[0].Reason, "timed out") || time.Since(start) > 20*time.Second {
		t.Fatalf("results %+v after %v", results, time.Since(start))
	}
	exits := eventsOf(t, d, id, "process.exited")
	if len(exits) != 1 || exits[0]["execution_outcome"] == "exited" || exits[0]["descendants_cleaned"] != true {
		t.Fatalf("supervised exits %v", exits)
	}
}

// TestKimiPairQualifies proves H2: the Kimi declaration is named kimi-code, and a
// qualified Kimi pair pinned as kimi-code is eligible under the daemon's version rule.
func TestKimiPairQualifies(t *testing.T) {
	caps := kimi.Declaration()
	if caps.Adapter != "kimi-code" {
		t.Fatalf("declared harness %q", caps.Adapter)
	}
	caps.OS, caps.Models = runtime.GOOS, []string{"k"}
	for op := range caps.Ops {
		caps.Ops[op] = engine.Capability{Status: engine.Qualified}
	}
	pair := routing.Pair{Harness: "kimi-code", Model: "k"}
	result := routing.Select(routing.Request{
		HostOS: runtime.GOOS, TaskKind: "coding", Default: pair, Pin: &pair,
		Candidates: []routing.Candidate{{Pair: pair, HarnessVersion: lowerBound(caps.HarnessVersions), Capabilities: caps}},
		Permitted:  func(routing.Pair) bool { return true },
	})
	if !result.Eligible() {
		t.Fatalf("kimi-code pair not eligible: %+v", result)
	}
}

// TestAuditSettlesVerifyingThread proves H3: with audit level1 the coordinator waits in
// verifying after the child's checks, and the audit.status event moves it to
// awaiting_acceptance by T26.
func TestAuditSettlesVerifyingThread(t *testing.T) {
	worktree := passingWorktree(t)
	cfg, starts := loopConfig(t, worktree)
	d, base := serveGate(t, cfg)
	d.mu.Lock()
	d.projects = nil // hold delegation until the grant exists
	d.mu.Unlock()
	id := gateMutation(t, base+"/v1/threads", "audit", `{"project_id":"local","brief":"Audit","audit":"level1"}`, 201)["thread_id"].(string)
	grantWorker(t, d, id)
	d.mu.Lock()
	d.projects = map[string]ProjectConfig{"local": {Dir: worktree}}
	d.mu.Unlock()
	session := <-starts
	session.events <- engine.EngineEvent{Channel: "exit"}
	waitPhase(t, d, id, "verifying")
	waitGate(t, func() bool { // the child's check is reported while the audit is still unsettled
		s, err := d.threads.Status(t.Context(), id)
		return err == nil && len(s.Checks) == 1
	})
	time.Sleep(100 * time.Millisecond)
	state, err := d.threads.Status(t.Context(), id)
	if err != nil || state.Phase != "verifying" {
		t.Fatalf("thread left verifying before the audit settled: %+v, %v", state, err)
	}
	if err := d.record(id, "audit.status", map[string]any{"revision": state.Facts.Revision, "status": "pass"}, ""); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, d, id, "awaiting_acceptance")
}

// TestTransientDispatchFailureRetries proves H5: a failure that is neither a routing
// pause nor a policy denial is retried with backoff until it succeeds.
func TestTransientDispatchFailureRetries(t *testing.T) {
	worktree := passingWorktree(t)
	cfg, starts := loopConfig(t, worktree)
	cfg.Projects = nil // no checkout yet: delegation fails for a fixable reason
	cfg.retryBase = 20 * time.Millisecond
	d, base := serveGate(t, cfg)
	id := gateThread(t, base, "retry")
	waitGate(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.failed[id] != nil && d.failed[id].attempts >= 2
	})
	grantWorker(t, d, id) // the checkout is fixed next, and delegation needs the operator's grant
	d.mu.Lock()
	d.projects = map[string]ProjectConfig{"local": {Dir: worktree}}
	d.mu.Unlock()
	if session := <-starts; session.req.Workdir != worktree {
		t.Fatalf("retried with %q", session.req.Workdir)
	}
	waitPhase(t, d, id, "waiting_children")
}

// TestPolicyDenyIsNotRetried proves H5 for children: a denied dispatch is attempted
// once, the child ends failed and the loop never retries it.
func TestPolicyDenyIsNotRetried(t *testing.T) {
	worktree := passingWorktree(t)
	cfg, starts := loopConfig(t, worktree)
	cfg.retryBase = 5 * time.Millisecond
	writeGateFile(t, cfg.DataDir, "policy.json", `{"policy_version":3,"source":"operator","rules":[
		{"id":"no","effect":"deny","actions":["assign.dispatch"]}]}`)
	d, base := serveGate(t, cfg)
	id := grantedThread(t, d, base, "deny-dispatch", worktree)
	waitPhase(t, d, id, "awaiting_acceptance")
	child := onlyChild(t, d, id)
	decided := len(eventsOf(t, d, child, "policy.decided"))
	time.Sleep(300 * time.Millisecond) // ~15 poll ticks and ~60 retry delays
	if got := len(eventsOf(t, d, child, "policy.decided")); got != decided || decided == 0 || len(starts) != 0 {
		t.Fatalf("policy decisions %d -> %d, starts %d", decided, got, len(starts))
	}
	if state, err := d.threads.Status(t.Context(), child); err != nil || state.Phase != "failed" {
		t.Fatalf("denied child %+v, %v", state, err)
	}
}

// passingWorktree is gateWorktree with a gate file whose one check passes.
func passingWorktree(t *testing.T) string {
	t.Helper()
	dir := gateWorktree(t)
	hardenedGates(t, dir, `{"id":"pass","command":["go","version"],"required":true}`)
	return dir
}

// onlyChild finds the single managed child of a parent.
func onlyChild(t *testing.T, d *Daemon, parent string) string {
	t.Helper()
	ids, err := d.threadsIn(t.Context(), "managed", "draft", "queued", "running", "waiting_children", "paused", "verifying",
		"awaiting_acceptance", "awaiting_decision", "completed", "failed", "cancelled")
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, id := range ids {
		if s, err := d.threads.Status(t.Context(), id); err == nil && s.Facts.ManagedParent == parent {
			found = append(found, id)
		}
	}
	if len(found) != 1 {
		t.Fatalf("children of %s: %v", parent, found)
	}
	return found[0]
}
