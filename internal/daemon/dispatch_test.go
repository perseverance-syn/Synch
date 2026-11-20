package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh/agent"

	"synch/internal/engine"
	"synch/internal/engine/claudecode"
	"synch/internal/policy"
	"synch/internal/remote"
	"synch/internal/thread"
)

const threadBody = `{"project_id":"local","brief":"Dispatch scenario","audit":"off"%s}`

// loopConfig enables the real dispatch loop with the test-qualified fake engine.
func loopConfig(t *testing.T, worktree string) (Config, <-chan *fakeSession) {
	t.Helper()
	cfg, starts := gateConfig(t)
	cfg.noLoop = false
	cfg.poll = 20 * time.Millisecond
	cfg.Projects = map[string]ProjectConfig{"local": {Dir: worktree}}
	allowExec(t, &cfg) // gate commands are policy-checked host.exec launches
	return cfg, starts
}

// gateWorktree makes a committed checkout whose gate file has one passing and one failing check.
func gateWorktree(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	fixtureGit(t, dir, "init", "--initial-branch=main")
	fixtureGit(t, dir, "config", "user.name", "Dispatch Test")
	fixtureGit(t, dir, "config", "user.email", "dispatch@synch.local")
	fixtureGit(t, dir, "config", "commit.gpgsign", "false")
	writeGateFile(t, dir, "README.md", "work\n")
	if err := os.Mkdir(filepath.Join(dir, ".synch"), 0700); err != nil {
		t.Fatal(err)
	}
	writeGateFile(t, filepath.Join(dir, ".synch"), "gates.json", `{"version":1,"checks":[
 {"id":"pass","command":["go","version"],"required":false},
 {"id":"fail","command":["go","tool","no-such-tool-xyz"],"required":false}]}`)
	fixtureGit(t, dir, "add", ".")
	fixtureGit(t, dir, "commit", "-m", "work")
	return dir
}

// childOf delegates a Builder child of the parent through the coordinator's own path.
func childOf(t *testing.T, d *Daemon, parent string) string {
	t.Helper()
	state, err := d.threads.Status(t.Context(), parent)
	if err != nil {
		t.Fatal(err)
	}
	id, err := d.delegate(t.Context(), state, "Gate scenario", "unversioned")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// waitPhase waits for the thread to reach a phase and returns its state.
func waitPhase(t *testing.T, d *Daemon, id, phase string) thread.State {
	t.Helper()
	var state thread.State
	waitGate(t, func() bool {
		var err error
		state, err = d.threads.Status(t.Context(), id)
		return err == nil && state.Phase == phase
	})
	return state
}

// eventsOf returns the persisted payloads of one event type on a thread, in order.
func eventsOf(t *testing.T, d *Daemon, id, kind string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, event := range gateHistory(t, d, id) {
		if event.Type != kind {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		out = append(out, payload)
	}
	return out
}

// TestShippedDeclarationsPause proves D1: with every shipped operation untested a
// real daemon pauses the thread with no_eligible_pair and dispatches nothing.
func TestShippedDeclarationsPause(t *testing.T) {
	cfg, _ := loopConfig(t, t.TempDir())
	cfg.adapter = nil
	cfg.CodexBinary = filepath.Join(cfg.DataDir, "absent-codex")
	cfg.ClaudeBinary = filepath.Join(cfg.DataDir, "absent-claude")
	cfg.KimiBinary = filepath.Join(cfg.DataDir, "absent-kimi")
	d, base := serveGate(t, cfg)
	id := gateThread(t, base, "shipped")
	state := waitPhase(t, d, id, "paused")
	if state.Reason != "no_eligible_pair" || len(eventsOf(t, d, id, "assignment.dispatched")) != 0 {
		t.Fatalf("state %s/%s, dispatched %v", state.Phase, state.Reason, eventsOf(t, d, id, "assignment.dispatched"))
	}
}

// TestPinnedUnavailableHarnessPauses proves D5: an operator pin that is not offered
// pauses the thread; routing never falls back to the qualified default.
func TestPinnedUnavailableHarnessPauses(t *testing.T) {
	cfg, starts := loopConfig(t, t.TempDir())
	d, base := serveGate(t, cfg)
	body := strings.Replace(threadBody, "%s", `,"pin":{"harness":"claude-code","model":"opus"}`, 1)
	id := gateMutation(t, base+"/v1/threads", "pinned", body, 201)["thread_id"].(string)
	state := waitPhase(t, d, id, "paused")
	if state.Reason != "no_eligible_pair" || len(eventsOf(t, d, id, "assignment.dispatched")) != 0 || len(starts) != 0 {
		t.Fatalf("state %s/%s, started %d", state.Phase, state.Reason, len(starts))
	}
}

type failingAdapter struct{ fakeAdapter }

// Start always reports a harness failure.
func (failingAdapter) Start(context.Context, engine.StartRequest) (engine.Session, error) {
	return nil, &engine.HarnessError{Operation: "launch", Cause: errors.New("binary crashed")}
}

// TestHarnessStartFailurePauses proves D2: a harness that cannot start is T15.
func TestHarnessStartFailurePauses(t *testing.T) {
	cfg, _ := gateConfig(t)
	cfg.adapter = func(engine.Sink) (engine.EngineAdapter, error) { return &failingAdapter{}, nil }
	d, base := serveGate(t, cfg)
	id := childOf(t, d, gateThread(t, base, "failing"))
	err := d.StartAssignment(t.Context(), engine.StartRequest{ThreadID: id, Workdir: t.TempDir()})
	var harnessErr *engine.HarnessError
	if !errors.As(err, &harnessErr) {
		t.Fatalf("start error %v", err)
	}
	state, err := d.threads.Status(t.Context(), id)
	if err != nil || state.Phase != "paused" || state.Reason != "harness_unavailable" {
		t.Fatalf("state %+v, %v", state, err)
	}
}

type stubBroker struct{}

// Agent refuses authentication because the runner is never contacted.
func (stubBroker) Agent(context.Context, remote.Target) (agent.Agent, error) {
	return nil, errors.New("offline")
}

// ForwardingAgent refuses forwarding.
func (stubBroker) ForwardingAgent(context.Context, remote.Target, string) (agent.Agent, error) {
	return nil, errors.New("offline")
}

// TestRemoteDisconnectedMapsToT19 proves D4: the remote driver registers under the
// remote kind and a disconnected runner pauses a new assignment (T19).
func TestRemoteDisconnectedMapsToT19(t *testing.T) {
	registry := &remote.Registry{}
	if err := registry.Register(remote.Target{Runner: "runner-1", Host: "127.0.0.1", Port: 22, User: "synch",
		Operations: []string{remote.OpSpawn}}); err != nil {
		t.Fatal(err)
	}
	cfg, _ := gateConfig(t)
	cfg.Remote = &RemoteConfig{Registry: registry, Runner: "runner-1", Broker: stubBroker{}}
	d, base := serveGate(t, cfg)
	if d.Driver("remote") == nil || d.Driver("direct") == nil || d.Driver("sandbox") != nil {
		t.Fatal("remote driver kind was not registered")
	}
	id := childOf(t, d, gateThread(t, base, "remote"))
	if err := d.threads.Update(t.Context(), id, func(f *thread.Facts) error { f.RunnerID = "runner-1"; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := d.StartAssignment(t.Context(), engine.StartRequest{ThreadID: id, Workdir: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	state, err := d.threads.Status(t.Context(), id)
	if err != nil || state.Phase != "paused" || state.Reason != "runner_disconnected" {
		t.Fatalf("state %+v, %v", state, err)
	}
	if len(eventsOf(t, d, id, "assignment.dispatched")) != 0 {
		t.Fatal("dispatched to a disconnected runner")
	}
}

// TestNoRemoteDriverByDefault proves D4: without configuration there is no remote driver.
func TestNoRemoteDriverByDefault(t *testing.T) {
	cfg, _ := gateConfig(t)
	d, _ := serveGate(t, cfg)
	if d.Driver("remote") != nil {
		t.Fatal("remote driver registered without configuration")
	}
}

// TestToolPermissionsFailClosed proves D2: Claude Code tool prompts follow policy,
// and anything policy does not allow is refused.
func TestToolPermissionsFailClosed(t *testing.T) {
	for _, allow := range []bool{false, true} {
		cfg, _ := gateConfig(t)
		if allow {
			writeGateFile(t, cfg.DataDir, "policy.json", `{"policy_version":3,"source":"operator","rules":[
		{"id":"tool","effect":"allow","actions":["tool.use"]}]}`)
		}
		d, base := serveGate(t, cfg)
		id := gateThread(t, base, "tool")
		if allow {
			// A coordinator can never use tools (dec. 05); a builder child can when a rule allows it.
			var err error
			if id, err = d.CreateWritingThread(t.Context(), id); err != nil {
				t.Fatal(err)
			}
		}
		request := claudecode.PermissionRequest{Context: t.Context(), ThreadID: id, ToolName: "Bash"}
		if got := d.toolAllowed(request); got != allow {
			dec, err := d.policy.Decide(t.Context(), policy.Request{Thread: id, Action: "tool.use", Target: "Bash", Machine: d.driver.Describe().Host})
			t.Fatalf("allow rule=%v: tool allowed=%v (%+v, %v)", allow, got, dec, err)
		}
		if got := d.toolAllowed(claudecode.PermissionRequest{Context: t.Context(), ThreadID: "missing", ToolName: "Bash"}); got {
			t.Fatal("unknown thread was allowed")
		}
	}
}
