package daemon

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"synch/internal/engine"
	"synch/internal/thread"
)

// watchedChild starts the real daemon with a fake engine and returns the running
// child's session and ID, so a test can drive its event stream.
func watchedChild(t *testing.T, tune func(*Config)) (*Daemon, *fakeSession, string) {
	t.Helper()
	worktree := passingWorktree(t)
	cfg, starts := loopConfig(t, worktree)
	if tune != nil {
		tune(&cfg)
	}
	d, base := serveGate(t, cfg)
	grantedThread(t, d, base, "watchdog", worktree)
	session := <-starts
	waitPhase(t, d, session.req.ThreadID, "running")
	return d, session, session.req.ThreadID
}

// pausedWithReason waits for the child's T15 and checks that the reason is on its stream
// and in the failure checkpoint.
func pausedWithReason(t *testing.T, d *Daemon, child, reason string) {
	t.Helper()
	waitPhase(t, d, child, "paused")
	var state thread.State
	waitGate(t, func() bool { // the checkpoint follows T15 asynchronously
		state, _ = d.threads.Status(t.Context(), child)
		return state.Facts.CheckpointID != ""
	})
	if state.Reason != "harness_unavailable" || state.Facts.CheckpointID == "" || state.Slot {
		t.Fatalf("child %+v", state)
	}
	found := false
	for _, out := range eventsOf(t, d, child, "engine.output") {
		found = found || out["channel"] == "stderr" && strings.Contains(fmt.Sprint(out["chunk"]), reason)
	}
	if !found {
		t.Fatalf("reason %q not on the stream: %v", reason, eventsOf(t, d, child, "engine.output"))
	}
}

// TestWatchdogRuntimeBudget proves W1 and W4a: past its runtime budget the child's
// session is stopped and the child pauses by T15 with the budget as the reason.
func TestWatchdogRuntimeBudget(t *testing.T) {
	d, session, child := watchedChild(t, func(c *Config) { c.ChildMaxRuntime = "300ms" })
	pausedWithReason(t, d, child, "runtime budget of 300ms exceeded")
	select {
	case <-session.stopped:
	default:
		t.Fatal("the session was not stopped")
	}
	if exits := eventsOf(t, d, child, "process.exited"); len(exits) != 1 || exits[0]["work"] != "lost" || exits[0]["execution_outcome"] != "killed" {
		t.Fatalf("exit records %v", exits)
	}
}

// TestWatchdogLoopDetected proves W2 and W4b: five identical tool calls in a row stop
// the session and pause the child by T15 naming the loop.
func TestWatchdogLoopDetected(t *testing.T) {
	d, session, child := watchedChild(t, nil)
	for range 5 {
		session.events <- engine.EngineEvent{Channel: "tool_call", Chunk: `{"cmd":"ls"}`}
	}
	pausedWithReason(t, d, child, "loop detected: 5 identical tool calls")
	select {
	case <-session.stopped:
	default:
		t.Fatal("the session was not stopped")
	}
}

// TestWatchdogIgnoresInterleavedCalls proves W4c: identical tool calls with other
// output in between, or varying payloads, are not a loop.
func TestWatchdogIgnoresInterleavedCalls(t *testing.T) {
	d, session, child := watchedChild(t, nil)
	call := func(body string) { session.events <- engine.EngineEvent{Channel: "tool_call", Chunk: body} }
	for range 4 {
		call("a")
	}
	session.events <- engine.EngineEvent{Channel: "message", Chunk: "thinking"}
	for range 4 {
		call("a")
	}
	call("b")
	for range 3 {
		call("a")
	}
	time.Sleep(300 * time.Millisecond)
	if state, err := d.threads.Status(t.Context(), child); err != nil || state.Phase != "running" {
		t.Fatalf("child %+v, %v", state, err)
	}
	select {
	case <-session.stopped:
		t.Fatal("a healthy session was stopped")
	default:
	}
}

// TestSupervisorStallPausesChild proves W3 and W4d: a stalled exit reported by the
// supervisor (an exit event carrying the stall error) lands the child in T15
// with that reason, not in a failed state.
func TestSupervisorStallPausesChild(t *testing.T) {
	d, session, child := watchedChild(t, nil)
	session.events <- engine.EngineEvent{Channel: "exit", ExitCode: -1, Err: errors.New("process stalled: no output for 10m0s")}
	pausedWithReason(t, d, child, "stalled: no output for 10m0s")
	if exits := eventsOf(t, d, child, "process.exited"); len(exits) != 1 || exits[0]["execution_outcome"] != "stalled" || exits[0]["work"] != "lost" {
		t.Fatalf("exit records %v", exits)
	}
}

// TestWatchPrecedence proves project > daemon default > built-in (2h, 5) and the off switch.
func TestWatchPrecedence(t *testing.T) {
	project := func(runtime, repeats Setting) map[string]ProjectConfig {
		return map[string]ProjectConfig{"p": {MaxRuntime: runtime, LoopRepeats: repeats}}
	}
	for _, c := range []struct {
		name string
		cfg  Config
		want watchLimits
	}{
		{"built-in", Config{Projects: project("", "")}, watchLimits{2 * time.Hour, 5}},
		{"default wins over built-in", Config{ChildMaxRuntime: "45m", ChildLoopRepeats: "8", Projects: project("", "")}, watchLimits{45 * time.Minute, 8}},
		{"project wins over default", Config{ChildMaxRuntime: "45m", ChildLoopRepeats: "8", Projects: project("10m", "3")}, watchLimits{10 * time.Minute, 3}},
		{"default off", Config{ChildMaxRuntime: "off", ChildLoopRepeats: "0", Projects: project("", "")}, watchLimits{0, 0}},
		{"project off beats default", Config{ChildMaxRuntime: "45m", ChildLoopRepeats: "8", Projects: project("off", "off")}, watchLimits{0, 0}},
		{"project on beats default off", Config{ChildMaxRuntime: "off", Projects: project("30m", "")}, watchLimits{30 * time.Minute, 5}},
	} {
		_, projects, err := resolveWatch(c.cfg)
		if err != nil || projects["p"] != c.want {
			t.Errorf("%s: got %+v, %v want %+v", c.name, projects["p"], err, c.want)
		}
	}
}

// TestInvalidWatchSettingsFailStartup proves a bad default or project value stops the daemon with a clear error.
func TestInvalidWatchSettingsFailStartup(t *testing.T) {
	for name, tune := range map[string]func(*Config){
		"runtime":         func(c *Config) { c.ChildMaxRuntime = "soon" },
		"negative":        func(c *Config) { c.ChildMaxRuntime = "-5m" },
		"repeats":         func(c *Config) { c.ChildLoopRepeats = "1" },
		"project runtime": func(c *Config) { c.Projects = map[string]ProjectConfig{"p": {MaxRuntime: "2 hours"}} },
		"project repeats": func(c *Config) { c.Projects = map[string]ProjectConfig{"p": {LoopRepeats: "many"}} },
	} {
		cfg, _ := gateConfig(t)
		tune(&cfg)
		if d, err := New(cfg); err == nil || !strings.Contains(err.Error(), "must be") {
			if d != nil {
				_ = d.Close()
			}
			t.Errorf("%s: startup error %v", name, err)
		}
	}
}

// TestProjectBudgetOverridesDefault proves the override in a running daemon: the
// default is off, the project's 300ms budget still stops the child.
func TestProjectBudgetOverridesDefault(t *testing.T) {
	d, session, child := watchedChild(t, func(c *Config) {
		c.ChildMaxRuntime = "off"
		p := c.Projects["local"]
		p.MaxRuntime = "300ms"
		c.Projects["local"] = p
	})
	pausedWithReason(t, d, child, "runtime budget of 300ms exceeded")
	<-session.stopped
}

// TestBudgetOffNeverStops proves the off switch: with the budget off and no project
// override a child outlives what would have been a short budget.
func TestBudgetOffNeverStops(t *testing.T) {
	d, session, child := watchedChild(t, func(c *Config) { c.ChildMaxRuntime = "off" })
	time.Sleep(500 * time.Millisecond)
	if state, err := d.threads.Status(t.Context(), child); err != nil || state.Phase != "running" {
		t.Fatalf("child %+v, %v", state, err)
	}
	select {
	case <-session.stopped:
		t.Fatal("stopped with the budget off")
	default:
	}
}
