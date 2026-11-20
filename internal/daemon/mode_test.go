package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"synch/internal/engine"
	"synch/internal/eventlog"
	"synch/internal/execdriver"
)

type modeSandbox struct {
	execdriver.ExecutionDriver
}

// Kind identifies the configured fake sandbox.
func (modeSandbox) Kind() string { return execdriver.KindSandbox }

// Describe declares the tested fixture configuration.
func (modeSandbox) Describe() execdriver.DriverInfo {
	return execdriver.DriverInfo{Label: "Sandbox", CertifiedConfigs: []string{"test-filesystem-credentials-network"}}
}

// Settle confirms no fake Sandbox processes remain.
func (modeSandbox) Settle(context.Context, string) (execdriver.SettleReport, error) {
	return execdriver.SettleReport{HostProcessesStopped: true}, nil
}

// Reopen allows this fixture to be reused by another switch.
func (modeSandbox) Reopen(string) error { return nil }

// TestModeSleeper provides a portable long-running subprocess for M5.
func TestModeSleeper(t *testing.T) {
	if os.Getenv("SYNCH_TEST_MODE_SLEEPER") == "1" {
		time.Sleep(time.Minute)
	}
}

// modeHTTP sends a change_mode command using the authenticated operator.
func modeHTTP(t *testing.T, d *Daemon, base, id, request, to string, status int) string {
	t.Helper()
	body := fmt.Sprintf(`{"command":"change_mode","target":%q,"to":%q}`, d.driver.Describe().Host, to)
	res := gateHTTP(t, "POST", base+"/v1/threads/"+id+"/commands", request, body, "")
	raw := readBody(t, res)
	if res.StatusCode != status {
		t.Fatalf("switch %s: %d %s", to, res.StatusCode, raw)
	}
	return raw
}

// startModeChild constructs a live Builder and captures its actual daemon-managed session.
func startModeChild(t *testing.T, sandbox SandboxDriver) (*Daemon, string, string, *fakeSession, string) {
	t.Helper()
	dir := passingWorktree(t)
	cfg, starts := loopConfig(t, dir)
	cfg.noLoop = true
	cfg.Sandbox = sandbox
	allowExec(t, &cfg)
	d, base := serveGate(t, cfg)
	parent := gateThread(t, base, "mode-parent")
	grantWorker(t, d, parent)
	child := childOf(t, d, parent)
	if err := d.StartAssignment(t.Context(), engine.StartRequest{ThreadID: child, Workdir: dir}); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, d, child, "running")
	return d, base, parent, <-starts, dir
}

// launchModeProcess starts a real long-running host tree through policy and the supervisor.
func launchModeProcess(t *testing.T, d *Daemon, id, dir string) execdriver.Proc {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p, err := (policyDriver{ExecutionDriver: d.driver, daemon: d}).Spawn(t.Context(), execdriver.ProcSpec{
		ID: eventlog.NewID(), ThreadID: id, Dir: dir, Path: binary, Args: []string{"-test.run=^TestModeSleeper$"},
		Env: append(os.Environ(), "SYNCH_TEST_MODE_SLEEPER=1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// modeEventSeq returns the global position of a mode event with the requested phase.
func modeEventSeq(t *testing.T, d *Daemon, id, phase string) int64 {
	t.Helper()
	for _, e := range modeEvents(t, d, id, "mode.changed") {
		var p struct {
			Phase, Actor, Target, From, To string
			Stopped                        bool `json:"host_processes_stopped"`
		}
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			t.Fatal(err)
		}
		if p.Phase == phase {
			if p.Actor != "operator:gate" || p.Target != d.driver.Describe().Host || p.From == p.To || e.TypeVersion != 2 || e.RecordedAt.IsZero() ||
				(phase == "active" && p.To == sandboxMode && !p.Stopped) {
				t.Fatalf("invalid mode event: %+v", e)
			}
			return e.GlobalSeq
		}
	}
	return 0
}

// TestModeSwitchSettlesAndRevokes proves M1, M2, M4 and M5 through the daemon's HTTP and supervisor paths.
func TestModeSwitchSettlesAndRevokes(t *testing.T) {
	d, base, parent, session, dir := startModeChild(t, modeSandbox{})
	child := session.req.ThreadID
	p := launchModeProcess(t, d, child, dir)
	// The interrupted assignment has a previously classified restart point.
	if err := d.record(child, "process.exited", map[string]any{"assignment_id": session.req.AssignmentID,
		"work": "resumable", "execution_outcome": "killed", "descendants_cleaned": true}, ""); err != nil {
		t.Fatal(err)
	}
	modeHTTP(t, d, base, parent, "downgrade", sandboxMode, 202)
	select {
	case <-session.stopped:
	default:
		t.Fatal("Sandbox activated with an open session")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	exit, err := p.Wait(ctx)
	if err != nil || !exit.DescendantsCleaned || (exit.ExecutionOutcome != "killed" && exit.ExecutionOutcome != "cancelled") {
		t.Fatalf("Host tree exit: %+v, %v", exit, err)
	}
	active := modeEventSeq(t, d, parent, "active")
	if active == 0 || modeEventSeq(t, d, child, "active") == 0 {
		t.Fatal("missing active event")
	}
	revoked := modeEvents(t, d, parent, "grant.revoked")
	if len(revoked) != 2 || revoked[1].GlobalSeq >= active ||
		!strings.Contains(string(revoked[1].Payload), child) || !strings.Contains(string(revoked[1].Payload), `"settled":true`) {
		t.Fatalf("grant not fully revoked for the subtree before activation: %+v", revoked)
	}
	var paused, released int64
	for _, e := range modeEvents(t, d, child, "thread.state_changed") {
		if strings.Contains(string(e.Payload), `"T18"`) {
			paused = e.GlobalSeq
		}
		if strings.Contains(string(e.Payload), `"T21"`) {
			released = e.GlobalSeq
		}
	}
	if paused == 0 || paused >= revoked[0].GlobalSeq || released <= active {
		t.Fatalf("transition ordering T18=%d revoke=%d active=%d T21=%d", paused, revoked[0].GlobalSeq, active, released)
	}
	var processExit int64
	for _, event := range modeEvents(t, d, child, "process.exited") {
		if strings.Contains(string(event.Payload), p.ID()) && strings.Contains(string(event.Payload), `"descendants_cleaned":true`) {
			processExit = event.GlobalSeq
		}
	}
	if processExit <= paused || processExit >= active {
		t.Fatalf("process cleanup ordering T18=%d exit=%d active=%d", paused, processExit, active)
	}
	waitPhase(t, d, child, "queued")

	if raw := modeHTTP(t, d, base, parent, "upgrade-refused", hostMode, 403); !strings.Contains(raw, "grant_required") {
		t.Fatal(raw)
	}
	grantWorker(t, d, parent)
	modeHTTP(t, d, base, parent, "upgrade", hostMode, 202)
	info := d.driver.Describe()
	want := "synch grants create " + child + " --host " + info.Host + " --identity " + info.Identity
	response := gateHTTP(t, "GET", base+"/v1/threads/"+child, "", "", "")
	raw := readBody(t, response)
	var view struct {
		ID, Role, State string
		Project         string `json:"project_id"`
		Depth           int
		WaitingFor      string `json:"waiting_for"`
	}
	if err := json.Unmarshal([]byte(raw), &view); err != nil || response.StatusCode != 200 ||
		view.ID != child || view.Role != "builder" || view.State != "queued" ||
		view.Project == "" || view.Depth != 1 || view.WaitingFor != "waiting for an operator grant: run `"+want+"`" {
		t.Fatalf("managed child view: status %d, body %s, decode error %v", response.StatusCode, raw, err)
	}
	if err := d.StartAssignment(t.Context(), engine.StartRequest{ThreadID: child, Workdir: dir}); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, d, child, "queued")
	gateMutation(t, base+"/v1/threads/"+child+"/grants", "grant-existing-child",
		fmt.Sprintf(`{"host":%q,"identity":%q}`, info.Host, info.Identity), 201)
	if err := d.StartAssignment(t.Context(), engine.StartRequest{ThreadID: child, Workdir: dir}); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, d, child, "running")
	reopened := launchModeProcess(t, d, child, dir)
	cleanup, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	if err := reopened.Cancel(cleanup); err != nil {
		t.Fatal(err)
	}
}

// TestModeSwitchRefusesUnavailableSandbox proves M3 and M5 without revoking or pausing anything.
func TestModeSwitchRefusesUnavailableSandbox(t *testing.T) {
	d, base, parent, session, _ := startModeChild(t, nil)
	if raw := modeHTTP(t, d, base, parent, "sandbox-missing", sandboxMode, 409); !strings.Contains(raw, "sandbox_unavailable") {
		t.Fatal(raw)
	}
	waitPhase(t, d, session.req.ThreadID, "running")
	if len(modeEvents(t, d, parent, "grant.revoked")) != 0 || len(modeEvents(t, d, parent, "mode.changed")) != 0 {
		t.Fatal("refusal mutated the mode or grants")
	}
}

// TestModeSwitchRevocationFailureStaysIncomplete proves M5 when the policy completion event fails.
func TestModeSwitchRevocationFailureStaysIncomplete(t *testing.T) {
	d, base, parent, session, _ := startModeChild(t, modeSandbox{})
	if err := d.store.Write(t.Context(), func(tx *eventlog.Transaction) error {
		_, err := tx.ExecContext(t.Context(), `CREATE TRIGGER fail_mode_revoke BEFORE INSERT ON events
			WHEN json_extract(CAST(NEW.envelope AS TEXT),'$.type')='grant.revoked'
			AND json_extract(CAST(NEW.envelope AS TEXT),'$.payload.settled')=1
			BEGIN SELECT RAISE(ABORT,'injected revocation failure'); END`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	modeHTTP(t, d, base, parent, "revoke-failure", sandboxMode, 202)
	if s := waitPhase(t, d, session.req.ThreadID, "paused"); s.Reason != "mode_switch" {
		t.Fatalf("pause: %+v", s)
	}
	if len(modeEvents(t, d, parent, "grant.revoked")) != 1 || modeEventSeq(t, d, parent, "active") != 0 {
		t.Fatal("failed revocation did not leave an incomplete switch")
	}
	if err := d.store.Write(t.Context(), func(tx *eventlog.Transaction) error {
		_, err := tx.ExecContext(t.Context(), "DROP TRIGGER fail_mode_revoke")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	modeHTTP(t, d, base, parent, "revoke-retry", sandboxMode, 202)
	if modeEventSeq(t, d, parent, "active") == 0 {
		t.Fatal("retry did not finish settlement")
	}
}

// modeEvents reads raw envelopes in stream order for lifecycle ordering assertions.
func modeEvents(t *testing.T, d *Daemon, id, kind string) []eventlog.Event {
	t.Helper()
	events, err := d.store.ReadStream("thread:"+id, 0, 1024)
	if err != nil {
		t.Fatal(err)
	}
	var selected []eventlog.Event
	for _, e := range events {
		if e.Type == kind {
			selected = append(selected, e)
		}
	}
	return selected
}
