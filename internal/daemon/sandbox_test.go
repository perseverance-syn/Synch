package daemon

import (
	"context"
	"strings"
	"testing"
	"time"

	"synch/internal/eventlog"
	"synch/internal/execdriver"
	"synch/internal/sandbox"
	"synch/internal/sandbox/sandboxtest"
)

// qualifiedSandbox builds the real Sandbox driver on a fake provider and runs its boundary checks.
func qualifiedSandbox(t *testing.T, driver string, inspect bool) (*sandbox.Driver, *sandboxtest.Setup) {
	t.Helper()
	s, err := sandboxtest.NewSetup(driver)
	if err != nil {
		t.Fatal(err)
	}
	if !inspect {
		s.Config.Inspect = nil
	}
	sbx, err := sandbox.New(s.Config)
	if err != nil {
		t.Fatal(err)
	}
	sbx.Qualify(t.Context())
	return sbx, s
}

// TestSandboxModeEndToEnd proves 406 B3: change_mode to Sandbox settles Host work, revokes grants,
// confirms host processes stopped, then runs work in a fresh VM; the downgrade needs a fresh grant and destroys the VM.
func TestSandboxModeEndToEnd(t *testing.T) {
	sbx, s := qualifiedSandbox(t, "proxmox", true)
	d, base, parent, session, dir := startModeChild(t, sbx)
	child := session.req.ThreadID
	host := launchModeProcess(t, d, child, dir)

	modeHTTP(t, d, base, parent, "to-sandbox", sandboxMode, 202)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if exit, err := host.Wait(ctx); err != nil || !exit.DescendantsCleaned {
		t.Fatalf("Host tree not stopped before Sandbox: %+v %v", exit, err)
	}
	active := modeEventSeq(t, d, parent, "active") // also asserts host_processes_stopped on to:sandbox
	revoked := modeEvents(t, d, parent, "grant.revoked")
	if active == 0 || len(revoked) == 0 || revoked[len(revoked)-1].GlobalSeq >= active {
		t.Fatalf("grants not revoked before Sandbox active: active=%d revoked=%d", active, len(revoked))
	}

	// Work now goes through the daemon's admission path into a disposable VM.
	p, err := (policyDriver{ExecutionDriver: d.driver, daemon: d}).Spawn(t.Context(), execdriver.ProcSpec{
		ID: eventlog.NewID(), ThreadID: child, Dir: dir, Path: "codex", Args: []string{"exec"}})
	if err != nil {
		t.Fatal(err)
	}
	vms, err := s.Provider.Inventory(t.Context(), sandboxtest.Endpoint)
	if err != nil || len(vms) != 1 || !strings.HasPrefix(vms[0].Name, sandbox.NamePrefix) {
		t.Fatalf("Sandbox active without a task VM: %v %v", vms, err)
	}
	if specs, in, _ := s.Runner.Recorded(); len(specs) != 1 || in[0] != vms[0].Ref || specs[0].Dir == dir {
		t.Fatalf("process not run inside the VM: %+v %v", specs, in)
	}

	// 407 rules unchanged: returning to Host access needs a fresh grant.
	if raw := modeHTTP(t, d, base, parent, "to-host-refused", hostMode, 403); !strings.Contains(raw, "grant_required") {
		t.Fatal(raw)
	}
	grantWorker(t, d, parent)
	modeHTTP(t, d, base, parent, "to-host", hostMode, 202)
	if _, err := p.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if vms, _ := s.Provider.Inventory(t.Context(), sandboxtest.Endpoint); len(vms) != 0 {
		t.Fatalf("Sandbox settle left VMs: %v", vms)
	}
}

// TestUnqualifiedSandboxNotOffered proves 406 B2 at the daemon: a configuration without passed checks is never offered.
func TestUnqualifiedSandboxNotOffered(t *testing.T) {
	sbx, s := qualifiedSandbox(t, "esxi", false)
	d, base, parent, _, _ := startModeChild(t, sbx)
	if raw := modeHTTP(t, d, base, parent, "to-sandbox", sandboxMode, 409); !strings.Contains(raw, "sandbox_unavailable") {
		t.Fatal(raw)
	}
	if len(s.Provider.Created()) != 0 {
		t.Fatal("unqualified configuration created VMs")
	}
}
