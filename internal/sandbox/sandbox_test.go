package sandbox_test

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"synch/internal/eventlog"
	"synch/internal/execdriver"
	"synch/internal/sandbox"
	"synch/internal/sandbox/sandboxtest"
	"synch/internal/vmdriver"
)

// newDriver builds and qualifies a Sandbox driver on the named fake provider.
func newDriver(t *testing.T, driver string, edit func(*sandbox.Config)) (*sandbox.Driver, *sandboxtest.Setup) {
	t.Helper()
	s, err := sandboxtest.NewSetup(driver)
	if err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		edit(&s.Config)
	}
	d, err := sandbox.New(s.Config)
	if err != nil {
		t.Fatal(err)
	}
	d.Qualify(t.Context())
	return d, s
}

// spec returns a fresh root process spec for the thread, with a host working directory.
func spec(thread string) execdriver.ProcSpec {
	return execdriver.ProcSpec{ID: eventlog.NewID(), ThreadID: thread, Path: "codex", Args: []string{"exec", "--json"},
		Dir: `C:\Users\host\worktree`, Env: []string{"PATH=/usr/bin", "SYNCH_THREAD=" + thread}}
}

// inventory returns the provider's current VMs.
func inventory(t *testing.T, s *sandboxtest.Setup) []vmdriver.VM {
	t.Helper()
	vms, err := s.Provider.Inventory(t.Context(), sandboxtest.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	return vms
}

// wait bounds a proc Wait so a broken lifecycle fails instead of hanging.
func wait(t *testing.T, p execdriver.Proc) execdriver.ExitInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	exit, err := p.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return exit
}

// TestLifecyclePerDriver proves B1/B5: per driver fake, create from base with creation ownership, run inside, destroy at end.
func TestLifecyclePerDriver(t *testing.T) {
	for _, driver := range sandboxtest.Drivers {
		t.Run(driver, func(t *testing.T) {
			d, s := newDriver(t, driver, nil)
			if got := d.Describe().CertifiedConfigs; !slices.Equal(got, []string{sandboxtest.Clean}) {
				t.Fatalf("advertised %v", got)
			}
			if len(inventory(t, s)) != 0 {
				t.Fatal("qualification VM was not destroyed")
			}
			thread := eventlog.NewID()
			p, err := d.Spawn(t.Context(), spec(thread))
			if err != nil {
				t.Fatal(err)
			}
			vms := inventory(t, s)
			if len(vms) != 1 || vms[0].State != "running" || !strings.HasPrefix(vms[0].Name, sandbox.NamePrefix) ||
				!strings.HasPrefix(vms[0].Ref, driver+"-vm-") {
				t.Fatalf("task VM: %+v", vms)
			}
			owner, err := s.Journal.Ownership(t.Context(), sandboxtest.Endpoint, vms[0].Ref)
			if err != nil || owner.Basis != vmdriver.Created || owner.OwnerThread != thread {
				t.Fatalf("creation ownership not recorded: %+v %v", owner, err)
			}
			specs, in, procs := s.Runner.Recorded()
			if len(specs) != 1 || in[0] != vms[0].Ref || specs[0].Dir != "/work" || specs[0].Path != "codex" {
				t.Fatalf("process not run inside the task VM: %+v %v", specs, in)
			}
			procs[0].Finish("exited", 0)
			if exit := wait(t, p); !exit.DescendantsCleaned || exit.ExitCode != 0 {
				t.Fatalf("exit: %+v", exit)
			}
			if len(inventory(t, s)) != 0 {
				t.Fatal("task VM survived task end")
			}
			if owner, _ := s.Journal.Ownership(t.Context(), sandboxtest.Endpoint, vms[0].Ref); owner.OwnerThread != "" {
				t.Fatal("ownership not retired by delete")
			}
		})
	}
}

// TestDestroyedOnCancelAndFailure proves B1/B5: the VM is destroyed on cancel and on in-VM start failure.
func TestDestroyedOnCancelAndFailure(t *testing.T) {
	d, s := newDriver(t, "proxmox", nil)
	p, err := d.Spawn(t.Context(), spec(eventlog.NewID()))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Cancel(t.Context()); err != nil {
		t.Fatal(err)
	}
	if exit := wait(t, p); exit.ExecutionOutcome != "cancelled" || len(inventory(t, s)) != 0 {
		t.Fatalf("cancel left VM: %+v %v", exit, inventory(t, s))
	}

	s.Runner.SetFailSpawn(true)
	if _, err := d.Spawn(t.Context(), spec(eventlog.NewID())); err == nil {
		t.Fatal("spawn failure not reported")
	}
	// Qualification, the cancelled task and the failed task each created exactly one VM.
	if len(inventory(t, s)) != 0 || len(s.Provider.Created()) != 3 {
		t.Fatalf("failed task left VM: %v created %v", inventory(t, s), s.Provider.Created())
	}
}

// TestNoVMReuse proves B1/B5: two tasks never share a VM, while a child process joins its own task's VM.
func TestNoVMReuse(t *testing.T) {
	d, s := newDriver(t, "esxi", nil)
	thread := eventlog.NewID()
	first, err := d.Spawn(t.Context(), spec(thread))
	if err != nil {
		t.Fatal(err)
	}
	second, err := d.Spawn(t.Context(), spec(thread))
	if err != nil {
		t.Fatal(err)
	}
	child := spec(thread)
	child.ParentID = first.ID()
	if _, err := d.Spawn(t.Context(), child); err != nil {
		t.Fatal(err)
	}
	_, in, procs := s.Runner.Recorded()
	if len(in) != 3 || in[0] == in[1] || in[2] != in[0] || len(inventory(t, s)) != 2 {
		t.Fatalf("VM per process %v, inventory %v", in, inventory(t, s))
	}
	procs[0].Finish("exited", 0)
	wait(t, first)
	if vms := inventory(t, s); len(vms) != 1 || vms[0].Ref != in[1] {
		t.Fatalf("first task's VM not destroyed or second's touched: %v", vms)
	}
	procs[1].Finish("exited", 0)
	wait(t, second)
	created := s.Provider.Created()
	if len(created) != 3 || len(slices.Compact(slices.Sorted(slices.Values(created)))) != 3 {
		t.Fatalf("VM refs reused: %v", created)
	}
}

// TestBoundaryFailureKeepsUnadvertised proves B2/B5: each failing, erroring or unrun check keeps its configuration unadvertised.
func TestBoundaryFailureKeepsUnadvertised(t *testing.T) {
	bad := map[string]sandbox.Properties{
		"host-mount":     {Mounts: []sandbox.Mount{{HostPath: `C:\Users\host`, Writable: true}}},
		"readonly-mount": {Mounts: []sandbox.Mount{{HostPath: "/home/host/.config"}}},
		"ssh-agent":      {Credentials: []string{"/run/host-ssh-agent.sock"}},
		"env-secret":     {Env: []string{"PROVIDER_PASSWORD=" + sandboxtest.InfraPassword}},
		"open-egress":    {Egress: []string{"proxy.internal:3128", "0.0.0.0/0"}},
		"inspect-error":  {},
	}
	d, s := newDriver(t, "virtualbox", func(c *sandbox.Config) {
		insp := c.Inspect.(sandboxtest.Inspector)
		insp.Err = map[string]error{"inspect-error": errors.New("guest probe unreachable")}
		for _, name := range slices.Sorted(maps.Keys(bad)) {
			cfg := c.Configurations[0]
			cfg.Name = name
			c.Configurations = append(c.Configurations, cfg)
			insp.Props[name] = bad[name]
		}
		c.Inspect = insp
	})
	if got := d.Describe().CertifiedConfigs; !slices.Equal(got, []string{sandboxtest.Clean}) {
		t.Fatalf("advertised %v", got)
	}
	want := map[string]string{
		"host-mount/filesystem": sandbox.Failed, "readonly-mount/filesystem": sandbox.Failed,
		"ssh-agent/credentials": sandbox.Failed, "env-secret/credentials": sandbox.Failed,
		"open-egress/network": sandbox.Failed, "inspect-error/network": sandbox.Error,
		"clean/filesystem": sandbox.Passed, "clean/credentials": sandbox.Passed, "clean/network": sandbox.Passed,
	}
	for _, e := range d.Evidence() {
		t.Logf("| %s | %s | %s | %s |", e.Configuration, e.Check, e.Result, e.Reason)
		if r, ok := want[e.Configuration+"/"+e.Check]; ok && r != e.Result {
			t.Errorf("%s/%s = %s, want %s", e.Configuration, e.Check, e.Result, r)
		}
		if e.Check == sandbox.RealHostCertification && (e.Result != sandbox.NotRun || e.Reason != "deferred by owner") {
			t.Errorf("real-host certification reported %s", e.Result)
		}
		if strings.Contains(e.Reason, sandboxtest.InfraPassword) {
			t.Error("evidence leaks a secret")
		}
	}
	if len(inventory(t, s)) != 0 {
		t.Fatal("qualification VMs left behind")
	}

	// With no inspector nothing is checked, so nothing is advertised and nothing spawns.
	d, _ = newDriver(t, "virtualbox", func(c *sandbox.Config) { c.Inspect = nil })
	for _, e := range d.Evidence() {
		if e.Result != sandbox.NotRun {
			t.Fatalf("unrun check reported %+v", e)
		}
		t.Logf("| no-inspector | %s | %s | %s |", e.Check, e.Result, e.Reason)
	}
	if len(d.Describe().CertifiedConfigs) != 0 {
		t.Fatal("unchecked configuration advertised")
	}
	if _, err := d.Spawn(t.Context(), spec(eventlog.NewID())); !errors.Is(err, sandbox.ErrNotQualified) {
		t.Fatalf("spawn on unqualified driver: %v", err)
	}

	// A configuration without a pinned host key is refused outright.
	cfg := s.Config
	cfg.Configurations = []sandbox.Configuration{{Name: "nopin", Endpoint: "lab", Base: "b", WorkDir: "/w"}}
	if _, err := sandbox.New(cfg); err == nil {
		t.Fatal("unpinned host key accepted")
	}
}

// TestNoInfrastructureSecretInside proves B4: provider credentials stay daemon-side; no VM-side command line or env carries one.
func TestNoInfrastructureSecretInside(t *testing.T) {
	d, s := newDriver(t, "workstation", nil)
	if !slices.Contains(s.Broker.Redactor().Values(), sandboxtest.InfraPassword) {
		t.Fatal("provider never used its brokered credential")
	}
	leaky := spec(eventlog.NewID())
	leaky.Env = append(leaky.Env, "VM_PASSWORD="+sandboxtest.InfraPassword)
	if _, err := d.Spawn(t.Context(), leaky); !errors.Is(err, sandbox.ErrSecretInVM) {
		t.Fatalf("secret env admitted: %v", err)
	}
	leaky = spec(eventlog.NewID())
	leaky.Args = append(leaky.Args, "--token="+sandboxtest.InfraPassword)
	if _, err := d.Spawn(t.Context(), leaky); !errors.Is(err, sandbox.ErrSecretInVM) {
		t.Fatalf("secret argument admitted: %v", err)
	}
	p, err := d.Spawn(t.Context(), spec(eventlog.NewID()))
	if err != nil {
		t.Fatal(err)
	}
	specs, _, procs := s.Runner.Recorded()
	known := s.Broker.Redactor().Values()
	for _, sp := range specs {
		for _, field := range slices.Concat([]string{sp.Path, sp.Dir}, sp.Args, sp.Env) {
			for _, secret := range known {
				if strings.Contains(field, secret) {
					t.Fatalf("VM-side field %q carries a known secret", field)
				}
			}
			if strings.Contains(field, `C:\Users\host`) {
				t.Fatalf("host path crossed into VM: %q", field)
			}
		}
	}
	for _, c := range s.Runner.Connections() {
		if strings.Contains(strings.Join(slices.Concat([]string{c.Base, c.Endpoint, c.HostKey, c.WorkDir}, c.Egress), " "), sandboxtest.InfraPassword) {
			t.Fatal("runner connection received a provider credential")
		}
	}
	procs[0].Finish("exited", 0)
	wait(t, p)
}

// TestSettleReopenAndReconcile proves §5 settle semantics and B1 orphan reconciliation through the driver.
func TestSettleReopenAndReconcile(t *testing.T) {
	d, s := newDriver(t, "hyperv", nil)
	thread := eventlog.NewID()
	p, err := d.Spawn(t.Context(), spec(thread))
	if err != nil {
		t.Fatal(err)
	}
	report, err := d.Settle(t.Context(), thread)
	if err != nil || !report.HostProcessesStopped || len(inventory(t, s)) != 0 {
		t.Fatalf("settle: %+v %v %v", report, err, inventory(t, s))
	}
	wait(t, p)
	if _, err := d.Spawn(t.Context(), spec(thread)); !errors.Is(err, execdriver.ErrThreadSettling) {
		t.Fatalf("settled thread admitted: %v", err)
	}
	if err := d.Reopen(thread); err != nil {
		t.Fatal(err)
	}

	// A failed delete is reported as unconfirmed, never as stopped.
	p, err = d.Spawn(t.Context(), spec(thread))
	if err != nil {
		t.Fatal(err)
	}
	s.Provider.SetFailDelete(true)
	report, err = d.Settle(t.Context(), thread)
	if err != nil || report.HostProcessesStopped || report.Reason == "" {
		t.Fatalf("leaked VM reported stopped: %+v %v", report, err)
	}
	if exit := wait(t, p); exit.DescendantsCleaned {
		t.Fatal("leaked VM reported as cleaned")
	}
	s.Provider.SetFailDelete(false)

	// A crashed earlier daemon's task VM: created ownership in the shared journal, no live task here.
	prior, err := sandbox.New(s.Config)
	if err != nil {
		t.Fatal(err)
	}
	prior.Qualify(t.Context())
	if _, err := prior.Spawn(t.Context(), spec(eventlog.NewID())); err != nil {
		t.Fatal(err)
	}
	foreign := vmdriver.VM{Ref: "operator-vm", Name: "build-agent", State: "running"}
	s.Provider.Put(foreign)
	if len(inventory(t, s)) != 3 {
		t.Fatalf("setup: %v", inventory(t, s))
	}
	left, err := d.Reconcile(t.Context())
	if err != nil || len(left) != 0 {
		t.Fatalf("reconcile: %v %v", left, err)
	}
	if vms := inventory(t, s); len(vms) != 1 || vms[0] != foreign {
		t.Fatalf("after reconcile: %v", vms)
	}
}
