package sandbox

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"

	"synch/internal/execdriver"
	"synch/internal/vmdriver"
)

// --- Configuration and construction ---

// NamePrefix marks every VM this driver creates, so reconciliation never touches other VMs.
const NamePrefix = "synch-sbx-"

// Sentinel errors callers can match with errors.Is.
var (
	ErrNotQualified = errors.New("sandbox: no configuration passed its boundary checks")
	ErrSecretInVM   = errors.New("sandbox: command line or environment carries a known secret value")
	ErrInvalidSpec  = errors.New("sandbox: invalid process specification")
	ErrOwnership    = errors.New("sandbox: created VM has no recorded creation ownership")
)

// Configuration is one operator-declared Sandbox configuration on the provider.
type Configuration struct {
	// Name is the label advertised to the mode switch once its boundary checks pass.
	Name string
	// Endpoint and Base select the provider endpoint and the operator-configured base.
	Endpoint, Base string
	// HostKey is the base's pinned SSH host key fingerprint ("SHA256:<base64>").
	HostKey string
	// WorkDir is the VM-side working directory; host paths are never sent into the VM.
	WorkDir string
	// Egress lists the only network destinations the VM may reach.
	Egress []string
}

// Connector opens the in-VM runner of one created VM. Production connectors reach it
// over SSH pinned to cfg.HostKey (internal/remote) and hold no provider credentials.
type Connector func(ctx context.Context, cfg Configuration, vm vmdriver.VM) (execdriver.ExecutionDriver, error)

// Config wires the provider, its journal and policy, the runner connector and the
// boundary inspector. The provider is any of the five vmdriver drivers.
type Config struct {
	Provider       vmdriver.VMDriver
	Journal        vmdriver.Journal
	Authorizer     vmdriver.Authorizer
	Configurations []Configuration
	Connect        Connector
	Inspect        Inspector
	// KnownSecrets returns every brokered value (credentials.Broker.Redactor().Values).
	KnownSecrets func() []string
}

// Driver is the Sandbox execution driver: one disposable VM per task.
type Driver struct {
	cfg Config
	vms *vmdriver.Dispatcher

	mu         sync.Mutex
	advertised []string
	evidence   []Evidence
	closed     map[string]bool            // threads whose admission is stopped
	inflight   map[string]*sync.WaitGroup // launches in progress per thread
	tasks      map[string]*task           // live tasks by root process ID
	procs      map[string]*task           // every process ID to its task, for parent joins
	names      map[string]bool            // VM names held by live or launching tasks
	pending    []string                   // external requests with an unresolved outcome
}

// task is one sandboxed task: its VM, runner and root process.
type task struct {
	thread     string
	cfg        Configuration
	vm         vmdriver.VM
	runner     execdriver.ExecutionDriver
	root       execdriver.Proc
	ids        []string
	done       chan struct{} // closed after the VM is destroyed or its destruction failed
	exit       execdriver.ExitInfo
	waitErr    error
	destroyErr error
}

var _ execdriver.ExecutionDriver = (*Driver)(nil)

// New validates the configuration. Nothing is advertised until Qualify passes a configuration.
func New(cfg Config) (*Driver, error) {
	if cfg.Provider == nil || cfg.Journal == nil || cfg.Authorizer == nil || cfg.Connect == nil || len(cfg.Configurations) == 0 {
		return nil, errors.New("sandbox: provider, journal, authorizer, connector and a configuration are required")
	}
	if cfg.KnownSecrets == nil {
		return nil, errors.New("sandbox: KnownSecrets is required so no brokered value can enter a VM")
	}
	seen := map[string]bool{}
	for _, c := range cfg.Configurations {
		if c.Name == "" || seen[c.Name] || c.Endpoint == "" || c.Base == "" || c.WorkDir == "" || !pinned(c.HostKey) {
			return nil, fmt.Errorf("sandbox: configuration %q needs a unique name, endpoint, base, VM work dir and pinned SHA-256 host key", c.Name)
		}
		seen[c.Name] = true
	}
	return &Driver{
		cfg: cfg, vms: vmdriver.NewDispatcher(cfg.Provider, cfg.Journal, cfg.Authorizer),
		closed: map[string]bool{}, inflight: map[string]*sync.WaitGroup{},
		tasks: map[string]*task{}, procs: map[string]*task{}, names: map[string]bool{},
	}, nil
}

// pinned accepts only a well-formed SHA-256 SSH fingerprint, so an unpinned base is refused.
func pinned(fp string) bool {
	encoded, ok := strings.CutPrefix(fp, "SHA256:")
	sum, err := base64.RawStdEncoding.Strict().DecodeString(encoded)
	return ok && err == nil && len(sum) == 32
}

// Kind identifies the sandbox execution driver.
func (*Driver) Kind() string { return execdriver.KindSandbox }

// Describe lists only configurations whose boundary checks all passed.
func (d *Driver) Describe() execdriver.DriverInfo {
	d.mu.Lock()
	defer d.mu.Unlock()
	return execdriver.DriverInfo{
		Label:             "Sandbox",
		EvidenceIntegrity: "boundary checks per configuration; real-host certification not run: deferred by owner",
		CertifiedConfigs:  slices.Clone(d.advertised),
	}
}

// --- Spawn: one VM per task, processes run inside it ---

// Spawn runs a root process in a fresh VM, or a child process in its parent's task VM.
func (d *Driver) Spawn(ctx context.Context, spec execdriver.ProcSpec) (execdriver.Proc, error) {
	if spec.ID == "" || spec.ThreadID == "" || spec.Path == "" {
		return nil, ErrInvalidSpec
	}
	if err := d.scan(spec); err != nil {
		return nil, err
	}
	d.mu.Lock()
	if d.closed[spec.ThreadID] {
		d.mu.Unlock()
		return nil, execdriver.ErrThreadSettling
	}
	if d.procs[spec.ID] != nil {
		d.mu.Unlock()
		return nil, fmt.Errorf("%w: duplicate process ID", ErrInvalidSpec)
	}
	if parent := d.procs[spec.ParentID]; spec.ParentID != "" && parent != nil {
		d.mu.Unlock()
		return d.join(ctx, parent, spec)
	}
	cfg, ok := d.selected()
	if !ok {
		d.mu.Unlock()
		return nil, ErrNotQualified
	}
	wg := d.inflight[spec.ThreadID]
	if wg == nil {
		wg = new(sync.WaitGroup)
		d.inflight[spec.ThreadID] = wg
	}
	// Settle sets closed under mu before waiting, so no Add can follow its Wait.
	wg.Add(1)
	name := NamePrefix + strings.ToLower(spec.ID)
	d.names[name] = true
	d.mu.Unlock()
	defer wg.Done()

	t, err := d.launch(ctx, cfg, spec, name)
	if err != nil {
		d.mu.Lock()
		delete(d.names, name)
		d.mu.Unlock()
		return nil, err
	}
	return proc{Proc: t.root, t: t}, nil
}

// selected returns the first advertised configuration, in operator order. Callers hold mu.
func (d *Driver) selected() (Configuration, bool) {
	for _, c := range d.cfg.Configurations {
		if slices.Contains(d.advertised, c.Name) {
			return c, true
		}
	}
	return Configuration{}, false
}

// scan refuses a launch whose VM-side command line or environment holds a known secret.
func (d *Driver) scan(spec execdriver.ProcSpec) error {
	fields := append([]string{spec.Path}, spec.Args...)
	fields = append(fields, spec.Env...)
	for _, secret := range d.cfg.KnownSecrets() {
		if secret == "" {
			continue
		}
		for _, f := range fields {
			if strings.Contains(f, secret) {
				return ErrSecretInVM
			}
		}
	}
	return nil
}

// inside rewrites a spec for the VM: the host working directory never crosses the boundary.
func inside(cfg Configuration, spec execdriver.ProcSpec) execdriver.ProcSpec {
	spec.Dir = cfg.WorkDir
	spec.Args = slices.Clone(spec.Args)
	spec.Env = slices.Clone(spec.Env)
	return spec
}

// launch creates, starts and connects the task VM and starts the root process; any
// failure after creation destroys the VM before returning.
func (d *Driver) launch(ctx context.Context, cfg Configuration, spec execdriver.ProcSpec, name string) (*task, error) {
	vm, err := d.create(ctx, cfg, spec.ThreadID, name)
	if err != nil {
		return nil, err
	}
	t := &task{thread: spec.ThreadID, cfg: cfg, vm: vm, ids: []string{spec.ID}, done: make(chan struct{})}
	fail := func(err error) (*task, error) {
		return nil, errors.Join(err, d.destroy(context.WithoutCancel(ctx), t))
	}
	if t.runner, err = d.cfg.Connect(ctx, cfg, vm); err != nil {
		return fail(fmt.Errorf("sandbox: connect VM runner: %w", err))
	}
	if t.root, err = t.runner.Spawn(ctx, inside(cfg, spec)); err != nil {
		return fail(fmt.Errorf("sandbox: start process in VM: %w", err))
	}
	d.mu.Lock()
	d.tasks[spec.ID] = t
	d.procs[spec.ID] = t
	d.mu.Unlock()
	go d.watch(t)
	return t, nil
}

// join runs a child process inside its parent's task VM.
func (d *Driver) join(ctx context.Context, parent *task, spec execdriver.ProcSpec) (execdriver.Proc, error) {
	if parent.thread != spec.ThreadID {
		return nil, fmt.Errorf("%w: parent belongs to another thread", ErrInvalidSpec)
	}
	p, err := parent.runner.Spawn(ctx, inside(parent.cfg, spec))
	if err != nil {
		return nil, fmt.Errorf("sandbox: start process in VM: %w", err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.tasks[parent.ids[0]] != parent {
		// The parent ended while the child started: its VM is gone or going.
		return nil, errors.Join(errors.New("sandbox: parent task ended"), p.Cancel(context.WithoutCancel(ctx)))
	}
	parent.ids = append(parent.ids, spec.ID)
	d.procs[spec.ID] = parent
	return p, nil
}

// watch destroys the task VM when the root process ends, then releases waiters.
func (d *Driver) watch(t *task) {
	exit, err := t.root.Wait(context.Background())
	destroyErr := d.destroy(context.Background(), t)
	if destroyErr != nil {
		exit.DescendantsCleaned = false
		exit.CleanupReason = strings.TrimSpace(exit.CleanupReason + " sandbox VM not destroyed: " + destroyErr.Error())
	}
	t.exit, t.waitErr, t.destroyErr = exit, err, destroyErr
	d.mu.Lock()
	delete(d.tasks, t.ids[0])
	for _, id := range t.ids {
		delete(d.procs, id)
	}
	delete(d.names, t.vm.Name)
	d.mu.Unlock()
	close(t.done)
}

// proc is the root process handle; Wait and Cancel also cover VM destruction.
type proc struct {
	execdriver.Proc
	t *task
}

// Wait returns the root process result once its VM has been destroyed.
func (p proc) Wait(ctx context.Context) (execdriver.ExitInfo, error) {
	select {
	case <-p.t.done:
		return p.t.exit, p.t.waitErr
	case <-ctx.Done():
		return execdriver.ExitInfo{}, ctx.Err()
	}
}

// Cancel stops the root process and waits for the VM to be destroyed.
func (p proc) Cancel(ctx context.Context) error {
	err := p.Proc.Cancel(ctx)
	select {
	case <-p.t.done:
		return errors.Join(err, p.t.destroyErr)
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}
}

// --- VM lifecycle through the vmdriver dispatcher ---

// create makes and starts a VM from the base and confirms the journal recorded creation ownership.
func (d *Driver) create(ctx context.Context, cfg Configuration, thread, name string) (vmdriver.VM, error) {
	if err := d.mutate(ctx, vmdriver.Command{Endpoint: cfg.Endpoint, Thread: thread, Operation: vmdriver.CreateOp,
		Base: cfg.Base, Spec: vmdriver.VMSpec{Name: name}}); err != nil {
		return vmdriver.VM{}, fmt.Errorf("sandbox: create VM: %w", err)
	}
	inventory, err := d.cfg.Provider.Inventory(ctx, cfg.Endpoint)
	if err != nil {
		// The created VM stays visible to Reconcile by its prefixed name.
		return vmdriver.VM{}, fmt.Errorf("sandbox: find created VM: %w", err)
	}
	var found []vmdriver.VM
	for _, vm := range inventory {
		if vm.Name == name {
			found = append(found, vm)
		}
	}
	if len(found) != 1 {
		return vmdriver.VM{}, fmt.Errorf("sandbox: find created VM: %d matches for %s", len(found), name)
	}
	vm := found[0]
	owner, err := d.cfg.Journal.Ownership(ctx, cfg.Endpoint, vm.Ref)
	if err != nil || owner.Basis != vmdriver.Created || !owner.CanMutate(cfg.Endpoint, vm.Ref, thread) {
		return vmdriver.VM{}, errors.Join(ErrOwnership, err)
	}
	t := &task{thread: thread, cfg: cfg, vm: vm}
	if err := d.mutate(ctx, vmdriver.Command{Endpoint: cfg.Endpoint, Thread: thread, Operation: vmdriver.StartOp, VM: vm.Ref}); err != nil {
		return vmdriver.VM{}, errors.Join(fmt.Errorf("sandbox: start VM: %w", err), d.destroy(context.WithoutCancel(ctx), t))
	}
	return vm, nil
}

// destroy stops and deletes the task VM; a VM left by a failed delete is swept by Reconcile.
func (d *Driver) destroy(ctx context.Context, t *task) error {
	if c, ok := t.runner.(io.Closer); ok {
		_ = c.Close()
	}
	// Stop is best effort: a VM whose processes already ended may be powered off.
	_ = d.mutate(ctx, vmdriver.Command{Endpoint: t.cfg.Endpoint, Thread: t.thread, Operation: vmdriver.StopOp, VM: t.vm.Ref})
	err := d.mutate(ctx, vmdriver.Command{Endpoint: t.cfg.Endpoint, Thread: t.thread, Operation: vmdriver.DeleteOp, VM: t.vm.Ref})
	if err != nil {
		// The VM keeps its prefixed name and creation ownership, so Reconcile's sweep finds it.
		return fmt.Errorf("sandbox: delete VM %s: %w", t.vm.Ref, err)
	}
	return nil
}

// mutate dispatches one provider mutation and requires a settled success; an
// unresolved outcome is remembered so Reconcile can settle it later.
func (d *Driver) mutate(ctx context.Context, cmd vmdriver.Command) error {
	res, err := d.vms.Dispatch(ctx, cmd)
	if errors.Is(err, vmdriver.ErrReconciliation) && res.ID != "" {
		d.mu.Lock()
		d.pending = append(d.pending, res.ID)
		d.mu.Unlock()
	}
	if err != nil {
		return err
	}
	if res.Outcome != vmdriver.Succeeded {
		return fmt.Errorf("%s %s", cmd.Operation, res.Outcome)
	}
	return nil
}

// --- Settlement, reopening and orphan reconciliation ---

// Settle stops admission for the thread, cancels its tasks and reports stopped only
// when every task VM of the thread is destroyed.
func (d *Driver) Settle(ctx context.Context, threadID string) (execdriver.SettleReport, error) {
	d.mu.Lock()
	d.closed[threadID] = true
	wg := d.inflight[threadID]
	d.mu.Unlock()
	if wg != nil {
		launched := make(chan struct{})
		go func() { wg.Wait(); close(launched) }()
		select {
		case <-launched:
		case <-ctx.Done():
			return execdriver.SettleReport{Reason: "sandbox launch still in progress"}, ctx.Err()
		}
	}
	d.mu.Lock()
	var tasks []*task
	for _, t := range d.tasks {
		if t.thread == threadID {
			tasks = append(tasks, t)
		}
	}
	d.mu.Unlock()
	var reasons []string
	for _, t := range tasks {
		_ = t.root.Cancel(ctx) // The VM deletion below is what proves the processes are gone.
		select {
		case <-t.done:
			if t.destroyErr != nil {
				reasons = append(reasons, t.destroyErr.Error())
			}
		case <-ctx.Done():
			return execdriver.SettleReport{Reason: "sandbox VM destruction unconfirmed"}, ctx.Err()
		}
	}
	return execdriver.SettleReport{HostProcessesStopped: len(reasons) == 0, Reason: strings.Join(reasons, "; ")}, nil
}

// Reopen restores admission for a thread after a clean settle.
func (d *Driver) Reopen(threadID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.closed, threadID)
	return nil
}

// Reconcile settles unresolved provider requests, retries failed deletions and
// deletes prefixed VMs with creation ownership that no live task holds (orphans
// after a crash or restart). It returns the VM refs still left behind.
func (d *Driver) Reconcile(ctx context.Context) ([]string, error) {
	d.mu.Lock()
	pending := d.pending
	d.pending = nil
	d.mu.Unlock()
	var errs []error
	for _, id := range pending {
		if _, err := d.vms.Reconcile(ctx, id); err != nil {
			errs = append(errs, err)
			d.mu.Lock()
			d.pending = append(d.pending, id)
			d.mu.Unlock()
		}
	}
	endpoints := map[string]bool{}
	for _, c := range d.cfg.Configurations {
		endpoints[c.Endpoint] = true
	}
	var left []string
	for endpoint := range endpoints {
		orphans, err := d.orphans(ctx, endpoint)
		errs = append(errs, err)
		for _, o := range orphans {
			t := &task{thread: o.OwnerThread, cfg: Configuration{Endpoint: endpoint}, vm: vmdriver.VM{Ref: o.VMRef}}
			if err := d.destroy(ctx, t); err != nil {
				errs = append(errs, err)
				left = append(left, o.VMRef)
			}
		}
	}
	return left, errors.Join(errs...)
}

// orphans lists created, undecided, prefixed VMs on the endpoint not held by a live task.
func (d *Driver) orphans(ctx context.Context, endpoint string) ([]vmdriver.Ownership, error) {
	inventory, err := d.cfg.Provider.Inventory(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	held := maps.Clone(d.names)
	d.mu.Unlock()
	var candidates []vmdriver.VM
	var owners []vmdriver.Ownership
	for _, vm := range inventory {
		if !strings.HasPrefix(vm.Name, NamePrefix) || held[vm.Name] {
			continue
		}
		owner, err := d.cfg.Journal.Ownership(ctx, endpoint, vm.Ref)
		if err != nil {
			return nil, err
		}
		if owner.Basis == vmdriver.Created {
			candidates = append(candidates, vm)
			owners = append(owners, owner)
		}
	}
	// Every candidate's task is over, so each owner counts as terminal for this VM.
	return vmdriver.Orphans(endpoint, candidates, owners, func(string) bool { return true }), nil
}
