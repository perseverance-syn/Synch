// Package sandboxtest supplies in-memory fakes for testing the Sandbox driver: a
// provider for each of the five vmdriver drivers (carrying that driver's real
// packaged capability matrix), a journal, an authorizer, an in-VM runner that
// records every command line and environment it receives, and a boundary
// inspector returning declared properties. Nothing here touches a hypervisor or
// the network; it certifies nothing.
package sandboxtest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"

	"synch/internal/credentials"
	"synch/internal/execdriver"
	"synch/internal/sandbox"
	"synch/internal/vmdriver"
	"synch/internal/vmdriver/esxi"
	"synch/internal/vmdriver/hyperv"
	"synch/internal/vmdriver/proxmox"
	"synch/internal/vmdriver/virtualbox"
	"synch/internal/vmdriver/workstation"
)

// --- Provider fakes: one per V1 driver, real capability matrix, in-memory VMs ---

// Endpoint is the single endpoint every fake provider serves.
const Endpoint = "lab"

// errNoTransport proves the fake path never reaches a provider transport.
var errNoTransport = errors.New("sandboxtest: no transport")

// refusingVBox never runs VBoxManage: the real driver is built only for its matrix.
type refusingVBox struct{}

// Run refuses every VBoxManage invocation.
func (refusingVBox) Run(context.Context, string, []string) (virtualbox.Result, error) {
	return virtualbox.Result{}, errNoTransport
}

// refusingWS never runs vmrun: the real driver is built only for its matrix.
type refusingWS struct{}

// Run refuses every vmrun invocation.
func (refusingWS) Run(context.Context, string, []string) (workstation.Result, error) {
	return workstation.Result{}, errNoTransport
}

// Matrices returns each V1 driver's packaged capability matrix, keyed by driver
// name, read through the real drivers' public constructors without any transport.
func Matrices(broker vmdriver.Broker) (map[string]vmdriver.MatrixRow, error) {
	esx, err := esxi.New(map[string]esxi.Endpoint{Endpoint: {URL: "https://esxi.invalid"}}, broker, http.DefaultClient)
	if err != nil {
		return nil, err
	}
	pve, err := proxmox.New(map[string]string{Endpoint: "https://pve.invalid"}, broker, http.DefaultClient)
	if err != nil {
		return nil, err
	}
	vbox, err := virtualbox.New([]string{Endpoint}, refusingVBox{})
	if err != nil {
		return nil, err
	}
	ws, err := workstation.New(map[string]string{Endpoint: "https://ws.invalid"}, refusingWS{}, broker, http.DefaultClient)
	if err != nil {
		return nil, err
	}
	hv, err := hyperv.New(map[string]hyperv.Host{Endpoint: {}}, func(context.Context, hyperv.Command, vmdriver.Credentials) ([]byte, error) {
		return nil, errNoTransport
	}, broker, nil)
	if err != nil {
		return nil, err
	}
	out := map[string]vmdriver.MatrixRow{}
	for _, d := range []vmdriver.VMDriver{esx, pve, vbox, ws, hv} {
		row, err := d.Capabilities(context.Background())
		if err != nil {
			return nil, err
		}
		out[row.Driver] = row
	}
	return out, nil
}

// Provider is an in-memory hypervisor. Like a real driver it fetches its
// infrastructure credential from the broker on every mutation.
type Provider struct {
	Matrix vmdriver.MatrixRow
	Broker vmdriver.Broker

	mu         sync.Mutex
	vms        map[string]vmdriver.VM
	tasks      map[string]string
	next       int
	failDelete bool
	created    []string
}

var _ vmdriver.VMDriver = (*Provider)(nil)

// NewProvider returns an empty provider reporting the given matrix.
func NewProvider(matrix vmdriver.MatrixRow, broker vmdriver.Broker) *Provider {
	return &Provider{Matrix: matrix, Broker: broker, vms: map[string]vmdriver.VM{}, tasks: map[string]string{}}
}

// Capabilities returns the real driver's packaged matrix.
func (p *Provider) Capabilities(context.Context) (vmdriver.MatrixRow, error) { return p.Matrix, nil }

// Inventory lists the VMs currently present, ordered by ref.
func (p *Provider) Inventory(context.Context, string) ([]vmdriver.VM, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []vmdriver.VM
	for _, vm := range p.vms {
		out = append(out, vm)
	}
	slices.SortFunc(out, func(a, b vmdriver.VM) int { return strings.Compare(a.Ref, b.Ref) })
	return out, nil
}

// Created returns every VM ref ever created, in order.
func (p *Provider) Created() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.created)
}

// Put adds a VM directly, as if created outside this sandbox instance.
func (p *Provider) Put(vm vmdriver.VM) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.vms[vm.Ref] = vm
}

// SetFailDelete toggles deletion failure.
func (p *Provider) SetFailDelete(fail bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failDelete = fail
}

// use fetches the infrastructure credential, as every real transport does.
func (p *Provider) use(ctx context.Context) error {
	_, err := p.Broker.Credentials(ctx, Endpoint)
	return err
}

// CreateFromBase clones the base into a stopped VM and returns a provider task.
func (p *Provider) CreateFromBase(ctx context.Context, _ vmdriver.ExternalRequest, _ string, spec vmdriver.VMSpec, _ string) (vmdriver.TaskRef, error) {
	if err := p.use(ctx); err != nil {
		return vmdriver.TaskRef{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next++
	ref := fmt.Sprintf("%s-vm-%d", p.Matrix.Driver, p.next)
	p.vms[ref] = vmdriver.VM{Ref: ref, Name: spec.Name, State: "stopped"}
	p.created = append(p.created, ref)
	task := fmt.Sprintf("task-%d", p.next)
	p.tasks[task] = ref
	return vmdriver.TaskRef{ID: task}, nil
}

// power sets a VM's state and fails for an unknown VM.
func (p *Provider) power(ctx context.Context, ref, state string) (vmdriver.TaskRef, error) {
	if err := p.use(ctx); err != nil {
		return vmdriver.TaskRef{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	vm, ok := p.vms[ref]
	if !ok {
		return vmdriver.TaskRef{}, vmdriver.ErrProvider
	}
	vm.State = state
	p.vms[ref] = vm
	return vmdriver.TaskRef{}, nil
}

// Start powers a VM on.
func (p *Provider) Start(ctx context.Context, _ vmdriver.ExternalRequest, ref string) (vmdriver.TaskRef, error) {
	return p.power(ctx, ref, "running")
}

// Stop powers a VM off.
func (p *Provider) Stop(ctx context.Context, _ vmdriver.ExternalRequest, ref string) (vmdriver.TaskRef, error) {
	return p.power(ctx, ref, "stopped")
}

// Delete removes a VM unless deletion failure is set.
func (p *Provider) Delete(ctx context.Context, _ vmdriver.ExternalRequest, ref string) (vmdriver.TaskRef, error) {
	if err := p.use(ctx); err != nil {
		return vmdriver.TaskRef{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.vms[ref]; !ok || p.failDelete {
		return vmdriver.TaskRef{}, vmdriver.ErrProvider
	}
	delete(p.vms, ref)
	return vmdriver.TaskRef{}, nil
}

// Snapshot is not used by the Sandbox driver.
func (p *Provider) Snapshot(context.Context, vmdriver.ExternalRequest, string, string) (vmdriver.TaskRef, error) {
	return vmdriver.TaskRef{}, vmdriver.ErrUnavailable
}

// Restore is not used by the Sandbox driver.
func (p *Provider) Restore(context.Context, vmdriver.ExternalRequest, string, string) (vmdriver.TaskRef, error) {
	return vmdriver.TaskRef{}, vmdriver.ErrUnavailable
}

// GetTask reports a create task as succeeded with its resulting VM.
func (p *Provider) GetTask(_ context.Context, _ string, task vmdriver.TaskRef) (vmdriver.TaskStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ref, ok := p.tasks[task.ID]
	if !ok {
		return vmdriver.TaskStatus{}, vmdriver.ErrInvalid
	}
	return vmdriver.TaskStatus{State: vmdriver.Succeeded, ResultResource: ref}, nil
}

// FindByToken finds nothing; create results come from the provider task.
func (p *Provider) FindByToken(context.Context, string, string, string) ([]vmdriver.ResourceEvidence, error) {
	return nil, nil
}

// --- Journal and authorizer ---

// Journal is an in-memory vmdriver journal recording creation ownership.
type Journal struct {
	mu      sync.Mutex
	records map[string]vmdriver.RequestRecord
	owners  map[string]vmdriver.Ownership
}

var _ vmdriver.Journal = (*Journal)(nil)

// NewJournal returns an empty journal.
func NewJournal() *Journal {
	return &Journal{records: map[string]vmdriver.RequestRecord{}, owners: map[string]vmdriver.Ownership{}}
}

// Ownership returns the recorded owner of a VM.
func (j *Journal) Ownership(_ context.Context, endpoint, ref string) (vmdriver.Ownership, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.owners[endpoint+"/"+ref], nil
}

// Begin persists a request, rejecting duplicates.
func (j *Journal) Begin(_ context.Context, r vmdriver.RequestRecord, _ vmdriver.Limits) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, ok := j.records[r.Request.ID]; ok {
		return vmdriver.ErrInvalid
	}
	j.records[r.Request.ID] = r
	return nil
}

// Bind stores a provider task on its request.
func (j *Journal) Bind(_ context.Context, _ string, b vmdriver.TaskBinding) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	r := j.records[b.ID]
	r.Task.ID = b.TaskID
	j.records[b.ID] = r
	return nil
}

// Resolve appends observations and records or retires ownership.
func (j *Journal) Resolve(_ context.Context, _ string, events []vmdriver.Resolution, owner *vmdriver.Ownership) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	last := events[len(events)-1]
	r := j.records[last.ID]
	r.History = append(r.History, events...)
	j.records[last.ID] = r
	if owner != nil {
		j.owners[owner.Endpoint+"/"+owner.VMRef] = *owner
	}
	if last.Operation == vmdriver.DeleteOp && last.Outcome == vmdriver.Succeeded {
		delete(j.owners, r.Request.Endpoint+"/"+r.Request.Resource)
	}
	return nil
}

// Load returns a persisted request.
func (j *Journal) Load(_ context.Context, id string) (vmdriver.RequestRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	r, ok := j.records[id]
	if !ok {
		return vmdriver.RequestRecord{}, vmdriver.ErrInvalid
	}
	return r, nil
}

// Authorizer allows owned operations in disposable Sandbox mode.
type Authorizer struct{}

// Authorize grants Sandbox-mode, disposable permits for owned resources.
func (Authorizer) Authorize(_ context.Context, _, _ string, _ vmdriver.Operation, _ string, owned bool) (vmdriver.Permit, error) {
	return vmdriver.Permit{Allowed: owned, Mode: "sandbox", Disposable: true, Limits: vmdriver.Limits{Endpoint: 50, Thread: 50}}, nil
}

// --- In-VM runner and inspector ---

// Runner records what each VM was asked to run; it stands in for the SSH runner.
type Runner struct {
	mu        sync.Mutex
	specs     []execdriver.ProcSpec
	vms       []string
	procs     []*Proc
	connected []sandbox.Configuration
	failSpawn bool
}

// Connect returns a per-VM runner handle; it is a sandbox.Connector.
func (r *Runner) Connect(_ context.Context, cfg sandbox.Configuration, vm vmdriver.VM) (execdriver.ExecutionDriver, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connected = append(r.connected, cfg)
	return &vmRunner{r: r, vm: vm.Ref}, nil
}

// Recorded returns copies of every spec run, the VM ref each ran in, and the processes.
func (r *Runner) Recorded() ([]execdriver.ProcSpec, []string, []*Proc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.specs), slices.Clone(r.vms), slices.Clone(r.procs)
}

// Connections returns the configurations handed to Connect, in order.
func (r *Runner) Connections() []sandbox.Configuration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.connected)
}

// SetFailSpawn toggles process start failure inside the VM.
func (r *Runner) SetFailSpawn(fail bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failSpawn = fail
}

// vmRunner is one VM's runner connection.
type vmRunner struct {
	r  *Runner
	vm string
}

// Kind identifies the in-VM runner as remote.
func (*vmRunner) Kind() string { return execdriver.KindRemote }

// Describe names the VM.
func (v *vmRunner) Describe() execdriver.DriverInfo { return execdriver.DriverInfo{Host: v.vm} }

// Spawn records the spec and returns a process that runs until finished or cancelled.
func (v *vmRunner) Spawn(_ context.Context, spec execdriver.ProcSpec) (execdriver.Proc, error) {
	v.r.mu.Lock()
	defer v.r.mu.Unlock()
	if v.r.failSpawn {
		return nil, errors.New("sandboxtest: spawn failed")
	}
	p := &Proc{id: spec.ID, VM: v.vm, done: make(chan struct{})}
	v.r.specs = append(v.r.specs, spec)
	v.r.vms = append(v.r.vms, v.vm)
	v.r.procs = append(v.r.procs, p)
	return p, nil
}

// Settle is not used: the Sandbox driver settles by destroying the VM.
func (*vmRunner) Settle(context.Context, string) (execdriver.SettleReport, error) {
	return execdriver.SettleReport{HostProcessesStopped: true}, nil
}

// Proc is a fake in-VM process.
type Proc struct {
	id   string
	VM   string
	once sync.Once
	done chan struct{}
	exit execdriver.ExitInfo
}

// ID returns the process run ID.
func (p *Proc) ID() string { return p.id }

// Subscribe returns a closed stream: the fake produces no output.
func (p *Proc) Subscribe(int) (<-chan execdriver.OutputChunk, func()) {
	c := make(chan execdriver.OutputChunk)
	close(c)
	return c, func() {}
}

// ProtocolStreams returns nil handles (capture mode).
func (p *Proc) ProtocolStreams() (io.WriteCloser, io.ReadCloser) { return nil, nil }

// Progress is a no-op.
func (p *Proc) Progress() {}

// Finish ends the process with the given outcome and exit code.
func (p *Proc) Finish(outcome string, code int) {
	p.once.Do(func() {
		p.exit = execdriver.ExitInfo{ExecutionOutcome: outcome, Work: "complete", DescendantsCleaned: true, ExitCode: code}
		close(p.done)
	})
}

// Cancel ends the process as cancelled.
func (p *Proc) Cancel(context.Context) error {
	p.Finish("cancelled", -1)
	return nil
}

// Wait blocks until the process ends.
func (p *Proc) Wait(ctx context.Context) (execdriver.ExitInfo, error) {
	select {
	case <-p.done:
		return p.exit, nil
	case <-ctx.Done():
		return execdriver.ExitInfo{}, ctx.Err()
	}
}

// Inspector returns declared properties per configuration name.
type Inspector struct {
	Props map[string]sandbox.Properties
	Err   map[string]error
}

// Inspect returns the configuration's declared properties.
func (i Inspector) Inspect(_ context.Context, cfg sandbox.Configuration, _ vmdriver.VM) (sandbox.Properties, error) {
	return i.Props[cfg.Name], i.Err[cfg.Name]
}

// --- Credentials ---

// keychain is an in-memory credentials.Keychain.
type keychain struct {
	mu sync.Mutex
	m  map[string]string
}

// Get returns a stored secret.
func (k *keychain) Get(a string) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	v, ok := k.m[a]
	if !ok {
		return "", errors.New("not found")
	}
	return v, nil
}

// Set stores a secret.
func (k *keychain) Set(a, s string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.m[a] = s
	return nil
}

// Delete removes a secret.
func (k *keychain) Delete(a string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.m, a)
	return nil
}

// Kind names the fake backend.
func (*keychain) Kind() string { return "memory" }

// InfraPassword is the provider credential the fakes hold daemon-side.
const InfraPassword = "infra-pass-7Qx9wLm2Zr"

// Broker returns a real credentials broker holding the provider credential under vm/<Endpoint>.
func Broker() (*credentials.Broker, error) {
	b := credentials.NewBroker(credentials.NewStore(&keychain{m: map[string]string{}}), nil)
	err := b.Put(credentials.UserScope, "vm/"+Endpoint, `{"username":"root","password":"`+InfraPassword+`"}`)
	return b, err
}

// HostKey is a well-formed pinned fingerprint for test configurations.
const HostKey = "SHA256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU"

// Setup bundles a ready Sandbox config on one fake provider.
type Setup struct {
	Provider *Provider
	Journal  *Journal
	Runner   *Runner
	Broker   *credentials.Broker
	Config   sandbox.Config
}

// Clean is a configuration name whose declared properties pass every boundary check.
const Clean = "clean"

// NewSetup wires a fake provider for the named driver with one clean configuration.
func NewSetup(driver string) (*Setup, error) {
	broker, err := Broker()
	if err != nil {
		return nil, err
	}
	matrices, err := Matrices(broker.VM())
	if err != nil {
		return nil, err
	}
	row, ok := matrices[driver]
	if !ok {
		return nil, fmt.Errorf("sandboxtest: unknown driver %q", driver)
	}
	s := &Setup{Provider: NewProvider(row, broker.VM()), Journal: NewJournal(), Runner: &Runner{}, Broker: broker}
	s.Config = sandbox.Config{
		Provider: s.Provider, Journal: s.Journal, Authorizer: Authorizer{},
		Configurations: []sandbox.Configuration{{Name: Clean, Endpoint: Endpoint, Base: "base-golden",
			HostKey: HostKey, WorkDir: "/work", Egress: []string{"proxy.internal:3128"}}},
		Connect: s.Runner.Connect,
		Inspect: Inspector{Props: map[string]sandbox.Properties{
			Clean: {Env: []string{"PATH=/usr/bin"}, Egress: []string{"proxy.internal:3128"}},
		}},
		KnownSecrets: broker.Redactor().Values,
	}
	return s, nil
}

// Drivers names the five V1 drivers.
var Drivers = []string{"esxi", "proxmox", "virtualbox", "workstation", "hyperv"}
