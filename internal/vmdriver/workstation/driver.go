package workstation

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"synch/internal/vmdriver"
)

//go:embed capabilities.json
var matrixJSON []byte

var (
	vmxPath      = regexp.MustCompile(`^([A-Za-z]:[\\/]|/)[^\x00-\x1f"]*\.vmx$`)
	vmName       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)
	snapshotName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,39}$`)
)

// --- Driver, runner and validation ---

// Result is one finished command: its standard output and exit status.
type Result struct {
	Stdout   string
	ExitCode int
}

// Runner executes a command on the named host. argv[0] is the tool name. An
// error means the command's outcome is unknown (timeout, lost connection);
// a non-zero ExitCode is a definite tool failure. Tests inject a fake.
type Runner interface {
	Run(ctx context.Context, endpoint string, argv []string) (Result, error)
}

// Driver drives one or more Workstation hosts: vmrun via Runner, vmrest via HTTPS.
type Driver struct {
	rest   map[string]*url.URL
	runner Runner
	broker vmdriver.Broker
	client *http.Client
}

var _ vmdriver.VMDriver = (*Driver)(nil)

// New registers each host's vmrest HTTPS origin, the vmrun runner and the
// basic-auth credential broker.
func New(hosts map[string]string, runner Runner, broker vmdriver.Broker, client *http.Client) (*Driver, error) {
	parsed := make(map[string]*url.URL, len(hosts))
	for id, raw := range hosts {
		u, err := vmdriver.EndpointURL(raw)
		if err != nil || id == "" {
			return nil, vmdriver.ErrInvalid
		}
		parsed[id] = u
	}
	if runner == nil || broker == nil {
		return nil, vmdriver.ErrInvalid
	}
	return &Driver{rest: parsed, runner: runner, broker: broker, client: vmdriver.HTTPClient(client)}, nil
}

// Capabilities returns a fresh copy of the packaged real-host matrix.
func (d *Driver) Capabilities(_ context.Context) (vmdriver.MatrixRow, error) {
	var row vmdriver.MatrixRow
	err := json.Unmarshal(matrixJSON, &row)
	return row, err
}

// ValidateCommand rejects malformed .vmx paths, names and snapshot names before
// the dispatcher persists a request, so nothing malformed reaches vmrun.
func (d *Driver) ValidateCommand(cmd vmdriver.Command) error {
	if d.rest[cmd.Endpoint] == nil {
		return vmdriver.ErrInvalid
	}
	switch cmd.Operation {
	case vmdriver.CreateOp:
		if !vmxPath.MatchString(cmd.Base) || !vmName.MatchString(cmd.Spec.Name) {
			return vmdriver.ErrInvalid
		}
		return nil
	case vmdriver.SnapshotOp, vmdriver.RestoreOp:
		if !snapshotName.MatchString(cmd.Snapshot) {
			return vmdriver.ErrInvalid
		}
	}
	if !vmxPath.MatchString(cmd.VM) {
		return vmdriver.ErrInvalid
	}
	return nil
}

// vmrun runs `vmrun -T ws args...`. A runner error is uncertain; a non-zero
// exit is a definite failure (ErrProvider for mutations, ErrInvalid for reads).
func (d *Driver) vmrun(ctx context.Context, endpoint string, mutating bool, args ...string) (string, error) {
	res, err := d.runner.Run(ctx, endpoint, append([]string{"vmrun", "-T", "ws"}, args...))
	if err != nil {
		return "", vmdriver.ErrUncertain
	}
	if res.ExitCode != 0 {
		kind := vmdriver.ErrInvalid
		if mutating {
			kind = vmdriver.ErrProvider
		}
		return "", fmt.Errorf("vmrun %s exit %d: %w", args[0], res.ExitCode, kind)
	}
	return res.Stdout, nil
}

// --- Inventory and lifecycle ---

// vmEntry is one vmrest library entry: opaque id and .vmx path.
type vmEntry struct{ ID, Path string }

// library lists the VMs vmrest knows (GET /api/vms: [{id, path}]).
func (d *Driver) library(ctx context.Context, endpoint string) ([]vmEntry, error) {
	var list []struct {
		ID   string `json:"id"`
		Path string `json:"path"`
	}
	if err := d.call(ctx, endpoint, http.MethodGet, "/vms", nil, &list); err != nil {
		return nil, err
	}
	out := make([]vmEntry, 0, len(list))
	for _, e := range list {
		if e.ID != "" && vmxPath.MatchString(e.Path) {
			out = append(out, vmEntry{e.ID, e.Path})
		}
	}
	return out, nil
}

// samePath compares .vmx paths across Windows/Linux separators and case.
func samePath(a, b string) bool {
	norm := func(s string) string { return strings.ToLower(strings.ReplaceAll(s, "\\", "/")) }
	return norm(a) == norm(b)
}

// Inventory lists library VMs; `vmrun list` supplies which are running.
func (d *Driver) Inventory(ctx context.Context, endpoint string) ([]vmdriver.VM, error) {
	lib, err := d.library(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("workstation inventory: %w", err)
	}
	out, err := d.vmrun(ctx, endpoint, false, "list")
	if err != nil {
		return nil, fmt.Errorf("workstation inventory: %w", err)
	}
	vms := make([]vmdriver.VM, 0, len(lib))
	for _, e := range lib {
		state := "poweredOff"
		for line := range strings.SplitSeq(out, "\n") {
			if samePath(strings.TrimSpace(line), e.Path) {
				state = "poweredOn"
			}
		}
		vms = append(vms, vmdriver.VM{Ref: e.Path, Name: baseName(e.Path), State: state})
	}
	return vms, nil
}

// baseName returns the .vmx file name without directory or extension.
func baseName(p string) string {
	p = strings.ReplaceAll(p, "\\", "/")
	return strings.TrimSuffix(p[strings.LastIndex(p, "/")+1:], ".vmx")
}

// CreateFromBase full-clones the base with vmrun, registers the clone in vmrest
// and writes the ownership marker. Once the clone ran, every later failure is
// ErrUncertain: the clone exists, so the request must never settle as failed.
func (d *Driver) CreateFromBase(ctx context.Context, req vmdriver.ExternalRequest, base string, spec vmdriver.VMSpec, thread string) (vmdriver.TaskRef, error) {
	if !vmxPath.MatchString(base) || !vmName.MatchString(spec.Name) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	lib, err := d.library(ctx, req.Endpoint)
	if err != nil {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	dest := cloneDest(base, spec.Name)
	for _, e := range lib {
		if samePath(e.Path, dest) {
			return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
		}
	}
	if _, err := d.vmrun(ctx, req.Endpoint, true, "clone", base, dest, "full", "-cloneName="+spec.Name); err != nil {
		return vmdriver.TaskRef{}, err
	}
	var reg struct {
		ID string `json:"id"`
	}
	body := map[string]string{"name": spec.Name, "path": dest}
	if err := d.call(ctx, req.Endpoint, http.MethodPost, "/vms/registration", body, &reg); err != nil || reg.ID == "" {
		return vmdriver.TaskRef{}, vmdriver.ErrUncertain
	}
	if err := d.annotate(ctx, req.Endpoint, reg.ID, req.Marker(thread)); err != nil {
		return vmdriver.TaskRef{}, vmdriver.ErrUncertain
	}
	return vmdriver.TaskRef{}, nil
}

// cloneDest places the clone beside the base's folder: <parent>/<name>/<name>.vmx.
func cloneDest(base, name string) string {
	sep := "/"
	if strings.Contains(base, "\\") {
		sep = "\\"
	}
	dir := base[:strings.LastIndexAny(base, "\\/")]
	parent := dir[:strings.LastIndexAny(dir, "\\/")+1]
	return parent + name + sep + name + ".vmx"
}

// Start powers the VM on without a GUI window.
func (d *Driver) Start(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.simple(ctx, req.Endpoint, vm, "start", vm, "nogui")
}

// Stop powers the VM off (hard), matching the other drivers' power-off semantics.
func (d *Driver) Stop(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.simple(ctx, req.Endpoint, vm, "stop", vm, "hard")
}

// Delete removes the VM and its files from disk.
func (d *Driver) Delete(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.simple(ctx, req.Endpoint, vm, "deleteVM", vm)
}

// simple validates the .vmx ref, then runs one mutating vmrun command.
func (d *Driver) simple(ctx context.Context, endpoint, vm string, args ...string) (vmdriver.TaskRef, error) {
	if !vmxPath.MatchString(vm) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	_, err := d.vmrun(ctx, endpoint, true, args...)
	return vmdriver.TaskRef{}, err
}

// GetTask is unavailable: vmrun and vmrest expose no task API (Q-40).
func (d *Driver) GetTask(context.Context, string, vmdriver.TaskRef) (vmdriver.TaskStatus, error) {
	return vmdriver.TaskStatus{}, vmdriver.ErrUnavailable
}

// --- Snapshots: verified before restore (vm-driver §3.8) ---

// snapshots parses `vmrun listSnapshots`: "Total snapshots: N" then N names.
// A count that disagrees with the listed names is treated as unverifiable.
func (d *Driver) snapshots(ctx context.Context, endpoint, vm string) ([]string, error) {
	out, err := d.vmrun(ctx, endpoint, false, "listSnapshots", vm)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(strings.ReplaceAll(out, "\r", "")), "\n")
	n, ok := strings.CutPrefix(lines[0], "Total snapshots:")
	count, convErr := strconv.Atoi(strings.TrimSpace(n))
	if !ok || convErr != nil || count != len(lines)-1 {
		return nil, vmdriver.ErrSnapshot
	}
	names := make([]string, 0, count)
	for _, l := range lines[1:] {
		names = append(names, strings.TrimSpace(l))
	}
	return names, nil
}

// Snapshot takes a named snapshot. The name must be new, so a later listing
// unambiguously names this snapshot. vmrun has no description, so no token.
func (d *Driver) Snapshot(ctx context.Context, req vmdriver.ExternalRequest, vm, name string) (vmdriver.TaskRef, error) {
	if !vmxPath.MatchString(vm) || !snapshotName.MatchString(name) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	names, err := d.snapshots(ctx, req.Endpoint, vm)
	if err != nil || slices.Contains(names, name) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	_, err = d.vmrun(ctx, req.Endpoint, true, "snapshot", vm, name)
	return vmdriver.TaskRef{}, err
}

// Restore reverts only to a snapshot that is listed exactly once.
func (d *Driver) Restore(ctx context.Context, req vmdriver.ExternalRequest, vm, name string) (vmdriver.TaskRef, error) {
	if !vmxPath.MatchString(vm) || !snapshotName.MatchString(name) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrSnapshot)
	}
	names, err := d.snapshots(ctx, req.Endpoint, vm)
	if err != nil {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(errors.Join(vmdriver.ErrSnapshot, err))
	}
	count := 0
	for _, n := range names {
		if n == name {
			count++
		}
	}
	if count != 1 {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrSnapshot)
	}
	_, err = d.vmrun(ctx, req.Endpoint, true, "revertToSnapshot", vm, name)
	return vmdriver.TaskRef{}, err
}

// --- Orphan and recovery evidence ---

// FindByToken reads each library VM's annotation and returns VMs carrying this
// exact token and the requesting thread's owner line. Unreadable VMs never
// establish absence. Snapshots are not token-carrying on this platform.
func (d *Driver) FindByToken(ctx context.Context, endpoint, token, thread string) ([]vmdriver.ResourceEvidence, error) {
	lib, err := d.library(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	var out []vmdriver.ResourceEvidence
	for _, e := range lib {
		note, err := d.annotation(ctx, endpoint, e.ID)
		if err == nil && vmdriver.CarriesToken(note, token) && vmdriver.CarriesOwner(note, thread) {
			out = append(out, vmdriver.ResourceEvidence{Endpoint: endpoint, VMRef: e.Path,
				Operation: vmdriver.CreateOp, Resource: e.Path, ClientToken: token})
		}
	}
	return out, nil
}

// VerifySnapshot checks a unique snapshot on the specified VM without mutation.
func (d *Driver) VerifySnapshot(ctx context.Context, endpoint, vm, name string) error {
	if !vmxPath.MatchString(vm) || !snapshotName.MatchString(name) {
		return vmdriver.ErrSnapshot
	}
	names, err := d.snapshots(ctx, endpoint, vm)
	if err != nil {
		return errors.Join(vmdriver.ErrSnapshot, err)
	}
	count := 0
	for _, n := range names {
		if n == name {
			count++
		}
	}
	if count != 1 {
		return vmdriver.ErrSnapshot
	}
	return nil
}
