package virtualbox

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"synch/internal/vmdriver"
)

//go:embed capabilities.json
var matrixJSON []byte

var (
	vmUUID       = regexp.MustCompile(`^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$`)
	vmName       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)
	snapshotName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,39}$`)
	threadID     = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	listLine     = regexp.MustCompile(`^"(.*)" \{([0-9a-fA-F-]{36})\}$`)
	extraLine    = regexp.MustCompile(`^Key: (.+?), Value: (.*)$`)
	snapKey      = regexp.MustCompile(`^SnapshotName((?:-\d+)*)$`)
)

// Extradata keys carrying the ownership marker on a created VM.
const (
	tokenKey = "synch/token"
	ownerKey = "synch/owner"
)

// --- Driver, runner and validation ---

// Result is one finished command: output streams and exit status.
type Result struct {
	Stdout, Stderr string
	ExitCode       int
}

// Runner executes a command on the named host. argv[0] is the tool name. An
// error means the outcome is unknown (timeout, lost connection); a non-zero
// ExitCode is a definite tool failure. Tests inject a fake.
type Runner interface {
	Run(ctx context.Context, endpoint string, argv []string) (Result, error)
}

// Driver drives one or more VirtualBox hosts through VBoxManage.
type Driver struct {
	hosts  map[string]bool
	runner Runner
}

var _ vmdriver.VMDriver = (*Driver)(nil)

// New registers the known host endpoints and the VBoxManage runner.
func New(endpoints []string, runner Runner) (*Driver, error) {
	if runner == nil || len(endpoints) == 0 {
		return nil, vmdriver.ErrInvalid
	}
	hosts := make(map[string]bool, len(endpoints))
	for _, e := range endpoints {
		if e == "" {
			return nil, vmdriver.ErrInvalid
		}
		hosts[e] = true
	}
	return &Driver{hosts: hosts, runner: runner}, nil
}

// Capabilities returns a fresh copy of the packaged real-host matrix.
func (d *Driver) Capabilities(_ context.Context) (vmdriver.MatrixRow, error) {
	var row vmdriver.MatrixRow
	err := json.Unmarshal(matrixJSON, &row)
	return row, err
}

// ValidateCommand rejects bad UUID refs, names and thread IDs before the
// dispatcher persists a request, so nothing malformed reaches VBoxManage.
func (d *Driver) ValidateCommand(cmd vmdriver.Command) error {
	if !d.hosts[cmd.Endpoint] || !threadID.MatchString(cmd.Thread) {
		return vmdriver.ErrInvalid
	}
	switch cmd.Operation {
	case vmdriver.CreateOp:
		if !vmUUID.MatchString(cmd.Base) || !vmName.MatchString(cmd.Spec.Name) || vmUUID.MatchString(cmd.Spec.Name) {
			return vmdriver.ErrInvalid
		}
		return nil
	case vmdriver.SnapshotOp, vmdriver.RestoreOp:
		if !snapshotName.MatchString(cmd.Snapshot) {
			return vmdriver.ErrInvalid
		}
	}
	if !vmUUID.MatchString(cmd.VM) {
		return vmdriver.ErrInvalid
	}
	return nil
}

// vbox runs `VBoxManage args...`. A runner error is uncertain; a non-zero exit
// is a definite failure (ErrProvider for mutations, ErrInvalid for reads).
func (d *Driver) vbox(ctx context.Context, endpoint string, mutating bool, args ...string) (Result, error) {
	res, err := d.runner.Run(ctx, endpoint, append([]string{"VBoxManage"}, args...))
	if err != nil {
		return Result{}, vmdriver.ErrUncertain
	}
	if res.ExitCode != 0 {
		kind := vmdriver.ErrInvalid
		if mutating {
			kind = vmdriver.ErrProvider
		}
		return res, fmt.Errorf("VBoxManage %s exit %d: %w", args[0], res.ExitCode, kind)
	}
	return res, nil
}

// --- Inventory and lifecycle ---

// vmList parses `VBoxManage list vms|runningvms`: lines of `"name" {uuid}`.
func (d *Driver) vmList(ctx context.Context, endpoint, kind string) ([]vmdriver.VM, error) {
	res, err := d.vbox(ctx, endpoint, false, "list", kind)
	if err != nil {
		return nil, err
	}
	var vms []vmdriver.VM
	for line := range strings.SplitSeq(res.Stdout, "\n") {
		if m := listLine.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
			vms = append(vms, vmdriver.VM{Ref: strings.ToLower(m[2]), Name: m[1]})
		}
	}
	return vms, nil
}

// Inventory lists registered VMs by UUID; `list runningvms` supplies power state.
func (d *Driver) Inventory(ctx context.Context, endpoint string) ([]vmdriver.VM, error) {
	all, err := d.vmList(ctx, endpoint, "vms")
	if err != nil {
		return nil, fmt.Errorf("virtualbox inventory: %w", err)
	}
	running, err := d.vmList(ctx, endpoint, "runningvms")
	if err != nil {
		return nil, fmt.Errorf("virtualbox inventory: %w", err)
	}
	for i := range all {
		all[i].State = "poweroff"
		if slices.ContainsFunc(running, func(r vmdriver.VM) bool { return r.Ref == all[i].Ref }) {
			all[i].State = "running"
		}
	}
	return all, nil
}

// CreateFromBase full-clones the base, then writes the token and owner line as
// extradata. Once clonevm ran, every later failure is ErrUncertain: the clone
// exists, so the request must never settle as failed.
func (d *Driver) CreateFromBase(ctx context.Context, req vmdriver.ExternalRequest, base string, spec vmdriver.VMSpec, thread string) (vmdriver.TaskRef, error) {
	if !vmUUID.MatchString(base) || !vmName.MatchString(spec.Name) || vmUUID.MatchString(spec.Name) || !threadID.MatchString(thread) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	vms, err := d.vmList(ctx, req.Endpoint, "vms")
	if err != nil || slices.ContainsFunc(vms, func(v vmdriver.VM) bool { return v.Name == spec.Name }) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	if _, err := d.vbox(ctx, req.Endpoint, true, "clonevm", base, "--name", spec.Name, "--register"); err != nil {
		return vmdriver.TaskRef{}, err
	}
	after, err := d.vmList(ctx, req.Endpoint, "vms")
	if err != nil {
		return vmdriver.TaskRef{}, vmdriver.ErrUncertain
	}
	i := slices.IndexFunc(after, func(v vmdriver.VM) bool { return v.Name == spec.Name })
	if i < 0 {
		return vmdriver.TaskRef{}, vmdriver.ErrUncertain
	}
	for _, kv := range [][2]string{{tokenKey, req.Token()}, {ownerKey, vmdriver.OwnerLine(thread)}} {
		if _, err := d.vbox(ctx, req.Endpoint, true, "setextradata", after[i].Ref, kv[0], kv[1]); err != nil {
			return vmdriver.TaskRef{}, vmdriver.ErrUncertain
		}
	}
	return vmdriver.TaskRef{}, nil
}

// Start powers the VM on headless.
func (d *Driver) Start(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.simple(ctx, req.Endpoint, vm, "startvm", vm, "--type", "headless")
}

// Stop powers the VM off (hard), matching the other drivers' power-off semantics.
func (d *Driver) Stop(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.simple(ctx, req.Endpoint, vm, "controlvm", vm, "poweroff")
}

// Delete unregisters the VM and deletes its files.
func (d *Driver) Delete(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.simple(ctx, req.Endpoint, vm, "unregistervm", vm, "--delete")
}

// simple validates the UUID ref, then runs one mutating VBoxManage command.
func (d *Driver) simple(ctx context.Context, endpoint, vm string, args ...string) (vmdriver.TaskRef, error) {
	if !vmUUID.MatchString(vm) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	_, err := d.vbox(ctx, endpoint, true, args...)
	return vmdriver.TaskRef{}, err
}

// GetTask is unavailable: VBoxManage exposes no task API (Q-40).
func (d *Driver) GetTask(context.Context, string, vmdriver.TaskRef) (vmdriver.TaskStatus, error) {
	return vmdriver.TaskStatus{}, vmdriver.ErrUnavailable
}

// --- Snapshots: verified before restore (vm-driver §3.8) ---

// snap is one snapshot from `snapshot list --machinereadable`.
type snap struct{ Name, UUID, Description string }

// snapshots parses the machine-readable tree (SnapshotName[-n]=..., SnapshotUUID,
// SnapshotDescription). A VM without snapshots exits 1 with a known message.
func (d *Driver) snapshots(ctx context.Context, endpoint, vm string) ([]snap, error) {
	res, err := d.vbox(ctx, endpoint, false, "snapshot", vm, "list", "--machinereadable")
	if err != nil {
		if res.ExitCode != 0 && strings.Contains(res.Stderr, "does not have any snapshots") {
			return nil, nil
		}
		return nil, err
	}
	kv := map[string]string{}
	for line := range strings.SplitSeq(res.Stdout, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if ok {
			kv[k] = strings.Trim(v, `"`)
		}
	}
	var out []snap
	for k, name := range kv {
		if m := snapKey.FindStringSubmatch(k); m != nil {
			out = append(out, snap{name, kv["SnapshotUUID"+m[1]], kv["SnapshotDescription"+m[1]]})
		}
	}
	return out, nil
}

// verified reports whether name is listed exactly once with a UUID, so a
// restore or token match never relies on a half-written or ambiguous entry.
func verified(list []snap, name string) (snap, bool) {
	var found snap
	n := 0
	for _, s := range list {
		if s.Name == name {
			found, n = s, n+1
		}
	}
	return found, n == 1 && found.UUID != ""
}

// Snapshot takes a snapshot whose description is the request token. The name
// must be new, so a later listing unambiguously identifies this snapshot.
func (d *Driver) Snapshot(ctx context.Context, req vmdriver.ExternalRequest, vm, name string) (vmdriver.TaskRef, error) {
	if !vmUUID.MatchString(vm) || !snapshotName.MatchString(name) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	list, err := d.snapshots(ctx, req.Endpoint, vm)
	if err != nil || slices.ContainsFunc(list, func(s snap) bool { return s.Name == name }) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	_, err = d.vbox(ctx, req.Endpoint, true, "snapshot", vm, "take", name, "--description", req.Token())
	return vmdriver.TaskRef{}, err
}

// Restore reverts only to a verified snapshot.
func (d *Driver) Restore(ctx context.Context, req vmdriver.ExternalRequest, vm, name string) (vmdriver.TaskRef, error) {
	if !vmUUID.MatchString(vm) || !snapshotName.MatchString(name) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrSnapshot)
	}
	list, err := d.snapshots(ctx, req.Endpoint, vm)
	if err != nil {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(errors.Join(vmdriver.ErrSnapshot, err))
	}
	if _, ok := verified(list, name); !ok {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrSnapshot)
	}
	_, err = d.vbox(ctx, req.Endpoint, true, "snapshot", vm, "restore", name)
	return vmdriver.TaskRef{}, err
}

// --- Orphan and recovery evidence ---

// extradata reads `getextradata <vm> enumerate` into a key/value map.
func (d *Driver) extradata(ctx context.Context, endpoint, vm string) (map[string]string, error) {
	res, err := d.vbox(ctx, endpoint, false, "getextradata", vm, "enumerate")
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for line := range strings.SplitSeq(res.Stdout, "\n") {
		if m := extraLine.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
			out[m[1]] = m[2]
		}
	}
	return out, nil
}

// FindByToken returns VMs whose token extradata equals the token and whose
// owner line names thread, and verified snapshots whose description is the
// token. Unreadable VMs never establish absence.
func (d *Driver) FindByToken(ctx context.Context, endpoint, token, thread string) ([]vmdriver.ResourceEvidence, error) {
	vms, err := d.vmList(ctx, endpoint, "vms")
	if err != nil {
		return nil, err
	}
	var out []vmdriver.ResourceEvidence
	for _, v := range vms {
		if x, err := d.extradata(ctx, endpoint, v.Ref); err == nil && x[tokenKey] == token &&
			vmdriver.CarriesOwner(x[ownerKey], thread) {
			out = append(out, vmdriver.ResourceEvidence{Endpoint: endpoint, VMRef: v.Ref,
				Operation: vmdriver.CreateOp, Resource: v.Ref, ClientToken: token})
		}
		list, err := d.snapshots(ctx, endpoint, v.Ref)
		if err != nil {
			continue
		}
		for _, s := range list {
			if _, ok := verified(list, s.Name); ok && vmdriver.CarriesToken(s.Description, token) {
				out = append(out, vmdriver.ResourceEvidence{Endpoint: endpoint, VMRef: v.Ref,
					Operation: vmdriver.SnapshotOp, Resource: v.Ref + "/" + s.Name, ClientToken: token})
			}
		}
	}
	return out, nil
}
