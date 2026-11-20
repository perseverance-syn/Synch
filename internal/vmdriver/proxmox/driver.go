package proxmox

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"synch/internal/vmdriver"
)

//go:embed capabilities.json
var matrixJSON []byte

var (
	nodeName     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]*$`)
	snapshotName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,39}$`)
)

// --- Driver and validation ---

// Driver talks to Proxmox VE over HTTPS with broker-supplied API tokens.
type Driver struct {
	endpoints map[string]*url.URL
	broker    vmdriver.Broker
	client    *http.Client
}

var _ vmdriver.VMDriver = (*Driver)(nil)

// New configures registered Proxmox HTTPS origins and an API-token broker.
func New(endpoints map[string]string, broker vmdriver.Broker, client *http.Client) (*Driver, error) {
	parsed := make(map[string]*url.URL, len(endpoints))
	for id, raw := range endpoints {
		u, err := vmdriver.EndpointURL(raw)
		if err != nil || id == "" {
			return nil, vmdriver.ErrInvalid
		}
		parsed[id] = u
	}
	if broker == nil {
		return nil, vmdriver.ErrInvalid
	}
	return &Driver{endpoints: parsed, broker: broker, client: vmdriver.HTTPClient(client)}, nil
}

// Capabilities returns a fresh copy of the packaged real-host matrix.
func (d *Driver) Capabilities(_ context.Context) (vmdriver.MatrixRow, error) {
	var row vmdriver.MatrixRow
	err := json.Unmarshal(matrixJSON, &row)
	return row, err
}

// ValidateCommand rejects bad VM references and snapshot names before admission.
func (d *Driver) ValidateCommand(cmd vmdriver.Command) error {
	if d.endpoints[cmd.Endpoint] == nil {
		return vmdriver.ErrInvalid
	}
	ref := cmd.VM
	if cmd.Operation == vmdriver.CreateOp {
		ref = cmd.Base
	}
	id, err := strconv.Atoi(ref)
	if err != nil || id <= 0 || strconv.Itoa(id) != ref {
		return vmdriver.ErrInvalid
	}
	if (cmd.Operation == vmdriver.SnapshotOp || cmd.Operation == vmdriver.RestoreOp) &&
		(!snapshotName.MatchString(cmd.Snapshot) || cmd.Snapshot == "current") {
		return vmdriver.ErrInvalid
	}
	return nil
}

type machine struct {
	VMID   int    `json:"vmid"`
	Node   string `json:"node"`
	Type   string `json:"type"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

// machines retrieves QEMU inventory and omits LXC containers.
func (d *Driver) machines(ctx context.Context, endpoint string) ([]machine, error) {
	var list []machine
	if err := d.call(ctx, endpoint, http.MethodGet, "/cluster/resources?type=vm", nil, &list); err != nil {
		return nil, fmt.Errorf("proxmox inventory: %w", err)
	}
	vms := make([]machine, 0, len(list))
	for _, vm := range list {
		if vm.Type == "qemu" && vm.VMID > 0 && nodeName.MatchString(vm.Node) {
			vms = append(vms, vm)
		}
	}
	return vms, nil
}

// Inventory lists cluster QEMU VMs without granting mutation rights.
func (d *Driver) Inventory(ctx context.Context, endpoint string) ([]vmdriver.VM, error) {
	list, err := d.machines(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	vms := make([]vmdriver.VM, 0, len(list))
	for _, vm := range list {
		vms = append(vms, vmdriver.VM{Ref: strconv.Itoa(vm.VMID), Name: vm.Name, State: vm.Status})
	}
	return vms, nil
}

// path locates a VM by cluster-stable numeric ref even after a node migration.
func (d *Driver) path(ctx context.Context, endpoint, ref string) (string, error) {
	list, err := d.machines(ctx, endpoint)
	if err != nil {
		return "", err
	}
	for _, vm := range list {
		if strconv.Itoa(vm.VMID) == ref {
			return machinePath(vm), nil
		}
	}
	return "", vmdriver.ErrInvalid
}

// machinePath builds an API path from previously validated inventory fields.
func machinePath(vm machine) string {
	return fmt.Sprintf("/nodes/%s/qemu/%d", vm.Node, vm.VMID)
}

// CreateFromBase submits a full clone with a recovery marker in its description.
func (d *Driver) CreateFromBase(ctx context.Context, req vmdriver.ExternalRequest, base string, spec vmdriver.VMSpec, thread string) (vmdriver.TaskRef, error) {
	path, err := d.path(ctx, req.Endpoint, base)
	if err != nil {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(err)
	}
	var id int
	if err := d.call(ctx, req.Endpoint, http.MethodGet, "/cluster/nextid", nil, &id); err != nil {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(err)
	}
	if id <= 0 {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	form := url.Values{"newid": {strconv.Itoa(id)}, "name": {spec.Name},
		"description": {req.Marker(thread)}, "full": {"1"}}
	return d.task(ctx, req.Endpoint, http.MethodPost, path+"/clone", form)
}

// Start submits an asynchronous QEMU power-on operation.
func (d *Driver) Start(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.vmTask(ctx, req.Endpoint, vm, http.MethodPost, "/status/start")
}

// Stop submits an asynchronous QEMU power-off operation.
func (d *Driver) Stop(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.vmTask(ctx, req.Endpoint, vm, http.MethodPost, "/status/stop")
}

// Delete removes the owned VM and its disks through the QEMU destroy endpoint.
func (d *Driver) Delete(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.vmTask(ctx, req.Endpoint, vm, http.MethodDelete, "")
}

// vmTask resolves the current VM location before submitting a lifecycle task.
func (d *Driver) vmTask(ctx context.Context, endpoint, vm, method, suffix string) (vmdriver.TaskRef, error) {
	path, err := d.path(ctx, endpoint, vm)
	if err != nil {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(err)
	}
	return d.task(ctx, endpoint, method, path+suffix, nil)
}

// --- Provider task binding and status (vm-driver §3) ---

// task submits one mutation and validates the provider task identifier.
func (d *Driver) task(ctx context.Context, endpoint, method, path string, form url.Values) (vmdriver.TaskRef, error) {
	var id string
	if err := d.call(ctx, endpoint, method, path, form, &id); err != nil {
		return vmdriver.TaskRef{}, fmt.Errorf("proxmox submit task: %w", err)
	}
	if _, ok := taskNode(id); !ok {
		return vmdriver.TaskRef{}, vmdriver.ErrUncertain
	}
	return vmdriver.TaskRef{ID: id}, nil
}

// taskNode extracts the original node from a UPID without following provider URLs.
func taskNode(id string) (string, bool) {
	parts := strings.Split(id, ":")
	if len(parts) < 9 || parts[0] != "UPID" || !nodeName.MatchString(parts[1]) || strings.ContainsAny(id, "/\\\r\n") {
		return "", false
	}
	return parts[1], true
}

// GetTask queries the node that issued the UPID and classifies terminal status.
func (d *Driver) GetTask(ctx context.Context, endpoint string, task vmdriver.TaskRef) (vmdriver.TaskStatus, error) {
	node, ok := taskNode(task.ID)
	if !ok {
		return vmdriver.TaskStatus{}, vmdriver.ErrInvalid
	}
	var result struct {
		Status string `json:"status"`
		Exit   string `json:"exitstatus"`
	}
	path := "/nodes/" + node + "/tasks/" + url.PathEscape(task.ID) + "/status"
	if err := d.call(ctx, endpoint, http.MethodGet, path, nil, &result); err != nil {
		return vmdriver.TaskStatus{}, err
	}
	state := vmdriver.Unknown
	if result.Status == "stopped" && result.Exit != "" {
		state = vmdriver.Failed
		if result.Exit == "OK" {
			state = vmdriver.Succeeded
		}
	}
	return vmdriver.TaskStatus{State: state, InFlight: result.Status == "running"}, nil
}
