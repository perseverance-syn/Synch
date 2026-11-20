package esxi

import (
	"context"
	_ "embed"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"regexp"

	"synch/internal/vmdriver"
)

//go:embed capabilities.json
var matrixJSON []byte

var managedID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]*$`)

// Endpoint configures a vSphere SDK origin and optional ESXi clone placement.
type Endpoint struct{ URL, Folder, Pool, Host string }

type endpoint struct {
	Endpoint
	origin *url.URL
}

type Driver struct {
	endpoints map[string]endpoint
	broker    vmdriver.Broker
	client    *http.Client
}

var _ vmdriver.VMDriver = (*Driver)(nil)

// New configures registered HTTPS SDK origins and a credential broker.
func New(endpoints map[string]Endpoint, broker vmdriver.Broker, client *http.Client) (*Driver, error) {
	registered := make(map[string]endpoint, len(endpoints))
	for id, cfg := range endpoints {
		u, err := vmdriver.EndpointURL(cfg.URL)
		if err != nil || id == "" {
			return nil, vmdriver.ErrInvalid
		}
		registered[id] = endpoint{Endpoint: cfg, origin: u}
	}
	if broker == nil {
		return nil, vmdriver.ErrInvalid
	}
	return &Driver{endpoints: registered, broker: broker, client: vmdriver.HTTPClient(client)}, nil
}

// Capabilities returns the packaged real-host capability record.
func (d *Driver) Capabilities(_ context.Context) (vmdriver.MatrixRow, error) {
	var row vmdriver.MatrixRow
	err := json.Unmarshal(matrixJSON, &row)
	return row, err
}

// ValidateCommand checks endpoint configuration and references without opening a session.
func (d *Driver) ValidateCommand(cmd vmdriver.Command) error {
	cfg := d.endpoints[cmd.Endpoint]
	if cfg.origin == nil {
		return vmdriver.ErrInvalid
	}
	if cmd.Operation == vmdriver.CreateOp {
		if !managedID.MatchString(cmd.Base) {
			return vmdriver.ErrInvalid
		}
		if !cfg.cloneConfigured() {
			return vmdriver.ErrUnavailable
		}
	} else if !managedID.MatchString(cmd.VM) {
		return vmdriver.ErrInvalid
	}
	return nil
}

// cloneConfigured reports whether a vCenter clone destination was configured.
func (e endpoint) cloneConfigured() bool {
	return managedID.MatchString(e.Folder) && managedID.MatchString(e.Pool) && managedID.MatchString(e.Host)
}

// Inventory lists the VMs visible in the endpoint's inventory view.
func (d *Driver) Inventory(ctx context.Context, endpoint string) ([]vmdriver.VM, error) {
	s, err := d.open(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	defer s.close(ctx)
	objects, err := s.virtualMachines(ctx, "name", "runtime.powerState")
	if err != nil {
		return nil, err
	}
	vms := make([]vmdriver.VM, 0, len(objects))
	for _, vm := range objects {
		if vm.Ref.valid("VirtualMachine") {
			vms = append(vms, vmdriver.VM{Ref: vm.Ref.ID, Name: vm.text("name"), State: vm.text("runtime.powerState")})
		}
	}
	return vms, nil
}

// CreateFromBase clones onto a configured ESXi host and marks the VM annotation.
func (d *Driver) CreateFromBase(ctx context.Context, req vmdriver.ExternalRequest, base string, spec vmdriver.VMSpec, thread string) (vmdriver.TaskRef, error) {
	cfg := d.endpoints[req.Endpoint]
	if !cfg.cloneConfigured() {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrUnavailable)
	}
	location := object("pool", ref{"ResourcePool", cfg.Pool}) + object("host", ref{"HostSystem", cfg.Host})
	args := object("folder", ref{"Folder", cfg.Folder}) + tag("name", spec.Name) +
		"<spec><location>" + location + "</location><template>false</template><config>" +
		tag("annotation", req.Marker(thread)) + "</config><powerOn>false</powerOn></spec>"
	return d.vmTask(ctx, req.Endpoint, base, "CloneVM_Task", args)
}

// Start submits a VM power-on task.
func (d *Driver) Start(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.vmTask(ctx, req.Endpoint, vm, "PowerOnVM_Task", "")
}

// Stop submits a VM power-off task.
func (d *Driver) Stop(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.vmTask(ctx, req.Endpoint, vm, "PowerOffVM_Task", "")
}

// Delete submits a VM destruction task, including provider-managed storage.
func (d *Driver) Delete(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.vmTask(ctx, req.Endpoint, vm, "Destroy_Task", "")
}

// --- Task dispatch and inspection (vm-driver §3) ---

// vmTask opens a session and submits one VM mutation.
func (d *Driver) vmTask(ctx context.Context, endpoint, vm, method, args string) (vmdriver.TaskRef, error) {
	if !managedID.MatchString(vm) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	s, err := d.open(ctx, endpoint)
	if err != nil {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(err)
	}
	defer s.close(ctx)
	return s.task(ctx, ref{"VirtualMachine", vm}, method, args)
}

// GetTask checks terminal status using the bound provider task reference.
func (d *Driver) GetTask(ctx context.Context, endpoint string, task vmdriver.TaskRef) (vmdriver.TaskStatus, error) {
	obj := ref{"Task", task.ID}
	if !obj.valid("Task") {
		return vmdriver.TaskStatus{}, vmdriver.ErrInvalid
	}
	s, err := d.open(ctx, endpoint)
	if err != nil {
		return vmdriver.TaskStatus{}, err
	}
	defer s.close(ctx)
	props, err := s.get(ctx, obj, "info")
	if err != nil {
		return vmdriver.TaskStatus{}, err
	}
	var info struct {
		State  string `xml:"state"`
		Result ref    `xml:"result"`
	}
	if xml.Unmarshal(props.value("info"), &info) != nil {
		return vmdriver.TaskStatus{}, vmdriver.ErrInvalid
	}
	switch info.State {
	case "success":
		status := vmdriver.TaskStatus{State: vmdriver.Succeeded}
		if info.Result.valid("VirtualMachine") {
			status.ResultResource = info.Result.ID
		}
		return status, nil
	case "error":
		return vmdriver.TaskStatus{State: vmdriver.Failed}, nil
	default:
		return vmdriver.TaskStatus{State: vmdriver.Unknown, InFlight: info.State == "running" || info.State == "queued"}, nil
	}
}
