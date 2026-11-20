package esxi

import (
	"context"
	"encoding/xml"
	"time"

	"synch/internal/vmdriver"
)

type snapshot struct {
	Ref         ref        `xml:"snapshot"`
	VM          ref        `xml:"vm"`
	Name        string     `xml:"name"`
	Description string     `xml:"description"`
	Created     time.Time  `xml:"createTime"`
	State       string     `xml:"state"`
	Children    []snapshot `xml:"childSnapshotList"`
}

// Snapshot submits a disk snapshot with the exact token in its description.
func (d *Driver) Snapshot(ctx context.Context, req vmdriver.ExternalRequest, vm, name string) (vmdriver.TaskRef, error) {
	if name == "" {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	args := tag("name", name) + tag("description", req.Token()) + "<memory>false</memory><quiesce>false</quiesce>"
	return d.vmTask(ctx, req.Endpoint, vm, "CreateSnapshot_Task", args)
}

// Restore reverts a unique listed snapshot after verifying the live object.
func (d *Driver) Restore(ctx context.Context, req vmdriver.ExternalRequest, vm, name string) (vmdriver.TaskRef, error) {
	obj := ref{"VirtualMachine", vm}
	if !obj.valid("VirtualMachine") || name == "" {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrSnapshot)
	}
	s, err := d.open(ctx, req.Endpoint)
	if err != nil {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(err)
	}
	defer s.close(ctx)
	props, err := s.get(ctx, obj, "snapshot")
	if err != nil {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrSnapshot)
	}
	var found snapshot
	for _, snap := range snapshots(props) {
		if snap.Name != name && snap.Ref.ID != name {
			continue
		}
		if found.Ref.ID != "" {
			return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrSnapshot)
		}
		found = snap
	}
	if !s.verify(ctx, obj, found) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrSnapshot)
	}
	return s.task(ctx, found.Ref, "RevertToSnapshot_Task", "")
}

// snapshots flattens the provider tree, including descendant snapshots.
func snapshots(props properties) []snapshot {
	var tree struct {
		Roots []snapshot `xml:"rootSnapshotList"`
	}
	if xml.Unmarshal(props.value("snapshot"), &tree) != nil {
		return nil
	}
	list := tree.Roots
	for i := 0; i < len(list); i++ {
		list = append(list, list[i].Children...)
	}
	return list
}

// verify checks tree membership against the live snapshot object and configuration.
func (s *session) verify(ctx context.Context, vm ref, snap snapshot) bool {
	if !snap.Ref.valid("VirtualMachineSnapshot") || snap.VM != vm || snap.Created.IsZero() ||
		(snap.State != "poweredOn" && snap.State != "poweredOff" && snap.State != "suspended") {
		return false
	}
	props, err := s.get(ctx, snap.Ref, "vm", "config.name")
	return err == nil && props.reference("vm") == vm && props.text("config.name") != ""
}

// FindByToken finds exact matches in annotations and verified snapshot descriptions.
func (d *Driver) FindByToken(ctx context.Context, endpoint, token, thread string) ([]vmdriver.ResourceEvidence, error) {
	s, err := d.open(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	defer s.close(ctx)
	vms, err := s.virtualMachines(ctx, "config.annotation", "snapshot")
	if err != nil {
		return nil, err
	}
	var evidence []vmdriver.ResourceEvidence
	for _, vm := range vms {
		if !vm.Ref.valid("VirtualMachine") {
			continue
		}
		if vmdriver.CarriesToken(vm.text("config.annotation"), token) &&
			vmdriver.CarriesOwner(vm.text("config.annotation"), thread) {
			evidence = append(evidence, vmdriver.ResourceEvidence{Endpoint: endpoint, VMRef: vm.Ref.ID,
				Operation: vmdriver.CreateOp, Resource: vm.Ref.ID, ClientToken: token})
		}
		for _, snap := range snapshots(vm) {
			if vmdriver.CarriesToken(snap.Description, token) && s.verify(ctx, vm.Ref, snap) {
				evidence = append(evidence, vmdriver.ResourceEvidence{Endpoint: endpoint, VMRef: vm.Ref.ID,
					Operation: vmdriver.SnapshotOp, Resource: snap.Ref.ID, ClientToken: token})
			}
		}
	}
	return evidence, nil
}
