package proxmox

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"synch/internal/vmdriver"
)

type snapshot struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Time        int64  `json:"snaptime"`
	State       string `json:"snapstate"`
}

// Snapshot submits a snapshot whose description records the request token.
func (d *Driver) Snapshot(ctx context.Context, req vmdriver.ExternalRequest, vm, name string) (vmdriver.TaskRef, error) {
	if !snapshotName.MatchString(name) || name == "current" {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	path, err := d.path(ctx, req.Endpoint, vm)
	if err != nil {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(err)
	}
	form := url.Values{"snapname": {name}, "description": {req.Token()}}
	return d.task(ctx, req.Endpoint, http.MethodPost, path+"/snapshot", form)
}

// Restore rolls back only a listed snapshot with matching completed configuration.
func (d *Driver) Restore(ctx context.Context, req vmdriver.ExternalRequest, vm, name string) (vmdriver.TaskRef, error) {
	if !snapshotName.MatchString(name) || name == "current" {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrSnapshot)
	}
	path, err := d.path(ctx, req.Endpoint, vm)
	if err != nil {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(err)
	}
	list, err := d.snapshots(ctx, req.Endpoint, path)
	if err != nil {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(errors.Join(vmdriver.ErrSnapshot, err))
	}
	for _, item := range list {
		if item.Name == name && d.verify(ctx, req.Endpoint, path, item) {
			return d.task(ctx, req.Endpoint, http.MethodPost, path+"/snapshot/"+name+"/rollback", nil)
		}
	}
	return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrSnapshot)
}

// snapshots reads the QEMU snapshot tree's flat API representation.
func (d *Driver) snapshots(ctx context.Context, endpoint, path string) ([]snapshot, error) {
	var list []snapshot
	err := d.call(ctx, endpoint, http.MethodGet, path+"/snapshot", nil, &list)
	return list, err
}

// verify confirms snapshot metadata independently against its stored configuration.
func (d *Driver) verify(ctx context.Context, endpoint, path string, item snapshot) bool {
	if !snapshotName.MatchString(item.Name) || item.Name == "current" || item.Time <= 0 || item.State != "" {
		return false
	}
	var config snapshot
	err := d.call(ctx, endpoint, http.MethodGet, path+"/snapshot/"+item.Name+"/config", nil, &config)
	return err == nil && config.State == "" && config.Time == item.Time && config.Description == item.Description
}

// FindByToken inspects completed VM and snapshot descriptions on this endpoint.
func (d *Driver) FindByToken(ctx context.Context, endpoint, token, thread string) ([]vmdriver.ResourceEvidence, error) {
	vms, err := d.machines(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	var results []vmdriver.ResourceEvidence
	for _, vm := range vms {
		matches := d.findVMToken(ctx, endpoint, token, thread, vm)
		results = append(results, matches...)
	}
	return results, nil
}

// findVMToken ignores unreadable objects, which never establish absence.
func (d *Driver) findVMToken(ctx context.Context, endpoint, token, thread string, vm machine) []vmdriver.ResourceEvidence {
	path := machinePath(vm)
	ref := strconv.Itoa(vm.VMID)
	var results []vmdriver.ResourceEvidence
	var config struct {
		Description string `json:"description"`
		Lock        string `json:"lock"`
	}
	if err := d.call(ctx, endpoint, http.MethodGet, path+"/config", nil, &config); err == nil &&
		config.Lock == "" && vmdriver.CarriesToken(config.Description, token) &&
		vmdriver.CarriesOwner(config.Description, thread) {
		results = append(results, vmdriver.ResourceEvidence{Endpoint: endpoint, VMRef: ref,
			Operation: vmdriver.CreateOp, Resource: ref, ClientToken: token})
	}
	list, err := d.snapshots(ctx, endpoint, path)
	if err != nil {
		return results
	}
	for _, item := range list {
		if vmdriver.CarriesToken(item.Description, token) && d.verify(ctx, endpoint, path, item) {
			results = append(results, vmdriver.ResourceEvidence{Endpoint: endpoint, VMRef: ref,
				Operation: vmdriver.SnapshotOp, Resource: ref + "/" + item.Name, ClientToken: token})
		}
	}
	return results
}

// VerifySnapshot confirms a listed snapshot's metadata against its stored configuration without restoring it.
func (d *Driver) VerifySnapshot(ctx context.Context, endpoint, vm, name string) error {
	if !snapshotName.MatchString(name) || name == "current" {
		return vmdriver.ErrSnapshot
	}
	path, err := d.path(ctx, endpoint, vm)
	if err != nil {
		return errors.Join(vmdriver.ErrSnapshot, err)
	}
	list, err := d.snapshots(ctx, endpoint, path)
	if err != nil {
		return errors.Join(vmdriver.ErrSnapshot, err)
	}
	for _, item := range list {
		if item.Name == name && d.verify(ctx, endpoint, path, item) {
			return nil
		}
	}
	return vmdriver.ErrSnapshot
}
