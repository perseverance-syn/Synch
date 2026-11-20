package hyperv

import (
	"context"
	"strings"

	"synch/internal/vmdriver"
)

// checkpoint is the identity and parent identity projected from Get-VMCheckpoint.
type checkpoint struct {
	ID, VMID, Name string
}

const checkpointListScript = `param($id)
$ErrorActionPreference = 'Stop'
$vm = Get-VM -Id ([guid]$id)
$rows = @(Get-VMCheckpoint -VM $vm | ForEach-Object {
    @{ Id = [string]$_.Id; VMId = [string]$_.VMId; Name = $_.Name }
})
ConvertTo-Json -InputObject $rows -Compress`

// checkpoints lists checkpoint identities for a single exact VM.
func (d *Driver) checkpoints(ctx context.Context, endpoint, vm string) ([]checkpoint, error) {
	var rows []checkpoint
	err := d.execute(ctx, endpoint, checkpointListScript, &rows, false, vm)
	return rows, err
}

// checkpointMarker decodes the checkpoint name used in lieu of writable Notes.
func checkpointMarker(name string) (labelPart, token, owner string) {
	parts := strings.Split(name, "~")
	if len(parts) == 3 && label.MatchString(parts[0]) && strings.HasPrefix(parts[1], "synch-") &&
		requestID.MatchString(strings.TrimPrefix(parts[1], "synch-")) && label.MatchString(parts[2]) {
		return parts[0], parts[1], parts[2]
	}
	return name, "", ""
}

// verifiedCheckpoint requires a unique listed name or ID with an exact parent and valid identity.
func verifiedCheckpoint(list []checkpoint, vm, name string) (checkpoint, bool) {
	var found checkpoint
	count := 0
	for _, cp := range list {
		logical, _, _ := checkpointMarker(cp.Name)
		if strings.EqualFold(cp.Name, name) || strings.EqualFold(logical, name) || strings.EqualFold(cp.ID, name) {
			found = cp
			count++
		}
	}
	return found, count == 1 && validID(found.ID) && strings.EqualFold(found.VMID, vm) && found.Name != ""
}

const checkpointScript = `param($id, $logicalName, $name)
$ErrorActionPreference = 'Stop'
$vm = Get-VM -Id ([guid]$id)
if (@(Get-VMCheckpoint -VM $vm | Where-Object {
    $_.Name -eq $logicalName -or $_.Name.StartsWith($logicalName + '~', [StringComparison]::OrdinalIgnoreCase)
}).Count -ne 0) {
    ConvertTo-Json -InputObject $false -Compress
    return
}
Checkpoint-VM -VM $vm -SnapshotName $name | Out-Null
ConvertTo-Json -InputObject $true -Compress`

// Snapshot embeds the durable request token and acting owner when creating a checkpoint.
func (d *Driver) Snapshot(ctx context.Context, req vmdriver.ExternalRequest, vm, name string) (vmdriver.TaskRef, error) {
	if !validID(vm) || !label.MatchString(name) || !requestID.MatchString(req.ID) || d.requests == nil {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	// Snapshot has no thread argument. Read the same durable request the dispatcher sent.
	record, err := d.requests.Load(ctx, req.ID)
	if err != nil || record.Request != req || req.Operation != vmdriver.SnapshotOp ||
		req.Resource != vm || !label.MatchString(record.Thread) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	return d.mutate(ctx, req.Endpoint, checkpointScript, vm, name, name+"~"+req.Token()+"~"+record.Thread)
}

const restoreScript = `param($id, $checkpointID, $expectedName)
$ErrorActionPreference = 'Stop'
$vm = Get-VM -Id ([guid]$id)
# Recheck the identity on the host immediately before using the checkpoint object.
$matches = @(Get-VMCheckpoint -VM $vm | Where-Object {
    $_.Id -eq ([guid]$checkpointID) -and $_.VMId -eq $vm.Id -and $_.Name -ceq $expectedName
})
if ($matches.Count -ne 1) {
    ConvertTo-Json -InputObject $false -Compress
    return
}
Restore-VMSnapshot -VMSnapshot $matches[0] -Confirm:$false | Out-Null
ConvertTo-Json -InputObject $true -Compress`

// Restore applies only a verified checkpoint on the requested VM.
func (d *Driver) Restore(ctx context.Context, req vmdriver.ExternalRequest, vm, name string) (vmdriver.TaskRef, error) {
	if !validID(vm) || strings.TrimSpace(name) == "" {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrSnapshot)
	}
	list, err := d.checkpoints(ctx, req.Endpoint, vm)
	cp, ok := verifiedCheckpoint(list, vm, name)
	if err != nil || !ok {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrSnapshot)
	}
	task, err := d.mutate(ctx, req.Endpoint, restoreScript, vm, cp.ID, cp.Name)
	if vmdriver.IsPreDispatch(err) {
		return task, vmdriver.PreDispatch(vmdriver.ErrSnapshot)
	}
	return task, err
}

// --- Recovery evidence (vm-driver.md section 3) ---

// FindByToken reports exact token and owner matches on VMs and verified checkpoints.
// Missing or unreadable resources cannot establish non-execution.
func (d *Driver) FindByToken(ctx context.Context, endpoint, token, thread string) ([]vmdriver.ResourceEvidence, error) {
	if !strings.HasPrefix(token, "synch-") || !requestID.MatchString(strings.TrimPrefix(token, "synch-")) || !label.MatchString(thread) {
		return nil, vmdriver.ErrInvalid
	}
	vms, err := d.machines(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	var evidence []vmdriver.ResourceEvidence
	for _, vm := range vms {
		if !validID(vm.ID) {
			continue
		}
		ref := strings.ToLower(vm.ID)
		if vmdriver.CarriesToken(vm.Notes, token) && carriesSingleOwner(vm.Notes, thread) {
			evidence = append(evidence, vmdriver.ResourceEvidence{Endpoint: endpoint, VMRef: ref,
				Operation: vmdriver.CreateOp, Resource: ref, ClientToken: token})
		}
		list, err := d.checkpoints(ctx, endpoint, ref)
		if err != nil {
			// An unreadable VM might hide a second matching checkpoint.
			return nil, err
		}
		for _, cp := range list {
			_, foundToken, owner := checkpointMarker(cp.Name)
			if _, ok := verifiedCheckpoint(list, ref, cp.Name); ok && foundToken == token && owner == thread {
				evidence = append(evidence, vmdriver.ResourceEvidence{Endpoint: endpoint, VMRef: ref,
					Operation: vmdriver.SnapshotOp, Resource: strings.ToLower(cp.ID), ClientToken: token})
			}
		}
	}
	return evidence, nil
}

// carriesSingleOwner refuses conflicting ownership lines even when one matches the thread.
func carriesSingleOwner(notes, thread string) bool {
	for line := range strings.SplitSeq(notes, "\n") {
		if line = strings.TrimSuffix(line, "\r"); strings.HasPrefix(line, "synch-owner:") && line != vmdriver.OwnerLine(thread) {
			return false
		}
	}
	return vmdriver.CarriesOwner(notes, thread)
}
