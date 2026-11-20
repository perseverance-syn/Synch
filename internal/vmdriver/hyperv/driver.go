package hyperv

import (
	"context"
	_ "embed"
	"encoding/json"
	"regexp"
	"strings"

	"synch/internal/vmdriver"
)

//go:embed capabilities.json
var matrixJSON []byte

var (
	guid      = regexp.MustCompile("^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$")
	requestID = regexp.MustCompile("^[0-7][0-9A-HJKMNP-TV-Z]{25}$")
	label     = regexp.MustCompile("^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$")
)

// Capabilities reads a fresh copy of the deferred real-host matrix.
func (d *Driver) Capabilities(context.Context) (vmdriver.MatrixRow, error) {
	var matrix vmdriver.MatrixRow
	err := json.Unmarshal(matrixJSON, &matrix)
	return matrix, err
}

// machine is the stable JSON projection of Get-VM objects.
type machine struct {
	ID, Name, State, Notes string
}

// validID rejects malformed IDs and Hyper-V's empty GUID.
func validID(id string) bool {
	return guid.MatchString(id) && id != "00000000-0000-0000-0000-000000000000"
}

const inventoryScript = `$ErrorActionPreference = 'Stop'
$rows = @(Get-VM | ForEach-Object {
    @{ Id = [string]$_.Id; Name = $_.Name; State = [string]$_.State; Notes = $_.Notes }
})
ConvertTo-Json -InputObject $rows -Compress`

// machines reads VM identities and Notes from the registered endpoint.
func (d *Driver) machines(ctx context.Context, endpoint string) ([]machine, error) {
	var rows []machine
	if err := d.execute(ctx, endpoint, inventoryScript, &rows, false); err != nil {
		return nil, err
	}
	return rows, nil
}

// Inventory returns stable VM IDs, display names and current power states.
func (d *Driver) Inventory(ctx context.Context, endpoint string) ([]vmdriver.VM, error) {
	rows, err := d.machines(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	vms := make([]vmdriver.VM, 0, len(rows))
	for _, row := range rows {
		if !validID(row.ID) || row.Name == "" || row.State == "" {
			return nil, vmdriver.ErrInvalid
		}
		vms = append(vms, vmdriver.VM{Ref: strings.ToLower(row.ID), Name: row.Name, State: row.State})
	}
	return vms, nil
}

// --- Lifecycle operations (vm-driver.md sections 1–3) ---

const cloneScript = `param($base, $name, $token, $notes)
$ErrorActionPreference = 'Stop'
if (@(Get-VM | Where-Object { $_.Name -eq $name }).Count -ne 0) {
    ConvertTo-Json -InputObject $false -Compress
    return
}
$hostConfig = Get-VMHost
$configPath = Join-Path $hostConfig.VirtualMachinePath $token
$diskPath = Join-Path $hostConfig.VirtualHardDiskPath $token
if ((Test-Path -LiteralPath $configPath) -or (Test-Path -LiteralPath $diskPath)) {
    ConvertTo-Json -InputObject $false -Compress
    return
}
# The token gives the import its own configuration and disk directories.
New-Item -ItemType Directory -Path $configPath -Force | Out-Null
New-Item -ItemType Directory -Path $diskPath -Force | Out-Null
$vm = Import-VM -Path $base -Copy -GenerateNewId -VirtualMachinePath $configPath -VhdDestinationPath $diskPath
Rename-VM -VM $vm -NewName $name
# Publish attribution only after the clone has its requested name.
Set-VM -VM $vm -Notes $notes
ConvertTo-Json -InputObject $true -Compress`

// CreateFromBase copies a configured export to a fresh VM and writes its owner and token.
func (d *Driver) CreateFromBase(ctx context.Context, req vmdriver.ExternalRequest, base string, spec vmdriver.VMSpec, thread string) (vmdriver.TaskRef, error) {
	path := d.hosts[req.Endpoint].Exports[base]
	if path == "" || !label.MatchString(spec.Name) || !label.MatchString(thread) || !requestID.MatchString(req.ID) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	return d.mutate(ctx, req.Endpoint, cloneScript, path, spec.Name, req.Token(), req.Marker(thread))
}

// Start powers on a VM identified by its GUID.
func (d *Driver) Start(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.vmCommand(ctx, req, vm, "Start-VM -VM $vm")
}

// Stop powers off a VM without waiting on a guest shutdown.
func (d *Driver) Stop(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.vmCommand(ctx, req, vm, "Stop-VM -VM $vm -TurnOff -Confirm:$false")
}

// Delete removes the VM registration and configuration through Hyper-V.
func (d *Driver) Delete(ctx context.Context, req vmdriver.ExternalRequest, vm string) (vmdriver.TaskRef, error) {
	return d.vmCommand(ctx, req, vm, "Remove-VM -VM $vm -Force")
}

// vmCommand resolves an exact GUID and invokes a fixed lifecycle cmdlet.
func (d *Driver) vmCommand(ctx context.Context, req vmdriver.ExternalRequest, vm, cmdlet string) (vmdriver.TaskRef, error) {
	if !validID(vm) {
		return vmdriver.TaskRef{}, vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	script := `param($id)
$ErrorActionPreference = 'Stop'
$vm = Get-VM -Id ([guid]$id)
` + cmdlet + ` | Out-Null
ConvertTo-Json -InputObject $true -Compress`
	return d.mutate(ctx, req.Endpoint, script, vm)
}

// GetTask is unavailable because these Hyper-V cmdlet invocations run synchronously.
func (d *Driver) GetTask(context.Context, string, vmdriver.TaskRef) (vmdriver.TaskStatus, error) {
	return vmdriver.TaskStatus{}, vmdriver.ErrUnavailable
}
