# Hyper-V driver

The application supplies a PowerShell runner. The runner binds arguments as
PowerShell data and returns the script's JSON stdout. For remote endpoints, its
authentication input is separate from the recorded command. The runner supplies
the broker's username and password as a PSCredential to Invoke-Command. The
driver sanitizes broker and runner errors before returning them.

A base is an operator-configured alias for an exported, marker-free VM
configuration file on the Hyper-V host. Import copies the export with a new
VM ID. Its token names the separate configuration and virtual disk directories.
Remove-VM removes the registration and configuration. Hyper-V retains virtual
disk files for operator disposition. An interrupted import that has yet to write
Notes remains uncertain.

Create stores the request and owner lines in VM Notes. The checkpoint cmdlets
expose a name parameter for attribution, so the driver sends the logical name,
request token, and acting thread as three tilde-delimited fields. Restore checks
the listed checkpoint ID, parent VM ID, and name before invoking the restore
cmdlet. The driver reads the acting thread from the same journal used to dispatch
snapshot requests.

Hyper-V cmdlets run synchronously and expose no task ID in this integration.
Token evidence applies to create and snapshot. Power state and resource absence
provide no attribution. Real-host acceptance is deferred, so the matrix
advertises zero operations.

## Fake fixture references

The tests model the JSON projection of documented PowerShell objects and command
parameters. Fixture IDs, names, and outputs are synthetic.

- [Get-VM](https://learn.microsoft.com/powershell/module/hyper-v/get-vm), [Get-VMHost](https://learn.microsoft.com/powershell/module/hyper-v/get-vmhost), [ConvertTo-Json](https://learn.microsoft.com/powershell/module/microsoft.powershell.utility/convertto-json)
- [Import-VM](https://learn.microsoft.com/powershell/module/hyper-v/import-vm), [Rename-VM](https://learn.microsoft.com/powershell/module/hyper-v/rename-vm), [Set-VM](https://learn.microsoft.com/powershell/module/hyper-v/set-vm)
- [Start-VM](https://learn.microsoft.com/powershell/module/hyper-v/start-vm), [Stop-VM](https://learn.microsoft.com/powershell/module/hyper-v/stop-vm), [Remove-VM](https://learn.microsoft.com/powershell/module/hyper-v/remove-vm)
- [Checkpoint-VM](https://learn.microsoft.com/powershell/module/hyper-v/checkpoint-vm), [Get-VMSnapshot / Get-VMCheckpoint](https://learn.microsoft.com/powershell/module/hyper-v/get-vmsnapshot), [Restore-VMSnapshot](https://learn.microsoft.com/powershell/module/hyper-v/restore-vmsnapshot), [Rename-VMSnapshot](https://learn.microsoft.com/powershell/module/hyper-v/rename-vmsnapshot)
- [Invoke-Command](https://learn.microsoft.com/powershell/module/microsoft.powershell.core/invoke-command), [PSCredential](https://learn.microsoft.com/dotnet/api/system.management.automation.pscredential)
