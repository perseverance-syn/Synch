// Package sandbox is the Sandbox execution driver (execution-driver contract v1.1.0
// §1 kind "sandbox", §5 mode switch). It owns, for each sandboxed task, one
// dedicated disposable VM: creation from an operator-configured base through
// internal/vmdriver (creation ownership is recorded by the vmdriver journal),
// running the task's processes inside that VM through an in-VM runner reached
// over SSH with the configuration's pinned host key, and destroying the VM at
// task end, cancel or failure. VMs it could not destroy, or whose create or
// delete outcome is unresolved, are reconciled through the driver (Reconcile).
// A VM is never shared between tasks: a task is one root process and the
// processes spawned with it as parent.
//
// It also owns the boundary checks that gate exposure. A configuration is
// advertised (DriverInfo.CertifiedConfigs, read by the daemon's mode switch) only
// after its filesystem, credential and network checks all passed on a VM created
// from that configuration. Each check yields Evidence: passed, failed, error, or
// not run with a reason.
//
// Spec lines implemented:
//   - "Sandbox: Run one sandboxed task in a disposable VM. Expose this mode only on
//     a configuration where tests establish filesystem, credential and network
//     boundaries. Provider support and Sandbox certification have separate evidence."
//   - "12 · A Sandbox qualification: Use a dedicated disposable VM per sandboxed
//     task. Verify filesystem, credential and network restrictions on each
//     configuration advertised for Sandbox. All five hypervisors remain within V1
//     management scope."
//   - "Full Control and mode changes ... Confirm host processes have stopped before
//     reporting Sandbox active": Settle reports HostProcessesStopped only after every
//     task VM of the thread is destroyed.
//   - Provider: drivers hold infrastructure credentials; authority requires
//     assignment or creation ownership. This package never reads a credential: the
//     vmdriver provider holds them, and Spawn refuses any command line or
//     environment carrying a known brokered value, so nothing infrastructure-related
//     enters the VM.
//
// It does not own provider transports (internal/vmdriver/*), the durable request
// journal or the policy Authorizer (supplied by the daemon), SSH transport and
// host-key pinning mechanics (internal/remote, behind Connector), or mode-switch
// ordering (internal/daemon).
//
// Not certified: boundary checks here run against what the configured Inspector
// reports. In tests that is a fake's declared properties. Sandbox certification on
// a real hypervisor is "not run: deferred by owner"; every Qualify result carries
// that as separate real_host_certification evidence, and nothing in this package
// reports Sandbox certified.
package sandbox
