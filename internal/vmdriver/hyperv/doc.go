// Package hyperv translates VM driver operations to Hyper-V PowerShell commands.
// It owns transport, resource markers and snapshot verification under
// vm-driver.md sections 1–4. The dispatcher and journal own authorization,
// limits, orphan decisions, request serialization and durable recovery.
package hyperv
