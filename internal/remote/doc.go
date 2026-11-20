// Package remote owns SSH target admission, brokered SSH connections, runner
// process ownership, and journal replay (execution-driver §3, event catalogue
// §3, and thread-state-machine T19). It leaves grant issuance and thread state
// transitions to policy and the daemon.
package remote
