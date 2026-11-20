// Package supervisor owns daemon-launched processes, their standard streams,
// output artifacts, cancellation, descendant cleanup and disk-pressure admission.
// It emits facts to a caller-owned sink. The daemon owns client sessions, event
// storage and thread state transitions, and execution drivers select launch commands.
// In D-17 protocol mode the caller owns raw stdin and stdout, while the
// supervisor retains stderr capture and process ownership.
//
// It implements execution-driver ?2 and supplies process operations from ?4.
// It applies thread-state-machine ?5 for work disposition and T30 for child
// cancellation, and enforces the event catalogue ?1.7 output cap and D-07
// artifact references.
package supervisor
