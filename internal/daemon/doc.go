// Package daemon composes Synch's persistent task runtime and loopback API.
// It owns startup, recovery and shutdown under the core architecture,
// persistent-state and runtime specifications and thread-state-machine §§3–4.
// OS service installation, token provisioning and harness qualification belong
// to their respective operators and components.
//
// Wiring:
//   - HTTP commands enter the API, queue and durable thread machine.
//   - Authority receipts stage policy events and publish only after SQLite commits.
//   - Git operations pass live worktree evidence to the policy engine before dispatch.
//   - Codex launches use policy, Direct Host access and then the supervisor.
//   - Engine and supervisor facts enter the shared event log for SSE and recovery.
package daemon
