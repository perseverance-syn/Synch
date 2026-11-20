// Package daemon composes Synch's persistent task runtime and loopback API.
// It owns startup, recovery and shutdown under the core architecture,
// persistent-state and runtime specifications and thread-state-machine §§3–4.
// OS service installation, token provisioning and harness qualification belong
// to their respective operators and components.
//
// Injected Config.Adapters use the same capability gate, routing eligibility, policy
// and grant checks as built-in adapters; an unqualified adapter is not eligible.
// Adapter implementations are trusted to mediate their own external actions.
//
// Wiring:
//   - HTTP commands enter the API, queue and durable thread machine.
//   - Mode commands pause the coordinator subtree (T18), settle sessions and process trees,
//     and revoke Host grants before publishing Sandbox active and releasing T21.
//     Existing Builders need their own fresh grant after a return to Host access.
//   - Authority receipts stage policy events and publish only after SQLite commits.
//   - Git operations pass live worktree evidence to the policy engine before dispatch.
//   - Grant before delegation (D-43): a coordinator is not delegated until it holds an active
//     Full Control grant for this host. It stays queued (no child, no session) and the API
//     thread view carries a derived, read-only waiting_for note naming the exact `synch grants
//     create` command; `synch threads show` and `threads list` print it. The daemon only reads
//     the grant log, never issues grants; the next dispatch tick after the operator grants
//     delegates, and the child inherits that grant (Q-21).
//   - The API writes thread.created (project, depth 0) when it creates a thread, so the daemon
//     never backfills it. A delegation is durable once the child is recorded (Facts.Awaited).
//     A crash between a coordinator's T05 and T08 recovers without an operator: the interrupted
//     no-session dispatch is classified process.exited work=resumable, T22 requeues the parent
//     and it parks on its recorded child, or delegates again when none was recorded; never twice.
//   - A top-level thread is a Coordinator and never runs tools or commands (dec. 05,
//     D-41); policy reports it as a coordinator whatever it is assigned, and an engine
//     session on it is refused. When the dispatch loop promotes it (T01/T02) and
//     routing finds a pair (else T06 paused(no_eligible_pair)), the daemon creates one
//     managed Builder child through internal/children (purpose = the brief, input
//     revision = the project checkout's HEAD, expected evidence = the required checks),
//     records child.assignment_recorded and audit.setting_recorded through an
//     event-log-backed children.Recorder, and takes the parent T05 -> T08
//     (waiting_children, slot released). Child limits are the Q-27 defaults
//     (Config.ChildLimits overrides them).
//   - The child is promoted, routed (inheriting the parent's pin and project) and run by
//     the same loop as policy role builder, so engine launch and gates need the
//     operator's rules and an operator Full Control grant issued on the parent before
//     the child exists; the daemon never issues grants. A launch that policy denies,
//     or a child blocked by a failed dependency, ends failed (T31, or T06 then T32 for
//     a queued child) with a failed result instead of staying queued.
//   - An ended child gets child.result_recorded (outcome, revision, checks by status,
//     open requirements). A child whose checks all passed is accepted by its
//     coordinator (T28); a failed check takes T27 or T31 within the attempt limit;
//     failed or not-run evidence waits for the operator. When all awaited children
//     ended the parent takes T09, T05 (no session) and T25, reports each child check as
//     its own check.result (a failed or cancelled child is a failed check naming the
//     open requirements) and takes T26 when the audit setting allows.
//   - Child watchdog (D-42): a child session past its runtime budget (Config.ChildMaxRuntime,
//     default 2h, overridden per project by max_runtime, "off" disables), or one
//     making ChildLoopRepeats (default 5, project loop_repeats) identical tool calls in a row with no other output,
//     is stopped; an engine exit that is not clean (including a supervisor stall) is handled
//     the same way. The child records process.exited work=lost, a stderr line naming the
//     reason, and takes T15 with a failure checkpoint carrying that reason, for the
//     coordinator or operator to steer (decision 07).
//   - Roles and skills (internal/roles, card 301): the coordinator picks Builder, Ops or
//     Security from the brief and records the reason (dec. 06). The child's session gets that
//     role's instructions and its skills pinned by ID and sha256 (StartRequest.Skills,
//     assignment.dispatched.skills, Checkpoint.Skills); a rollover or harness switch resumes
//     the checkpoint's pins or fails closed. Config.RoleSkillsDir is the trusted pack. Loading
//     a role or skill never changes policy, grants or the thread's role.
//   - Context controls and handoff (internal/context, card 303): every child session starts
//     with its role instructions, the project's guidance file and the target's guidance file,
//     stable-first, each with source and digest; the coordinator gets none. Rollover (Rollover, SwitchHarness) and a failure checkpoint write a handoff file
//     into the worktree's git metadata directory (never the project's history; the data
//     directory when the thread has no git checkout), and the checkpoint lists it. The next
//     session, on any harness, starts with that handoff as stable instructions. CapOutput
//     and SearchOutput cap tool output (Config.OutputCap, default 16 KiB), retain it in full
//     under the data directory and search it by reference.
//   - Cancelling a parent cancels its children first (T30), each confirmed only once its
//     engine session has stopped.
//   - Codex, Claude Code and Kimi adapters are built from configured binaries (an
//     absent binary means the harness is not offered). Launches use policy, Direct
//     Host access and then the supervisor. Claude Code can_use_tool requests are
//     answered by the policy engine and fail closed; a harness that cannot start is T15.
//   - A finished assignment is T25; the project's gate file runs through
//     internal/gates, each check.result is recorded (a missing gate file is not_run),
//     and T26 follows when the audit setting allows it.
//   - An optional remote driver is registered under the remote execution kind; a
//     disconnected runner pauses new assignments and fresh approvals (T19).
//   - Engine and supervisor facts enter the shared event log for SSE and recovery.
//   - Credentials (internal/credentials, card 401): Config.Credentials (default: the OS
//     keychain) brokers secrets at use. Every event payload is redacted of known values
//     before persist (eventlog redactor hook), CapOutput redacts before writing to disk,
//     engine instructions and skills are redacted before Start and a known value in Env
//     refuses the launch. UseSecret derives the caller's role from thread facts: a
//     coordinator is refused, a worker reads its own scope after policy secret.use. The
//     remote driver defaults to the broker's SSH adapter. A child that ran under Direct
//     lists DirectLimitation on its result (native actions can exceed journal coverage).
package daemon
