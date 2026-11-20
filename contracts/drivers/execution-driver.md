# Execution driver contract — v1.1.0

Sources: spec "Core architecture" (component 6), "Supervision and failure", "Sandbox", "Full Control and mode changes", "Remote execution", "Policy and credentials"; decisions 12, 14, 15, 16.

## 1. Kinds

| kind | UI label | where processes run | evidence integrity |
|---|---|---|---|
| `direct` | **Host access** (never "Direct") | the named host, under the granted identity | limited to that host account ("Evidence and diagnostics") |
| `remote` | Remote: `<target>` | an SSH-registered target running a Synch runner | runner journal, replayed on reconnect |
| `sandbox` | Sandbox | one disposable VM per task (dec. 12) | only on certified configurations |

`direct` needs an active Full Control grant matching thread + host + identity, inherited or own (dec. 14). `sandbox` is offered only on configurations whose filesystem, credential and network boundary tests passed ("Sandbox"). Provider support and Sandbox certification are separate evidence.

## 2. Process supervision (all kinds)
- The driver owns stdin/stdout/stderr, cancellation and descendant cleanup. On Windows it uses a Job Object with kill-on-close. On Unix it uses a process group / cgroup (Q-11).
- Output is persisted as events before it is forwarded. A desktop close never stops a process.
- Each process ends with `process.exited` and `work = complete | resumable | lost` (thread-state-machine.md §4).
- Stall detection: no output and no progress for `stall_timeout` ⇒ `execution_outcome=stalled` (Q-12 for the default).

## 3. Remote specifics
- Targets are registered with host key and allowed operations. An unexpected host key is a blocking review item, never an auto-accept.
- SSH agent is brokered. Forwarding is off unless granted.
- The runner owns the process and journals numbered events locally (`runner:<id>` stream). On reconnect the daemon replays from its last ingested `source_seq` (events/catalogue.md §1.5).
- While disconnected, the runner may finish already dispatched work within its grant. New assignments and fresh-approval actions wait (dec. 15).

## 4. Go signatures (non-normative)

```go
type ExecutionDriver interface {
    Kind() string // "direct" | "remote" | "sandbox"
    Describe() DriverInfo // label, host, identity, certified configs
    Spawn(ctx context.Context, spec ProcSpec) (Proc, error)
    Settle(ctx context.Context, threadID string) (SettleReport, error) // stop new work, wait/stop active; confirms host processes gone
}

type Proc interface {
    ID() string
    Output() <-chan OutputChunk
    Signal(ctx context.Context, s Signal) error // interrupt | terminate
    Wait(ctx context.Context) (ExitInfo, error) // includes DescendantsCleaned
}
```

## 5. Mode switch
"Full Control and mode changes": pause new dispatch, then `Settle`. For a switch to Sandbox, `SettleReport.HostProcessesStopped` must be true before `mode.changed{phase:active}`. The event schema rejects `to:sandbox, phase:active` unless `host_processes_stopped: true`. Each `mode.changed` records the `actor`. Revoking a grant also settles every inheriting thread first (dec. 14). The record holds actor, time, target, from and to. A downgrade does not claim to undo prior effects.
