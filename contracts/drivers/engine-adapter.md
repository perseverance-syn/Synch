# Engine adapter contract — v1.1.0

Sources: spec "Extension points", "Harness capabilities", "Rollover and handoff", "Supervision and failure", "Context controls"; decisions 03, 05, 07, 23; role-skills/README.md ("How Synch loads the skills").
Targets in V1: Codex, Claude Code, Kimi Code CLI ("Integration scope").

## 1. Operations

| op | required | meaning |
|---|---|---|
| `start` | yes | start a session for an assignment with instruction context (brief, assignment, criteria, decisions, revision/target, allowed actions, knowledge refs; dec. 09) |
| `send` | yes | deliver a follow-up message to a live session |
| `stream` | yes | ordered output events (message, tool_call, tool_result, stdout, stderr, exit) |
| `interrupt` | optional | stop the current turn and keep the session |
| `stop` | yes | end the session; the supervisor then kills the process tree |
| `native_resume` | optional | reattach to a harness-native session by `session_ref` |
| `usage` | optional | token usage, when the harness reports it |
| `tool_interception` | optional | route the harness tool calls through Synch policy before execution (MCP or hook) |
| `skill_delivery` | yes | deliver pinned skill text: `native` (verified role-scoped native discovery) or `injection` (instruction at start). `injection` is always available |

The adapter never executes mediated actions itself. Tools are exposed by the daemon over MCP ("Extension points"). When `tool_interception` is unavailable, every harness-native action falls outside journal coverage, and the thread must show that ("Policy and credentials": Direct native actions can exceed journal coverage).

## 2. Capability declaration
Each adapter ships one declaration per (harness version range, OS). Operations are advertised only after the qualification run passes (dec. 23).

```json
{
  "capabilities_version": "1.0.0",
  "adapter": "claude-code",
  "adapter_version": "0.1.0",
  "harness_versions": ">=2.0 <3.0",
  "os": "windows",
  "models": ["claude-opus-5-5"],
  "ops": {
    "start":   { "status": "qualified", "evidence": "qual/2026-11-20-claude-win.json" },
    "interrupt": { "status": "unavailable", "reason": "no signal channel on Windows build" },
    "native_resume": { "status": "untested" }
  }
}
```
`status` is one of `qualified`, `unavailable`, `untested`. Only `qualified` ops may be used. Calling a non-qualified op returns `ErrUnavailable` and emits `engine.capability_unavailable`. Routing (dec. 03) only considers pairs whose required ops are all `qualified`.
A breaking change to an adapter's behaviour bumps the `capabilities_version` major.

## 3. Go signatures (non-normative; card 001 owns real code)

```go
type EngineAdapter interface {
    Capabilities(ctx context.Context) (CapabilitySet, error)
    Start(ctx context.Context, req StartRequest) (Session, error)
}

type Session interface {
    Ref() string // session_ref for native resume; may be empty
    Send(ctx context.Context, msg Message) error
    Events() <-chan EngineEvent // closed after exit event
    Interrupt(ctx context.Context) error // ErrUnavailable if not qualified
    Stop(ctx context.Context) error
    Usage(ctx context.Context) (Usage, error) // Usage{Reported:false} if not reported
}

type Resumer interface { // optional
    Resume(ctx context.Context, sessionRef string, req StartRequest) (Session, error)
}

type StartRequest struct {
    AssignmentID string
    Model        string
    Workdir      string
    Instructions []Instruction // stable instructions first ("Context controls")
    Skills       []PinnedSkill // id + sha256 digest; preserved across rollover/handoff
    MCPEndpoint  string        // daemon-provided tools
    Env          map[string]string // brokered; never infra credentials (dec. 05)
}

var ErrUnavailable = errors.New("engine: operation unavailable")
```

## 4. Rules
- Every adapter output is persisted as an event before it is shown (events/catalogue.md).
- The selected harness/model and routing reason are recorded on `assignment.dispatched`. An operator pin applies to the assignment ("Harness capabilities").
- Harness failure means pausing with `harness_unavailable` (dec. 07). The adapter never switches harness itself.
- `native_resume` only restores an engine session. It is **never** evidence about external (provider) operations. Those resolve only through dec. 18 reconciliation (thread-state-machine.md §2.1 G.1, vm-driver.md §3).
- A skill cannot widen authority. The skill text is instruction only, and tools and grants come from daemon policy ("Extension points").
- Fixtures vs authenticated runs are reported separately ("Cross-component checks").
