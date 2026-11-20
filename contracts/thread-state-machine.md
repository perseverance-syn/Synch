# Thread state machine — v3.1.0

Sources: spec "Core architecture" (component 4), "Supervision and failure", "Child work", "Rollover and handoff", "Harness capabilities", "Full Control and mode changes", "Accurate status"; decisions 01, 02, 03, 04, 07, 15, 18, 21.
Events: every transition emits `thread.state_changed` (type_version 3) on the thread's own stream. `events/catalogue.schema.json` encodes the §2 table, so a producer cannot persist a transition, `from`/`to` pair or pause reason that the table does not allow. The transition is persisted before any side effect it causes.

## 1. States

| state | meaning | terminal |
|---|---|---|
| `draft` | brief exists, not yet accepted | no |
| `plan_review` | operator chose "Review plan first" (dec. 01) | no |
| `queued` | ready, waiting for a slot, limit or guard G (§2.1) | no |
| `running` | an assignment is dispatched and live | no |
| `waiting_children` | waiting on child threads; the parent's execution slot is **freed** (Child work) | no |
| `awaiting_decision` | operator decision needed: out-of-scope plan revision (dec. 02) or unresolved material risk (dec. 04) | no |
| `paused` | stopped with a `pause_reason`; work is preserved | no |
| `rollover` | writing a checkpoint for a new session (Rollover and handoff) | no |
| `verifying` | named checks / audit running on the exact revision | no |
| `awaiting_acceptance` | evidence complete; waiting for human acceptance of the revision (dec. 21) | no |
| `completed` | human accepted the revision | yes |
| `failed` | unrecoverable, or the operator abandoned it; failed attempts retained | yes |
| `cancelled` | cancelled by the operator or with its parent | yes |

`pause_reason`, and the guard to leave it (in addition to G where the exit goes to `queued`):
| reason | entered by | left by |
|---|---|---|
| `operator` | T20 | T21 on operator resume |
| `harness_unavailable` | T15, T23 | T21 once the operator has picked a replacement pair (`select_harness`) (dec. 07) |
| `no_eligible_pair` | T06 | T21 once permissions, pin or routing change so that a pair qualifies (dec. 03) |
| `disk_pressure` | T07, T33 | T21 on `resource.pressure{level:ok}` |
| `reconciliation` | T16, T35 | T21 once **every** external request of the thread is resolved with a value other than `unknown` (dec. 18) |
| `disagreement` | T17 | T21 when the resolving check settles it; T34 when it does not settle it and the risk is material (dec. 04) |
| `mode_switch` | T18 | T21 once active work is settled and, for a switch to Sandbox, `mode.changed{to:sandbox, phase:active, host_processes_stopped:true}` is recorded |
| `runner_disconnected` | T19 | T21 once the runner has reconnected and its journal is replayed (dec. 15) |
| `daemon_recovery` | T24 | only T22, T23, T36, T38 (§3). While any external request is unresolved, the thread **stays** in `daemon_recovery` (it is shown as "recovering: awaiting reconciliation"). |

Disk pressure pauses **new** work only. Control and recovery commands (pause, cancel, resume, revoke, export, decide, resolve_external_request) are accepted in every state (Supervision and failure).

## 2. Transitions

### 2.1 Guard G (dispatch safety)
G must hold, evaluated against persisted records, on every transition **into `queued` or `running`**:
1. The thread has no external request without a `succeeded`, `failed` or `not_executed` resolution (dec. 18). Neither an engine session, nor a native-resume capability, nor a checkpoint is evidence that a provider operation did or did not execute.
2. The thread has no open operator decision (dec. 02, 04).
3. If the latest `plan.revised` has `within_scope=false`, the operator decision that answers it is recorded.
4. **Work classification.** Applies only if an assignment was dispatched (T05) since the thread last entered `queued`. A pause taken before any process started (T06, T07 from `queued`) has no work to classify: G.4 is inapplicable and T21 back to `queued` needs only the reason guard and G.1–G.3. Otherwise, every assignment interrupted by a pause or a restart has a recorded `process.exited.work`. Entering `queued` from `paused` (T21, T22) requires it to be `complete` or `resumable`. The one exception is T21 out of `harness_unavailable`, where the operator has explicitly chosen to restart from the last checkpoint with a selected pair (dec. 07). `lost` work leaves any pause only through **T23** (attempts left → `harness_unavailable`, so the operator chooses) or **T36** (attempts exhausted → `failed`). This applies to every pause reason, not just `daemon_recovery`.
5. For T05, also: free slot, depth/concurrency/attempt limits (Q-27), policy allows the assignment, an eligible pair exists, no disk pressure.

If G fails on a transition into `queued`, the thread takes the matching pause instead (T35 for clause 1 from rollover; T23/T36 for clause 4), or stays where it is (in `daemon_recovery`). It never "queues and waits".

### 2.2 Table
| id | from | to | trigger | guard |
|---|---|---|---|---|
| T01 | draft | queued | brief accepted | `review_plan_first=false` (dec. 01); G |
| T02 | draft | plan_review | brief accepted | `review_plan_first=true` |
| T03 | plan_review | queued | `approve_plan{plan_digest}` | `plan_digest` == digest of the current plan revision shown for review; G |
| T04 | plan_review | draft | operator requests brief change | new brief version |
| T05 | queued | running | dispatch | G (all clauses) |
| T06 | queued | paused(no_eligible_pair) | routing finds no pair | — |
| T07 | queued | paused(disk_pressure) | pressure on | — |
| T08 | running | waiting_children | parent waits on children | slot released in the same transaction |
| T09 | waiting_children | queued | all awaited children terminal | G |
| T10 | running | awaiting_decision | `plan.revised{within_scope:false}` (dec. 02) or escalated risk (dec. 04) | — |
| T11 | awaiting_decision | queued | operator decides | new brief/plan version persisted if changed; G |
| T12 | awaiting_decision | failed | operator rejects with no alternative | — |
| T13 | running | rollover | safe checkpoint reached | — |
| T14 | rollover | queued | `rollover.checkpointed` committed | G; if G.1 fails → T35 |
| T15 | running | paused(harness_unavailable) | harness fails (dec. 07) | checkpoint attempted (§4) |
| T16 | running | paused(reconciliation) | external request outcome `unknown` (dec. 18) | — |
| T17 | running | paused(disagreement) | disagreement recorded (dec. 04) | independent work elsewhere continues |
| T18 | running | paused(mode_switch) | mode switch requested | dispatch stops at once; active work settles |
| T19 | running | paused(runner_disconnected) | runner offline and a new assignment or fresh approval is needed (dec. 15) | dispatched work continues on the runner |
| T20 | running | paused(operator) | operator pause | interrupt if qualified, else stop |
| T21 | paused(any reason except daemon_recovery) | queued | reason guard (§1) | G (incl. G.4) |
| T22 | paused(daemon_recovery) | queued | recovery: all requests resolved and work `resumable` | §3; G |
| T23 | paused(any reason except harness_unavailable) | paused(harness_unavailable) | work `lost`, attempts remain | G.1 holds (for daemon_recovery: after §3 step 1) |
| T24 | running / rollover / verifying | paused(daemon_recovery) | daemon starts with the thread in one of these states | `interrupted_state` == `from` (schema-enforced) |
| T25 | running | verifying | assignment reports done | — |
| T26 | verifying | awaiting_acceptance | required checks reported (any status) and audit settled per its setting (dec. 08) | failed/not-run checks are **shown** (Accurate status; Q-17) |
| T27 | verifying | queued | a check fails and a retry fits within limits | attempt recorded as failed; G |
| T28 | awaiting_acceptance | completed | operator `accept{revision}` | revision == evaluated revision (dec. 21) |
| T29 | awaiting_acceptance | queued | operator rejects or requests a change, or the revision changes | dependent evidence invalidated; G |
| T30 | any non-terminal | cancelled | operator cancel, **or parent cancelled** | managed children cancelled first; descendant processes cleaned |
| T31 | running / verifying | failed | attempt limit exhausted or unrecoverable error | failed attempts retained |
| T32 | paused(any) | failed | operator abandons | — |
| T33 | running | paused(disk_pressure) | pressure on and new dispatch needed | — |
| T34 | paused(disagreement) | awaiting_decision | resolving check did not settle it; material risk (dec. 04) | — |
| T35 | rollover | paused(reconciliation) | checkpoint lists an unresolved external request | — |
| T36 | paused(any) | failed | work `lost`, attempts exhausted | for daemon_recovery: after §3 step 1 |
| T37 | — | — | **retired in 3.0.0**: the schema rejects it; recovery waits in `daemon_recovery` instead | — |
| T38 | paused(daemon_recovery) | verifying | recovery: all requests resolved, and the interrupted assignment has a durable `process.exited{work: complete}` with `interrupted_state` `running` or `verifying` | the step the normal exit leads to (T25); checks without a durable `check.result` for the revision are re-run |

Completeness: every non-terminal state has T30 and a path to a terminal state, and every state is reachable from `draft`. There are no orphan or dead-end states.

Cancel-with-parent: when a parent takes T30, every managed child that is not terminal takes T30 in the same reconciliation pass. A child counts as cancelled only once its process tree is gone (Job Object on Windows; "Supervision and failure").

## 3. Daemon restart (recovery)
Source: "Supervision and failure", dec. 07, 18. Thread state is durable (SQLite), so a restart **restores the persisted state** with all its obligations:

- `draft`, `plan_review`, `queued`, `waiting_children`, `awaiting_decision`, `awaiting_acceptance` and every `paused(reason)` are **unchanged**. A `plan_review` thread still needs plan approval. A `harness_unavailable` thread still needs a replacement pair. A `waiting_children` thread still waits, and its children are recovered independently. A `reconciliation` thread still needs every request resolved. No event is emitted for these threads.
- Only states that imply a live process (`running`, `rollover`, `verifying`) take **T24** to `paused(daemon_recovery)`, recording `interrupted_state`. The supervisor then confirms that the old process tree is gone.
- Recovery for a `daemon_recovery` thread, in this order:
  1. Run reconciliation (vm-driver.md §3) for every unresolved external request of the thread. If any is still `unknown`, the thread **stays in `paused(daemon_recovery)`**. Recovery resumes when a reconcile succeeds or the operator resolves the request (`resolve_external_request`). There is no transition to `paused(reconciliation)`, so the classification obligation cannot be dropped.
  2. Only then classify the interrupted work (§5):

     | work | interrupted_state | exit |
     |---|---|---|
     | `complete` | `running` or `verifying` | **T38** → `verifying` |
     | `complete` | `rollover` (the checkpoint is committed) | **T22** → `queued` (it counts as resumable) |
     | `resumable` | any | **T22** → `queued` (G re-checked) |
     | `lost`, attempts left | any | **T23**, so the operator chooses |
     | `lost`, attempts exhausted | any | **T36** |

     Every outcome has exactly one exit, besides T30 cancel and T32 abandon, so `daemon_recovery` has no undefined exit.
- **Repeated restarts:** a thread already in `paused(daemon_recovery)` is not touched by T24 again (T24 only leaves live states). Recovery restarts at step 1, and the pending classification is still owed because the state itself records it. A thread in `paused(reconciliation)` (entered while running, T16, or from rollover, T35) stays there across restarts until G.1 holds. Its exit T21 still needs G.4, so lost work goes through T23/T36.
- A queued thread whose checkpoint lists uncertain requests cannot be dispatched, because T05 checks G.1 against the request records, not the checkpoint.

## 4. Rollover checkpoint
Source: "Rollover and handoff", dec. 07. Written at T13 and attempted at T15. Stored as a file referenced (uri + digest) from `rollover.checkpointed`.

```json
{
  "checkpoint_version": 1,
  "thread_id": "ULID",
  "brief_version": 3,
  "plan": { "revision": 5, "digest": "sha256:..." },
  "revision": "git commit SHA of the thread working copy",
  "pending_edits": { "uncommitted_diff": { "uri": "...", "digest": "sha256:..." }, "worktree_digest": "sha256:..." },
  "failed_attempts": [ { "assignment_id": "ULID", "harness": "...", "model": "...", "reason": "..." } ],
  "open_decisions": [ { "id": "...", "kind": "scope|risk|disagreement|approval", "summary": "..." } ],
  "uncertain_external_requests": [ "external_request_id ULID" ],
  "skills": [ { "id": "...", "digest": "sha256:..." } ],
  "handoff_files": [ "PLAN.md", "STATUS.md", "CONVENTIONS.md", "prompt file" ]
}
```
The first seven content fields are what the spec requires. `skills` and `handoff_files` come from Q-26. `uncertain_external_requests` is informational for the next session: dispatch safety comes from G.1 and the request records, never from this list (T14/T35). A new session (same or different harness) continues from these records. Native chat transfer and live process migration are **not** guaranteed.

## 5. Resumable vs lost
Source: "Supervision and failure". Recorded as `process.exited.work`.
- `resumable`: a checkpoint (§4) exists at or after the last dispatched assignment's input revision, **or** the adapter has a qualified `native_resume` and the session ref is known. This classifies only the *engine work*. It never resolves external requests (G.1).
- `lost`: neither applies. Persisted output stays visible. The attempt is a failed attempt.
- `complete`: the process finished normally. The execution outcome says nothing about checks.

## 6. Outcome kinds are separate records
Source: "Command semantics", "Changes checks and evidence", "Accurate status".

| kind | record | values |
|---|---|---|
| command acceptance | receipt (`receipts.md`) / `request.accepted` | receipt `status`: `accepted` \| `rejected`. A payload conflict is a `409 request_id_conflict` problem with **no** new receipt |
| delivery | `assignment.delivered` | delivered, rejected, unknown |
| execution | `process.exited` | exited, killed, stalled, lost_on_reboot, cancelled |
| test / check result | `check.result` | passed, failed, error, partial, not_run |
| audit | `audit.status`, `finding.recorded` | off, pending, running, pass, corrections_requested, error |
| human approval | `approval.recorded` | bounded approval (policy/rule-file.md §5) |
| human acceptance | `acceptance.recorded` | accepted, rejected |

No transition infers one kind from another. T26 needs check results to *exist*, not to pass. T28 needs human acceptance whatever the audit status.
