# Event log contract — v4.2.0

Files: [`envelope.schema.json`](envelope.schema.json) (envelope 2.0.0, daemon-persisted form), [`runner-event.schema.json`](runner-event.schema.json) (runner journal 2.0.3), [`catalogue.schema.json`](catalogue.schema.json) (catalogue 4.2.0). In 4.2.0 (minor, additive, card 114b): new type `approval.declined` (type_version 1). In 4.1.0 (minor, additive, card 000-C1, D-27): new types `child.assignment_recorded`, `child.result_recorded`, `audit.setting_recorded`, all type_version 1. In 4.0.0: `external_request.resolved` 4 (evidence field sets, `provider_task_absent`) and `thread.state_changed` 4 (T38). Type versions changed in 3.0.0: `thread.state_changed` 3, `approval.recorded` 3, `grant.issued` 3, `grant.revoked` 2, `policy.decided` 2, `external_request.persisted` 3, `external_request.resolved` 3.
Examples: [`../examples/events/`](../examples/events/). Fixtures: `../tests/cases.json`.

## 1. Log rules
Source: spec "Persistent state", "Core architecture" (append-only event log), "Remote execution", decision 15.

1. **Append-only.** Events are never updated or deleted (Q-07).
2. **Persist before ack.** One SQLite (WAL) transaction assigns `seq`, `global_seq` and `recorded_at` and writes the row. Nothing reaches SSE subscribers or the HTTP response before commit.
3. **Numbering.** `seq` is per stream, starts at 1 and is contiguous. `global_seq` is daemon-wide and contiguous. A rolled-back transaction consumes no numbers.
4. **Streams.** `thread:<ulid>` carries facts about one thread. `system` carries `resource.pressure` and `runner.connection` only. The schema binds every type to its stream kind, and thread streams require `thread_id`.
5. **Typed and versioned.** `type` + `type_version` (payload major). Compatibility rules: README.md "Compatibility".
6. **Redaction** ("Policy and credentials"). Known secret values are redacted before persistence (`redactions` counts the replacements; the replacement text is `[REDACTED]`).
7. **Large output** ("Context controls"). `engine.output.chunk` is capped at **65 536 UTF-8 bytes**, enforced by the producer. The schema's `maxLength: 65536` counts code points and is only a loose upper bound (Q-39). When output is cut, `truncated: true` and `full_output` (uri + digest of the on-disk file) are required.

## 2. Catalogue

| type (v) | stream | meaning | spec source |
|---|---|---|---|
| `thread.created` (1) | thread | thread exists, with parent and depth | Child work |
| `thread.state_changed` (3) | thread | a transition from the thread-state-machine.md §2 table, enforced by the schema (T24 `interrupted_state` == `from`; T37 retired) | thread-state-machine.md |
| `brief.version_accepted` (1) | thread | new accepted brief version | Project memory |
| `plan.revised` (2) | thread | plan revision + digest; `within_scope=false` iff `scope_changes` non-empty | dec. 02 |
| `request.accepted` (1) | thread/system | **command acceptance** (receipt) | Command semantics; receipts.md |
| `assignment.dispatched` (1) | thread | dispatch; harness/model, routing reason, pinned skills | dec. 03, 05 |
| `assignment.delivered` (1) | thread | **delivery** outcome | Command semantics |
| `engine.output` (1) | thread | live output chunk | Context controls |
| `engine.usage` (1) | thread | usage, only when reported | Verification and measurement |
| `engine.capability_unavailable` (1) | thread | adapter cannot perform an operation | Harness capabilities |
| `process.exited` (1) | thread | **execution** outcome + resumable/lost | Supervision and failure |
| `check.result` (1) | thread | **check result** | Accurate status |
| `audit.status` (1) | thread | **audit** status | Independent audit, dec. 08 |
| `audit.setting_recorded` (1) | thread | the audit setting actually in force for the task, with a non-empty `reason`. `setting` is `off`, `level1`, `level2` or `level3` (the resolved value, never `default`). `thread_id` == envelope `thread_id` (C7). Not a status: `audit.status` carries that | dec. 08 (set audit per task, show actual status) |
| `child.assignment_recorded` (1) | thread (parent stream) | a child's focused assignment: `parent_thread_id`, `child_thread_id`, `assignment_id` (ULIDs), `purpose`, `role` (`builder`, `ops`, `security` or `auditor`), non-empty `role_reason`, `input_revision` and/or `target` (at least one), `allowed_capabilities`, `expected_evidence` (at least one), optional `dependencies` (assignment ids of already-created siblings). `parent_thread_id` == envelope `thread_id` (C7) | Child work (focused assignment, input revision, capabilities, expected evidence), dec. 06 (why each specialist is involved) |
| `child.result_recorded` (1) | thread (parent stream) | what the parent collects from a child: `child_thread_id`, `assignment_id`, `outcome` (`completed`, `failed` or `cancelled`), `revision`, `checks` grouped by `passed`, `failed`, `error`, `partial`, `not_run` (all five keys required, arrays may be empty; entries are `{check, reason}`, and `reason` is required in every group except `passed`), `limitations`, `open_requirements` (both required, may be empty) | Child work (collect outcome, revision, checks, limitations, open requirements) |
| `finding.recorded` (2) | thread | scanner finding (drivers/finding.schema.json) or audit/review finding (`review_finding`) | Findings and fixes |
| `approval.recorded` (3) | thread | **human approval**; payload = policy `$defs/approval` | dec. 20 |
| `approval.voided` (1) | thread | approval no longer valid | dec. 20 |
| `approval.declined` (1) | thread | an operator refused a pending approval request (a `policy.decided` event with `outcome: waiting_approval`): `request_id` (that event's id), `decision` (`rejected` or `changes_requested`), non-empty `reason`, `operator` | dec. 20 (approval requests), card 114b |
| `acceptance.recorded` (1) | thread | **human acceptance** of exact revision | dec. 21 |
| `grant.issued` (3) / `grant.revoked` (2) | thread | Full Control grant lifecycle; revocation names its operator (`revoked_by`) or `cause: thread_ended` | dec. 14 |
| `policy.decided` (2) | thread | mediated action decision with actor, machine, outcome, qualified `rule_id` | Policy and credentials |
| `external_request.persisted` (3) | thread | request identity before dispatch: endpoint, resource, params digest (token derived: `synch-<id>`) | dec. 18 |
| `external_request.task_bound` (1) | thread | provider task ID bound to the request | dec. 18 |
| `external_request.resolved` (3) | thread | resolution with sufficient evidence (vm-driver.md §3) | dec. 18 |
| `rollover.checkpointed` (1) | thread | checkpoint written | Rollover and handoff |
| `mode.changed` (2) | thread | mode switch phases; Sandbox `active` requires `host_processes_stopped: true` | Full Control and mode changes |
| `resource.pressure` (1) | system | disk pressure on/off | Supervision and failure |
| `runner.connection` (1) | system | runner connect/disconnect + replay range | Remote execution |

The outcome kinds in **bold** are distinct types, and none stands in for another.

## 3. Runner journals: local shape, ingested shape, mapping
Source: "Remote execution", dec. 15.

A runner writes `runner-event.schema.json` records to its local journal. It cannot assign `global_seq` or `recorded_at`, and it may only write the observable types listed in that schema. On reconnect the daemon pulls `source_seq > last_ingested(runner)` and, for each record, persists one envelope event in one transaction:

| runner-local field | ingested envelope field |
|---|---|
| `runner` | `source.runner`, and `actor = {kind:"runner", id: runner}` |
| `source_seq` | `source.seq` |
| `id` | `id` |
| `type`, `type_version`, `payload`, `redactions` | same (the payload is validated against the catalogue) |
| `occurred_at` | `occurred_at` |
| `thread_id` | `thread_id`, `stream = "thread:" + thread_id` |
| — | `seq`, `global_seq`, `recorded_at` assigned by the daemon |

- The dedup key is `(source.runner, source.seq)`, unique in the store. A re-sent record is skipped.
- The authenticated runner connection must equal `runner`, and `thread_id` must be a thread dispatched to that runner. Otherwise the record is rejected and the connection flagged.
- Gaps: if the runner's journal starts above `last_ingested + 1`, ingest stops and the runner is marked degraded. Ingest never skips.
- The envelope schema requires `source` (and `occurred_at`) when `actor.kind = runner`, and forbids it otherwise.
- `runner.connection{replayed_from_seq, replayed_to_seq}` records each replay range.
- **Local validation before acknowledgement.** The runner schema binds each local `type` to its catalogue `type_version` and payload schema (`$ref` into `catalogue.schema.json`). A runner validates the record before writing and acknowledging it, so an acknowledged record can always be ingested. A record that fails validation is not journaled: the runner reports the failure as a diagnostic and does not advance `source_seq`.

## 4. Producer cross-field checks
JSON Schema cannot compare values across fields. Producers (the daemon at persistence, the runner before acknowledgement) must also enforce the following, and `check.mjs` applies them to every fixture:

| id | rule |
|---|---|
| C1 | `thread_id` == the ULID in `stream` (`thread:<ulid>`) |
| C2 | `thread.created`: `payload.thread_id` == `thread_id` |
| C3 | `approval.recorded` / `grant.issued`: `payload.thread` == `thread_id` |
| C4 | ingested events: `actor.id` == `source.runner` |
| C5 | `external_request.resolved`: `resource_evidence.client_token` and `absence_lookup.client_token` == `synch-<external_request_id>` |
| C6 | `grant.revoked`: `grant_id` resolves to a recorded `grant.issued`. For `cause: thread_ended`, `ended_thread` == that grant's `thread`, and that thread reached a terminal state (a committed `thread.state_changed` to completed/failed/cancelled) **before** this event. In `check.mjs` the producer state comes from the fixture `context` |
| C7 | `child.assignment_recorded`: `payload.parent_thread_id` == `thread_id`. `audit.setting_recorded`: `payload.thread_id` == `thread_id` |
| P1 | approvals: `expires_at` > `approved_at` |

The schema itself enforces the single-record relationships:
- T24 `interrupted_state` == `from`;
- the `grant.revoked` issuer: `cause: operator` + `revoked_by`, or `cause: thread_ended` + `ended_thread`;
- evidence sufficiency for external-request resolutions (vm-driver.md §3).
