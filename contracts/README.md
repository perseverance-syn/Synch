# Synch V1 contracts

Language-neutral contracts that every Phase 1+ card builds against. They define shapes, states and rules, not implementations.
Authority: `docs/spec/synch-v1-locked-spec-2026-11-17.txt`. Open points: [QUESTIONS.md](QUESTIONS.md). Run all checks: `node contracts/check.mjs`.

## Index

| contract | file(s) | version | change in 000-F2 (F1 in brackets; F3 noted where it applies) | spec source |
|---|---|---|---|---|
| Event envelope | [events/envelope.schema.json](events/envelope.schema.json) | **2.0.0** | unchanged [F1 major: `source{runner,seq}`] | Persistent state, Remote execution |
| Runner journal event | [events/runner-event.schema.json](events/runner-event.schema.json) | **2.0.2** | 000-C1 patch: refs catalogue 4.1.0 [F3 patch: refs catalogue 4.0.0; F2 major: payloads validated against the catalogue before ack] | Remote execution, dec. 15 |
| Event catalogue | [events/catalogue.md](events/catalogue.md), [events/catalogue.schema.json](events/catalogue.schema.json) | **4.1.0** | 000-C1 minor: `child.assignment_recorded`, `child.result_recorded`, `audit.setting_recorded`, check C7 [F3 major: resolution evidence field sets, provider_task_absent, T38; F2 major: T24 consistency, T23/T36 generalized, T37 retired, revocation issuer, derived token, resolution evidence, qualified rule ids, cross-checks §4] | Persistent state, Command semantics, dec. 14/15/18/20 |
| Thread state machine | [thread-state-machine.md](thread-state-machine.md) | **3.1.0** | F3 minor: G.4 applicability, T38 complete-work recovery [F2 major: G.4 work classification, recovery waits in `daemon_recovery` | Supervision and failure, Child work, Rollover and handoff, dec. 01/02/04/07/18 |
| Request receipts | [receipts.md](receipts.md) | **3.0.0** | major: every member counts (`__proto__`), status ⇔ http_status | Command semantics |
| Local API | [api/openapi.yaml](api/openapi.yaml) | **3.0.0** | major: `CreateApproval` operational binding, no `rule`, `Receipt` status/http coupling | Persistent state, dec. 16/22 |
| Engine adapter | [drivers/engine-adapter.md](drivers/engine-adapter.md) | **1.1.0** | unchanged | Harness capabilities, dec. 03/07/23 |
| Execution driver | [drivers/execution-driver.md](drivers/execution-driver.md) | **1.1.0** | unchanged | Full Control and mode changes, Remote execution, dec. 12/14/15 |
| VM driver | [drivers/vm-driver.md](drivers/vm-driver.md) | **3.1.0** | F3 minor: evidence field-set table, authoritative absence, cross-record consistency [F2 major: token = `synch-<id>`, sufficient observations per resolution | Provider, Machine providers, dec. 13/17/18 |
| Scanner driver + finding | [drivers/scanner-driver.md](drivers/scanner-driver.md), [drivers/finding.schema.json](drivers/finding.schema.json) | **2.0.0** | unchanged | Findings and fixes |
| Policy rule file | [policy/rule-file.md](policy/rule-file.md), [policy/rule-file.schema.json](policy/rule-file.schema.json), [policy/examples/](policy/examples/) | **3.0.1** (schema 3.0.0) | F3 patch: thread-end revocation producer check (C6) [F2 major: built-in operational/deployment gates, plan-digest binding, all matching constraints apply, qualified rule ids, strict durations | Policy and credentials, dec. 05/13/14/20/21 |
| Fixtures and check | [tests/cases.json](tests/cases.json), [tests/digest-vectors.json](tests/digest-vectors.json), [check.mjs](check.mjs) | — | new | — |

## Versioning rule
- Each contract has its own semver, shown above, in the schema `$id` (`.../<name>/<semver>`) and in the doc title.
- **patch**: wording or clarification, no shape change.
- **minor**: additive and backward compatible (new optional field, new event type, new action name, new enum value that readers can ignore).
- **major**: anything breaking (removed or renamed field, new required field, changed meaning, narrowed enum, new constraint that rejects formerly valid data). A major bump creates a new `$id`, and the old version stays published until no component uses it.
- Event payloads are versioned per type with `type_version` (the payload major).

## Compatibility (producers vs readers)
- Published schemas are **producer schemas**. They are closed (`additionalProperties: false`), and a producer must emit records valid against exactly the version it declares. That is what catches typos and invented fields before persistence.
- **Readers are tolerant.** A reader built for `x.y` must accept any `x.z` with z ≥ y. It ignores unknown properties, unknown event types and unknown `type_version`s it does not handle, while still advancing its cursor. It never rejects a record only because a closed producer schema of its own version would. Readers that validate must use the reader mode: the same schema with unknown properties ignored.
- A minor bump adds optional properties to the producer schema. Old producers stay valid, and old readers ignore the new fields. Anything that would make an old reader misinterpret data is a major.
- Stored events are never rewritten. Readers must handle every `type_version` that was ever published for a type they consume.

## Freeze rule
- These versions freeze when the owner accepts card 000 (including fix rounds). After that, any change goes through a card that names the contract, the bump level and the affected consumers.
- No major change without the owner's approval. Minor and patch changes need a reviewed card.
- Answers to QUESTIONS.md entries that change a shape follow the same rule.

## Coverage boundary
Spec interfaces and decisions that R000 marked partial or missing, and where each one is (or will be) specified. "Later card" names the owning card in PLAN.md (read for 000-F2 as instructed). Later cards design those interfaces; card 000 does not. Where the mapping was a judgment call, the row says so (Q-51).

| spec item | status in card 000 | where |
|---|---|---|
| dec. 01 plan review | covered | thread-state-machine.md T02–T04, T03 digest; openapi `approve_plan` |
| dec. 02 scope revision | covered | `plan.revised` (within_scope ⇔ scope_changes); guard G.2–G.3 |
| dec. 03 routing: eligibility, pin, no-pair pause | covered | engine-adapter.md §2/§4; T05/T06 |
| dec. 03 / dec. 11 routing default and comparison evidence | later card | **304** |
| dec. 04 disagreement | covered | T17, T21, T34 |
| dec. 05 coordinator authority | covered | rule-file.md §4 |
| dec. 06 specialist reason, child assignment/result records | covered | `child.assignment_recorded`, `child.result_recorded` (catalogue 4.1.0); the child lifecycle itself is card **302** |
| dec. 06 plan dependencies | later card | **302** (child work) |
| dec. 07 harness failure | covered | T15, T21, T23; §3 recovery |
| Child work: assignment (purpose, input revision, capabilities, expected evidence), result collection | covered | `child.assignment_recorded`, `child.result_recorded`; limits, slots and cancellation stay with card **302** |
| dec. 08 audit setting persistence and rationale | covered (selection `CreateThread.audit`, status `audit.status`, Q-20) | `audit.setting_recorded` (catalogue 4.1.0) |
| dec. 09 specialist context, on-demand retrieval | later card (initial context covered: engine-adapter.md §1) | **303** (context/rollover) |
| dec. 10 durable knowledge, guidance acceptance | later card | **303** |
| dec. 12 sandbox qualification | covered | execution-driver.md §1, vm-driver.md §4 |
| dec. 13 provider scope | covered | vm-driver.md §2 |
| dec. 14 Full Control inheritance | covered | rule-file.md §6 |
| dec. 15 disconnected runners | covered | catalogue.md §3, T19, execution-driver.md §3 |
| dec. 16 remote operator access | covered | openapi servers/security |
| dec. 17 hypervisor matrix | covered (row format; actual rows are later evidence) | vm-driver.md §4 |
| dec. 18 uncertain operations | covered | vm-driver.md §3, guard G.1, T16/T35, recovery §3 |
| dec. 19 CI gate definitions, thresholds, parity across GitHub Actions / GitLab CI / Jenkins | later cards | **502 / 503 / 504** |
| dec. 20 bounded approval | covered | rule-file.md §5, schema `$defs/approval`, `CreateApproval` |
| dec. 21 merge / deployment gates | covered | rule-file.md §4, T28 |
| dec. 22 desktop/CLI parity: initial setup, project registration, other operator workflows | later cards | **201** (CLI incl. setup), **202** (desktop) |
| dec. 23 harness qualification rows; harness terms and versions (Q-35) | later cards (capability declaration covered: engine-adapter.md §2) | **601** (qualification harness), **305** (Claude Code + Kimi adapters) |
| dec. 24 coordinator qualification fault matrix | later card | **601** |
| Service/boot lifecycle; migration, backup and restore compatibility | later cards (restore detection covered: `log_epoch`, openapi streamEvents) | **101** (event log: migration/backup/restore), **102** (supervisor: service/boot) |
| Credentials: keychain, age fallback, user vs thread separation; redaction across exports | later card (event and scanner redaction covered) | **401** (credentials) |
| Evidence export with hash manifest; dependent-evidence invalidation keys | later card | **506** (proposals + evidence export) |
| Git: per-thread branch/working copy, fetch/reconcile before push, conflict reporting, commit secret scan | later card (destructive-Git denial covered: policy example) | **501** (git per thread) |
| Release-evidence traces (three writers, cross-client recovery, secret-redaction verification, CI-to-acceptance) | later cards | **602** (cross-component scenarios), **603** (release evidence) |
| 15 Nov baseline comparison (Q-09) | owner decision: baseline unavailable | owner |
