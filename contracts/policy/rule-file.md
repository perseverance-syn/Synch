# Policy rule file — v3.0.1

Schema: [`rule-file.schema.json`](rule-file.schema.json). Examples: [`examples/`](examples/). Fixtures: `../tests/cases.json`.
Sources: spec "Core architecture" (component 7: policy evaluation before mediated actions), "Policy and credentials", "Extension points", "Full Control and mode changes", "Project and Git", "Operational proposals"; decisions 01, 05, 13, 14, 20, 21.

## 1. Evaluation (authority first, restrictions second)
Every mediated action is evaluated **before** dispatch, and the result is recorded as `policy.decided` (actor, machine, target, outcome). Effects are ordered `allow < ask < deny`.

1. **Operator-authorized result `O`.** Collect the matching rules from `operator`-source files only. `O` is the most restrictive matching effect. **No matching operator rule ⇒ `O = deny`** (Q-01).
2. **Restrictions `R`.** Collect the matching rules from `project`, `role` and `skill` files. These may contain only `deny` and `ask` (schema-enforced). `R` is the most restrictive of them, or `allow` if none match.
3. **Built-in gates `B`** (not configurable; `builtin:` rules, §5.4). For operational actions (`net.firewall_change`, `net.network_change`, `deploy.production`) and every other `deploy.*`, `B = ask`. For all other actions `B = allow`.
4. **Effective = max(O, R, B).** So an operator `allow` on a firewall, network or production action still needs a human approval, and a deny stays deny.

A restriction file therefore can never raise authority. If `O = deny`, the effective result stays deny whatever R says, so a skill's `ask` cannot make an unmatched or denied action approvable. If `O = ask`, R can only keep ask or raise it to deny. If `O = allow`, R can lower it to ask or deny ("Extension points": loading a skill or role cannot increase a thread's authority).

Then, independently of rules, the authority checks of §4 must pass. Plan review (dec. 01) never satisfies an `ask`.

## 2. Trusted source (the loader, not the file, decides)
The `source` field is a declaration. The loader checks it against where the file came from and rejects the file on mismatch (logged as a daemon diagnostic; the file contributes nothing):

| source | the only accepted origin |
|---|---|
| `operator` | the daemon's policy store, written only by an authenticated caller holding the operator role via the API. The store sits under the daemon data dir, which is writable only by the service account. |
| `project` | the registered project's policy path, read from the accepted revision. It is restriction-only, because a worker can edit project files. |
| `role`, `skill` | the packaged role-skill pack in its trusted location outside worker-editable files (role-skills/README.md). Each file is pinned by digest and the pin is recorded with the assignment. |

A file whose declared `source` is `operator` but which arrives from any other origin is rejected. It is never downgraded and loaded.

**Qualified rule identity.** Rule `id`s are only unique within a file. Everywhere outside the file (`policy.decided.rule_id`, `approval.constraint_rules`, diagnostics) a rule is named `<source>:<origin-id>#<rule-id>`. The `origin-id` is loader-derived, never taken from the file:
- operator store: `store`;
- project: `<project-id>@<revision>`;
- role/skill: `<pack-file>@<sha256-prefix>`;
- built-in gates: `builtin:gates#<name>`.

The schema enforces this pattern. Two files may reuse a local id without ambiguity.

## 3. Records are not loaded from files
Grants, approvals and revocations confer authority **only** as API-created records. Each is persisted as an event (`grant.issued`, `approval.recorded`, `grant.revoked`) whose issuer is the authenticated operator caller identity (`granted_by`, `approver`, `revoked_by`). The `grants` / `approvals` / `revocations` sections of a rule file are the **export and fixture form** of those records (Q-10). The loader ignores them for authorization. Importing them requires an explicit operator API action that re-issues each record under the importing operator's identity.

## 4. Authority checks (independent of rules)
An effective `allow` (or a satisfied `ask`) still needs:
- the thread's access mode for that target (`sandbox` / `provider` / `full_control`; Q-19);
- for native host actions: a **live grant** (§6);
- for VM operations: the ownership rules of drivers/vm-driver.md §2 (dec. 13);
- for `merge`: human acceptance of the exact revision (dec. 21). For `deploy.*`: its own target-specific approval through the built-in gate (§5.4). Neither is implied by any `allow`.

The coordinator has no shell, host credentials or approval token (dec. 05). Its only actions are `plan.*`, `assign.*` and `evidence.read` (Q-33).

## 5. Bounded approvals (dec. 20, 21, Operational proposals)
### 5.1 Record
An approval record (schema `$defs/approval`) must have: `approver`, `thread`, exactly one of `revision` (full commit ID) **or** `plan_digest`, `worktree_digest`, non-empty `target`, explicit `actions` (no wildcards), `approved_at` (set by the daemon) and `expires_at`, with `expires_at > approved_at` (producer cross-check P1). `constraint_rules` (set by the daemon) lists the qualified ids of every matching ask rule and gate at creation, for display only.

**Operational actions** (`net.firewall_change`, `net.network_change`, `deploy.production`) must bind **`plan_digest`**: the digest of the exact reviewed plan, which includes the target and the rollback ("Operational proposals"). A revision-only approval for them is invalid (schema-enforced), because a changed operational plan can keep the same Git revision. They also need `rollback` (a command or a verified checkpoint ref) and `irreversible_effects` (empty list = the operator states there are none). The API input `CreateApproval`, the durable record and the `approval.recorded` event use the same constraints.

### 5.2 Worktree digest
`worktree_digest` = SHA-256 over the JCS of `{"revision": <HEAD commit>, "entries": [...]}`. Entries are sorted by path. There is one entry per tracked file that differs from HEAD, and per untracked file that is not ignored: `{"path", "mode", "sha256"}`, with deleted files as `sha256: null`. A clean tree has `entries: []`. It changes whenever uncommitted edits change, so an unchanged commit SHA cannot hide a changed diff.

### 5.3 Use-time check (unconditional)
On every use the daemon compares **all** of the following against current values. Any mismatch denies the action and emits `approval.voided` with the matching reason:

| check | void reason |
|---|---|
| approval `thread` == acting thread (Q-02) | `scope_changed` |
| `revision` == current HEAD, or `plan_digest` == current plan digest | `diff_changed` / `scope_changed` |
| `worktree_digest` == current worktree digest | `diff_changed` |
| `target` == action target | `target_changed` |
| action ∈ `actions` | `scope_changed` |
| `approved_at` ≤ now < `expires_at` | `expired` |
| **all** matching constraints (below): `expires_at − approved_at` ≤ the **minimum** `max_ttl` over every ask rule from every source that matches the action and target, and the built-in gate TTL | `scope_changed` (also checked at creation, where it is rejected) |
| the effective result (§1) for the action is still `ask`; if it became `deny`, the approval cannot override it | `scope_changed` |
| for operational actions: `plan_digest` bound, and `rollback` + `irreversible_effects` present | the approval is rejected at creation |

**Combining constraints.** Use-time checks never rely on one named rule. They recompute the set of matching ask rules (operator, project, role, skill) plus built-in gates from the **current** policy. Every constraint in that set applies: the strictest TTL, and every binding (bindings are uniform, §5.1). For example, a matching operator ask with `PT4H` plus a skill ask with `PT5M` gives a 5-minute limit. A restriction added after approval tightens it at the next use.

### 5.4 Built-in gates
| name | actions | effect | max_ttl |
|---|---|---|---|
| `builtin:gates#operational` | `net.firewall_change`, `net.network_change`, `deploy.production` | ask, plan digest required | `PT4H` (Q-53) |
| `builtin:gates#deployment` | other `deploy.*` (dec. 21: deployment needs its own target-specific approval) | ask | `PT4H` (Q-53) |

Thread end and revocation void it too (`thread_ended`, `revoked`).

## 6. Grants and inheritance (dec. 14)
- A grant binds `full_control` to **one thread, one host, one identity**, issued by an operator via the API. It ends at revocation or at the end of the granted thread (Q-21). A thread-end revocation is recorded as `grant.revoked{cause: thread_ended, ended_thread}` only after the producer has resolved `grant_id` to the recorded grant, checked that `ended_thread` is that grant's thread (not just any thread in the subtree), and checked that this thread is already terminal (cross-check C6, events/catalogue.md §4).
- **Dispatch-time check** for a native action by thread `T` on host `H` under identity `I`. It runs at every dispatch and every mediated action, never cached. Walk from `T` up to the root. The action is allowed only if some thread `A` on that path (`T` itself, or a managed ancestor) holds a grant `G` where:
  - `G.host == H` and `G.identity == I`;
  - `G` is not revoked;
  - `A` is not terminal;
  - every thread from `A` down to `T` is a managed descendant created while `G` was active.

  A different host or identity needs its own grant.
- **Revocation** always has `scope: subtree`. Its effects, in order:
  1. Commit the revocation. From this commit on, the dispatch check fails for the whole subtree.
  2. Stop dispatch.
  3. Settle active work under `G` (execution-driver.md §5).
  4. Record `grant.revoked{settled:true}`. Only now is a downgrade shown as complete ("Decision and access checks").

  Offline runners do not receive new actions under `G` (dec. 15). When they reconnect, revocation is applied before any further action.
- A downgrade cannot undo prior effects.

## 7. Action vocabulary (v2)
`git.{status,diff,commit,fetch,push,push_force,reset_hard,clean,branch_delete,history_rewrite,checkout_discard}` · `fs.{read,write,delete}` · `proc.exec` · `host.*` (native actions under Full Control) · `vm.{inventory,create,start,stop,delete,snapshot,restore}` · `net.{firewall_change,network_change}` · `deploy.{production,staging}` · `ci.{pipeline_edit,run}` · `secret.use` · `ssh.{connect,agent_forward}` · `skill.load` · `role.load` · `plan.*` · `assign.*` · `evidence.read` · `merge`.
Rule matchers may end in `.*`, which matches one or more further segments. A bare `*` matches every action. Approvals list exact actions only. New actions are a minor bump (Q-25).

## 8. Paths
Source: spec "Project and Git" ("Deny destructive Git operations on mediated paths"); Q-03.
`match.paths` uses globs. `@mediated` means every Synch-managed working copy of the thread. Matching uses the normalized absolute path, case-insensitive on Windows (Q-03).
