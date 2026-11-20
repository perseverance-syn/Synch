# VM (provider) driver contract — v3.1.0

Sources: spec "Provider", "Machine providers", "Sandbox"; decisions 12, 13, 17, 18. Hypervisors in V1: Proxmox VE, VMware ESXi, VMware Workstation, Oracle VirtualBox, Microsoft Hyper-V.

## 1. Operations

| op | mutating | required for acceptance (dec. 17) |
|---|---|---|
| `inventory` | no | yes |
| `create_from_base` | yes | yes |
| `start` | yes | yes |
| `stop` | yes | yes |
| `delete` | yes | yes |
| `snapshot` | yes | only if advertised |
| `restore` | yes | only if advertised |
| `get_task` | no | yes where the platform has a task API; otherwise recorded as `not_run` with a reason (Q-40) |

Drivers hold the infrastructure credentials. Workers never receive them ("Provider", dec. 05).

## 2. Authorization (dec. 13, applied exactly)
Each known VM has an ownership record `{endpoint, vm_ref, owner_thread, basis: created|assigned, assigned_by?, assigned_at?, disposition: keep|discard|undecided}`.

- **Visibility.** `inventory` grants visibility only. Listing a VM confers no mutation right.
- **Create** (`create_from_base`) needs all of:
  1. policy result allow, or a satisfied ask, for `vm.create` on that endpoint (rule-file.md §1);
  2. the thread's access mode on that endpoint is `provider` (or `sandbox`, for its own disposable VM per dec. 12);
  3. the endpoint and thread VM limits are not exceeded (Q-14).

  On success the new VM's owner is the **creating thread**, `basis: created`. No other thread gains rights from this.
- **Mutate** (`start`, `stop`, `delete`, `snapshot`, `restore`) on an existing VM is allowed only if `owner_thread == acting thread` and either:
  - `basis: created`, meaning this thread created it; or
  - `basis: assigned` by an **operator**, through the API under an authenticated operator identity, to this exact thread.

  There is no inheritance through ancestry and no assignment by the coordinator or any agent. A child that needs a VM its parent created must be explicitly assigned it by the operator, or create its own. Otherwise the request is denied before dispatch (`not_owner`, `policy.decided{decision:deny}`).
- The policy check applies to mutations too (`vm.start`, …). Ownership is necessary, not sufficient.
- Created resources are tagged with the thread ID and the request token (§3) where the platform supports tags. Orphans (owner terminal, disposition `undecided`) are listed for reconciliation and never auto-deleted. Closing a workspace needs an explicit keep or discard decision per lab resource ("Machine providers").

## 3. Uncertain operations (dec. 18)
1. **Serialize.** At most one mutating request is in flight per `(endpoint, resource)`. Concurrent requests queue. This makes "the operation on this resource" unambiguous.
2. **Persist before dispatch** `external_request.persisted` with `external_request_id`, `driver`, `endpoint`, `operation`, `resource` (VM ref, or for create the intended name) and `params_digest`.
3. **Client token.** The token is always exactly `synch-<external_request_id>`. It is derived, not chosen, so it is unique per request: a new ULID per request, never reused, including for retries, which are new requests. It is sent to the provider as idempotency token, tag, task description or snapshot name/description, whichever the platform supports. A driver that cannot carry a 32-character token on some operation declares that in its matrix row (Q-55), and that operation then has no `resulting_resource` evidence.
4. As soon as the provider returns a task ID, persist `external_request.task_bound{provider_task_id}`.
5. Response received ⇒ `external_request.resolved{via:response}`.
6. Response lost (timeout, disconnect, daemon crash) ⇒ reconcile. The resolution needs a **sufficient observation**. Anything else is `unknown`:

| resolution | sufficient observations (any one) | never sufficient |
|---|---|---|
| `succeeded` | provider response success; the bound provider task (`provider_task_id` from `task_bound`) finished successfully; for `create_from_base` and `snapshot` only: a resource on the same endpoint carrying **this** request's token (`via: resulting_resource`) | a power state or name match without the token; a token from another request; an engine session or checkpoint |
| `failed` | provider response error; the bound provider task finished with an error; operator | resource evidence of any kind |
| `not_executed` | the provider explicitly rejected the request before execution (`via: response`); **authoritative absence**: the provider task API, queried by this token on a platform whose matrix row declares token lookup authoritative, has no such task (`via: provider_task_absent` with `absence_lookup{client_token, queried_at}` and **no** `provider_task_id`); operator, with note | the absence of a token-bearing resource; a token-bearing resource (that proves execution) |
| `unknown` | none of the above (`via: none`) | — |

   **Evidence field sets.** The catalogue schema enforces these. Each evidence field may appear only with its own `via`, so a record cannot carry evidence that contradicts its resolution:

| via | required fields | allowed resolutions | forbidden fields |
|---|---|---|---|
| `response` | — | succeeded, failed, not_executed | resource_evidence, provider_task_id, absence_lookup, operator, note |
| `provider_task` | provider_task_id | succeeded, failed | resource_evidence, absence_lookup, operator, note |
| `provider_task_absent` | absence_lookup | not_executed | resource_evidence, provider_task_id, operator, note |
| `resulting_resource` | resource_evidence (operation create_from_base or snapshot) | succeeded | provider_task_id, absence_lookup, operator, note |
| `operator` | operator, note | succeeded, failed, not_executed | resource_evidence, provider_task_id, absence_lookup |
| `none` | — | unknown | every evidence field |

   For `start`, `stop`, `delete` and `restore`, resource state is never attribution. Cross-check C5 enforces that every token in `resource_evidence` or `absence_lookup` equals `synch-<external_request_id>`.

   **Consistency across records (guard G.1).** A request counts as settled only if its latest `external_request.resolved` is consistent with every observation recorded for the same request. If any later observation contradicts it (a token-bearing resource found after `not_executed` or `failed`, or a bound task reporting a different outcome), the producer records a new `resolved{resolution: unknown, via: none}`. The request is then unresolved again, and G.1 blocks dispatch until a consistent, sufficient resolution is recorded.
7. If the result is `unknown`, the thread does not dispatch (guard G.1): it pauses with `reconciliation`, or stays in `daemon_recovery`. **Never** repeat blindly. Resolution then comes from a later sufficient observation, or from the operator (`resolve_external_request`, recorded with `via: operator`, operator identity and a non-empty note).
8. Snapshots are verified (exists, listed, consistent) before anything relies on them ("Machine providers").

## 4. Capability matrix row (dec. 17)
One row per (driver, platform, platform version), with results from a real host:

```json
{
  "matrix_version": 1,
  "driver": "proxmox",
  "driver_version": "0.1.0",
  "platform": "Proxmox VE",
  "platform_version": "8.2",
  "tested_at": "2026-11-20T12:00:00Z",
  "host": "lab-pve-01",
  "ops": {
    "inventory": "passed", "create_from_base": "passed", "start": "passed", "stop": "passed", "delete": "passed",
    "get_task": "passed", "snapshot": "passed", "restore": "failed", "lost_response_reconcile_snapshot": "passed"
  },
  "advertised": ["inventory", "create_from_base", "start", "stop", "delete", "snapshot"],
  "sandbox_certified_configs": [],
  "evidence": "evidence/matrix/proxmox-8.2.json"
}
```
Op values use the check vocabulary (`passed|failed|error|partial|not_run`). An op is advertised only if `passed`. Snapshot is advertised only with a passing `lost_response_reconcile_snapshot` (Q-36). The JSON above is an illustrative example (Q-41), not a schema.

## 5. Go signatures (non-normative)

```go
type VMDriver interface {
    Capabilities(ctx context.Context) (MatrixRow, error)
    Inventory(ctx context.Context, endpoint string) ([]VM, error)
    CreateFromBase(ctx context.Context, xr ExternalRequest, base string, spec VMSpec) (TaskRef, error)
    Start(ctx context.Context, xr ExternalRequest, vm string) (TaskRef, error)
    Stop(ctx context.Context, xr ExternalRequest, vm string) (TaskRef, error)
    Delete(ctx context.Context, xr ExternalRequest, vm string) (TaskRef, error)
    Snapshot(ctx context.Context, xr ExternalRequest, vm, name string) (TaskRef, error) // ErrUnavailable if not advertised
    Restore(ctx context.Context, xr ExternalRequest, vm, snapshot string) (TaskRef, error)
    GetTask(ctx context.Context, endpoint string, t TaskRef) (TaskStatus, error) // ErrUnavailable if no task API
    // FindByToken returns resources on the endpoint carrying the client token (tag/name/description).
    FindByToken(ctx context.Context, endpoint, clientToken string) ([]ResourceEvidence, error)
}
// ExternalRequest carries ID, Endpoint, Operation, Resource, ParamsDigest (the persisted record); token = "synch-" + ID.
```
