# CI provider driver contract — v0.1.0 (proposed: pending owner acceptance with card 503)

Sources: spec "Gate runner and CI" ("Run them locally and integrate the same definition with GitHub Actions, GitLab CI and Jenkins. Surface errors and partial imports. A malformed or unsupported report cannot produce a pass."), decision 19·B ("Preserve equivalent check definitions, thresholds and outcome reporting across claimed integrations."), decision 18 (uncertain operations), "Policy and credentials" ("Broker secrets at use and redact known values from supported inputs, outputs, storage and exports"). Providers covered here: GitHub Actions and GitLab CI. Jenkins is card 504 and must implement the same interface.

## 1. Same definition as the local gate runner
The repository's gate file (`.synch/gates.json`, `internal/gates`) is the single definition of ordered checks, thresholds and report parsers. A CI integration does not redefine it. The CI workflow/pipeline in the repository runs the gate file, **one CI job per gate check, the job name equal to the check `id`**. Thresholds and report parsing happen inside that job by the same runner, so a job's conclusion already includes the threshold verdict. The driver reads jobs and maps each to one `check.result`-shaped record (§4); it never re-parses reports or re-evaluates thresholds. A job name that matches no gate check is reported as is and the consumer decides; a gate check with no job is `not_run`, reason `no CI job for check`.

A workflow/pipeline must carry the request token (§3): GitHub Actions via a `workflow_dispatch` input `synch_token` plus input `sha`, with `run-name: ${{ inputs.synch_token }}`; GitLab CI via the pipeline variables `SYNCH_TOKEN` and `SYNCH_SHA`.

## 2. Operations
| op | mutating | meaning |
|---|---|---|
| `trigger` | yes | start a run for the exact commit `sha` on `ref` (the ref must currently point at `sha`; the run is bound to `sha`, §5) |
| `find_by_token` | no | runs carrying a client token (reconciliation) |
| `status` | no | run state and the SHA the run used |
| `jobs` | no | jobs of a run with verdict and SHA |
| `logs` | no | full log of one job; the driver caps what it returns and retains the full redacted log by reference (§7) |
| `cancel` | yes | request cancellation of a run |

Go signatures (non-normative; `internal/ci`):
```go
type Provider interface {
    Name() string
    Trigger(ctx context.Context, req TriggerRequest) (RunRef, error)   // RunRef.ID may be empty (GitHub returns no run)
    FindByToken(ctx context.Context, req TriggerRequest) ([]RunRef, error)
    Status(ctx context.Context, run RunRef) (RunStatus, error)
    Jobs(ctx context.Context, run RunRef) ([]Job, error)
    Logs(ctx context.Context, run RunRef, jobID string) ([]byte, error)
    Cancel(ctx context.Context, run RunRef) error
}
```

## 3. Uncertain operations (dec. 18), reusing the existing events
The driver persists a trigger request with the **existing** catalogue events `external_request.persisted`, `external_request.task_bound`, `external_request.resolved`; no new event type is added.
1. `persisted{external_request_id, driver: "github-actions"|"gitlab-ci", endpoint, operation: "trigger", resource: "<project>@<sha>", params_digest}` is written **before** dispatch. If it cannot be written, nothing is dispatched.
2. The client token is `synch-<external_request_id>` (vm-driver.md §3.3), sent as the workflow input / pipeline variable (§1). A retry is a new request with a new token.
3. A returned run ID ⇒ `task_bound{provider_task_id: <run id>}`. A provider response success ⇒ `resolved{succeeded, via: response}`; an explicit provider rejection (HTTP 4xx other than 408) ⇒ `resolved{not_executed, via: response}`.
4. Lost response (timeout, 5xx, disconnect) ⇒ reconcile with `find_by_token`. Exactly one run carrying this token ⇒ `succeeded` (`via: resulting_resource`, bind the run). More than one, none, or a lookup error ⇒ `resolved{unknown, via: none}`. **Absence is never authoritative** (runs may appear late), so none ⇒ `unknown`, never `not_executed`. The driver never calls `trigger` again for that request; unknown pauses per guard G.1.
5. **Catalogue gap (open, needs owner decision):** the catalogue `operation` enum (`create_from_base|start|stop|delete|snapshot|restore`) has no `trigger`, and `via: resulting_resource` is limited to `create_from_base|snapshot`. This card does not touch the catalogue. The driver emits these records through an injected `Journal`; wiring them into the event log requires the catalogue to add operation `trigger` and allow `resulting_resource` for it.

## 4. Mapping to the five check statuses (`internal/gates` Status)
Provider verdicts map per job; a verdict not listed is `error` with reason `unrecognised <provider> state "<raw>"`. Never `passed` by default.

| neutral verdict | GitHub (`status`/`conclusion`) | GitLab (job `status`) | check status | reason |
|---|---|---|---|---|
| passed | `completed`/`success` | `success` | `passed` | — |
| failed | `completed`/`failure` | `failed` | `failed` | `CI job failed` |
| cancelled | `cancelled` | `canceled` | `error` | `cancelled` |
| timed out | `timed_out` | job `failed` with `failure_reason` `job_execution_timeout` or `stuck_or_timeout_failure` | `error` | `timed out` |
| skipped | `skipped` | `skipped`, `manual`, `created` | `not_run` | `skipped by CI` |
| not finished | `queued`, `in_progress`, `waiting`, `pending` | `pending`, `running`, `preparing`, `waiting_for_resource`, `scheduled` | `not_run` | `job not finished` |
| other | `neutral`, `action_required`, `stale`, `startup_failure`, anything else | `canceling`, anything else | `error` | unrecognised state |

Run-level rules: a run cancelled or timed out downgrades every job that would be `passed` to `error` (reason `run cancelled` / `run timed out`). If run status or jobs cannot be read, the result is a single `error` record `check: "ci-run"` with reason `status unreadable: <redacted cause>`. `partial` is produced only when a consumer imports a subset of results; the driver itself does not invent it.

## 5. Commit binding
Every result's `revision` is the SHA the run reports having used (GitHub `head_sha`, GitLab pipeline `sha`). If that differs from the SHA requested (run level or job level) every job of the run is `error`, reason `commit mismatch: run used <a>, requested <b>`. Comparison is exact on the full lower-case SHA; abbreviated SHAs are rejected at request time.

## 6. Credentials
Tokens come only from an injected `TokenBroker` (`Token(ctx, endpoint)` and `KnownValues()`), at the moment of use, and go only into the `Authorization` (GitHub, `Bearer`) or `PRIVATE-TOKEN` (GitLab) header. They never appear in URLs, error strings, journal records, results or retained logs. Every error text, response excerpt and log passes through redaction with `KnownValues()` before leaving the driver. `TokenBroker` is a minimal interpretation, to be replaced by the credentials broker of card 401.

## 7. Logs
`logs` returns the full job log. The driver redacts it, stores the full redacted log through an injected `LogStore` (returning a reference `ref` and `sha256` digest) and returns only a capped excerpt (the last `cap` bytes, `truncated: true` when cut) plus the reference. A hard retention limit (default 32 MiB) applies; beyond it the retained log is cut and flagged, and the tail the length of the longest secret minus one is dropped so a secret straddling the cut cannot survive partially.

## 8. Capability matrix row
One row per (provider, platform, version), with results from a real service:
```json
{
  "matrix_version": 1,
  "driver": "github-actions",
  "driver_version": "0.1.0",
  "platform": "GitHub Actions",
  "platform_version": "github.com REST 2022-11-28",
  "tested_at": null,
  "ops": {
    "trigger": "not_run: deferred by owner", "find_by_token": "not_run: deferred by owner",
    "status": "not_run: deferred by owner", "jobs": "not_run: deferred by owner",
    "logs": "not_run: deferred by owner", "cancel": "not_run: deferred by owner",
    "lost_response_reconcile_trigger": "not_run: deferred by owner"
  },
  "evidence": "internal/ci httptest mocks only (not a real service)"
}
```
V1 tests have no network: every real-provider row is `not_run` with reason `deferred by owner`. Mock tests prove the driver logic, not the service. The GitLab row is identical with `driver: "gitlab-ci"`, `platform: "GitLab CI"`, `platform_version: "REST v4"`.

Public sources the mocks follow: GitHub REST "Create a workflow dispatch event", "List workflow runs for a repository", "List jobs for a workflow run", "Download job logs for a workflow run", "Cancel a workflow run"; GitLab REST "Create a new pipeline", "List project pipelines", "Get pipeline variables", "List pipeline jobs", "Get a log file (trace)", "Cancel a pipeline's jobs".
