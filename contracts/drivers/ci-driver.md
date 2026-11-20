# CI provider driver contract — v0.2.0 (accepted v0.1.0 (D-46); v0.2.0 adds Jenkins, pending owner acceptance with card 504)

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

## 9. Jenkins (additive in v0.2.0)

### Pipeline and commit evidence

Configure a parameterized Pipeline job with string parameters `SYNCH_REF`, `SYNCH_SHA` and `SYNCH_TOKEN`. The project is the slash-delimited Jenkins folder and job name. The job must verify that `SYNCH_REF` resolves to `SYNCH_SHA`, check out that exact SHA using the Git plugin, and run the same gate definition as §1. Configure one Pipeline stage per gate check and use the check ID as the stage name. Read the build's Git plugin checkout evidence from `actions[].lastBuiltRevision.SHA1`. Missing checkout evidence for a successful build and conflicting checkout SHAs are errors. Apply §5 to an observed SHA mismatch.

### Dispatch, recovery and cancellation

Use the §3 dispatcher with driver `jenkins`. Send the three parameters as a form body to `POST .../buildWithParameters`. A 201 response supplies the queue item in `Location`. Only a queue item URL under the configured controller is accepted. Poll the item to obtain its executable build number. After queue eviction, look up that queue ID among the job's 100 most recent builds.

For a lost response, look for `SYNCH_TOKEN` in the job's queue items and its 100 most recent builds. A queue item and its executable represent one request. Exactly one matching run settles the request. Missing or ambiguous evidence stays unknown under §3. Use `POST .../stop` for an executable build and `POST /queue/cancelItem` for a pending item.

### State and logs

| Jenkins build or stage state | Check status | Interpretation |
|---|---|---|
| `SUCCESS` | `passed` | Stage succeeded on the verified commit |
| `FAILURE`, `FAILED`, `UNSTABLE` | `failed` | Stage failed or is unstable |
| `ABORTED` | `error` | Cancelled or interrupted, including Jenkins timeouts reported as aborted |
| `TIMED_OUT`, `TIMEOUT` | `error` | Explicit timeout |
| `NOT_BUILT`, `NOT_EXECUTED` | `not_run` | Skipped |
| `QUEUED`, `IN_PROGRESS`, `PAUSED_PENDING_INPUT` | `not_run` | Queue wait or ongoing work |
| Any other state | `error` | Unrecognised state |

Use the Pipeline REST API plugin's `wfapi/describe` for stages and the stage node's `wfapi/log` for text. The `console` log ID explicitly requests build-wide `consoleText`. A stage response that reports additional text below the retention boundary produces an error. Use the provider's `FetchLog` to apply §7 with the session credentials included. A cancelled or timed-out build suppresses passed stages, even when some stages completed before the interruption.

### Credentials

Read the API token and username from the configured broker keys `endpoint` and `endpoint/user`, then send them in the Basic authorization header. API-token authentication is crumb-exempt on modern Jenkins. For controllers configured to require a crumb, fetch `/crumbIssuer/api/json` before a mutation and send the returned `crumb` in the `crumbRequestField` header with the issuer's session cookie. A failed or malformed issuer response prevents the mutation. The driver handles the standard `Jenkins-Crumb` and legacy `.crumb` fields. Accepted credentials, crumb values, and session cookies are redacted from log evidence and excluded from error diagnostics.

### Capability matrix

```json
{
  "matrix_version": 1,
  "driver": "jenkins",
  "driver_version": "0.2.0",
  "platform": "Jenkins",
  "platform_version": "Remote Access API and Pipeline REST API plugin",
  "tested_at": null,
  "ops": {
    "trigger": "not_run: deferred by owner", "find_by_token": "not_run: deferred by owner",
    "status": "not_run: deferred by owner", "jobs": "not_run: deferred by owner",
    "logs": "not_run: deferred by owner", "cancel": "not_run: deferred by owner",
    "lost_response_reconcile_trigger": "not_run: deferred by owner"
  },
  "evidence": "internal/ci/jenkins httptest mocks only"
}
```

Public documentation for the mock shapes includes the [Jenkins Remote Access API](https://www.jenkins.io/doc/book/using/remote-access-api/), [Jenkins CSRF protection](https://www.jenkins.io/doc/book/security/csrf-protection/), [Pipeline REST API plugin](https://plugins.jenkins.io/pipeline-rest-api/), and the [Git plugin BuildData API](https://javadoc.jenkins.io/plugin/git/hudson/plugins/git/util/BuildData.html).
