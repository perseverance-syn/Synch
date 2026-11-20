# Scanner driver contract — v2.0.0

Sources: spec "Findings and fixes", "Gate runner and CI", "Accurate status", "Security", "Policy and credentials" (redact known values from storage), "Verification and measurement". Finding schema: [`finding.schema.json`](finding.schema.json) 2.0.0. Scanner findings are emitted as `finding.recorded{source:"scanner"}`, whose payload is validated against that schema (events/catalogue.schema.json).

## 1. Tools
V1: Trivy, Gitleaks. Each is a pinned release with a known SHA-256 per OS/arch, kept in a trusted manifest outside worker-editable files.

## 2. Checksum verification
Before every run the driver hashes the binary and compares it with the manifest. A mismatch or missing entry ⇒ the scan is `error` with reason `checksum_mismatch`, and nothing runs. The verified hash is recorded in every finding (`tool.binary_sha256`). Signatures are verified too where the release publishes them (Q-15).

## 3. Run, redact, retain (in this order)
1. Inputs: exact revision (or image digest), config digest and tool version. Results are reused only while all three match ("Verification and measurement").
2. **Tool output never reaches durable storage unredacted.** Gitleaks always runs with `--redact`. All tool stdout/report output goes to a pipe or an in-memory buffer, or to a scratch file in a per-scan directory readable only by the service account. The scratch file is deleted when the scan ends, including on failure, and is never exported or retained.
3. The driver applies the known-secret redaction pass (the broker's known values → `[REDACTED]`) and, for Gitleaks, replaces every match field with `[REDACTED]`. If redaction itself fails, the scan is `error` with reason `redaction_failed`, and nothing is retained.
4. Only then is the **retained report** written. Its provenance is `{uri, digest: sha256 of the retained bytes, redacted: true}` (`$defs/report_ref`), referenced from each finding's `evidence.report` and `provenance[]`.
5. Parse result ⇒ `check.result`:
   - `passed`: report parsed, zero findings at or above the threshold;
   - `failed`: findings at or above the threshold;
   - `partial`: some targets scanned or some sections unparsed (reason required);
   - `error`: tool crash, checksum or redaction failure, malformed or unsupported report. **A malformed report never yields `passed`** ("Gate runner and CI");
   - `not_run`: not executed (reason required).

## 4. Normalization and grouping
- Severity: Trivy `CRITICAL/HIGH/MEDIUM/LOW/UNKNOWN` ⇒ `critical/high/medium/low/unknown`. Gitleaks has none ⇒ `high` (Q-16). The native value goes in `native_severity` when the tool has one.
- `component` is **required**. Trivy: the package, image or config it reports. Gitleaks: `{type:"secret", name:<rule id>}`.
- `fingerprint = "sha256:" + hex(SHA-256(JCS({tool: tool.name, rule: rule.id, component: {type, name, version or ""}, location: {path, start_line or 0}})))`. Findings with the same fingerprint are grouped, and `provenance[]` keeps every scan and report that produced them.
- Triage `false_positive` or `accepted_risk` requires a reason and an actor ("Security").

## 5. Go signatures (non-normative)

```go
type ScannerDriver interface {
    Tool() string // "trivy" | "gitleaks"
    Verify(ctx context.Context) (BinaryInfo, error) // checksum check; error => scan status error
    Scan(ctx context.Context, req ScanRequest) (ScanResult, error) // redacts before any durable write
}

type ScanResult struct {
    Status   string    // passed|failed|error|partial|not_run
    Reason   string
    Findings []Finding // finding.schema.json 2.0.0
    Report   ReportRef // retained, redacted
}
```
