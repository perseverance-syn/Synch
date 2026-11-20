---
name: synch-security-triage
description: Triage defensive scan or configuration findings against approved Synch baselines and verify remediation as Security.
---

# Security finding triage

1. Confirm the assigned revision or target, applicable trust boundary and approved baseline, and source of the finding. For Direct execution, use trusted inputs for defensive scan and review. Use checksum-verified Trivy or Gitleaks when the assignment calls for them. Record tool and version.
2. Capture rule, severity, affected component, location, and redacted evidence. Group duplicates while preserving each source. Assess exposure against the approved configuration and exceptions. Record the rationale and evidence for false positives and identify uncertainty explicitly.
3. Route code remediation to Builder. Give it the finding, source revision, expected safe behavior, and named verification check. Route network and production mutations to Ops for the applicable approval.
4. Review the daemon's rerun results on the exact fix revision and compare before and after evidence. Keep failed attempts, scan errors, partial results, and unresolved findings visible. Retain finding provenance and keep secrets out of evidence.
