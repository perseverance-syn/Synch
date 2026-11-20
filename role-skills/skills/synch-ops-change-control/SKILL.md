---
name: synch-ops-change-control
description: Diagnose, plan, verify, or recover Synch infrastructure, pipeline, and deployment changes as Ops.
---

# Ops change control

1. Confirm the registered target, current state, and assignment. Gather read-only health evidence within the current grant. Check ownership or explicit assignment before requesting a Provider action that changes a VM.
2. Before a real change, write down its target, purpose, failure modes, verification checks, and rollback. For production, firewall, and network changes, include a rollback command or verified checkpoint and state which external effects rollback cannot undo. Validate in a disposable environment when available and identify checks the environment could not run.
3. Submit the exact plan through Synch for the required human approval. Check that the approval applies to the thread, target, plan or revision, permitted operation, and expiry. Stop the affected action if the revision, target, or scope changes and request a current decision through Synch.
4. For CI work, use the repository's ordered gate definitions, thresholds, and parsers across the local runner and the applicable GitHub Actions, GitLab CI, or Jenkins integration. Preserve failed, error, partial, and unrun results. Malformed or unsupported reports require an explicit error or partial outcome as appropriate.
5. Use the assigned execution path and retain the recorded request ID. If a response is lost, reconcile the provider task or resulting resource before proceeding. Leave an unknown outcome pending reconciliation.
6. Compare post-change health against the planned checks. Record the exact revision or target, checks, evidence, deviations, and recovery outcome. Update the assigned runbook, monitoring, or restore evidence when the change affects them. Submit changes to standing runbooks for operator acceptance.
