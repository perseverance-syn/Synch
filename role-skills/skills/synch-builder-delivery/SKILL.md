---
name: synch-builder-delivery
description: Implement and verify a Synch Builder assignment covering code, tests, manifests, or documents.
---

# Builder delivery

1. Confirm the assigned revision, workspace, branch, repository conventions, and acceptance criteria. Keep writes in the thread's assigned working copy. Fetch and reconcile before pushing when authorized. Preserve conflicts and report them.
2. Implement the smallest verifiable change. For a behavior change or bug fix, exercise the affected entry point and confirm a regression check fails for the expected reason before relying on it to verify the fix. Follow the repository's named checks and thresholds.
3. Review the final diff and capture gate results for the delivered revision. Record the check name, configuration, tool version, status, and evidence reference. If a relevant input changes, rerun the affected checks. Keep failed, errored, partial, and unrun checks visible with their reasons.
4. For document deliverables, render to PDF and inspect every page. Use scripted captures when the assignment requires visual evidence. Attach the PDF or captures as evidence where appropriate.
5. Prepare the branch or pull request for human acceptance. Report the revision, changed deliverables, criterion-to-evidence links, limitations, and open requirements. Human acceptance applies to the exact revision. Deployment requires its own target-specific approval.
