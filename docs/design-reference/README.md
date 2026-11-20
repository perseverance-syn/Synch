# Owner design reference (2026-11-19, D-33)

The owner's chosen look for the desktop client (card 202). Build to these; they replace the parked card-200 design pack.

- `threads.png`: Threads view. Left rail (projects, views, departments with colour dots and counts), thread tree with sub-tasks, state / execution / audit pills, latest output, summary chips (running, paused, decision, to accept, failed), right detail panel with pause reason, actions, checks by status and live output.
- `approvals.png`: Approvals view. Pending list with expiry, detail with target, revision, plan and worktree digests, granted actions, expiry countdown, rollback and irreversible-effects panels, evidence shown separately from approval, "Approve exactly this" / Reject / Ask for changes.
- `findings.png`: Findings view. Severity filter chips, table (severity, tool, rule, component, location, scans, fix status), detail with fingerprint, revision, grouped scans with provenance, redacted evidence, fix-status actions.

Common: dark theme, amber primary action, monospace for ids/digests/commands, top bar with search (Ctrl K), daemon health and global seq. Check statuses always use the five words passed / failed / error / partial / not run.
