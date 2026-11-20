# Synch V1 role skills

This starter pack implements the proposed skill assignments for the five roles in the 17 November 2026 locked specification. Each skill is a portable `SKILL.md`. The role mapping is a starter configuration for the Synch daemon and adapters.

| Role | Skill | Trigger |
| --- | --- | --- |
| Coordinator | `synch-coordinator-planning` | Plan, assign, or reconcile accepted work |
| Builder | `synch-builder-delivery` | Deliver a change with evidence for the submitted revision |
| Ops | `synch-ops-change-control` | Diagnose or change infrastructure, CI, or deployments |
| Security | `synch-security-triage` | Triage defensive findings or verify a fix |
| Auditor | `synch-independent-audit` | Perform an independent review at the selected audit level |

## How Synch loads the skills

1. Package these files with Synch as reviewed, versioned instructions. Use `role-skills.json` as the proposed mapping from a Synch role to the skills eligible for its assignments. Resolve paths relative to this pack. Pin the skill contents for the assignment and record the loaded skill IDs and versions or content digests with the assignment evidence.
2. In each adapter, deliver the same selected skill text in the assignment's instruction context. Native skill discovery can serve as a delivery mechanism after the adapter verifies that the correct role-scoped version is used. Instruction injection at session start is a portable fallback. Preserve the pin across rollover and harness handoff. Stage 0 must check integration terms, supported versions, and authenticated behavior for Codex, Claude Code, and Kimi Code CLI.
3. Resolve tools, grants, policy, and approvals through Synch. The skill files carry workflow instructions only. Keep the role mapping and its skill contents in a trusted location outside worker-editable task files. A skill request uses the same daemon checks as any other mediated action. Keep the documented host-access and journal limits for Direct execution.

## Upstream patterns used

- Addy Osmani's planning and test-driven development skills informed the Coordinator's criteria and dependency mapping and the Builder's regression checks. [Planning and task breakdown](https://github.com/addyosmani/agent-skills/blob/39b723ceebf81474d0557c1fecd01eddab585563/skills/planning-and-task-breakdown/SKILL.md) · [Test-driven development](https://github.com/addyosmani/agent-skills/blob/39b723ceebf81474d0557c1fecd01eddab585563/skills/test-driven-development/SKILL.md)
- Addy's CI, review, and hardening workflows informed named gates, evidence-based audit findings, and defensive triage. [CI/CD and automation](https://github.com/addyosmani/agent-skills/blob/39b723ceebf81474d0557c1fecd01eddab585563/skills/ci-cd-and-automation/SKILL.md) · [Code review and quality](https://github.com/addyosmani/agent-skills/blob/39b723ceebf81474d0557c1fecd01eddab585563/skills/code-review-and-quality/SKILL.md) · [Security and hardening](https://github.com/addyosmani/agent-skills/blob/39b723ceebf81474d0557c1fecd01eddab585563/skills/security-and-hardening/SKILL.md)
- Claude Code Templates contributed deployment preparation and rollback patterns for Ops and fresh verification before completion for Builder. [Deployment procedures](https://github.com/davila7/claude-code-templates/blob/5e5cc3db7f9fb71bc2316417293cc2fc778ce482/cli-tool/components/skills/development/deployment-procedures/SKILL.md) · [Verification before completion](https://github.com/davila7/claude-code-templates/blob/5e5cc3db7f9fb71bc2316417293cc2fc778ce482/cli-tool/components/skills/development/verification-before-completion/SKILL.md)

Both repository roots publish MIT licenses. Check the individual component terms and carry required notices before vendoring files. For example, the Templates catalog's `docx` component declares its own proprietary terms. This pack uses Synch-specific instructions authored from the locked spec and the workflow ideas above. [Addy license](https://github.com/addyosmani/agent-skills/blob/39b723ceebf81474d0557c1fecd01eddab585563/LICENSE) · [Templates license](https://github.com/davila7/claude-code-templates/blob/5e5cc3db7f9fb71bc2316417293cc2fc778ce482/LICENSE) · [DOCX component terms](https://github.com/davila7/claude-code-templates/blob/5e5cc3db7f9fb71bc2316417293cc2fc778ce482/cli-tool/components/skills/document-processing/docx/LICENSE.txt)

## Runtime checks for integration

- Submit a representative assignment for each role through each adapter. Confirm the intended skill is available and the assignment's tool grants and approvals still follow daemon policy. Request an out-of-role skill and an unauthorized mediated action to verify both checks.
- Resume a paused assignment through another supported harness. Verify the selected skill version and accepted brief survive handoff alongside unsettled requests and evidence references.
- Exercise a stale approval, a missing acceptance criterion, and a gate result tied to a different revision. Check that the daemon blocks the action or surfaces the missing evidence as appropriate.

This workspace contains the skill pack and proposed mapping. Live adapter integration and authenticated qualification require the Synch checkout.
