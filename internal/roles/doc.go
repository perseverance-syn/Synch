// Package roles owns Synch's role registry: for each role (coordinator, builder, ops,
// security, auditor) the instructions, the eligible skills and the name of the
// tool/policy profile, loaded from a trusted pack and pinned per assignment. It also
// owns the deterministic specialist choice the coordinator uses to pick Builder, Ops or
// Security. It implements:
//
//   - spec line 29: "Role definitions package instructions, tools and policy. Skills
//     carry reusable instructions. ... Loading a skill or role cannot increase a
//     thread's authority." Load reads only the pack directory the daemon is configured
//     with (the shipped role-skills pack by default), never a worker-editable task
//     file. A Role names its policy profile but carries no grants, tools or rules: the
//     package imports neither internal/policy nor internal/thread, so loading a role
//     or skill has no way to change policy state, grants or a thread's role.
//   - spec lines 50-51 (role knowledge) and role-skills/README.md "How Synch loads the
//     skills": skills are pinned (ID plus sha256 of the text) at assignment time and
//     delivered through engine.StartRequest.Skills; Resolve re-checks a recorded pin
//     against the pack so a rollover or harness handoff continues with the same text or
//     fails closed.
//   - decision 06: Choose assigns Builder, Ops or Security from the required outcome and
//     returns the reason to record in child.assignment_recorded.
//
// The package does not read guidance files (internal/context), route harnesses, record
// events or evaluate policy; the daemon wires those. Every failure is a typed error
// (ErrUnknownRole, ErrMissingSkill, ErrDigestMismatch, ErrPathEscape, ErrBadPack) and a
// failed Load returns no registry at all.
package roles
