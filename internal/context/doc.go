// Package context decides what a Synch agent session reads and how much tool
// output it is handed. It owns three things and nothing else:
//
//   - Guidance loading (spec "Context controls": load guidance by role, project and
//     target, stable instructions first): Guidance reads the role's pack in
//     role-skills/, an optional project guidance file and an optional target file,
//     and returns blocks that each carry their source and a sha256 digest.
//     Assemble orders them stable-first and is byte-for-byte deterministic.
//   - Output caps (same paragraph: cap output from Synch tools, retain the full
//     output on disk, return relevant excerpts and a searchable reference): Store
//     retains the full output under a data directory, returns a head, a tail and the
//     lines matching given keywords inside a configurable cap (default 16 KiB) and a
//     reference id, and Search finds lines in a retained output by that reference.
//   - Handoff files (spec "Rollover and handoff" and thread-state-machine T13, T14,
//     T35 and section 4): Handoff holds exactly the spec's list (brief version, plan,
//     revision, pending edits, failed attempts, open decisions, uncertain external
//     request IDs). WriteHandoff stores it content-addressed, ReadHandoff verifies it
//     and Handoff.Block turns it into a stable instruction block so a new session,
//     on the same or another harness, continues from the records. Native chat
//     transfer and live process migration are outside the guarantee.
//
// The package does not decide when a rollover happens, run git, route harnesses or
// touch the event log; the daemon wires those. Role skill files carry workflow
// text only: tools, grants and approvals stay with the daemon and policy.
package context
