// Package policy is the policy engine the daemon calls before every mediated
// action (core component 7).
//
// It owns: rule-file loading with trusted-source checks (rule-file.md §2),
// authority-first evaluation Effective = max(O, R, B) (§1), the authority
// checks that follow the rules (§4), bounded approvals with unconditional
// use-time checks (§5), Full Control grants with inheritance and subtree
// revocation (§6), action matchers (§7) and path matching (§8). Every
// decision is recorded as policy.decided through internal/eventlog before
// the caller dispatches.
//
// It does not own: computing worktree or plan digests (card 501, injected via
// State.Current), VM ownership records and settling active work (injected via
// State), credentials and redaction (card 401), or HTTP (card 105). Grant and
// approval records live in memory and are rebuilt automatically from the
// event log by each engine before its first call (see Durability and recovery).
//
// Match dimensions (D-13). A missing list dimension (targets, hosts, roles,
// modes, args, paths) matches anything; an explicitly empty list matches
// nothing. Several args entries must all be present in the request (AND); the
// other lists match when the request value is any listed entry.
//
// Path containment (§8, D-14). Request paths must be absolute. Each path and
// each managed working copy is taken in two forms: lexical (cleaned) and
// resolved (every symlink and Windows junction in the existing part followed,
// the non-existent remainder cleaned lexically). Both are case-folded on
// Windows. A path-scoped allow matches only the resolved form, so a path that
// escapes through a link or fails to resolve is never allowed by it. A
// path-scoped deny or ask matches either form, so a path lexically inside the
// denied area stays denied even when a link points elsewhere.
//
// Windows spellings (G4). The extended-length forms \\?\C:\... and
// \\?\UNC\server\share\... are rewritten to C:\... and \\server\share\...
// before matching, for request paths, managed roots, rule globs and link
// targets, so allow, ask and deny see one spelling. The rewrite is made only
// when both spellings name the same object (K4, V3): every component must be
// statically stable (no trailing dot or space; not ".", ".." or empty; none
// of < > : " | ? * or control characters; not a DOS device name such as NUL,
// nul.txt, NUL: or COM¹), and the OS normalizer (GetFullPathNameW) must
// return the ordinary spelling exactly unchanged. Anything else is rejected
// and the request blocked, never mapped to another file; a rule glob in such
// a spelling makes the rule file fail to load. Every other namespace
// (\\.\ devices, \\?\Volume{...}, \\?\GLOBALROOT, \??\, a bare \\?\C:) is
// rejected: a request naming one is blocked before matching (deny-safe). A
// mapped drive and the UNC share behind it, and two distinct shares, are
// different locations and are never equated.
//
// Ordering (§5.3, §6, G2). Decide holds the read side of a dispatch boundary
// from evaluation through recording and Request.Start. A grant revocation
// commit and an explicit approval revoke take the write side, so they wait
// for every in-flight decision. A decision that passed an approval's checks
// also holds that approval's use lock for reading until it returns; a
// use-time void found by another decision takes it for writing. So no
// decision that read a grant or approval before its invalidation committed
// returns Allowed=true after it.
//
// Durability and recovery (G1). An operation exists once its first event is
// persisted. If that append fails, the call returns an error, nothing is
// half-done, and the caller retries: a revocation commit
// (grant.revoked{settled:false}) and an explicit approval revoke
// (approval.voided) change no state until their event is recorded. A void
// detected at use time is in force at once (use is denied); if its record
// fails it stays pending and is retried. Every engine replays the event log
// once, before its first public call does anything (Recover), rebuilding
// grants, approvals and committed-but-unsettled revocations; FlushPending
// then records pending voids, re-detects request-independent void conditions
// (a void lost to a restart is found again), and resumes revocations to
// settled:true. Replay (V1) reads the log in global_seq order and checks each
// consumed record (approval.recorded, approval.voided, grant.issued,
// grant.revoked, policy.decided, thread.state_changed) against the state as
// of that record, never the state at recovery time. Each payload is checked
// property by property against its catalogue schema version: no unknown
// properties; required ones present; forbidden ones absent, where an empty
// or null value counts as present; types, formats and enums; the
// thread.state_changed transition rows and conditional fields. Then the
// cross-checks: C1, C3, C6 (a thread_ended revocation needs the granted
// thread's terminal state_changed earlier in the log), P1, no duplicates, a
// void or decision naming only records live at that point, and one commit
// before at most one completion with an identical payload. Any failure is
// ErrCorruptHistory and fails every call closed, so replay never authorizes
// from, or cancels an obligation because of, bad history.
//
// A use that finds a request-dependent void (wrong thread, target or action;
// effective deny) is denied and appends the void before it returns. If that
// append fails, the call returns an error, the approval stays blocked in
// memory, and FlushPending retries until the void is durable. Known limit
// (owner-approved, K1): if the daemon restarts before that retry succeeds,
// that one void is lost, because the request that revealed it is gone; the
// denied use itself stayed denied. RevokeApproval returns nil only once a
// void is durable, waiting for any append already in flight (K2).
//
// The loader is stricter than rule-file.schema.json in these places only:
// duplicate object keys are rejected (JSON Schema sees one parsed value);
// path globs that path.Match cannot parse are rejected (they would silently
// never match, turning a deny into nothing); date-time values must parse as
// RFC 3339 (format is an annotation in draft 2020-12); duplicate local rule
// ids are rejected (§2 qualified ids must be unique); a policy_version token
// longer than 4096 bytes is rejected with its own error (G6), so a
// worker-editable project file cannot carry a huge number. Within that limit,
// policy_version accepts any JSON number exactly equal to 3 (3, 3.0, 0.3e1,
// 3 followed by any number of fractional zeros), as JSON Schema const does.
package policy
