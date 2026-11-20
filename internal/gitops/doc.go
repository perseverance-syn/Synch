// Package gitops owns policy-mediated Git operations for Synch writing threads.
// It implements Project and Git and policy rule-file §§5.2, 7, and 8. The
// daemon persists registration and supplies policy decisions and secret scans.
//
// Registration records the origin remote's fetch and push URLs in the
// Project, recording both when they differ. A project without an origin
// remote records no URLs and cannot reconcile: it is refused with
// ErrNoPinnedRemote before any network call. A remote with more than one
// fetch or push URL is refused as ambiguous. Fetch and push pass the recorded
// URLs to Git directly, never the remote name — the fetch URL for fetches and
// the push URL for pushes — and reconciliation re-checks the repository
// config against the pins before the fetch and again before the push: any
// drift refuses the operation with ErrRemoteURLChanged before a network call.
// Policy requests for fetch, merge, and push carry the used URL as
// "<URL>:<ref>" in Target.
// With NewWithEvidence, merge and push also carry the current HEAD and
// worktree digest so the decider can verify approval bindings while the
// service lock is held.
//
// RemoveThread refuses to remove a worktree that contains ignored files,
// which Git's non-forced remove would silently delete, unless the caller
// passes DiscardIgnored.
//
// Classification is conservative about disguised mutations. A fetch with a
// forced refspec (a + argument) or --force updates refs without
// fast-forwarding and is classified as git.push_force; merge --abort restores
// the pre-merge working copy and is git.checkout_discard; diff --output
// writes an arbitrary file and is fs.write. Run still executes only fixed
// status and diff commands.
//
// Reconciliation merges the fetched target into the thread branch. A push
// uses one explicit refspec and leaves enforcement of remote branch protection
// to the hosting service. Configure protected targets there to require review
// or checks when a hosting service is available.
package gitops
