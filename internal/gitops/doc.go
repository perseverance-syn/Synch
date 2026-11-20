// Package gitops owns policy-mediated Git operations for Synch writing threads.
// It implements Project and Git and policy rule-file §§5.2, 7, and 8. The
// daemon persists registration and supplies policy decisions and secret scans.
//
// Registration records the origin remote's fetch and push URLs in the
// Project, recording both when they differ. Local paths, relative or
// absolute, are resolved against the repository root, cleaned, and
// symlink-resolved where they exist, so every recorded local endpoint is an
// absolute path that does not depend on the working directory of the process
// using it. Network URLs (scheme:// forms such as https, ssh, git, and
// scp-style user@host:path) are recorded verbatim. The live config is
// resolved the same way before it is compared with the pins. A project without an origin
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
// The transport is bound to those pinned endpoints by configuration
// isolation, not by checking alone. Each reconciliation creates a synthetic
// git directory owned by the runner whose config carries no url.*.insteadOf
// or url.*.pushInsteadOf members, and its fetch and push processes run
// against that directory with system and global config disabled
// (GIT_CONFIG_NOSYSTEM and an empty GIT_CONFIG_GLOBAL) while the
// repository's real object store is wired in through GIT_OBJECT_DIRECTORY.
// URL rewriting rules in any config scope therefore cannot redirect the
// transport, including rules added between the endpoint checks and the
// transport; the recorded URLs are used verbatim. A repository config that
// carries URL rewriting rules is refused with ErrRemoteURLChanged before the
// fetch and again before the push, so the URL probes the checks rely on are
// not trusted when rewriting is configured.
//
// RemoveThread requires the Settled option, by which the caller attests the
// thread's processes are stopped (execdriver Settle returned true); without
// it the call returns ErrNotSettled before touching anything. External
// writers are excluded by that precondition, not detected. It then refuses
// to remove a worktree that contains ignored files, which Git's non-forced
// remove would silently delete, unless the caller also passes DiscardIgnored.
// The ignored-file set is scanned, re-checked after the removal is authorized,
// and removed in one step under the service lock.
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
