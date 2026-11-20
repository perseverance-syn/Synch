package gitops

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// ScanStage tells the scanner whether to inspect the index or outgoing commits.
type ScanStage string

const (
	PreCommit ScanStage = "pre-commit"
	PrePush   ScanStage = "pre-push"
)

// ScanRequest identifies the index or immutable revision range to scan.
type ScanRequest struct {
	Stage    ScanStage
	Worktree string
	From     string
	To       string
}

// Scanner returns nil only if the requested scan passes. It runs with the
// service lock held and must not call the service.
type Scanner func(context.Context, ScanRequest) error

// ScanResult is the outcome of the secret-scan hook. The zero value means the
// operation did not reach that hook.
type ScanResult string

const (
	ScanUnconfigured ScanResult = "not run: scanner not configured"
	ScanPassed       ScanResult = "passed"
	ScanFailed       ScanResult = "failed"
)

// scanSecrets runs the configured hook and preserves a distinct missing-scanner outcome.
func (s *Service) scanSecrets(ctx context.Context, request ScanRequest) (ScanResult, error) {
	if s.scan == nil {
		return ScanUnconfigured, nil
	}
	if err := s.scan(ctx, request); err != nil {
		return ScanFailed, fmt.Errorf("%s secret scan: %w", request.Stage, err)
	}
	return ScanPassed, nil
}

// StageAll stages changes throughout the verified thread worktree.
func (s *Service) StageAll(ctx context.Context, t Thread) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.verifyThread(ctx, t); err != nil {
		return err
	}
	_, err := t.run(ctx, s, Commit, t.Branch(), "add", "--all")
	return err
}

// Commit creates a commit from the thread's staged changes after the scan hook.
func (s *Service) Commit(ctx context.Context, t Thread, message string) (ScanResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.verifyThread(ctx, t); err != nil {
		return "", err
	}
	return s.commit(ctx, t, "-m", message)
}

// commit shares the scan hook for authored and reconciliation commits.
func (s *Service) commit(ctx context.Context, t Thread, args ...string) (ScanResult, error) {
	result, err := s.scanSecrets(ctx, ScanRequest{Stage: PreCommit, Worktree: t.path})
	if err != nil {
		return result, err
	}
	_, err = t.run(ctx, s, Commit, t.Branch(), append([]string{"commit"}, args...)...)
	return result, err
}

// --- Reconciliation: preserve conflicts and publish only a scanned descendant ---

// pinnedRemote is the remote whose endpoints Register pins for reconciliation.
const pinnedRemote = "origin"

// RemoteTarget identifies a configured Git remote and an existing target branch.
type RemoteTarget struct {
	Remote string
	Branch string
}

// PushResult records the revisions and scans reached in a reconciliation attempt.
type PushResult struct {
	Fetched   string
	Revision  string
	PreCommit ScanResult
	PrePush   ScanResult
}

// ConflictError lists unresolved paths relative to the thread worktree.
type ConflictError struct {
	Paths []string
	Cause error
}

// Error reports the paths without changing the conflicted working copy.
func (e *ConflictError) Error() string { return fmt.Sprintf("%s: %q", ErrConflict, e.Paths) }

// Unwrap supports errors.Is and retains Git's diagnostic if the merge failed.
func (e *ConflictError) Unwrap() []error {
	if e.Cause == nil {
		return []error{ErrConflict}
	}
	return []error{ErrConflict, e.Cause}
}

// PushError retains Git's porcelain report and diagnostic for a failed push.
type PushError struct {
	Report string
	Cause  error
}

// Error describes an unsuccessful non-forced push.
func (e *PushError) Error() string {
	return fmt.Sprintf("%s: %s: %v", ErrPushRejected, e.Report, e.Cause)
}

// Unwrap exposes the push failure kind and the underlying Git error.
func (e *PushError) Unwrap() []error { return []error{ErrPushRejected, e.Cause} }

// ReconcileAndPush fetches the target branch, merges it into the thread, and
// pushes the resulting commit without force. It never retries a refused push.
// Fetch and push pass the URLs recorded at registration to Git directly, so a
// config edit after registration cannot redirect either network call.
func (s *Service) ReconcileAndPush(ctx context.Context, t Thread, target RemoteTarget) (PushResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result PushResult
	if err := s.verifyThread(ctx, t); err != nil {
		return result, err
	}
	if target.Remote == "" || strings.HasPrefix(target.Remote, "-") || target.Branch == "" {
		return result, errors.New("git remote and target branch are required")
	}
	if t.project.FetchURL == "" || t.project.PushURL == "" {
		return result, fmt.Errorf("%w: project %s was registered without an origin remote", ErrNoPinnedRemote, t.project.Path)
	}
	ref := branchRef(target.Branch)
	key := t.project.FetchURL + ":" + ref
	if _, err := t.run(ctx, s, Status, key, "check-ref-format", ref); err != nil {
		return result, fmt.Errorf("validate target branch: %w", err)
	}
	// The recorded URLs must still be the repository's config before anything
	// touches the network.
	if err := s.verifyPinnedEndpoint(ctx, t, target.Remote, key); err != nil {
		return result, err
	}
	fetched, err := s.fetchTarget(ctx, t, ref)
	if err != nil {
		return result, err
	}
	result.Fetched = fetched
	result.PreCommit, err = s.mergeTarget(ctx, t, key, fetched)
	if err != nil {
		return result, err
	}
	head, err := t.run(ctx, s, Status, key, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return result, err
	}
	result.Revision = line(head)
	if _, err := t.run(ctx, s, Status, key, "merge-base", "--is-ancestor", fetched, result.Revision); err != nil {
		return result, fmt.Errorf("verify pushed revision contains fetched target: %w", err)
	}
	result.PrePush, err = s.scanSecrets(ctx, ScanRequest{Stage: PrePush, Worktree: t.path, From: fetched, To: result.Revision})
	if err != nil {
		return result, err
	}
	// A scan hook can edit the config between fetch and push, so the pins are
	// re-checked before the push for the same reason as before the fetch.
	if err := s.verifyPinnedEndpoint(ctx, t, target.Remote, key); err != nil {
		return result, err
	}
	return result, s.pushRevision(ctx, t, ref, result.Revision)
}

// --- Remote dispatch: pin the fetched revision and use an explicit refspec ---

// verifyPinnedEndpoint refuses reconciliation when the repository's current
// remote config no longer carries the fetch and push URLs that Register
// recorded, or when the remote has more than one URL for a direction. It runs
// before the fetch and again before the push, so no network call ever uses a
// URL the operator did not pin at registration.
func (s *Service) verifyPinnedEndpoint(ctx context.Context, t Thread, remote, key string) error {
	fetch, err := t.run(ctx, s, Status, key, "remote", "get-url", "--all", "--", remote)
	if err != nil {
		return fmt.Errorf("%w: resolve %s fetch URL: %w", ErrRemoteURLChanged, remote, err)
	}
	push, err := t.run(ctx, s, Status, key, "remote", "get-url", "--push", "--all", "--", remote)
	if err != nil {
		return fmt.Errorf("%w: resolve %s push URL: %w", ErrRemoteURLChanged, remote, err)
	}
	fetchURLs, pushURLs := urlLines(fetch), urlLines(push)
	if len(fetchURLs) != 1 || len(pushURLs) != 1 {
		return fmt.Errorf("%w: remote %s must have exactly one fetch URL and one push URL", ErrRemoteURLChanged, remote)
	}
	// A registration that pinned one shared endpoint must keep seeing one
	// shared endpoint; a split config contradicts at least one of the pins.
	if t.project.FetchURL == t.project.PushURL && fetchURLs[0] != pushURLs[0] {
		return fmt.Errorf("%w: git fetch and push must use the same single remote URL: %s (fetch), %s (push)", ErrRemoteURLChanged, fetchURLs[0], pushURLs[0])
	}
	if fetchURLs[0] != t.project.FetchURL || pushURLs[0] != t.project.PushURL {
		return fmt.Errorf("%w: %s now uses %s (fetch) and %s (push), registration pinned %s (fetch) and %s (push)", ErrRemoteURLChanged, remote, fetchURLs[0], pushURLs[0], t.project.FetchURL, t.project.PushURL)
	}
	return nil
}

// urlLines splits Git's remote get-url report into non-empty URL lines.
func urlLines(out []byte) []string {
	var urls []string
	for _, raw := range strings.Split(string(out), "\n") {
		if url := strings.TrimSuffix(raw, "\r"); url != "" {
			urls = append(urls, url)
		}
	}
	return urls
}

// fetchTarget reads the fetched object ID from this fetch's porcelain output
// so a separate Git invocation cannot substitute another FETCH_HEAD value.
// It fetches the recorded fetch URL directly instead of the remote name, so
// the config's current URL cannot choose the source.
func (s *Service) fetchTarget(ctx context.Context, t Thread, ref string) (string, error) {
	out, err := t.run(ctx, s, Fetch, t.project.FetchURL+":"+ref, "fetch", "--porcelain", "--no-tags", "--no-prune", "--no-prune-tags", "--no-recurse-submodules", "--no-auto-maintenance", "--refmap=", "--", t.project.FetchURL, ref)
	if err != nil {
		return "", fmt.Errorf("fetch reconciliation target: %w", err)
	}
	fields := strings.Fields(string(out))
	if len(fields) == 4 && fields[3] == "FETCH_HEAD" && (len(fields[2]) == 40 || len(fields[2]) == 64) {
		if _, err := hex.DecodeString(fields[2]); err == nil {
			return fields[2], nil
		}
	}
	return "", errors.New("git fetch did not report one target revision")
}

// mergeTarget permits fast-forwards and pauses before a merge commit so it
// can scan the index. Git's rerere and autostash are disabled to preserve conflicts.
func (s *Service) mergeTarget(ctx context.Context, t Thread, key, revision string) (ScanResult, error) {
	request, err := s.reconciliationRequest(ctx, t, Merge, key)
	if err != nil {
		return "", err
	}
	_, err = s.run(ctx, request, t.path, "merge", "--ff", "--no-commit", "--no-edit",
		"--no-autostash", "--no-overwrite-ignore", "--", revision)
	if err != nil {
		return "", s.mergeFailure(ctx, t, key, err)
	}
	_, err = t.run(ctx, s, Status, key, "rev-parse", "--verify", "--quiet", "MERGE_HEAD")
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return s.commit(ctx, t, "--no-edit")
}

// mergeFailure reports unmerged index paths and leaves the index and working
// copy for the thread to resolve. It retains failures from the conflict probe.
func (s *Service) mergeFailure(ctx context.Context, t Thread, key string, cause error) error {
	if errors.Is(cause, ErrDenied) {
		return cause
	}
	out, err := t.run(ctx, s, Diff, key, "diff", "--no-ext-diff", "--no-textconv", "--name-only", "--diff-filter=U", "-z", "--")
	if err != nil {
		return errors.Join(cause, fmt.Errorf("inspect unmerged paths: %w", err))
	}
	paths := records(out)
	if len(paths) != 0 {
		return &ConflictError{Paths: paths, Cause: cause}
	}
	return cause
}

// pushRevision uses an explicit object ID so a moving local branch cannot
// substitute a different revision after reconciliation or scanning. It pushes
// to the recorded push URL directly instead of the remote name, so the
// config's current URL cannot choose the destination and no configured push
// refspec can override the explicit one.
func (s *Service) pushRevision(ctx context.Context, t Thread, ref, revision string) error {
	request, err := s.reconciliationRequest(ctx, t, Push, t.project.PushURL+":"+ref)
	if err != nil {
		return err
	}
	if s.evidence && request.Revision != revision {
		return errors.New("thread HEAD changed after the outgoing revision was selected")
	}
	out, err := s.run(ctx, request, t.path, "push", "--porcelain", "--no-force", "--no-force-with-lease",
		"--no-force-if-includes", "--no-mirror", "--no-prune", "--no-follow-tags", "--recurse-submodules=no", "--", t.project.PushURL, revision+":"+ref)
	if err != nil && !errors.Is(err, ErrDenied) {
		return &PushError{Report: strings.TrimSpace(string(out)), Cause: err}
	}
	return err
}

// reconciliationRequest measures the worktree before the decider runs. The
// decider runs under the service lock and cannot look up this evidence itself.
func (s *Service) reconciliationRequest(ctx context.Context, t Thread, action Action, key string) (Request, error) {
	request := t.request(action, key)
	if !s.evidence {
		return request, nil
	}
	head, digest, err := s.worktreeState(ctx, t)
	if err != nil {
		return Request{}, fmt.Errorf("read worktree evidence for %s: %w", action, err)
	}
	request.Revision, request.WorktreeDigest = head, digest
	return request, nil
}
