package gitops

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

// Project is registration metadata that the daemon can persist. FetchURL and
// PushURL are the origin endpoints pinned at registration; every mediated
// fetch and push uses them directly instead of the remote's current config.
type Project struct {
	Path        string
	BaseBranch  string
	Conventions string
	FetchURL    string
	PushURL     string
}

// Thread identifies a registered worktree. Reopen it from the stored
// project, thread ID, and path after restarting the daemon.
type Thread struct {
	project Project
	id      string
	path    string
}

// Path returns the thread's normalized absolute mediated path.
func (t Thread) Path() string { return t.path }

// Branch returns the branch allocated to this writing thread.
func (t Thread) Branch() string { return "synch/" + t.id }

// request binds the exact Git action to the worktree that it affects.
func (t Thread) request(action Action, target string) Request {
	return Request{Action: action, ThreadID: t.id, Path: t.path, Mediated: true, Target: target}
}

// run invokes Git in the verified thread worktree.
func (t Thread) run(ctx context.Context, s *Service, action Action, target string, args ...string) ([]byte, error) {
	return s.run(ctx, t.request(action, target), t.path, args...)
}

// Register records a Git root, the name of its base branch, and its conventions
// file path. A relative conventions path is resolved from the Git root.
func (s *Service) Register(ctx context.Context, path, baseBranch, conventions string) (Project, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	root, err := normalize(path)
	if err != nil {
		return Project{}, fmt.Errorf("%w: %w", ErrNotRepository, err)
	}
	request := Request{Action: Status, Path: root}
	bare, err := s.run(ctx, request, root, "rev-parse", "--is-bare-repository")
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) && strings.Contains(err.Error(), "not a git repository") {
			return Project{}, fmt.Errorf("%w: %w", ErrNotRepository, err)
		}
		return Project{}, err
	}
	rootFlag := "--show-toplevel"
	if line(bare) == "true" {
		rootFlag = "--absolute-git-dir"
	}
	out, err := s.run(ctx, request, root, "rev-parse", rootFlag)
	if err != nil {
		return Project{}, err
	}
	if !samePath(root, line(out)) {
		return Project{}, fmt.Errorf("%w: %s", ErrNotRepository, root)
	}
	if baseBranch == "" {
		return Project{}, errors.New("git base branch is required")
	}
	if _, err := s.run(ctx, request, root, "check-ref-format", branchRef(baseBranch)); err != nil {
		return Project{}, fmt.Errorf("validate base branch: %w", err)
	}
	// Pin the remote's endpoints now, so reconciliation can refuse drift and
	// pass these recorded URLs to Git directly instead of the remote name. A
	// project without the remote records no pins and can never reconcile; a
	// remote with several URLs for one direction is refused as ambiguous.
	remotes, err := s.run(ctx, request, root, "remote")
	if err != nil {
		return Project{}, fmt.Errorf("list remotes: %w", err)
	}
	var fetchURLs, pushURLs []string
	if slices.Contains(strings.Fields(string(remotes)), pinnedRemote) {
		fetch, err := s.run(ctx, request, root, "remote", "get-url", "--all", "--", pinnedRemote)
		if err != nil {
			return Project{}, fmt.Errorf("pin remote %s fetch URL: %w", pinnedRemote, err)
		}
		push, err := s.run(ctx, request, root, "remote", "get-url", "--push", "--all", "--", pinnedRemote)
		if err != nil {
			return Project{}, fmt.Errorf("pin remote %s push URL: %w", pinnedRemote, err)
		}
		fetchURLs, pushURLs = urlLines(fetch), urlLines(push)
		if len(fetchURLs) != 1 || len(pushURLs) != 1 {
			return Project{}, fmt.Errorf("remote %s must have exactly one fetch URL and one push URL", pinnedRemote)
		}
	}
	if conventions == "" {
		conventions = "CONVENTIONS.md"
	}
	if !filepath.IsAbs(conventions) {
		conventions = filepath.Join(root, conventions)
	}
	project := Project{Path: root, BaseBranch: baseBranch, Conventions: filepath.Clean(conventions)}
	if len(fetchURLs) == 1 {
		project.FetchURL, project.PushURL = fetchURLs[0], pushURLs[0]
	}
	return project, nil
}

// normalize resolves filesystem aliases before comparing or mediating paths.
func normalize(path string) (string, error) {
	if path == "" {
		return "", errors.New("path is required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(absolute)
}

// samePath applies rule-file §8's Windows case-insensitive path comparison.
func samePath(left, right string) bool {
	left, right = filepath.Clean(left), filepath.Clean(right)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

// branchRef anchors a branch name under refs/heads.
func branchRef(branch string) string { return "refs/heads/" + branch }

// threadAt builds a thread handle with a normalized parent directory. Git
// can create the final directory if it does not exist.
func threadAt(project Project, id, path string) (Thread, error) {
	if id == "" || strings.ContainsAny(id, "/\\") || path == "" {
		return Thread{}, fmt.Errorf("%w: invalid thread ID or path", ErrWorktreeOwnership)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return Thread{}, err
	}
	parent, err := normalize(filepath.Dir(absolute))
	if err != nil {
		return Thread{}, fmt.Errorf("resolve worktree parent: %w", err)
	}
	return Thread{project: project, id: id, path: filepath.Join(parent, filepath.Base(absolute))}, nil
}

// --- Worktree lifecycle: bind ownership to the registered path and branch ---

// CreateThread creates a new synch/<id> branch and worktree. An empty
// baseRevision uses the project's registered base branch. The parent of path
// must exist, and Git must be able to create or populate the final directory.
func (s *Service) CreateThread(ctx context.Context, project Project, id, path, baseRevision string) (Thread, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, err := threadAt(project, id, path)
	if err != nil {
		return Thread{}, err
	}
	info, err := os.Lstat(t.path)
	if err == nil && (!info.IsDir() || samePath(project.Path, t.path)) {
		return Thread{}, fmt.Errorf("%w: %s", ErrWorktreeOwnership, t.path)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return Thread{}, fmt.Errorf("inspect worktree path: %w", err)
	}
	if baseRevision == "" {
		baseRevision = branchRef(project.BaseBranch)
	}
	revision, err := s.run(ctx, t.request(Status, baseRevision), project.Path, "rev-parse", "--verify", "--end-of-options", baseRevision+"^{commit}")
	if err != nil {
		return Thread{}, fmt.Errorf("resolve worktree base: %w", err)
	}
	// -b refuses to reuse an existing thread branch, including after a worktree is removed.
	_, err = s.run(ctx, t.request(WriteWorktree, t.Branch()), project.Path, "worktree", "add", "--no-track", "-b", t.Branch(), "--", t.path, line(revision))
	if err != nil {
		return Thread{}, fmt.Errorf("create thread worktree: %w", err)
	}
	return t, nil
}

// OpenThread reconstructs a handle after verifying the path and branch in Git.
func (s *Service) OpenThread(ctx context.Context, project Project, id, path string) (Thread, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, err := threadAt(project, id, path)
	if err != nil {
		return Thread{}, err
	}
	if err := s.verifyThread(ctx, t); err != nil {
		return Thread{}, err
	}
	return t, nil
}

// RemoveOption relaxes one RemoveThread safeguard, but only when the caller
// passes it explicitly.
type RemoveOption func(*removeOptions)

// removeOptions holds the relaxations that RemoveThread received.
type removeOptions struct {
	discardIgnored bool
}

// DiscardIgnored lets RemoveThread delete ignored files together with the
// worktree. Without it, RemoveThread refuses to remove a worktree that
// contains ignored files.
func DiscardIgnored() RemoveOption {
	return func(o *removeOptions) { o.discardIgnored = true }
}

// RemoveThread removes only the verified thread worktree. Git refuses dirty
// or locked worktrees, and this refuses to delete ignored files unless the
// caller passes DiscardIgnored. The branch stays available for recovery and
// review.
func (s *Service) RemoveThread(ctx context.Context, t Thread, options ...RemoveOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var opts removeOptions
	for _, option := range options {
		option(&opts)
	}
	if err := s.checkThread(ctx, t, true); err != nil {
		return err
	}
	// Git's non-forced worktree remove silently deletes ignored files such as
	// .env or build output, so their presence must refuse the removal first.
	if !opts.discardIgnored {
		if err := s.checkIgnoredFiles(ctx, t); err != nil {
			return err
		}
	}
	_, err := s.run(ctx, t.request(DeleteWorktree, t.Branch()), t.project.Path, "worktree", "remove", "--", t.path)
	return err
}

// IgnoredFilesError lists the ignored paths that RemoveThread found, keeping
// Git's trailing slash for ignored directories.
type IgnoredFilesError struct {
	Paths []string
}

// Error reports the ignored paths and keeps the worktree unchanged.
func (e *IgnoredFilesError) Error() string { return fmt.Sprintf("%s: %q", ErrIgnoredFiles, e.Paths) }

// Unwrap supports errors.Is matching of the ignored-files sentinel.
func (e *IgnoredFilesError) Unwrap() []error { return []error{ErrIgnoredFiles} }

// checkIgnoredFiles lists the worktree's ignored paths and refuses removal if
// there are any, because Git's non-forced worktree remove would silently
// delete them with the worktree.
func (s *Service) checkIgnoredFiles(ctx context.Context, t Thread) error {
	out, err := t.run(ctx, s, Status, t.Branch(), "status", "--porcelain=v1", "-z",
		"--untracked-files=all", "--ignored=matching", "--no-renames", "--")
	if err != nil {
		return err
	}
	var paths []string
	for _, entry := range records(out) {
		if len(entry) >= 4 && entry[2] == ' ' && entry[:2] == "!!" {
			paths = append(paths, entry[3:])
		}
	}
	if len(paths) == 0 {
		return nil
	}
	slices.Sort(paths)
	return &IgnoredFilesError{Paths: paths}
}

// verifyThread confirms the thread's directory and branch before using it.
func (s *Service) verifyThread(ctx context.Context, t Thread) error {
	return s.checkThread(ctx, t, false)
}

// checkThread requires a real linked-worktree entry with the exact branch.
// For removal, it also protects other worktrees nested under the owned path.
func (s *Service) checkThread(ctx context.Context, t Thread, removing bool) error {
	path, err := normalize(t.path)
	if err != nil || !samePath(t.path, path) || samePath(t.project.Path, path) {
		return fmt.Errorf("%w: %s", ErrWorktreeOwnership, t.path)
	}
	out, err := s.run(ctx, t.request(Status, t.Branch()), t.project.Path, "worktree", "list", "--porcelain", "-z")
	if err != nil {
		return err
	}
	primary, matchingPath, owned := true, false, false
	for _, item := range records(out) {
		switch {
		case item == "":
			primary, matchingPath = false, false
		case strings.HasPrefix(item, "worktree "):
			registeredPath := strings.TrimPrefix(item, "worktree ")
			matchingPath = !primary && samePath(path, registeredPath)
			if removing && nestedPath(path, registeredPath) {
				return fmt.Errorf("%w: removal would affect nested worktree %s", ErrWorktreeOwnership, registeredPath)
			}
		case matchingPath && item == "branch "+branchRef(t.Branch()):
			owned = true
		}
	}
	if owned {
		return nil
	}
	return fmt.Errorf("%w: %s on %s", ErrWorktreeOwnership, t.path, t.Branch())
}

// nestedPath matches a strict descendant on the same volume, never a sibling prefix.
func nestedPath(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
