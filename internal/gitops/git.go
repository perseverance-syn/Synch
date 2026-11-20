package gitops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// Action is an exact name from policy rule-file §7.
type Action string

const (
	Status          Action = "git.status"
	Diff            Action = "git.diff"
	Commit          Action = "git.commit"
	Fetch           Action = "git.fetch"
	Push            Action = "git.push"
	PushForce       Action = "git.push_force"
	ResetHard       Action = "git.reset_hard"
	Clean           Action = "git.clean"
	BranchDelete    Action = "git.branch_delete"
	HistoryRewrite  Action = "git.history_rewrite"
	CheckoutDiscard Action = "git.checkout_discard"
	Merge           Action = "merge"
	WriteWorktree   Action = "fs.write"
	DeleteWorktree  Action = "fs.delete"
)

// Request describes the policy subject and target of a command. Path is the
// normalized absolute project or thread worktree path.
type Request struct {
	Action   Action
	ThreadID string
	Path     string
	Mediated bool
	Target   string
	// Revision and WorktreeDigest describe the live worktree for merge and push.
	Revision       string
	WorktreeDigest string
}

// Decider returns nil only for an authorized request, including any required
// approval. It runs with the service lock held and must not call the service.
type Decider func(context.Context, Request) error

// Service serializes mediated Git operations, including each fetch-to-push
// sequence. Use one service for the daemon's Git operations.
type Service struct {
	mu       sync.Mutex
	decide   Decider
	scan     Scanner
	evidence bool
}

// New creates a Git service. An absent decider denies every command.
func New(decide Decider, scan Scanner) *Service {
	return &Service{decide: decide, scan: scan}
}

// NewWithEvidence adds worktree bindings to merge and push requests for a
// decider that verifies exact approvals.
func NewWithEvidence(decide Decider, scan Scanner) *Service {
	return &Service{decide: decide, scan: scan, evidence: true}
}

var (
	ErrDenied            = errors.New("git action denied")
	ErrNotRepository     = errors.New("path is not a Git repository root")
	ErrWorktreeOwnership = errors.New("path is not the thread's registered Git worktree")
	ErrUnsupported       = errors.New("git command is not supported by the mediated interface")
	ErrConflict          = errors.New("git reconciliation has conflicts")
	ErrPushRejected      = errors.New("git push was unsuccessful")
	ErrRemoteURLChanged  = errors.New("git remote URL changed since registration")
	ErrNoPinnedRemote    = errors.New("project has no pinned remote")
	ErrIgnoredFiles      = errors.New("worktree contains ignored files")
)

// DeniedError carries the rejected policy request without running the command.
type DeniedError struct {
	Request Request
	Cause   error
}

// Error describes the denied action and policy reason.
func (e *DeniedError) Error() string {
	return fmt.Sprintf("%s: %s: %v", ErrDenied, e.Request.Action, e.Cause)
}

// Unwrap permits matching the denied sentinel and underlying policy reason.
func (e *DeniedError) Unwrap() []error { return []error{ErrDenied, e.Cause} }

// GitError reports a failed Git invocation, including Git's diagnostic.
type GitError struct {
	Command    string
	Diagnostic string
	Cause      error
}

// Error describes a Git command failure.
func (e *GitError) Error() string {
	return fmt.Sprintf("git %s: %v: %s", e.Command, e.Cause, e.Diagnostic)
}

// Unwrap retains the execution error, including exit status.
func (e *GitError) Unwrap() error { return e.Cause }

// authorize denies by default so missing policy wiring cannot dispatch Git.
func (s *Service) authorize(ctx context.Context, request Request) error {
	if s.decide == nil {
		return &DeniedError{Request: request, Cause: errors.New("policy decider not configured")}
	}
	if err := s.decide(ctx, request); err != nil {
		return &DeniedError{Request: request, Cause: err}
	}
	return nil
}

// run is the only Git execution path. Its callers construct fixed command
// lines and attach the corresponding policy request.
func (s *Service) run(ctx context.Context, request Request, dir string, args ...string) ([]byte, error) {
	if err := s.authorize(ctx, request); err != nil {
		return nil, err
	}
	options := []string{"--no-optional-locks", "-c", "core.fsmonitor=false", "-C", dir}
	if request.Action == Merge {
		options = append(options, "-c", "rerere.enabled=false", "-c", "merge.autoStash=false")
	}
	cmd := exec.CommandContext(ctx, "git", append(options, args...)...)
	// Environment overrides such as GIT_DIR can redirect a mediated command outside its worktree.
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		key = strings.ToUpper(key)
		if (!strings.HasPrefix(key, "GIT_") && key != "LC_ALL") || key == "GIT_ALLOW_PROTOCOL" {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env, "LC_ALL=C", "GIT_TERMINAL_PROMPT=0")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return stdout.Bytes(), &GitError{Command: args[0], Diagnostic: strings.TrimSpace(stderr.String()), Cause: err}
	}
	return stdout.Bytes(), nil
}

// records splits Git's NUL-terminated output without treating whitespace as path syntax.
func records(out []byte) []string {
	if len(out) == 0 {
		return nil
	}
	return strings.Split(strings.TrimSuffix(string(out), "\x00"), "\x00")
}

// line strips the terminator from commands that return exactly one line.
func line(out []byte) string { return strings.TrimSuffix(strings.TrimSuffix(string(out), "\n"), "\r") }
