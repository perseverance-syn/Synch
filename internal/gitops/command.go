package gitops

import (
	"context"
	"fmt"
	"strings"
)

// Classify identifies a Git verb and its options after the executable name.
// Unsupported commands and global Git options are rejected by the dispatcher.
func Classify(args ...string) (Action, error) {
	if len(args) == 0 {
		return "", ErrUnsupported
	}
	options := args[1:]
	switch args[0] {
	case "status":
		return Status, nil
	case "diff":
		if hasOption(options, "", "output") {
			// --output writes the diff to an arbitrary filesystem path.
			return WriteWorktree, nil
		}
		return Diff, nil
	case "add", "commit":
		if args[0] == "commit" && hasOption(options, "", "amend") {
			return HistoryRewrite, nil
		}
		return Commit, nil
	case "fetch":
		return classifyFetch(options), nil
	case "merge":
		if hasOption(options, "", "abort") {
			// --abort discards the merge in progress and restores the
			// pre-merge working copy.
			return CheckoutDiscard, nil
		}
		return Merge, nil
	case "push":
		return classifyPush(options), nil
	case "reset":
		if hasOption(options, "", "hard") {
			return ResetHard, nil
		}
		return HistoryRewrite, nil
	case "clean":
		return Clean, nil
	case "branch":
		if hasOption(options, "dD", "delete") {
			return BranchDelete, nil
		}
		if hasOption(options, "fMmCc", "force", "move", "copy") {
			return HistoryRewrite, nil
		}
	case "rebase", "filter-branch", "filter-repo", "replace":
		return HistoryRewrite, nil
	case "checkout", "restore", "switch":
		return CheckoutDiscard, nil
	}
	return "", fmt.Errorf("%w: %s", ErrUnsupported, args[0])
}

// classifyPush detects force options and refspecs, including ones after --,
// and classifies ref deletion independently of ordinary updates.
func classifyPush(args []string) Action {
	if hasOption(args, "f", "force", "force-with-lease", "force-if-includes", "mirror", "prune") {
		return PushForce
	}
	for _, arg := range args {
		if strings.HasPrefix(arg, "+") {
			return PushForce
		}
	}
	if hasOption(args, "d", "delete") {
		return BranchDelete
	}
	for _, arg := range args {
		if strings.HasPrefix(arg, ":") && arg != ":" {
			return BranchDelete
		}
	}
	return Push
}

// classifyFetch treats a fetch that can update refs without fast-forwarding,
// through a forced refspec or --force, as destructive at the same policy level
// as a forced push: an argument beginning with + is a forced refspec whose
// destination under refs/heads would rewrite local branches.
func classifyFetch(args []string) Action {
	if hasOption(args, "f", "force") {
		return PushForce
	}
	for _, arg := range args {
		if strings.HasPrefix(arg, "+") {
			return PushForce
		}
	}
	return Fetch
}

// hasOption recognizes short flag clusters and abbreviated long options. An
// ambiguous abbreviation is conservatively classified even if Git rejects it.
func hasOption(args []string, shorts string, longs ...string) bool {
	for _, arg := range args {
		if arg == "--" {
			break
		}
		if option, ok := strings.CutPrefix(arg, "--"); ok {
			name, _, _ := strings.Cut(option, "=")
			for _, long := range longs {
				if strings.HasPrefix(long, name) {
					return true
				}
			}
		} else if flags, ok := strings.CutPrefix(arg, "-"); ok && strings.ContainsAny(flags, shorts) {
			return true
		}
	}
	return false
}

// Run mediates a command submitted as Git arguments. Only fixed status and
// diff commands can execute through this entry point. Mutations use dedicated
// operations whose invariants cannot be bypassed with Git flags or aliases.
func (s *Service) Run(ctx context.Context, t Thread, args ...string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	action, err := Classify(args...)
	if err != nil {
		return nil, err
	}
	if len(args) != 1 || (action != Status && action != Diff) {
		if err := s.authorize(ctx, t.request(action, t.Branch())); err != nil {
			return nil, err
		}
		return nil, ErrUnsupported
	}
	if err := s.verifyThread(ctx, t); err != nil {
		return nil, err
	}
	if action == Status {
		return t.run(ctx, s, Status, t.Branch(), "status", "--porcelain=v1", "--untracked-files=all")
	}
	return t.run(ctx, s, Diff, t.Branch(), "diff", "--no-ext-diff", "--no-textconv", "HEAD", "--")
}
