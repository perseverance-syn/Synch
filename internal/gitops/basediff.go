package gitops

import "context"

// BaseDiff is a thread worktree's change against the base it branched from.
type BaseDiff struct {
	// Base is the merge base of the thread's HEAD and the project's registered base branch.
	Base string
	// Revision is the thread's current HEAD commit.
	Revision string
	// Patch holds tracked changes since Base, committed or not. Untracked files are not listed.
	Patch []byte
}

// BaseDiff reads the verified worktree's diff against its base. It is read-only.
func (s *Service) BaseDiff(ctx context.Context, t Thread) (BaseDiff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.verifyThread(ctx, t); err != nil {
		return BaseDiff{}, err
	}
	out, err := t.run(ctx, s, Status, t.Branch(), "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return BaseDiff{}, err
	}
	d := BaseDiff{Revision: line(out)}
	if out, err = t.run(ctx, s, Status, t.Branch(), "merge-base", "HEAD", branchRef(t.project.BaseBranch)); err != nil {
		return BaseDiff{}, err
	}
	d.Base = line(out)
	d.Patch, err = t.run(ctx, s, Diff, t.Branch(), "diff", "--no-ext-diff", "--no-textconv", "--no-color", d.Base, "--")
	return d, err
}
