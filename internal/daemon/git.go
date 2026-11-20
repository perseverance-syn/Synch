package daemon

import (
	"context"
	"errors"
	"fmt"

	"synch/internal/gitops"
)

// RegisterProject reads a local project's pinned endpoints.
func (d *Daemon) RegisterProject(ctx context.Context, path, branch, conventions string) (gitops.Project, error) {
	return d.git.Register(ctx, path, branch, conventions)
}

// CreateWritingThread creates a managed Builder child of the parent through the
// coordinator, with its assignment recorded, before its worktree is provisioned.
func (d *Daemon) CreateWritingThread(ctx context.Context, parent string) (string, error) {
	if parent == "" {
		return "", errors.New("a managed writing thread requires a parent")
	}
	state, err := d.threads.Status(ctx, parent)
	if err != nil {
		return "", err
	}
	return d.delegate(ctx, state, "Edit the project in an isolated worktree.", "unversioned")
}

// CreateWorktree provisions a verified worktree for an existing thread.
func (d *Daemon) CreateWorktree(ctx context.Context, project gitops.Project, id, path string) (gitops.Thread, error) {
	worktree, err := d.git.CreateThread(ctx, project, id, path, "")
	if err == nil {
		d.mu.Lock()
		d.worktrees[id] = worktree.Path()
		d.mu.Unlock()
	}
	return worktree, err
}

// CommitWorktree stages and commits a writing thread's changes through policy.
func (d *Daemon) CommitWorktree(ctx context.Context, worktree gitops.Thread, message string) error {
	if err := d.git.StageAll(ctx, worktree); err != nil {
		return fmt.Errorf("stage thread edits: %w", err)
	}
	_, err := d.git.Commit(ctx, worktree, message)
	return err
}

// WorktreeDigest measures a thread's current approval binding.
func (d *Daemon) WorktreeDigest(ctx context.Context, worktree gitops.Thread) (string, error) {
	digest, err := d.git.WorktreeDigest(ctx, worktree)
	if err != nil {
		return "", err
	}
	return "sha256:" + digest, nil
}

// Integrate applies designated approvals to the corresponding Git effects.
func (d *Daemon) Integrate(ctx context.Context, worktree gitops.Thread, target gitops.RemoteTarget, approvals map[gitops.Action]string) (gitops.PushResult, error) {
	return d.git.ReconcileAndPush(context.WithValue(ctx, approvalKey{}, approvals), worktree, target)
}
