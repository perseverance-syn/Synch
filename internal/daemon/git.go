package daemon

import (
	"context"
	"errors"
	"fmt"

	"synch/internal/eventlog"
	"synch/internal/gitops"
	"synch/internal/thread"
)

// RegisterProject reads a local project's pinned endpoints.
func (d *Daemon) RegisterProject(ctx context.Context, path, branch, conventions string) (gitops.Project, error) {
	return d.git.Register(ctx, path, branch, conventions)
}

// CreateWritingThread stores a managed draft before provisioning its worktree.
func (d *Daemon) CreateWritingThread(ctx context.Context, parent string) (string, error) {
	if parent == "" {
		return "", errors.New("a managed writing thread requires a parent")
	}
	id := eventlog.NewID()
	err := d.threads.Create(ctx, id, thread.Facts{ManagedParent: parent, BriefVersion: 1, AuditSetting: "default"})
	return id, err
}

// CreateWorktree provisions a verified worktree for an existing thread.
func (d *Daemon) CreateWorktree(ctx context.Context, project gitops.Project, id, path string) (gitops.Thread, error) {
	return d.git.CreateThread(ctx, project, id, path, "")
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
