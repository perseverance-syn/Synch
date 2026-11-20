package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"synch/internal/api"
	sctx "synch/internal/context"
	"synch/internal/vmdriver"
)

// diffInline is the largest patch returned inline; the full diff always stays retained.
const diffInline = 64 << 10

// threadDiff reads a writing thread's worktree diff against its base and retains the whole
// patch in the output store, which hands back a searchable reference.
func (d *Daemon) threadDiff(ctx context.Context, id string) (api.Diff, error) {
	d.mu.Lock()
	path, project, ok := d.worktrees[id], d.worktreeProject[id], d.worktrees[id] != ""
	d.mu.Unlock()
	if !ok {
		return api.Diff{}, api.ErrNoWorktree
	}
	if _, err := os.Stat(path); err != nil {
		return api.Diff{}, api.ErrNoWorktree
	}
	tree, err := d.git.OpenThread(ctx, project, id, path)
	if err != nil {
		return api.Diff{}, fmt.Errorf("open worktree: %w", err)
	}
	base, err := d.git.BaseDiff(ctx, tree)
	if err != nil {
		return api.Diff{}, err
	}
	ex, err := d.CapOutputFor(id, bytes.NewReader(base.Patch))
	if err != nil {
		return api.Diff{}, err
	}
	out := api.Diff{Base: base.Base, Revision: base.Revision, Patch: ex.Text, Truncated: ex.Truncated, Bytes: ex.Bytes, Digest: ex.Digest, Ref: ex.Ref}
	if len(out.Patch) > diffInline {
		out.Patch, out.Truncated = out.Patch[:diffInline], true
	}
	return out, nil
}

// verifyCheckpoint accepts a checkpoint rollback only when the thread owns the VM and the
// owning driver verifies the snapshot.
func (d *Daemon) verifyCheckpoint(ctx context.Context, id string, ref api.CheckpointRef) error {
	driver := d.vmDrivers[ref.Driver]
	if driver == nil {
		return errors.New("unknown VM driver")
	}
	owns := d.vmOwns
	if owns == nil {
		owns = policyState{daemon: d}.OwnsVM
	}
	if ok, err := owns(id, ref.VM); err != nil || !ok {
		return errors.Join(vmdriver.ErrSnapshot, err)
	}
	return vmdriver.VerifySnapshot(ctx, driver, ref.Endpoint, ref.VM, ref.Snapshot)
}

// ownerFile records which thread retained an output, next to the output itself.
func (d *Daemon) ownerFile(ref string) string {
	return filepath.Join(d.dataDir, "outputs", ref+".owner")
}

// CapOutputFor is CapOutput for a thread: the retained reference is searchable through the API
// by that thread and by its ancestors, never by unrelated threads.
func (d *Daemon) CapOutputFor(thread string, r io.Reader, keywords ...string) (sctx.Excerpt, error) {
	ex, err := d.outputs.Retain(r, keywords...)
	if err != nil {
		return ex, err
	}
	if err := os.WriteFile(d.ownerFile(ex.Ref), []byte(thread), 0o600); err != nil {
		return ex, fmt.Errorf("record output owner: %w", err)
	}
	return ex, nil
}

// searchOutput lets a thread search references owned by itself or any descendant. Anything else,
// including a reference with no recorded owner, is reported as not found.
func (d *Daemon) searchOutput(ctx context.Context, id, ref, pattern string, limit int) ([]api.OutputMatch, error) {
	raw, err := os.ReadFile(d.ownerFile(ref)) // an invalid ref cannot name a path: it has no .owner file
	if err != nil || !d.within(id, strings.TrimSpace(string(raw))) {
		return nil, api.ErrOutputNotFound
	}
	if _, err := regexp.Compile(pattern); err != nil {
		return nil, api.ErrBadPattern
	}
	found, err := d.outputs.Search(ref, pattern, limit)
	if err != nil {
		return nil, err
	}
	out := make([]api.OutputMatch, len(found))
	for i, m := range found {
		out[i] = api.OutputMatch{Line: m.Line, Text: m.Text}
	}
	return out, nil
}

// within reports whether owner is the thread itself or one of its descendants, following each
// thread's parent_thread_id from its creation fact.
func (d *Daemon) within(thread, owner string) bool {
	for range 16 { // deeper than the child depth limit
		if owner == "" {
			return false
		}
		if owner == thread {
			return true
		}
		owner = d.parentOf(owner)
	}
	return false
}

// parentOf reads a thread's parent from its thread.created fact, "" for a root.
func (d *Daemon) parentOf(id string) string {
	events, err := d.store.ReadStream("thread:"+id, 0, 64)
	if err != nil {
		return ""
	}
	for _, e := range events {
		if e.Type == "thread.created" {
			var p struct {
				Parent string `json:"parent_thread_id"`
			}
			if json.Unmarshal(e.Payload, &p) == nil && p.Parent != "" {
				return p.Parent
			}
		}
	}
	return ""
}
