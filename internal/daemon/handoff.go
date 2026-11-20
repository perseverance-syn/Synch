package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	sctx "synch/internal/context"
	"synch/internal/engine"
	"synch/internal/thread"
)

// builderRole is the role every managed child session runs as; the coordinator gets no
// tool guidance because it never runs tools (dec. 05, D-41).
const builderRole = "builder"

// --- Session guidance: role, project and target blocks for every session start (card 303 X1, X4) ---

// guidanceFor loads the Builder role pack and the project's and target's optional
// guidance files. With no role pack and no files configured it loads nothing.
func (d *Daemon) guidanceFor(project string) ([]sctx.Block, error) {
	pc := d.projects[project]
	in := sctx.Inputs{PackDir: d.roleSkills, Role: builderRole}
	var err error
	if in.ProjectFile, err = guidancePath(pc.Dir, pc.GuidanceFile); err != nil {
		return nil, err
	}
	if in.TargetFile, err = guidancePath(pc.Dir, pc.TargetGuidance[d.driver.Describe().Host]); err != nil {
		return nil, err
	}
	return sctx.Guidance(in)
}

// guidancePath resolves a configured guidance file: absolute paths stand, relative
// ones live inside the project checkout and may not climb out of it.
func guidancePath(dir, path string) (string, error) {
	switch {
	case path == "":
		return "", nil
	case filepath.IsAbs(path):
		return path, nil
	case dir == "" || !filepath.IsLocal(path):
		return "", fmt.Errorf("guidance file %q must stay inside the project checkout", path)
	}
	return filepath.Join(dir, path), nil
}

// sessionInstructions assembles what a session starts with: guidance blocks, then (when the
// session continues a checkpoint) the handoff, all stable, then the request's own
// instructions, with stable text first. A checkpoint without handoff files, such as
// one written before handoffs existed, is passed on as its JSON.
func sessionInstructions(guidance []sctx.Block, req engine.StartRequest, cp *thread.Checkpoint) ([]engine.Instruction, error) {
	blocks := slices.Clone(guidance)
	extra := slices.Clone(req.Instructions)
	if cp != nil {
		block, err := handoffBlock(cp, req.ThreadID)
		switch {
		case err != nil:
			return nil, err
		case block != nil:
			blocks = append(blocks, *block)
		default:
			data, err := json.Marshal(cp)
			if err != nil {
				return nil, err
			}
			extra = append(extra, engine.Instruction{Text: "Continue from the verified checkpoint:\n" + string(data), Stable: true})
		}
	}
	return sctx.Assemble(blocks, extra), nil
}

// handoffBlock reads the checkpoint's handoff file, verifying its digest and thread. It
// returns nil when the checkpoint lists no handoff file.
func handoffBlock(cp *thread.Checkpoint, threadID string) (*sctx.Block, error) {
	for _, path := range cp.Handoff {
		if !sctx.IsHandoffFile(path) {
			continue
		}
		h, sum, err := sctx.ReadHandoff(path, threadID)
		if err != nil {
			return nil, fmt.Errorf("load handoff: %w", err)
		}
		block := h.Block(path, sum, cp.Handoff)
		return &block, nil
	}
	return nil, nil
}

// --- Handoff files: written at the checkpoint, outside the project's git history (card 303 X3) ---

// writeHandoff records the spec's handoff list for a thread about to checkpoint and
// adds the file to the checkpoint's handoff files and pending-edit reference. The
// values come from the thread's current state, which T13 does not change.
func (d *Daemon) writeHandoff(ctx context.Context, cp *thread.Checkpoint, workdir string, state thread.State) error {
	var h sctx.Handoff
	h.ThreadID, h.BriefVersion = cp.ThreadID, state.Facts.BriefVersion
	if state.Facts.PlanDigest != "" {
		h.Plan.Digest, h.Plan.Revision = state.Facts.PlanDigest, state.Facts.PlanRevision
	}
	if h.Revision = cp.Revision; state.Facts.Revision != "" {
		h.Revision = state.Facts.Revision
	}
	h.FailedAttempts, h.OpenDecisions = cp.FailedAttempts, cp.OpenDecisions
	for id, v := range state.Requests {
		if v == "" || v == "unknown" {
			h.Uncertain = append(h.Uncertain, id)
		}
	}
	slices.Sort(h.Uncertain)
	dir := d.handoffDir(ctx, workdir, cp.ThreadID)
	summary, diff := pendingEdits(ctx, workdir)
	h.PendingEdits.Summary = summary
	if cp.PendingEdits.Diff.URI != "" {
		h.PendingEdits.Diff = cp.PendingEdits.Diff
	} else if len(diff) > 0 {
		sum := fmt.Sprintf("%x", sha256.Sum256(diff))
		path := filepath.Join(dir, "pending-"+sum+".diff")
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		if err := os.WriteFile(path, diff, 0o600); err != nil {
			return err
		}
		h.PendingEdits.Diff = thread.Reference{URI: fileURI(path), Digest: "sha256:" + sum}
	}
	if h.PendingEdits.WorktreeDigest = cp.PendingEdits.WorktreeDigest; h.PendingEdits.WorktreeDigest == "" {
		h.PendingEdits.WorktreeDigest = h.PendingEdits.Diff.Digest
	}
	path, _, err := sctx.WriteHandoff(dir, h)
	if err != nil {
		return err
	}
	cp.Handoff = append(cp.Handoff, path)
	cp.PendingEdits.Diff, cp.PendingEdits.WorktreeDigest = h.PendingEdits.Diff, h.PendingEdits.WorktreeDigest
	return nil
}

// handoffDir is the thread worktree's own git metadata directory (for a linked worktree,
// <repo>/.git/worktrees/<name>/synch), which git never tracks or commits. A directory that
// is not a git checkout uses the daemon data directory instead.
func (d *Daemon) handoffDir(ctx context.Context, workdir, threadID string) string {
	if out, err := gitOutput(ctx, workdir, "rev-parse", "--absolute-git-dir"); err == nil {
		if gitDir := strings.TrimSpace(string(out)); gitDir != "" {
			return filepath.Join(filepath.FromSlash(gitDir), "synch")
		}
	}
	return filepath.Join(d.dataDir, "handoffs", threadID)
}

// pendingEdits summarizes the uncommitted changes of a worktree, and returns the full diff
// against HEAD. Both are empty when the directory is not a git checkout.
// ponytail: the whole diff is read into memory; stream it to disk if worktrees carry huge uncommitted diffs.
func pendingEdits(ctx context.Context, workdir string) (summary string, diff []byte) {
	status, err := gitOutput(ctx, workdir, "--no-optional-locks", "-c", "core.quotepath=false", "status", "--porcelain")
	if err != nil {
		return "", nil
	}
	stat, _ := gitOutput(ctx, workdir, "-c", "core.quotepath=false", "diff", "HEAD", "--stat")
	diff, _ = gitOutput(ctx, workdir, "-c", "core.quotepath=false", "diff", "HEAD", "--no-color", "--no-ext-diff")
	return strings.TrimSpace(string(status) + string(stat)), diff
}

// gitOutput runs a read-only git command in dir.
func gitOutput(ctx context.Context, dir string, args ...string) ([]byte, error) {
	if dir == "" {
		return nil, errors.New("no directory")
	}
	if _, err := os.Stat(dir); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = filepath.Clean(dir)
	return cmd.Output()
}

// fileURI renders an absolute path as the file URI form checkpoint references use.
func fileURI(path string) string {
	p := filepath.ToSlash(path)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return (&url.URL{Scheme: "file", Path: p}).String()
}

// --- Output caps: tool and check output handed to agents (card 303 X2) ---

// CapOutput retains the whole output on disk under the data directory and returns the
// capped excerpt (head, tail and lines matching the keywords) with a reference id.
func (d *Daemon) CapOutput(r io.Reader, keywords ...string) (sctx.Excerpt, error) {
	return d.outputs.Retain(r, keywords...)
}

// SearchOutput finds lines matching a pattern in a retained output by its reference.
func (d *Daemon) SearchOutput(ref, pattern string, limit int) ([]sctx.Match, error) {
	return d.outputs.Search(ref, pattern, limit)
}
