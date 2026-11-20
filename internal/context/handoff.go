package context

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"synch/internal/thread"
)

// Handoff is exactly the list the spec requires a new session to continue from.
// ThreadID only binds the file to its thread so a stray file cannot be loaded as another's.
type Handoff struct {
	ThreadID     string `json:"thread_id"`
	BriefVersion int    `json:"brief_version"`
	Plan         struct {
		Revision int    `json:"revision"`
		Digest   string `json:"digest"`
	} `json:"plan"`
	Revision     string `json:"revision"`
	PendingEdits struct {
		// Summary is the worktree diff summary: changed files and their line counts.
		Summary string `json:"summary"`
		// Diff references the full uncommitted diff stored beside the handoff.
		Diff           thread.Reference `json:"uncommitted_diff"`
		WorktreeDigest string           `json:"worktree_digest"`
	} `json:"pending_edits"`
	FailedAttempts []thread.FailedAttempt `json:"failed_attempts"`
	OpenDecisions  []thread.Decision      `json:"open_decisions"`
	Uncertain      []string               `json:"uncertain_external_request_ids"`
}

// handoffName matches the content-addressed file name WriteHandoff produces.
var handoffName = regexp.MustCompile(`^handoff-([0-9a-f]{64})\.json$`)

// IsHandoffFile reports whether a path names a handoff file written by WriteHandoff.
func IsHandoffFile(path string) bool { return handoffName.MatchString(filepath.Base(path)) }

// WriteHandoff stores the handoff in dir under a name derived from its sha256, so the
// path itself is the digest the checkpoint records. Writing the same handoff twice
// is idempotent. It returns the absolute path and the "sha256:<hex>" digest.
func WriteHandoff(dir string, h Handoff) (path, sum string, err error) {
	if h.FailedAttempts == nil {
		h.FailedAttempts = []thread.FailedAttempt{}
	}
	if h.OpenDecisions == nil {
		h.OpenDecisions = []thread.Decision{}
	}
	if h.Uncertain == nil {
		h.Uncertain = []string{}
	}
	raw, err := json.MarshalIndent(h, "", "  ")
	if err != nil {
		return "", "", err
	}
	sum = digest(raw)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", "", fmt.Errorf("create handoff directory: %w", err)
	}
	if path, err = filepath.Abs(filepath.Join(dir, "handoff-"+strings.TrimPrefix(sum, "sha256:")+".json")); err != nil {
		return "", "", err
	}
	// Write then rename so a crash never leaves a half file under the digest's name.
	tmp, err := os.CreateTemp(dir, "handoff-*.tmp")
	if err != nil {
		return "", "", err
	}
	_, err = tmp.Write(raw)
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return "", "", fmt.Errorf("write handoff: %w", err)
	}
	return path, sum, nil
}

// ReadHandoff loads a handoff file and verifies its content against the digest in its
// name and its thread binding. A tampered or foreign file is an error.
func ReadHandoff(path, threadID string) (Handoff, string, error) {
	m := handoffName.FindStringSubmatch(filepath.Base(path))
	if m == nil {
		return Handoff{}, "", ErrNotHandoff
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Handoff{}, "", fmt.Errorf("read handoff: %w", err)
	}
	if got := digest(raw); got != "sha256:"+m[1] {
		return Handoff{}, "", fmt.Errorf("handoff digest mismatch: file is %s", got)
	}
	var h Handoff
	if err := json.Unmarshal(raw, &h); err != nil {
		return Handoff{}, "", fmt.Errorf("parse handoff: %w", err)
	}
	if h.ThreadID != threadID {
		return Handoff{}, "", fmt.Errorf("handoff belongs to thread %q", h.ThreadID)
	}
	return h, "sha256:" + m[1], nil
}

// Block renders the handoff as a stable instruction block for a new session. files lists every
// handoff file of the checkpoint so the agent knows what else to read. The block's digest is
// the handoff file's own digest.
func (h Handoff) Block(path, sum string, files []string) Block {
	raw, _ := json.MarshalIndent(h, "", "  ") // plain fields cannot fail to marshal
	var b strings.Builder
	b.WriteString("Continue this thread from its handoff records. Treat uncertain external requests as unknown until reconciled; do not repeat them.\nHandoff files:\n")
	for _, f := range files {
		b.WriteString("- " + f + "\n")
	}
	b.WriteString("\n" + string(raw) + "\n")
	blk := newBlock("handoff", path, b.String(), true)
	blk.Digest = sum
	return blk
}
