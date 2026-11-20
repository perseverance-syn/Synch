package api

import (
	"context"
	"errors"
	"net/http"
)

// ErrNoWorktree reports a thread with no worktree to diff.
var ErrNoWorktree = errors.New("thread has no worktree")

// Diff is a thread worktree's change against its base. Patch is capped; when Truncated the full
// diff stays retained under Ref, which the output store can search, with its Digest.
type Diff struct {
	Base      string `json:"base"`
	Revision  string `json:"revision"`
	Patch     string `json:"patch"`
	Truncated bool   `json:"truncated"`
	Bytes     int64  `json:"bytes"`
	Digest    string `json:"digest"`
	Ref       string `json:"ref"`
}

// DiffFunc reads and retains a thread's worktree diff.
type DiffFunc func(ctx context.Context, thread string) (Diff, error)

// VerifyCheckpointFunc verifies a checkpoint reference on its owning VM driver. nil means verified.
type VerifyCheckpointFunc func(ctx context.Context, thread string, ref CheckpointRef) error

// getDiff serves GET /v1/threads/{thread_id}/diff.
func (s *Server) getDiff(w http.ResponseWriter, r *http.Request, id string) {
	if !ulidPattern.MatchString(id) || s.config.Threads == nil || s.config.Diff == nil {
		writeProblem(w, newProblem(404, "not_found", ""))
		return
	}
	if _, err := s.config.Threads.Status(r.Context(), id); err != nil {
		writeProblem(w, newProblem(404, "not_found", ""))
		return
	}
	d, err := s.config.Diff(r.Context(), id)
	switch {
	case errors.Is(err, ErrNoWorktree):
		writeProblem(w, newProblem(404, "not_found", "thread has no worktree"))
	case err != nil:
		writeProblem(w, newProblem(500, "internal", ""))
	default:
		writeJSON(w, d)
	}
}
