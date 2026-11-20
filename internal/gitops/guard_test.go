package gitops

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// TestRemoveThreadGuardsLateIgnoredFiles proves H2: an ignored file created
// between the ignored-file check and the removal — here by the decider during
// the DeleteWorktree authorization — is re-detected by the removal's guard,
// the call returns IgnoredFilesError, and the file and worktree survive.
func TestRemoveThreadGuardsLateIgnoredFiles(t *testing.T) {
	f := newFixture(t)
	var thread Thread
	s := New(func(_ context.Context, request Request) error {
		if request.Action == DeleteWorktree {
			// Runs after the ignored-file check and immediately before git;
			// the thread handle is assigned by the time this fires.
			if err := os.WriteFile(filepath.Join(thread.Path(), "late.ignored"), []byte("late"), 0600); err != nil {
				t.Error(err)
			}
		}
		return nil
	}, nil)
	thread = f.thread(t, s, "late")

	err := s.RemoveThread(t.Context(), thread, Settled())
	var refused *IgnoredFilesError
	if !errors.As(err, &refused) || !errors.Is(err, ErrIgnoredFiles) ||
		!slices.Contains(refused.Paths, "late.ignored") {
		t.Fatalf("remove with a late ignored file = %v (%v)", err, refused)
	}
	if got := get(t, thread.Path(), "late.ignored"); got != "late" {
		t.Fatal("the late ignored file was deleted")
	}
	if _, statErr := os.Stat(thread.Path()); statErr != nil {
		t.Fatalf("worktree removed despite the guard: %v", statErr)
	}

	// With the explicit discard option the removal proceeds and deletes the
	// late ignored file with the worktree.
	if err := s.RemoveThread(t.Context(), thread, Settled(), DiscardIgnored()); err != nil {
		t.Fatalf("remove with DiscardIgnored = %v", err)
	}
	if _, statErr := os.Stat(thread.Path()); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("worktree survived the DiscardIgnored removal: %v", statErr)
	}
}
