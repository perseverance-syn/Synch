package gitops

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// TestRemoveThreadIgnoredFiles proves AC2 (O4): RemoveThread refuses to delete
// ignored files with the worktree unless the caller passes DiscardIgnored.
func TestRemoveThreadIgnoredFiles(t *testing.T) {
	f := newFixture(t)
	// An empty excludes file pins status output against the operator's
	// global ignore rules.
	excludes := filepath.Join(f.root, "excludes")
	put(t, f.root, "excludes", "")
	git(t, f.repo, "config", "core.excludesFile", excludes)
	s := New(allow, nil)
	thread := f.thread(t, s, "ignored")
	put(t, thread.Path(), "secrets.ignored", "kept")
	if err := os.MkdirAll(filepath.Join(thread.Path(), "cache.ignored"), 0755); err != nil {
		t.Fatal(err)
	}
	put(t, thread.Path(), filepath.Join("cache.ignored", "artifact.bin"), "output")

	err := s.RemoveThread(t.Context(), thread, Settled())
	var refused *IgnoredFilesError
	if !errors.As(err, &refused) || !errors.Is(err, ErrIgnoredFiles) ||
		!slices.Equal(refused.Paths, []string{"cache.ignored/", "secrets.ignored"}) {
		t.Fatalf("remove with ignored files = %v (%v)", err, refused)
	}
	if got := get(t, thread.Path(), "secrets.ignored"); got != "kept" {
		t.Fatal("refused removal changed the ignored file")
	}
	if err := s.RemoveThread(t.Context(), thread, Settled(), DiscardIgnored()); err != nil {
		t.Fatalf("remove with DiscardIgnored = %v", err)
	}
	if _, statErr := os.Stat(thread.Path()); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("worktree survived the DiscardIgnored removal: %v", statErr)
	}
	if got := git(t, f.repo, "rev-parse", "refs/heads/"+thread.Branch()); got == "" {
		t.Fatal("thread branch missing after removal")
	}
}
