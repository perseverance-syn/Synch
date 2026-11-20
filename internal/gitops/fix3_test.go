package gitops

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestRelativeRemoteURLReachesOnlyRecordedRepository proves J1: a relative
// origin URL is recorded as an absolute path resolved against the repository
// root, so fetch and push (which run from the transport sandbox directory)
// reach only that repository even when a different bare repository sits at
// the same relative path from the sandbox directory.
func TestRelativeRemoteURLReachesOnlyRecordedRepository(t *testing.T) {
	f := newFixture(t)
	recorded := filepath.Join(f.root, "origin.git")
	git(t, f.root, "init", "--bare", "-b", "main", recorded)
	git(t, f.repo, "push", recorded, "HEAD:refs/heads/main")
	git(t, f.repo, "remote", "set-url", "origin", "../origin.git")

	// The sandbox directory is created in the temp dir, so ../origin.git from
	// there is the decoy: a bare repository with a different tip.
	tmp := filepath.Join(f.root, "tmpdir", "inner")
	if err := os.MkdirAll(tmp, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"TMP", "TEMP", "TMPDIR"} {
		t.Setenv(name, tmp)
	}
	decoy := filepath.Join(tmp, "origin.git")
	git(t, f.root, "init", "--bare", "-b", "main", decoy)
	decoyTip := git(t, f.repo, "commit-tree", "HEAD^{tree}", "-p", "HEAD", "-m", "decoy")
	git(t, f.repo, "push", decoy, decoyTip+":refs/heads/main")

	s := New(allow, nil)
	if p := f.project(t, s); !samePath(p.FetchURL, recorded) || !filepath.IsAbs(p.PushURL) {
		t.Fatalf("relative URL recorded as %q and %q, want the absolute %q", p.FetchURL, p.PushURL, recorded)
	}
	thread := f.thread(t, s, "relative")
	base := git(t, recorded, "rev-parse", "main")
	editThread(t, s, thread, "alpha", "relative edit")
	result, err := s.ReconcileAndPush(t.Context(), thread, f.target())
	if err != nil || result.Fetched != base {
		t.Fatalf("reconcile = %+v, %v; want a fetch of the recorded tip %s", result, err, base)
	}
	if got := git(t, recorded, "rev-parse", "main"); got != result.Revision {
		t.Fatalf("recorded repo main = %s, want the pushed %s", got, result.Revision)
	}
	if got := git(t, decoy, "rev-parse", "main"); got != decoyTip {
		t.Fatalf("decoy repo main = %s, want the untouched %s", got, decoyTip)
	}
}

// TestResolveEndpointRecordsNetworkURLsVerbatim proves J1/J3: scheme URLs and
// scp-style URLs are recorded verbatim, local paths become absolute.
func TestResolveEndpointRecordsNetworkURLsVerbatim(t *testing.T) {
	root := t.TempDir()
	for _, url := range []string{"https://example.com/a.git", "ssh://git@example.com/a.git", "git://example.com/a.git", "git@example.com:org/a.git"} {
		if got := resolveEndpoint(url, root); got != url {
			t.Errorf("resolveEndpoint(%q) = %q, want it verbatim", url, got)
		}
	}
	if got := resolveEndpoint("sub/../x.git", root); !filepath.IsAbs(got) || !samePath(got, filepath.Join(root, "x.git")) {
		t.Errorf("resolveEndpoint of a relative path = %q", got)
	}
}

// TestRemoveThreadRefusesWithoutSettled proves J2: without the Settled
// attestation RemoveThread returns ErrNotSettled before touching anything —
// no policy request, no ignored-file scan, no deletion — so an ignored file
// that appears afterwards cannot be lost. With Settled, an ignored file still
// refuses the removal unless DiscardIgnored is passed.
func TestRemoveThreadRefusesWithoutSettled(t *testing.T) {
	f := newFixture(t)
	var deletes int
	s := New(func(_ context.Context, request Request) error {
		if request.Action == DeleteWorktree {
			deletes++
		}
		return nil
	}, nil)
	thread := f.thread(t, s, "unsettled")
	put(t, thread.Path(), "late.ignored", "keep")

	if err := s.RemoveThread(t.Context(), thread); !errors.Is(err, ErrNotSettled) || deletes != 0 {
		t.Fatalf("remove without Settled = %v (%d delete requests), want ErrNotSettled and none", err, deletes)
	}
	if err := s.RemoveThread(t.Context(), thread, Settled()); !errors.Is(err, ErrIgnoredFiles) {
		t.Fatalf("settled remove with an ignored file = %v, want ErrIgnoredFiles", err)
	}
	if got := get(t, thread.Path(), "late.ignored"); got != "keep" {
		t.Fatal("a refused removal changed the ignored file")
	}
	if err := s.RemoveThread(t.Context(), thread, Settled(), DiscardIgnored()); err != nil {
		t.Fatalf("settled remove with DiscardIgnored = %v", err)
	}
}
