package gitops

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// TestThreeThreadGate proves AC4 and AC7 using three real linked worktrees and a bare remote.
func TestThreeThreadGate(t *testing.T) {
	for _, overlap := range []bool{false, true} {
		name := "non-overlapping"
		if overlap {
			name = "overlapping"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			var effects []Action
			s := New(func(_ context.Context, request Request) error {
				if request.Action == Fetch || request.Action == Merge || request.Action == Push {
					effects = append(effects, request.Action)
				}
				return nil
			}, nil)
			first := f.thread(t, s, "one")
			second := f.thread(t, s, "two")
			third := f.thread(t, s, "three")
			editThread(t, s, first, "alpha", "first edit")
			editThread(t, s, second, "beta", "second edit")
			if overlap {
				editThread(t, s, third, "alpha", "conflicting edit")
			} else {
				editThread(t, s, third, "gamma", "third edit")
			}
			for _, thread := range []Thread{first, second} {
				prior := git(t, f.remote, "rev-parse", "main")
				effects = nil
				result, err := s.ReconcileAndPush(t.Context(), thread, f.target())
				if err != nil {
					t.Fatalf("%s: %v", thread.Branch(), err)
				}
				if result.Fetched != prior || result.Revision != git(t, f.remote, "rev-parse", "main") ||
					!slices.Equal(effects, []Action{Fetch, Merge, Push}) {
					t.Fatalf("%s: result = %+v, effects = %v", thread.Branch(), result, effects)
				}
			}
			prior := git(t, f.remote, "rev-parse", "main")
			effects = nil
			result, err := s.ReconcileAndPush(t.Context(), third, f.target())
			remoteContent := git(t, f.remote, "show", "main:"+sharedFile)
			if !overlap {
				want := strings.NewReplacer("alpha", "first edit", "beta", "second edit", "gamma", "third edit").Replace(f.content)
				if err != nil || result.Fetched != prior || remoteContent != strings.TrimSpace(want) ||
					!slices.Equal(effects, []Action{Fetch, Merge, Push}) {
					t.Fatalf("non-overlapping merge = %+v, %v; target = %q; effects = %v", result, err, remoteContent, effects)
				}
				return
			}
			var conflict *ConflictError
			if !errors.As(err, &conflict) || !slices.Equal(conflict.Paths, []string{sharedFile}) ||
				!slices.Equal(effects, []Action{Fetch, Merge}) {
				t.Fatalf("overlapping merge = %+v, %v; effects = %v", result, err, effects)
			}
			conflicted := get(t, third.Path(), sharedFile)
			for _, marker := range []string{"<<<<<<<", "=======", ">>>>>>>", "conflicting edit", "first edit"} {
				if !strings.Contains(conflicted, marker) {
					t.Fatalf("conflicted file lacks %q:\n%s", marker, conflicted)
				}
			}
			if git(t, f.remote, "rev-parse", "main") != prior || !strings.Contains(remoteContent, "second edit") {
				t.Fatal("conflict changed the remote target")
			}
			if _, err := s.ReconcileAndPush(t.Context(), third, f.target()); !errors.As(err, &conflict) ||
				get(t, third.Path(), sharedFile) != conflicted {
				t.Fatalf("retry lost preserved conflict: %v", err)
			}
		})
	}
}

// TestRemoteAdvanceRejectsPush proves AC3 when another writer advances the remote after the fetch.
func TestRemoteAdvanceRejectsPush(t *testing.T) {
	f := newFixture(t)
	other := f.thread(t, New(allow, nil), "concurrent")
	editThread(t, New(allow, nil), other, "gamma", "concurrent edit")
	s := New(allow, func(_ context.Context, scan ScanRequest) error {
		if scan.Stage == PrePush {
			git(t, other.Path(), "push", "origin", "HEAD:refs/heads/main")
		}
		return nil
	})
	thread := f.thread(t, s, "push")
	editThread(t, s, thread, "alpha", "local edit")
	// This configured force refspec must not override the explicit push refspec.
	git(t, f.repo, "config", "remote.origin.push", "+HEAD:refs/heads/main")
	result, err := s.ReconcileAndPush(t.Context(), thread, f.target())
	var rejected *PushError
	if !errors.As(err, &rejected) || !strings.Contains(rejected.Report, "[rejected]") {
		t.Fatalf("concurrent advance = %+v, %v", result, err)
	}
	if got := git(t, f.remote, "rev-parse", "main"); got != git(t, other.Path(), "rev-parse", "HEAD") {
		t.Fatal("rejected push changed the target")
	}
	if get(t, thread.Path(), sharedFile) == f.content {
		t.Fatal("rejected push discarded the local edit")
	}
}

// TestChangedRemoteEndpoint proves AC3 when a scan-time config edit redirects the remote.
func TestChangedRemoteEndpoint(t *testing.T) {
	f := newFixture(t)
	other := f.remote + "-redirected"
	git(t, f.root, "init", "--bare", "-b", "main", other)
	s := New(allow, func(_ context.Context, scan ScanRequest) error {
		if scan.Stage == PrePush {
			git(t, f.repo, "remote", "set-url", "origin", other)
		}
		return nil
	})
	thread := f.thread(t, s, "changed-url")
	editThread(t, s, thread, "alpha", "local edit")
	result, err := s.ReconcileAndPush(t.Context(), thread, f.target())
	if err == nil || !strings.Contains(err.Error(), "remote URL changed") || result.Fetched == "" {
		t.Fatalf("changed target = %+v, %v", result, err)
	}
	if got := git(t, other, "for-each-ref"); got != "" {
		t.Fatalf("redirected remote has refs: %s", got)
	}
}

// TestMismatchedPushURL proves AC3 refuses to reconcile against one endpoint and push another.
func TestMismatchedPushURL(t *testing.T) {
	f := newFixture(t)
	s := New(allow, nil)
	thread := f.thread(t, s, "split-url")
	other := f.remote + "-other"
	git(t, f.root, "init", "--bare", "-b", "main", other)
	git(t, f.repo, "remote", "set-url", "--push", "origin", other)
	result, err := s.ReconcileAndPush(t.Context(), thread, f.target())
	if err == nil || !strings.Contains(err.Error(), "same single remote URL") || result.Fetched != "" {
		t.Fatalf("mismatched target = %+v, %v", result, err)
	}
}

// --- Secret scanner: distinguish configured, blocked, and unreached hooks ---

// TestScanHooks proves AC8 for authored commits, merge commits, and pushes without a scanner.
func TestScanHooks(t *testing.T) {
	f := newFixture(t)
	unconfigured := New(allow, nil)
	first := f.thread(t, unconfigured, "first")
	if result := editThread(t, unconfigured, first, "alpha", "first edit"); result != ScanUnconfigured {
		t.Fatalf("unconfigured pre-commit = %q", result)
	}
	result, err := unconfigured.ReconcileAndPush(t.Context(), first, f.target())
	if err != nil || result.PrePush != ScanUnconfigured || result.PreCommit != "" {
		t.Fatalf("unconfigured push = %+v, %v", result, err)
	}
	var seen []ScanRequest
	s := New(allow, func(_ context.Context, request ScanRequest) error {
		seen = append(seen, request)
		return nil
	})
	second := f.thread(t, s, "second")
	if result := editThread(t, s, second, "gamma", "second edit"); result != ScanPassed {
		t.Fatalf("configured pre-commit = %q", result)
	}
	result, err = s.ReconcileAndPush(t.Context(), second, f.target())
	want := []ScanRequest{
		{Stage: PreCommit, Worktree: second.Path()},
		{Stage: PreCommit, Worktree: second.Path()},
		{Stage: PrePush, Worktree: second.Path(), From: result.Fetched, To: result.Revision},
	}
	if err != nil || result.PreCommit != ScanPassed || result.PrePush != ScanPassed || !slices.Equal(seen, want) {
		t.Fatalf("configured scan results = %+v, %v; hooks = %+v", result, err, seen)
	}
}

// TestScanFailureBlocksCommitAndPush proves AC8 lets a configured scanner block either mutation.
func TestScanFailureBlocksCommitAndPush(t *testing.T) {
	f := newFixture(t)
	block := errors.New("scanner blocked mutation")
	s := New(allow, func(context.Context, ScanRequest) error { return block })
	thread := f.thread(t, s, "scanner-block")
	prior := git(t, thread.Path(), "rev-parse", "HEAD")
	put(t, thread.Path(), sharedFile, "staged edit")
	if err := s.StageAll(t.Context(), thread); err != nil {
		t.Fatal(err)
	}
	if result, err := s.Commit(t.Context(), thread, "blocked"); result != ScanFailed || !errors.Is(err, block) {
		t.Fatalf("blocked commit = %q, %v", result, err)
	}
	if git(t, thread.Path(), "rev-parse", "HEAD") != prior {
		t.Fatal("commit ran after scan failure")
	}
	editThread(t, New(allow, nil), thread, "staged edit", "local commit")
	result, err := s.ReconcileAndPush(t.Context(), thread, f.target())
	if result.PrePush != ScanFailed || !errors.Is(err, block) {
		t.Fatalf("blocked push = %+v, %v", result, err)
	}
	if git(t, f.remote, "rev-parse", "main") != prior {
		t.Fatal("push ran after scan failure")
	}
}
