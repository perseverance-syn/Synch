package gitops

import (
	"context"
	"strings"
	"testing"
)

// TestReconciliationPolicyEvidence proves D-24 binds both actions to their live worktrees.
func TestReconciliationPolicyEvidence(t *testing.T) {
	f := newFixture(t)
	seed := New(allow, nil)
	first := f.thread(t, seed, "first")
	second := f.thread(t, seed, "second")
	editThread(t, seed, first, "alpha", "first change")
	editThread(t, seed, second, "beta", "second change")
	if _, err := seed.ReconcileAndPush(t.Context(), first, f.target()); err != nil {
		t.Fatal(err)
	}
	var decisions []Request
	before := git(t, second.Path(), "rev-parse", "HEAD")
	digestBefore, err := seed.WorktreeDigest(t.Context(), second)
	if err != nil {
		t.Fatal(err)
	}
	service := NewWithEvidence(func(_ context.Context, request Request) error {
		if request.Action == Merge || request.Action == Push {
			decisions = append(decisions, request)
		}
		return nil
	}, nil)
	result, err := service.ReconcileAndPush(t.Context(), second, f.target())
	if err != nil {
		t.Fatal(err)
	}
	digestAfter, err := seed.WorktreeDigest(t.Context(), second)
	if err != nil {
		t.Fatal(err)
	}
	if len(decisions) != 2 || result.Revision == before || digestBefore == digestAfter {
		t.Fatalf("expected evidence on both sides of a merge commit: %+v", decisions)
	}
	merge, push := decisions[0], decisions[1]
	if merge.Action != Merge || merge.Revision != before || merge.WorktreeDigest != digestBefore ||
		push.Action != Push || push.Revision != result.Revision || push.WorktreeDigest != digestAfter {
		t.Fatalf("invalid reconciliation evidence: merge=%+v push=%+v", merge, push)
	}
}

// TestBaseDiff proves the read-only diff covers committed and uncommitted tracked changes
// since the merge base and leaves the worktree untouched.
func TestBaseDiff(t *testing.T) {
	f := newFixture(t)
	s := New(allow, nil)
	tree := f.thread(t, s, "diff")
	baseRev := git(t, f.repo, "rev-parse", "main")
	put(t, tree.Path(), sharedFile, strings.Replace(f.content, "alpha", "committed", 1))
	git(t, tree.Path(), "commit", "-am", "edit")
	// The base branch moving on must not leak into the thread diff.
	put(t, f.repo, "other.txt", "other\n")
	git(t, f.repo, "add", "other.txt")
	git(t, f.repo, "commit", "-m", "other")
	put(t, tree.Path(), sharedFile, strings.Replace(strings.Replace(f.content, "alpha", "committed", 1), "beta", "dirty", 1))
	before := git(t, tree.Path(), "status", "--porcelain")
	d, err := s.BaseDiff(t.Context(), tree)
	if err != nil {
		t.Fatal(err)
	}
	patch := string(d.Patch)
	if d.Base != baseRev || d.Revision != git(t, tree.Path(), "rev-parse", "HEAD") || !strings.Contains(patch, "+committed") || !strings.Contains(patch, "+dirty") || strings.Contains(patch, "other.txt") {
		t.Fatalf("base %s revision %s patch %q", d.Base, d.Revision, patch)
	}
	if after := git(t, tree.Path(), "status", "--porcelain"); after != before {
		t.Fatalf("BaseDiff changed the worktree: %q -> %q", before, after)
	}
}
