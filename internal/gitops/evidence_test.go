package gitops

import (
	"context"
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
