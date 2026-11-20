package gitops

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// TestClassifyHardening proves AC3 (O5) classification of the disguised
// mutations: forced fetch refspecs, merge --abort, and diff --output, without
// reclassifying their plain forms.
func TestClassifyHardening(t *testing.T) {
	tests := []struct {
		args   []string
		action Action
	}{
		{[]string{"fetch", "+refs/heads/*:refs/heads/*"}, PushForce},
		{[]string{"fetch", "--", "+refs/heads/main:refs/heads/main"}, PushForce},
		{[]string{"fetch", "origin", "+refs/heads/main:main"}, PushForce},
		{[]string{"fetch", "--force", "origin", "main"}, PushForce},
		{[]string{"fetch", "origin", "main"}, Fetch},
		{[]string{"merge", "--abort"}, CheckoutDiscard},
		{[]string{"merge", "--ab", "HEAD"}, CheckoutDiscard},
		{[]string{"merge", "--no-commit", "origin/main"}, Merge},
		{[]string{"diff", "--output=patch.txt"}, WriteWorktree},
		{[]string{"diff", "--output", "patch.txt"}, WriteWorktree},
		{[]string{"diff", "--output-indicator-new=>"}, Diff},
		{[]string{"diff", "HEAD", "--"}, Diff},
	}
	for _, test := range tests {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			action, err := Classify(test.args...)
			if err != nil || action != test.action {
				t.Fatalf("Classify(%q) = %s, %v; want %s", test.args, action, err, test.action)
			}
		})
	}
}

// TestRunDeniesDisguisedMutations proves AC3 (O5) end to end: the dispatcher
// authorizes the hardened classification before refusing execution, so the
// example policy denies a disguised forced fetch and a merge --abort, and a
// diff --output is surfaced to the decider as fs.write before refusal.
func TestRunDeniesDisguisedMutations(t *testing.T) {
	f := newFixture(t)
	var seen []Request
	s := New(exampleDecision(t, &seen), nil)
	thread := f.thread(t, s, "disguised")
	denied := []struct {
		args   []string
		action Action
	}{
		{[]string{"fetch", "+refs/heads/*:refs/heads/*"}, PushForce},
		{[]string{"merge", "--abort"}, CheckoutDiscard},
	}
	for _, test := range denied {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			seen = nil
			_, err := s.Run(t.Context(), thread, test.args...)
			want := thread.request(test.action, thread.Branch())
			var refused *DeniedError
			if !errors.As(err, &refused) || !slices.Equal(seen, []Request{want}) || refused.Request != want {
				t.Fatalf("Run(%q) = %v, requests %+v; want denied %s", test.args, err, seen, test.action)
			}
		})
	}
	seen = nil
	if _, err := s.Run(t.Context(), thread, "diff", "--output=patch.txt"); !errors.Is(err, ErrUnsupported) ||
		!slices.Contains(seen, thread.request(WriteWorktree, thread.Branch())) {
		t.Fatalf("diff --output = %v, requests %+v; want an fs.write request then refusal", err, seen)
	}
}
