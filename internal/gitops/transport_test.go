package gitops

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// insteadOfRulesFor installs insteadOf URL rewriting rules — one in the
// repository config, one in the global config — redirecting the recorded URL
// to the evil target. The global rule is written through the test process's
// GIT_CONFIG_GLOBAL so the operator's real global config is untouched.
func insteadOfRulesFor(t *testing.T, f fixture, evil string) {
	t.Helper()
	git(t, f.repo, "config", "url."+evil+"/.insteadOf", f.remote)
	git(t, f.repo, "config", "--global", "url."+evil+"/.insteadOf", f.remote)
}

// TestTransportIgnoresInsteadOfRulesAddedAtPushAuthorization proves H1: URL
// rewriting rules installed when the push is authorized — in the repository
// config and in the global config — cannot redirect the transport. The push
// reaches the recorded bare repository and the redirect target stays empty.
func TestTransportIgnoresInsteadOfRulesAddedAtPushAuthorization(t *testing.T) {
	f := newFixture(t)
	evil := filepath.Join(f.root, "evil-target")
	git(t, f.root, "init", "--bare", "-b", "main", evil)
	// The global rule must be written to a test-owned global config, not the
	// operator's real one.
	t.Setenv("GIT_CONFIG_GLOBAL", filepath.Join(f.root, "global.gitconfig"))

	base := git(t, f.remote, "rev-parse", "main")
	s := New(func(_ context.Context, request Request) error {
		if request.Action == Push {
			insteadOfRulesFor(t, f, evil)
		}
		return nil
	}, nil)
	thread := f.thread(t, s, "redirect")
	editThread(t, s, thread, "alpha", "redirected edit")

	if _, err := s.ReconcileAndPush(t.Context(), thread, f.target()); err != nil {
		t.Fatalf("reconcile with insteadOf rules = %v", err)
	}
	if got := git(t, f.remote, "rev-parse", "main"); got == base {
		t.Fatal("recorded remote did not advance: the push was redirected or lost")
	}
	if refs := git(t, evil, "for-each-ref"); refs != "" {
		t.Fatalf("redirect target has refs: %s", refs)
	}
}

// TestFetchTransportIgnoresInsteadOfRulesAtFetchAuthorization proves H1 for
// the fetch: rules installed when the fetch is authorized do not redirect the
// fetch (the recorded repository's commit is fetched), and the pre-push
// endpoint check still refuses the now-rewritten repository config with
// ErrRemoteURLChanged, leaving the redirect target empty.
func TestFetchTransportIgnoresInsteadOfRulesAtFetchAuthorization(t *testing.T) {
	f := newFixture(t)
	evil := filepath.Join(f.root, "evil-target")
	git(t, f.root, "init", "--bare", "-b", "main", evil)
	t.Setenv("GIT_CONFIG_GLOBAL", filepath.Join(f.root, "global.gitconfig"))

	base := git(t, f.remote, "rev-parse", "main")
	s := New(func(_ context.Context, request Request) error {
		if request.Action == Fetch {
			insteadOfRulesFor(t, f, evil)
		}
		return nil
	}, nil)
	thread := f.thread(t, s, "redirect-fetch")
	editThread(t, s, thread, "alpha", "redirected edit")

	result, err := s.ReconcileAndPush(t.Context(), thread, f.target())
	if !errors.Is(err, ErrRemoteURLChanged) {
		t.Fatalf("reconcile = %v, want ErrRemoteURLChanged from the pre-push check", err)
	}
	if result.Fetched != base {
		t.Fatalf("fetched = %s, want the recorded repository's commit %s", result.Fetched, base)
	}
	if refs := git(t, evil, "for-each-ref"); refs != "" {
		t.Fatalf("redirect target has refs: %s", refs)
	}
}

// TestInsteadOfRulesAreRefusedAtEndpointChecks proves the refusal half of H1:
// once insteadOf rules are configured, the endpoint check refuses the
// reconciliation before any transport runs.
func TestInsteadOfRulesAreRefusedAtEndpointChecks(t *testing.T) {
	f := newFixture(t)
	evil := filepath.Join(f.root, "evil-target")
	git(t, f.root, "init", "--bare", "-b", "main", evil)
	// The rules are installed on the first status authorization, which happens
	// while the fixture thread is created, so the reconciliation starts with
	// rewriting already configured.
	s := New(func(_ context.Context, request Request) error {
		if request.Action == Status {
			insteadOfRulesFor(t, f, evil)
		}
		return nil
	}, nil)
	thread := f.thread(t, s, "refused")
	editThread(t, s, thread, "alpha", "redirected edit")

	_, err := s.ReconcileAndPush(t.Context(), thread, f.target())
	if !errors.Is(err, ErrRemoteURLChanged) || !strings.Contains(err.Error(), "URL rewriting rules") {
		t.Fatalf("reconcile = %v, want the rewriting-rules refusal", err)
	}
	if refs := git(t, evil, "for-each-ref"); refs != "" {
		t.Fatalf("redirect target has refs: %s", refs)
	}
}
