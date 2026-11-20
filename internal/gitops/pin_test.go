package gitops

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// TestRegistrationRecordsRemoteURLs proves F2 and F1 registration: a shared
// endpoint records equal pins, split endpoints record both URLs, and a
// remote-less project registers with empty pins.
func TestRegistrationRecordsRemoteURLs(t *testing.T) {
	f := newFixture(t)
	s := New(allow, nil)
	p := f.project(t, s)
	if p.FetchURL != f.remote || p.PushURL != f.remote {
		t.Fatalf("shared-endpoint registration pinned %q and %q, want %q", p.FetchURL, p.PushURL, f.remote)
	}
	pushURL := f.remote + "-push"
	git(t, f.root, "init", "--bare", "-b", "main", pushURL)
	git(t, f.repo, "remote", "set-url", "--push", "origin", pushURL)
	split, err := s.Register(t.Context(), f.repo, "main", "")
	if err != nil || split.FetchURL != f.remote || split.PushURL != pushURL {
		t.Fatalf("split-endpoint registration = %+v, %v", split, err)
	}
	git(t, f.repo, "remote", "remove", "origin")
	remoteless, err := s.Register(t.Context(), f.repo, "main", "")
	if err != nil || remoteless.FetchURL != "" || remoteless.PushURL != "" {
		t.Fatalf("remote-less registration = %+v, %v", remoteless, err)
	}
}

// TestRegistrationRefusesAmbiguousRemoteURLs proves F3: a remote with more
// than one fetch URL or more than one push URL is refused as ambiguous.
func TestRegistrationRefusesAmbiguousRemoteURLs(t *testing.T) {
	for _, push := range []bool{false, true} {
		name := "fetch"
		if push {
			name = "push"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			extra := f.remote + "-extra"
			git(t, f.root, "init", "--bare", "-b", "main", extra)
			if push {
				// One added push URL would only split the endpoints, which
				// registration allows, so two make the direction ambiguous.
				second := f.remote + "-extra2"
				git(t, f.root, "init", "--bare", "-b", "main", second)
				git(t, f.repo, "remote", "set-url", "--push", "--add", "origin", extra)
				git(t, f.repo, "remote", "set-url", "--push", "--add", "origin", second)
			} else {
				git(t, f.repo, "remote", "set-url", "--add", "origin", extra)
			}
			if _, err := New(allow, nil).Register(t.Context(), f.repo, "main", ""); err == nil ||
				!strings.Contains(err.Error(), "exactly one fetch URL and one push URL") {
				t.Fatalf("ambiguous registration = %v", err)
			}
		})
	}
}

// TestRemotelessProjectCannotReconcile proves F1: a remote-less project
// registers with empty pins and its reconciliation is refused with
// ErrNoPinnedRemote before any network call.
func TestRemotelessProjectCannotReconcile(t *testing.T) {
	f := newFixture(t)
	git(t, f.repo, "remote", "remove", "origin")
	s := New(allow, nil)
	thread := f.thread(t, s, "local")
	base := git(t, f.remote, "rev-parse", "main")
	editThread(t, s, thread, "alpha", "local edit")
	result, err := s.ReconcileAndPush(t.Context(), thread, f.target())
	if !errors.Is(err, ErrNoPinnedRemote) || errors.Is(err, ErrRemoteURLChanged) || result.Fetched != "" {
		t.Fatalf("remote-less reconcile = %+v, %v", result, err)
	}
	if got := git(t, f.remote, "rev-parse", "main"); got != base {
		t.Fatalf("remote-less reconcile touched the remote: %s", got)
	}
}

// TestSplitRemoteURLsPushAndFetchToPins proves F2: with split URLs recorded,
// reconciliation fetches from the recorded fetch URL, pushes to the recorded
// push URL, and the decider sees each recorded URL in Target.
func TestSplitRemoteURLsPushAndFetchToPins(t *testing.T) {
	f := newFixture(t)
	pushURL := f.remote + "-push"
	git(t, f.root, "init", "--bare", "-b", "main", pushURL)
	git(t, f.repo, "remote", "set-url", "--push", "origin", pushURL)
	var seen []Request
	s := New(func(_ context.Context, request Request) error {
		seen = append(seen, request)
		return nil
	}, nil)
	p := f.project(t, s)
	if p.FetchURL != f.remote || p.PushURL != pushURL {
		t.Fatalf("split registration = %+v", p)
	}
	thread := f.thread(t, s, "split")
	base := git(t, f.remote, "rev-parse", "main")
	editThread(t, s, thread, "alpha", "local edit")
	seen = nil
	result, err := s.ReconcileAndPush(t.Context(), thread, f.target())
	if err != nil || result.Fetched != base || result.Revision != git(t, thread.Path(), "rev-parse", "HEAD") {
		t.Fatalf("split reconcile = %+v, %v", result, err)
	}
	if got := git(t, pushURL, "rev-parse", "main"); got != result.Revision {
		t.Fatalf("push URL main = %s, want the pushed revision %s", got, result.Revision)
	}
	if got := git(t, f.remote, "rev-parse", "main"); got != base {
		t.Fatalf("fetch URL main = %s, want the unchanged %s", got, base)
	}
	for _, want := range []Request{
		thread.request(Fetch, f.remote+":refs/heads/main"),
		thread.request(Merge, f.remote+":refs/heads/main"),
		thread.request(Push, pushURL+":refs/heads/main"),
	} {
		if !slices.Contains(seen, want) {
			t.Fatalf("decider did not see %+v: %+v", want, seen)
		}
	}
}

// TestSplitRemoteURLDriftRefused proves F2: with split URLs recorded, a
// changed fetch or push URL, or an added second URL, refuses reconciliation
// with ErrRemoteURLChanged and leaves the swapped remote untouched.
func TestSplitRemoteURLDriftRefused(t *testing.T) {
	f := newFixture(t)
	pushURL := f.remote + "-push"
	swapped := f.remote + "-swapped"
	git(t, f.root, "init", "--bare", "-b", "main", pushURL)
	git(t, f.root, "init", "--bare", "-b", "main", swapped)
	git(t, f.repo, "remote", "set-url", "--push", "origin", pushURL)
	s := New(allow, nil)
	thread := f.thread(t, s, "drift")
	base := git(t, f.remote, "rev-parse", "main")
	editThread(t, s, thread, "alpha", "local edit")

	// (a) The fetch URL changes before the call.
	git(t, f.repo, "remote", "set-url", "origin", swapped)
	result, err := s.ReconcileAndPush(t.Context(), thread, f.target())
	if !errors.Is(err, ErrRemoteURLChanged) || result.Fetched != "" {
		t.Fatalf("fetch drift = %+v, %v", result, err)
	}
	if refs := git(t, swapped, "for-each-ref"); refs != "" {
		t.Fatalf("swapped remote has refs: %s", refs)
	}

	// (b) The push URL changes between fetch and push, during the scan hook.
	git(t, f.repo, "remote", "set-url", "origin", f.remote)
	s = New(allow, func(_ context.Context, scan ScanRequest) error {
		if scan.Stage == PrePush {
			git(t, f.repo, "remote", "set-url", "--push", "origin", swapped)
		}
		return nil
	})
	result, err = s.ReconcileAndPush(t.Context(), thread, f.target())
	if !errors.Is(err, ErrRemoteURLChanged) || result.Fetched == "" {
		t.Fatalf("push drift = %+v, %v", result, err)
	}
	if refs := git(t, swapped, "for-each-ref"); refs != "" {
		t.Fatalf("swapped remote has refs: %s", refs)
	}
	if refs := git(t, pushURL, "for-each-ref"); refs != "" {
		t.Fatalf("recorded push remote has refs: %s", refs)
	}

	// (c) A second fetch URL is added, which is ambiguous.
	git(t, f.repo, "remote", "set-url", "--push", "origin", pushURL)
	git(t, f.repo, "remote", "set-url", "--add", "origin", swapped)
	s = New(allow, nil)
	result, err = s.ReconcileAndPush(t.Context(), thread, f.target())
	if !errors.Is(err, ErrRemoteURLChanged) || !strings.Contains(err.Error(), "exactly one fetch URL and one push URL") || result.Fetched != "" {
		t.Fatalf("added fetch URL = %+v, %v", result, err)
	}
	if refs := git(t, swapped, "for-each-ref"); refs != "" {
		t.Fatalf("swapped remote has refs: %s", refs)
	}
	if got := git(t, f.remote, "rev-parse", "main"); got != base {
		t.Fatalf("fetch URL main = %s, want the unchanged %s", got, base)
	}
}

// TestPinnedEndpointsRefuseRedirect proves AC1 (O1) cases (a) and (b): a URL
// changed before the call refuses the fetch, and a pushurl changed between
// fetch and push refuses the push, always leaving a swapped remote untouched.
func TestPinnedEndpointsRefuseRedirect(t *testing.T) {
	f := newFixture(t)
	swapped := f.remote + "-swapped"
	git(t, f.root, "init", "--bare", "-b", "main", swapped)

	// (a) The remote's fetch URL changes before the call.
	s := New(allow, nil)
	moved := f.thread(t, s, "moved")
	base := git(t, moved.Path(), "rev-parse", "HEAD")
	editThread(t, s, moved, "alpha", "local edit")
	git(t, f.repo, "remote", "set-url", "origin", swapped)
	result, err := s.ReconcileAndPush(t.Context(), moved, f.target())
	if !errors.Is(err, ErrRemoteURLChanged) || result.Fetched != "" {
		t.Fatalf("redirect before fetch = %+v, %v", result, err)
	}
	if refs := git(t, swapped, "for-each-ref"); refs != "" {
		t.Fatalf("swapped remote has refs: %s", refs)
	}
	if got := git(t, f.remote, "rev-parse", "main"); got != base {
		t.Fatalf("recorded remote advanced: %s", got)
	}

	// (b) The push URL changes between fetch and push, during the scan hook.
	// The fetch URL is restored first so registration pins the recorded
	// remote again after case (a) redirected it.
	git(t, f.repo, "remote", "set-url", "origin", f.remote)
	s = New(allow, func(_ context.Context, scan ScanRequest) error {
		if scan.Stage == PrePush {
			git(t, f.repo, "remote", "set-url", "--push", "origin", swapped)
		}
		return nil
	})
	pushed := f.thread(t, s, "pushed")
	editThread(t, s, pushed, "beta", "local edit")
	result, err = s.ReconcileAndPush(t.Context(), pushed, f.target())
	if !errors.Is(err, ErrRemoteURLChanged) || result.Fetched == "" {
		t.Fatalf("redirect between fetch and push = %+v, %v", result, err)
	}
	if refs := git(t, swapped, "for-each-ref"); refs != "" {
		t.Fatalf("swapped remote has refs: %s", refs)
	}
	if got := git(t, f.remote, "rev-parse", "main"); got != base {
		t.Fatalf("recorded remote advanced: %s", got)
	}
}

// TestPolicyRequestCarriesPinnedURL proves AC1 (O1) case (c): the decider
// receives the recorded URL in Target for fetch, merge, and push.
func TestPolicyRequestCarriesPinnedURL(t *testing.T) {
	f := newFixture(t)
	var seen []Request
	s := New(func(_ context.Context, request Request) error {
		seen = append(seen, request)
		return nil
	}, nil)
	thread := f.thread(t, s, "policy-url")
	editThread(t, s, thread, "gamma", "local edit")
	seen = nil
	if _, err := s.ReconcileAndPush(t.Context(), thread, f.target()); err != nil {
		t.Fatal(err)
	}
	for _, action := range []Action{Fetch, Merge, Push} {
		want := thread.request(action, f.remote+":refs/heads/main")
		if !slices.Contains(seen, want) {
			t.Fatalf("decider did not see %s with the pinned URL: %+v", action, seen)
		}
	}
}
