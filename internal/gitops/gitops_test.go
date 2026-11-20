package gitops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const sharedFile = "same.txt"

// TestMain makes every git process, ours and the code under test's, ignore the
// host's ~/.gitconfig and system config (card 111 H4), so an unreadable or
// customised host config cannot fail or change a test.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "hermetic-git-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	empty := filepath.Join(dir, "gitconfig")
	if err := os.WriteFile(empty, nil, 0600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for k, v := range map[string]string{"GIT_CONFIG_GLOBAL": empty, "GIT_CONFIG_SYSTEM": empty, "GIT_CONFIG_NOSYSTEM": "1"} {
		_ = os.Setenv(k, v)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// fixture provides an isolated repository with a local bare remote.
type fixture struct {
	root    string
	repo    string
	remote  string
	content string
}

// git executes setup or inspection commands against local test repositories.
func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_ALLOW_PROTOCOL=file", "GIT_TERMINAL_PROMPT=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// put writes a test fixture file.
func put(t *testing.T, dir, path, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, path), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

// get reads fixture bytes, retaining whitespace and conflict markers.
func get(t *testing.T, dir, path string) string {
	t.Helper()
	out, err := os.ReadFile(filepath.Join(dir, path))
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

// newFixture creates a base revision with well-separated editable lines.
func newFixture(t *testing.T) fixture {
	t.Helper()
	f := fixture{root: t.TempDir()}
	f.repo = filepath.Join(f.root, "project")
	f.remote = filepath.Join(f.root, "remote.git")
	git(t, f.root, "init", "--bare", "-b", "main", f.remote)
	git(t, f.root, "init", "-b", "main", f.repo)
	git(t, f.repo, "config", "user.name", "Synch Agent")
	git(t, f.repo, "config", "user.email", "agent@synch.local")
	git(t, f.repo, "config", "core.autocrlf", "false")
	git(t, f.repo, "config", "commit.gpgSign", "false")
	git(t, f.repo, "config", "core.hooksPath", filepath.Join(f.root, "unused-hooks"))
	git(t, f.repo, "config", "core.filemode", "false")
	separator := "\n" + strings.Repeat("context\n", 9)
	f.content = strings.Join([]string{"alpha", "beta", "gamma"}, separator) + "\n"
	put(t, f.repo, sharedFile, f.content)
	put(t, f.repo, "CONVENTIONS.md", "# Project\n")
	put(t, f.repo, ".gitignore", "*.ignored\n")
	git(t, f.repo, "add", "--all")
	git(t, f.repo, "commit", "-m", "base")
	git(t, f.repo, "remote", "add", "origin", f.remote)
	git(t, f.repo, "push", "origin", "HEAD:refs/heads/main")
	return f
}

// allow permits a policy action in a fixture that exercises another behavior.
func allow(context.Context, Request) error { return nil }

// project registers fixture metadata using the service.
func (f fixture) project(t *testing.T, s *Service) Project {
	t.Helper()
	p, err := s.Register(t.Context(), f.repo, "main", "")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// thread makes a linked fixture worktree off the registered base revision.
func (f fixture) thread(t *testing.T, s *Service, id string) Thread {
	t.Helper()
	thread, err := s.CreateThread(t.Context(), f.project(t, s), id, filepath.Join(f.root, id), "")
	if err != nil {
		t.Fatal(err)
	}
	return thread
}

// target identifies the fixture's local bare main branch.
func (f fixture) target() RemoteTarget { return RemoteTarget{Remote: "origin", Branch: "main"} }

// editThread changes a line and commits it using the mediated entry points.
func editThread(t *testing.T, s *Service, thread Thread, before, after string) ScanResult {
	t.Helper()
	content := strings.Replace(get(t, thread.Path(), sharedFile), before, after, 1)
	put(t, thread.Path(), sharedFile, content)
	if err := s.StageAll(t.Context(), thread); err != nil {
		t.Fatal(err)
	}
	result, err := s.Commit(t.Context(), thread, after)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// TestRegistrationAndOwnership proves AC1 and AC2, including restart and safe removal.
func TestRegistrationAndOwnership(t *testing.T) {
	f := newFixture(t)
	s := New(allow, nil)
	p := f.project(t, s)
	if !samePath(p.Path, f.repo) || p.BaseBranch != "main" || !samePath(p.Conventions, filepath.Join(f.repo, "CONVENTIONS.md")) {
		t.Fatalf("registration = %+v", p)
	}
	if _, err := s.Register(t.Context(), t.TempDir(), "main", ""); !errors.Is(err, ErrNotRepository) {
		t.Fatalf("non-repository registration = %v", err)
	}
	revision := git(t, f.repo, "rev-parse", "HEAD")
	first, err := s.CreateThread(t.Context(), p, "first", filepath.Join(f.root, "first"), revision)
	if err != nil {
		t.Fatal(err)
	}
	second := f.thread(t, s, "second")
	if got := git(t, first.Path(), "rev-parse", "HEAD"); got != revision {
		t.Fatalf("first HEAD = %s, want %s", got, revision)
	}
	if got := git(t, first.Path(), "symbolic-ref", "--short", "HEAD"); got != "synch/first" {
		t.Fatalf("thread branch = %s", got)
	}
	if _, err := s.CreateThread(t.Context(), p, "first", filepath.Join(f.root, "duplicate"), revision); err == nil {
		t.Fatal("duplicate thread branch was accepted")
	}
	s = New(allow, nil)
	if _, err := s.OpenThread(t.Context(), p, "first", second.Path()); !errors.Is(err, ErrWorktreeOwnership) {
		t.Fatalf("opening another thread's worktree = %v", err)
	}
	opened, err := s.OpenThread(t.Context(), p, "first", first.Path())
	if err != nil {
		t.Fatal(err)
	}
	put(t, first.Path(), "dirty", "preserve")
	if err := s.RemoveThread(t.Context(), opened, Settled()); err == nil {
		t.Fatal("dirty worktree was removed")
	}
	if err := os.Remove(filepath.Join(first.Path(), "dirty")); err != nil {
		t.Fatal(err)
	}
	nested, err := s.CreateThread(t.Context(), p, "nested", filepath.Join(first.Path(), "nested.ignored"), revision)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveThread(t.Context(), opened, Settled()); !errors.Is(err, ErrWorktreeOwnership) {
		t.Fatalf("remove worktree containing an ignored linked worktree = %v", err)
	}
	if got := get(t, nested.Path(), sharedFile); got != f.content {
		t.Fatal("nested thread's worktree changed")
	}
	if err := s.RemoveThread(t.Context(), nested, Settled()); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveThread(t.Context(), opened, Settled()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(first.Path()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("removed thread worktree still available: %v", err)
	}
	if got := get(t, second.Path(), sharedFile); got != f.content {
		t.Fatal("second thread's working file changed")
	}
	if got := git(t, f.repo, "rev-parse", "refs/heads/synch/first"); got != revision {
		t.Fatal("removing the first worktree changed its branch")
	}
}

// --- Policy fixture: exercise the example with an injected decider ---

// exampleDecision interprets the example's exact action and @mediated match.
func exampleDecision(t *testing.T, seen *[]Request) Decider {
	t.Helper()
	var policy struct {
		Rules []struct {
			Effect  string
			Actions []Action
			Match   struct{ Paths []string }
		}
	}
	out, err := os.ReadFile(filepath.Join("..", "..", "contracts", "policy", "examples", "deny-destructive-git.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(out, &policy); err != nil {
		t.Fatal(err)
	}
	return func(_ context.Context, request Request) error {
		*seen = append(*seen, request)
		for _, rule := range policy.Rules {
			if rule.Effect == "deny" && request.Mediated && slices.Contains(rule.Match.Paths, "@mediated") &&
				slices.Contains(rule.Actions, request.Action) {
				return errors.New("example policy denied destructive action")
			}
		}
		return nil
	}
}

// TestDestructiveCommands proves AC5 for direct options, aliases, and refspec disguises.
func TestDestructiveCommands(t *testing.T) {
	f := newFixture(t)
	var seen []Request
	s := New(exampleDecision(t, &seen), nil)
	thread := f.thread(t, s, "policy")
	put(t, thread.Path(), sharedFile, "unsaved edit")
	put(t, thread.Path(), "trash.ignored", "ignored data")

	tests := []struct {
		action Action
		args   []string
	}{
		{PushForce, []string{"push", "--force", "origin", "main"}},
		{PushForce, []string{"push", "-f", "origin", "main"}},
		{PushForce, []string{"push", "-vf", "origin", "main"}},
		{PushForce, []string{"push", "--force-with-lease", "origin", "main"}},
		{PushForce, []string{"push", "--force-with-lease=main:HEAD", "origin", "main"}},
		{PushForce, []string{"push", "--force-with-l=main:HEAD", "origin", "main"}},
		{PushForce, []string{"push", "--", "origin", "+HEAD:refs/heads/main"}},
		{PushForce, []string{"push", "--mirror", "origin"}},
		{ResetHard, []string{"reset", "--hard", "HEAD"}},
		{Clean, []string{"clean", "-fdx"}},
		{BranchDelete, []string{"branch", "-D", "main"}},
		{BranchDelete, []string{"branch", "--delete", "main"}},
		{BranchDelete, []string{"push", "origin", ":refs/heads/main"}},
		{HistoryRewrite, []string{"filter-branch", "--", "--all"}},
		{HistoryRewrite, []string{"rebase", "-i", "HEAD~1"}},
		{HistoryRewrite, []string{"commit", "--amend", "--no-edit"}},
		{CheckoutDiscard, []string{"checkout", "--", "."}},
		{CheckoutDiscard, []string{"checkout", "HEAD", sharedFile}},
		{CheckoutDiscard, []string{"restore", "."}},
		{CheckoutDiscard, []string{"switch", "--discard-changes", "main"}},
	}
	for _, test := range tests {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			seen = nil
			_, err := s.Run(t.Context(), thread, test.args...)
			var denied *DeniedError
			want := thread.request(test.action, thread.Branch())
			if !errors.As(err, &denied) || !slices.Equal(seen, []Request{want}) || denied.Request != want {
				t.Fatalf("decision requests = %+v, error = %v, want denied %+v", seen, err, want)
			}
		})
	}
	if got := get(t, thread.Path(), sharedFile); got != "unsaved edit" {
		t.Fatalf("working edit changed: %q", got)
	}
	if got := get(t, thread.Path(), "trash.ignored"); got != "ignored data" {
		t.Fatalf("ignored file changed: %q", got)
	}
	if out, err := s.Run(t.Context(), thread, "status"); err != nil || !strings.Contains(string(out), sharedFile) {
		t.Fatalf("routine status = %q, %v", out, err)
	}
	if _, err := s.Run(t.Context(), thread, "push", "origin", "HEAD:main"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("raw push bypassed reconcile entry point: %v", err)
	}
}

// TestPolicyDenialStopsFetch proves AC3 and AC5 at the real push entry point.
func TestPolicyDenialStopsFetch(t *testing.T) {
	f := newFixture(t)
	var effects []Action
	s := New(func(_ context.Context, request Request) error {
		if request.Action == Fetch || request.Action == Merge || request.Action == Push {
			effects = append(effects, request.Action)
		}
		if request.Action == Fetch {
			return fmt.Errorf("blocked fetch for %s", request.Target)
		}
		return nil
	}, nil)
	thread := f.thread(t, s, "fetch-denied")
	if _, err := s.ReconcileAndPush(t.Context(), thread, f.target()); !errors.Is(err, ErrDenied) {
		t.Fatalf("reconcile with denied fetch = %v", err)
	}
	if !slices.Equal(effects, []Action{Fetch}) {
		t.Fatalf("requested effects = %v", effects)
	}
	fetchHead := git(t, thread.Path(), "rev-parse", "--path-format=absolute", "--git-path", "FETCH_HEAD")
	if _, err := os.Stat(fetchHead); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("denied fetch wrote FETCH_HEAD: %v", err)
	}
}
