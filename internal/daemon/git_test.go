package daemon

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"synch/internal/engine"
	"synch/internal/gitops"
)

const gateFile = "shared.txt"
const gateContents = "alpha\n1\n2\n3\n4\nbeta\n5\n6\n7\n8\ngamma\n"

// fixtureGit runs Git for fixture setup and assertions outside the daemon.
func fixtureGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %s (%v)", args, out, err)
	}
	return strings.TrimSpace(string(out))
}

// fixtureRepository creates a local project and pins its bare remote.
func fixtureRepository(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	repo, remote := filepath.Join(root, "repo"), filepath.Join(root, "remote.git")
	fixtureGit(t, root, "init", "--bare", "--initial-branch=main", remote)
	fixtureGit(t, root, "init", "--initial-branch=main", repo)
	fixtureGit(t, repo, "config", "user.name", "Gate Test")
	fixtureGit(t, repo, "config", "user.email", "gate@synch.local")
	fixtureGit(t, repo, "config", "commit.gpgsign", "false")
	writeGateFile(t, repo, gateFile, gateContents)
	writeGateFile(t, repo, "CONVENTIONS.md", "Conventions\n")
	fixtureGit(t, repo, "add", ".")
	fixtureGit(t, repo, "commit", "-m", "initial")
	fixtureGit(t, repo, "remote", "add", "origin", filepath.ToSlash(remote))
	fixtureGit(t, repo, "push", "origin", "main")
	return repo, remote
}

// writeGateFile supplies fixture inputs and simulates edits made by a worker.
func writeGateFile(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}

// configureGitGate supplies operator rules for mediated setup and integration.
func configureGitGate(t *testing.T, cfg *Config, ask bool) {
	t.Helper()
	merge := `{"id":"merge","effect":"allow","actions":["merge"]}`
	if ask {
		merge = `{"id":"merge","effect":"ask","actions":["merge"],"approval":{"max_ttl":"PT1H"}}`
	}
	writeGateFile(t, cfg.DataDir, "policy.json", `{"policy_version":3,"source":"operator","rules":[
		{"id":"git","effect":"allow","actions":["git.*","fs.write","assign.dispatch"]},`+merge+`]}`)
	cfg.PolicyStore = filepath.Join(cfg.DataDir, "policy.json")
}

// gateAcceptedWorktree produces a committed result and obtains operator acceptance.
func gateAcceptedWorktree(t *testing.T, d *Daemon, base, id, revision string, starts <-chan *fakeSession) {
	t.Helper()
	if err := d.StartAssignment(t.Context(), engine.StartRequest{ThreadID: id}); err != nil {
		t.Fatal(err)
	}
	session := <-starts
	session.events <- engine.EngineEvent{Channel: "exit"}
	waitGate(t, func() bool {
		state, err := d.threads.Status(t.Context(), id)
		return err == nil && state.Phase == "verifying"
	})
	finishGateReview(t, d, base, id, revision)
}

// --- Phase 1 scenarios: reconcile multiple worktrees and reject stale approvals ---

// TestGateThreeWorktrees proves AC6 through the daemon using a local bare remote.
func TestGateThreeWorktrees(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint("overlap=", conflict), func(t *testing.T) {
			cfg, starts := gateConfig(t)
			configureGitGate(t, &cfg, false)
			d, base := serveGate(t, cfg)
			repo, remote := fixtureRepository(t)
			project, err := d.RegisterProject(t.Context(), repo, "main", "CONVENTIONS.md")
			if err != nil {
				t.Fatal(err)
			}
			target := gitops.RemoteTarget{Remote: "origin", Branch: "main"}
			worktrees := make([]gitops.Thread, 3)
			parent := gateThread(t, base, "git-parent")
			for i, line := range []string{"alpha", "beta", "gamma"} {
				if conflict && i == 2 {
					line = "alpha"
				}
				id, err := d.CreateWritingThread(t.Context(), parent)
				if err != nil {
					t.Fatal(err)
				}
				tree, err := d.CreateWorktree(t.Context(), project, id, filepath.Join(cfg.DataDir, id))
				if err != nil {
					t.Fatal(err)
				}
				edit := fmt.Sprintf("edit-%d", i)
				writeGateFile(t, tree.Path(), gateFile, strings.Replace(gateContents, line, edit, 1))
				if err := d.CommitWorktree(t.Context(), tree, edit); err != nil {
					t.Fatal(err)
				}
				gateAcceptedWorktree(t, d, base, id, fixtureGit(t, tree.Path(), "rev-parse", "HEAD"), starts)
				worktrees[i] = tree
			}
			for i, tree := range worktrees {
				_, err := d.Integrate(t.Context(), tree, target, nil)
				if conflict && i == 2 {
					var reported *gitops.ConflictError
					raw, readErr := os.ReadFile(filepath.Join(tree.Path(), gateFile))
					if !errors.As(err, &reported) || len(reported.Paths) != 1 || reported.Paths[0] != gateFile ||
						readErr != nil || !strings.Contains(string(raw), "<<<<<<<") ||
						!strings.Contains(string(raw), "edit-0") || !strings.Contains(string(raw), "edit-2") {
						t.Fatalf("preserved conflict: %v %v %s", err, readErr, raw)
					}
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			got := fixtureGit(t, remote, "show", "main:"+gateFile)
			last := "edit-2"
			if conflict {
				last = "gamma"
			}
			want := strings.NewReplacer("alpha", "edit-0", "beta", "edit-1", "gamma", last).Replace(gateContents)
			if got != strings.TrimSpace(want) {
				t.Fatalf("remote file = %q, want %q", got, want)
			}
		})
	}
}

// TestGateStaleGitApproval proves D-24 denies stale approval evidence via AC6.
func TestGateStaleGitApproval(t *testing.T) {
	cfg, _ := gateConfig(t)
	configureGitGate(t, &cfg, true)
	d, base := serveGate(t, cfg)
	repo, remote := fixtureRepository(t)
	project, err := d.RegisterProject(t.Context(), repo, "main", "CONVENTIONS.md")
	if err != nil {
		t.Fatal(err)
	}
	parent := gateThread(t, base, "stale")
	id, err := d.CreateWritingThread(t.Context(), parent)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := d.CreateWorktree(t.Context(), project, id, filepath.Join(cfg.DataDir, id))
	if err != nil {
		t.Fatal(err)
	}
	remoteHead := fixtureGit(t, remote, "rev-parse", "HEAD")
	writeGateFile(t, tree.Path(), gateFile, "candidate\n")
	if err := d.CommitWorktree(t.Context(), tree, "candidate"); err != nil {
		t.Fatal(err)
	}
	head := fixtureGit(t, tree.Path(), "rev-parse", "HEAD")
	// Measure the stale HEAD case against the same clean worktree digest.
	digest, err := d.WorktreeDigest(t.Context(), tree)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, revision, digest string }{
		{"old-head", remoteHead, digest},
		{"wrong-digest", head, "sha256:" + strings.Repeat("e", 64)},
	} {
		target := project.FetchURL + ":refs/heads/main"
		body := fmt.Sprintf(`{"revision":%q,"worktree_digest":%q,"target":%q,"actions":["merge"],"expires_at":%q}`,
			test.revision, test.digest, target, time.Now().UTC().Add(15*time.Minute).Format(time.RFC3339))
		receipt := gateMutation(t, base+"/v1/threads/"+id+"/approvals", test.name, body, 201)
		approval := receipt["result"].(map[string]any)["approval_id"].(string)
		_, err := d.Integrate(t.Context(), tree, gitops.RemoteTarget{Remote: "origin", Branch: "main"}, map[gitops.Action]string{gitops.Merge: approval})
		if !errors.Is(err, gitops.ErrDenied) || !strings.Contains(err.Error(), "diff_changed") {
			t.Fatalf("%s: expected stale approval, got %v", test.name, err)
		}
	}
	if got := fixtureGit(t, remote, "rev-parse", "HEAD"); got != remoteHead {
		t.Fatalf("stale approval advanced target: %s", got)
	}
}
