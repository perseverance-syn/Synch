package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"synch/internal/api"
	"synch/internal/vmdriver"
)

// fakeVM verifies one snapshot name and embeds the interface so nothing else is callable.
type fakeVM struct {
	vmdriver.VMDriver
	good string
}

func (f fakeVM) VerifySnapshot(_ context.Context, endpoint, vm, name string) error {
	if endpoint == "lab" && vm == "100" && name == f.good {
		return nil
	}
	return vmdriver.ErrSnapshot
}

// TestDiffEndpointRetainsFullDiff covers A2 through the daemon: base and revision from Git,
// the capped patch inline, and the full diff retained under a searchable reference.
func TestDiffEndpointRetainsFullDiff(t *testing.T) {
	cfg, _ := gateConfig(t)
	cfg.OutputCap = 1024
	configureGitGate(t, &cfg, false)
	d, base := serveGate(t, cfg)
	repo, _ := fixtureRepository(t)
	project, err := d.RegisterProject(t.Context(), repo, "main", "CONVENTIONS.md")
	if err != nil {
		t.Fatal(err)
	}
	parent := gateThread(t, base, "diff-parent")
	id, err := d.CreateWritingThread(t.Context(), parent)
	if err != nil {
		t.Fatal(err)
	}
	get := func(thread string) (int, []byte) {
		res := gateHTTP(t, "GET", base+"/v1/threads/"+thread+"/diff", "", "", "")
		raw, _ := io.ReadAll(res.Body)
		return res.StatusCode, raw
	}
	if code, _ := get(id); code != 404 {
		t.Fatalf("no worktree: %d", code)
	}
	tree, err := d.CreateWorktree(t.Context(), project, id, filepath.Join(cfg.DataDir, id))
	if err != nil {
		t.Fatal(err)
	}
	writeGateFile(t, tree.Path(), gateFile, strings.Replace(gateContents, "alpha", "committed", 1))
	if err := d.CommitWorktree(t.Context(), tree, "first"); err != nil {
		t.Fatal(err)
	}
	// An uncommitted edit large enough to exceed the output cap.
	writeGateFile(t, tree.Path(), gateFile, strings.Replace(gateContents, "alpha", "committed", 1)+strings.Repeat("uncommitted line\n", 400))
	code, raw := get(id)
	if code != 200 {
		t.Fatalf("diff: %d %s", code, raw)
	}
	var out api.Diff
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if out.Base != fixtureGit(t, repo, "rev-parse", "main") || out.Revision != fixtureGit(t, tree.Path(), "rev-parse", "HEAD") || out.Base == out.Revision {
		t.Fatalf("base %s revision %s", out.Base, out.Revision)
	}
	if !out.Truncated || len(out.Patch) > 1024 || out.Bytes <= 1024 || out.Ref == "" || !strings.HasPrefix(out.Digest, "sha256:") || !strings.Contains(out.Patch, "committed") {
		t.Fatalf("diff cap: truncated=%v patch=%d bytes=%d ref=%q", out.Truncated, len(out.Patch), out.Bytes, out.Ref)
	}
	matches, err := d.SearchOutput(out.Ref, "uncommitted line", 5)
	if err != nil || len(matches) != 5 {
		t.Fatalf("search retained diff: %v %v", matches, err)
	}
	if code, _ := get("01JD3Q8Z7Y6X5W4V3T2S1R0QPN"); code != 404 {
		t.Fatalf("unknown thread: %d", code)
	}
}

// TestCheckpointVerificationAtApproval covers A7 through the daemon: the approval is admitted
// only when the owning VM driver verifies the snapshot and the thread owns the VM.
func TestCheckpointVerificationAtApproval(t *testing.T) {
	cfg, _ := gateConfig(t)
	cfg.VMDrivers = map[string]vmdriver.VMDriver{"fake": fakeVM{good: "snap"}}
	owned := true
	cfg.VMOwns = func(string, string) (bool, error) { return owned, nil }
	d, base := serveGate(t, cfg)
	id := gateThread(t, base, "ckpt")
	ref := func(name string) api.CheckpointRef { return api.CheckpointRef{Driver: "fake", Endpoint: "lab", VM: "100", Snapshot: name} }
	if err := d.verifyCheckpoint(t.Context(), id, ref("snap")); err != nil {
		t.Fatalf("verified snapshot refused: %v", err)
	}
	if err := d.verifyCheckpoint(t.Context(), id, ref("other")); !errors.Is(err, vmdriver.ErrSnapshot) {
		t.Fatalf("unverified snapshot admitted: %v", err)
	}
	if err := d.verifyCheckpoint(t.Context(), id, api.CheckpointRef{Driver: "none", Endpoint: "lab", VM: "100", Snapshot: "snap"}); err == nil {
		t.Fatal("unknown driver admitted")
	}
	owned = false
	if err := d.verifyCheckpoint(t.Context(), id, ref("snap")); !errors.Is(err, vmdriver.ErrSnapshot) {
		t.Fatalf("unowned VM admitted: %v", err)
	}
	// Over HTTP an unverified checkpoint is refused before policy runs, as a stored 403.
	sha := "sha256:" + strings.Repeat("a", 64)
	body := `{"plan_digest":"` + sha + `","worktree_digest":"` + sha + `","target":"prod","actions":["deploy.production"],"rollback":"vmsnap:fake/lab/100/other","irreversible_effects":[],"expires_at":"2099-01-01T00:00:00Z"}`
	res := gateHTTP(t, "POST", base+"/v1/threads/"+id+"/approvals", "ckpt-bad", body, "")
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode != 403 || !strings.Contains(string(raw), "checkpoint_unverified") {
		t.Fatalf("approval with unverified checkpoint: %d %s", res.StatusCode, raw)
	}
}

// TestOutputSearchOwnership proves a thread searches references of itself and its children only,
// and only gets capped excerpts.
func TestOutputSearchOwnership(t *testing.T) {
	cfg, _ := gateConfig(t)
	d, base := serveGate(t, cfg)
	parent, other := gateThread(t, base, "own-parent"), gateThread(t, base, "own-other")
	child, err := d.CreateWritingThread(t.Context(), parent)
	if err != nil {
		t.Fatal(err)
	}
	mine, err := d.CapOutputFor(child, strings.NewReader(strings.Repeat("filler line\n", 50)+"needle here\n"))
	if err != nil {
		t.Fatal(err)
	}
	search := func(thread, ref string) (int, string) {
		res := gateHTTP(t, "GET", base+"/v1/outputs/"+ref+"/search?thread="+thread+"&pattern=needle", "", "", "")
		raw, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(raw)
	}
	for _, thread := range []string{child, parent} {
		if code, body := search(thread, mine.Ref); code != 200 || !strings.Contains(body, "needle here") || strings.Contains(body, "filler") {
			t.Fatalf("%s: %d %s", thread, code, body)
		}
	}
	if code, _ := search(other, mine.Ref); code != 404 {
		t.Fatalf("unrelated thread read a child's output: %d", code)
	}
	// A child cannot read its parent's output, and unowned references are never searchable.
	parents, err := d.CapOutputFor(parent, strings.NewReader("needle\n"))
	if err != nil {
		t.Fatal(err)
	}
	unowned, err := d.CapOutput(strings.NewReader("needle\n"))
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := search(child, parents.Ref); code != 404 {
		t.Fatalf("child read its parent's output: %d", code)
	}
	if code, _ := search(parent, unowned.Ref); code != 404 {
		t.Fatalf("unowned reference searchable: %d", code)
	}
}
