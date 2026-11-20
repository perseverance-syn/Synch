package daemon

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	sctx "synch/internal/context"
	"synch/internal/engine"
	"synch/internal/eventlog"
	"synch/internal/thread"
)

// secondAdapter is the fake under another harness name, so a pin change can move the thread to it.
type secondAdapter struct{ *fakeAdapter }

// Capabilities declares the same qualified operations under the name "fake2".
func (secondAdapter) Capabilities(context.Context) (engine.CapabilitySet, error) {
	caps := fakeCaps()
	caps.Adapter = "fake2"
	return caps, nil
}

// handoffConfig configures guidance (real role pack, one project file) over the gate fixture.
func handoffConfig(t *testing.T, worktree string) (Config, <-chan *fakeSession) {
	t.Helper()
	cfg, starts := loopConfig(t, worktree)
	cfg.noLoop = true
	guideDir := t.TempDir()
	writeGateFile(t, guideDir, "GUIDE.md", "project conventions: run make test\n")
	cfg.RoleSkillsDir = filepath.Join("..", "..", "role-skills")
	cfg.Projects = map[string]ProjectConfig{"local": {Dir: worktree, GuidanceFile: filepath.Join(guideDir, "GUIDE.md")}}
	return cfg, starts
}

// kinds lists the "[synch:<kind> ..." tags of the instructions in order and whether every
// stable instruction precedes every changing one.
func kinds(instructions []engine.Instruction) (tags []string, stableFirst bool) {
	stableFirst = true
	seenChanging := false
	for _, in := range instructions {
		if !in.Stable {
			seenChanging = true
		} else if seenChanging {
			stableFirst = false
		}
		if rest, ok := strings.CutPrefix(in.Text, "[synch:"); ok {
			tags = append(tags, rest[:strings.IndexAny(rest, " ]")])
		}
	}
	return tags, stableFirst
}

// storedCheckpoint reads the checkpoint file the thread's checkpoint reference names.
func storedCheckpoint(t *testing.T, d *Daemon, id string) thread.Checkpoint {
	t.Helper()
	state, err := d.threads.Status(t.Context(), id)
	if err != nil || state.Checkpoint.URI == "" {
		t.Fatalf("no checkpoint reference: %+v %v", state, err)
	}
	u, err := url.Parse(state.Checkpoint.URI)
	if err != nil {
		t.Fatal(err)
	}
	path := u.Path
	if runtime.GOOS == "windows" {
		path = strings.TrimPrefix(path, "/")
	}
	raw, err := os.ReadFile(filepath.FromSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	var cp thread.Checkpoint
	if err := json.Unmarshal(raw, &cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

// TestSessionGuidanceAndRolloverHandoff proves X1, X3 and X4: a Builder child starts with role and
// project guidance stable-first; a forced rollover writes the handoff in the worktree's git
// metadata (not the project's history), records it in the checkpoint, and the new session starts with it.
func TestSessionGuidanceAndRolloverHandoff(t *testing.T) {
	tree := gateWorktree(t)
	if err := os.WriteFile(filepath.Join(tree, "README.md"), []byte("work\npending edit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, starts := handoffConfig(t, tree)
	d, base := serveGate(t, cfg)
	id := childOf(t, d, gateThread(t, base, "handoff"))
	req := engine.StartRequest{ThreadID: id, Workdir: tree, Instructions: []engine.Instruction{{Text: "the brief"}}}
	if err := d.StartAssignment(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	first := <-starts
	tags, stableFirst := kinds(first.req.Instructions)
	if strings.Join(tags, ",") != "role,project" || !stableFirst || first.req.Instructions[2].Text != "the brief" {
		t.Fatalf("fresh session instructions: tags %v, stable-first %v, %+v", tags, stableFirst, first.req.Instructions)
	}
	if !strings.Contains(first.req.Instructions[0].Text, "digest=sha256:") || !strings.Contains(first.req.Instructions[1].Text, "make test") {
		t.Fatal("blocks lack their digest or project text")
	}

	cp := thread.Checkpoint{ThreadID: id, Revision: strings.Repeat("a", 40),
		FailedAttempts: []thread.FailedAttempt{{Assignment: "old", Harness: "fake", Model: "m1", Reason: "context full"}},
		OpenDecisions:  []thread.Decision{{ID: "d1", Kind: "approval", Summary: "push the branch?"}}}
	if err := d.Rollover(t.Context(), cp, req); err != nil {
		t.Fatal(err)
	}
	second := <-starts
	tags, stableFirst = kinds(second.req.Instructions)
	if strings.Join(tags, ",") != "role,project,handoff" || !stableFirst {
		t.Fatalf("continued session instructions: tags %v, stable-first %v", tags, stableFirst)
	}
	handoff := second.req.Instructions[2].Text
	for _, want := range []string{"context full", "push the branch?", "README.md", "brief_version", "uncertain_external_request_ids"} {
		if !strings.Contains(handoff, want) {
			t.Errorf("handoff lacks %q:\n%s", want, handoff)
		}
	}

	// The handoff lives in the worktree's git metadata, so git sees nothing new to commit.
	gitDir := fixtureGit(t, tree, "rev-parse", "--absolute-git-dir")
	files, err := filepath.Glob(filepath.Join(filepath.FromSlash(gitDir), "synch", "handoff-*.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("handoff files %v, %v", files, err)
	}
	if status := fixtureGit(t, tree, "status", "--porcelain"); strings.Contains(status, "handoff") || strings.Contains(status, ".diff") {
		t.Fatalf("handoff leaked into the work tree:\n%s", status)
	}

	// rollover.checkpointed references a checkpoint file that lists the handoff and the pending diff.
	if published := eventsOf(t, d, id, "rollover.checkpointed"); len(published) != 1 {
		t.Fatalf("rollover.checkpointed events: %v", published)
	}
	stored := storedCheckpoint(t, d, id)
	if len(stored.Handoff) != 1 || stored.Handoff[0] != files[0] || stored.PendingEdits.Diff.Digest == "" {
		t.Fatalf("checkpoint does not record the handoff: %+v", stored)
	}
	if _, sum, err := sctx.ReadHandoff(files[0], id); err != nil || !strings.HasSuffix(files[0], strings.TrimPrefix(sum, "sha256:")+".json") {
		t.Fatalf("handoff digest: %v", err)
	}
}

// TestHarnessSwitchHandoff proves X3: changing the pin and switching moves the thread to the other
// adapter, which receives the same handoff as stable instructions.
func TestHarnessSwitchHandoff(t *testing.T) {
	tree := gateWorktree(t)
	cfg, starts := handoffConfig(t, tree)
	d, base := serveGate(t, cfg)
	other := make(chan *fakeSession, 4)
	d.harnesses = append(d.harnesses, &harness{name: "fake2", version: "1.0.0",
		adapter: secondAdapter{&fakeAdapter{sink: engineSink{daemon: d}, next: other}}})
	parent := gateThread(t, base, "switch")
	id := childOf(t, d, parent)
	req := engine.StartRequest{ThreadID: id, Workdir: tree}
	if err := d.StartAssignment(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	<-starts
	err := d.store.Write(t.Context(), func(tx *eventlog.Transaction) error {
		_, err := tx.ExecContext(t.Context(), "UPDATE api_threads SET harness='fake2', model='m1' WHERE id=?", parent)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.SwitchHarness(t.Context(), thread.Checkpoint{ThreadID: id, Revision: strings.Repeat("b", 40)}, req); err != nil {
		t.Fatal(err)
	}
	moved := <-other
	tags, stableFirst := kinds(moved.req.Instructions)
	if strings.Join(tags, ",") != "role,project,handoff" || !stableFirst {
		t.Fatalf("switched session instructions: tags %v, stable-first %v", tags, stableFirst)
	}
	if got := eventsOf(t, d, id, "rollover.checkpointed"); len(got) != 1 || got[0]["trigger"] != "harness_switch" {
		t.Fatalf("checkpoint trigger: %v", got)
	}
	stored := storedCheckpoint(t, d, id)
	if len(stored.Handoff) != 1 || !strings.Contains(moved.req.Instructions[2].Text, stored.Handoff[0]) {
		t.Fatalf("the other adapter did not receive the checkpoint's handoff: %+v", stored.Handoff)
	}
}

// TestCoordinatorGetsNoToolGuidance proves X4: a top-level thread's request carries no stable guidance.
func TestCoordinatorGetsNoToolGuidance(t *testing.T) {
	cfg, _ := handoffConfig(t, gateWorktree(t))
	d, base := serveGate(t, cfg)
	req, err := d.requestFor(t.Context(), gateThread(t, base, "coordinator"))
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range req.Instructions {
		if in.Stable || strings.HasPrefix(in.Text, "[synch:") {
			t.Fatalf("coordinator instruction carries guidance: %+v", in)
		}
	}
}

// TestCapOutputRetainsAndSearches proves X2 through the daemon: output is capped by Config.OutputCap, kept under the data directory, and searchable.
func TestCapOutputRetainsAndSearches(t *testing.T) {
	cfg, _ := handoffConfig(t, t.TempDir())
	cfg.OutputCap = 4096
	d, _ := serveGate(t, cfg)
	noise := strings.Repeat("noise line\n", 50000)
	ex, err := d.CapOutput(strings.NewReader(noise+"FAIL: TestX\n"+noise), "fail")
	if err != nil || len(ex.Text) > 4096 || !ex.Truncated {
		t.Fatalf("cap: %d bytes, %v", len(ex.Text), err)
	}
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "outputs", ex.Ref+".out")); err != nil {
		t.Fatalf("output not retained under the data directory: %v", err)
	}
	if hits, err := d.SearchOutput(ex.Ref, "FAIL", 0); err != nil || len(hits) != 1 {
		t.Fatalf("search: %v %v", hits, err)
	}
}
