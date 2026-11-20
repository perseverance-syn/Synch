package context

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"synch/internal/engine"
	"synch/internal/thread"
)

// writeFile writes a fixture file, creating parent directories.
func writeFile(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

// pack builds a one-role pack fixture and returns its directory.
func pack(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "role-skills.json"), `{"version":1,"roles":{"builder":["skills/b/SKILL.md"],"escape":["../x.md"]}}`)
	writeFile(t, filepath.Join(dir, "skills", "b", "SKILL.md"), "builder rules\r\nrun tests\n")
	return dir
}

// TestGuidanceDeterministicStableFirst proves X1: role, project and target blocks load
// in that order with source and digest, are stable-first after assembly, and repeat byte for byte.
func TestGuidanceDeterministicStableFirst(t *testing.T) {
	dir := pack(t)
	proj, target := filepath.Join(t.TempDir(), "p.md"), filepath.Join(t.TempDir(), "t.md")
	writeFile(t, proj, "project conventions")
	writeFile(t, target, "target topology")
	in := Inputs{PackDir: dir, Role: "builder", ProjectFile: proj, TargetFile: target}
	blocks, err := Guidance(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 3 || blocks[0].Kind != "role" || blocks[1].Kind != "project" || blocks[2].Kind != "target" {
		t.Fatalf("unexpected blocks %+v", blocks)
	}
	for _, b := range blocks {
		if b.Source == "" || !strings.HasPrefix(b.Digest, "sha256:") || !b.Stable {
			t.Fatalf("block lacks source, digest or stability: %+v", b)
		}
	}
	if strings.Contains(blocks[0].Text, "\r") {
		t.Fatal("line endings are not normalized")
	}
	brief := engine.Instruction{Text: "brief"}
	first := Assemble(blocks, []engine.Instruction{brief, {Text: "late stable", Stable: true}})
	again, err := Guidance(in)
	if err != nil {
		t.Fatal(err)
	}
	second := Assemble(again, []engine.Instruction{brief, {Text: "late stable", Stable: true}})
	if fmt.Sprint(first) != fmt.Sprint(second) {
		t.Fatal("same inputs gave different instructions")
	}
	if len(first) != 5 || first[3].Text != "late stable" || first[4] != brief {
		t.Fatalf("stable instructions must precede changing ones: %+v", first)
	}
	for i, in := range first[:4] {
		if !in.Stable {
			t.Fatalf("instruction %d is not stable", i)
		}
	}
}

// TestGuidanceRefusals proves X1 failure paths: unknown role, escaping manifest path and missing configured file.
func TestGuidanceRefusals(t *testing.T) {
	dir := pack(t)
	for name, in := range map[string]Inputs{
		"unknown role": {PackDir: dir, Role: "auditor"},
		"escape":       {PackDir: dir, Role: "escape"},
		"missing file": {ProjectFile: filepath.Join(dir, "absent.md")},
	} {
		if _, err := Guidance(in); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if blocks, err := Guidance(Inputs{}); err != nil || len(blocks) != 0 {
		t.Fatalf("no inputs must load nothing: %v %v", blocks, err)
	}
}

// TestShippedRolePack proves X1 against the real role-skills pack for every role.
func TestShippedRolePack(t *testing.T) {
	root := filepath.Join("..", "..", "role-skills")
	for _, role := range []string{"coordinator", "builder", "ops", "security", "auditor"} {
		blocks, err := Guidance(Inputs{PackDir: root, Role: role})
		if err != nil || len(blocks) != 1 || blocks[0].Text == "" {
			t.Errorf("%s: %v %v", role, blocks, err)
		}
	}
}

// TestRetainHugeOutput proves X2: a 10 MiB output is retained in full on disk, the excerpt stays
// under the cap with head, tail and keyword lines, and Search finds the line by reference.
func TestRetainHugeOutput(t *testing.T) {
	store, err := NewStore(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	out.WriteString("HEAD-LINE\n")
	for i := 0; out.Len() < 10<<20; i++ {
		fmt.Fprintf(&out, "line %d filler filler filler filler\n", i)
		if i == 123456 {
			out.WriteString("ERROR: needle in the middle\n")
		}
	}
	out.WriteString("TAIL-LINE\n")
	total := out.Len()
	ex, err := store.Retain(bytes.NewReader(out.Bytes()), "error")
	if err != nil {
		t.Fatal(err)
	}
	if len(ex.Text) > DefaultCap || !ex.Truncated || ex.Bytes != int64(total) {
		t.Fatalf("excerpt %d bytes, truncated %v, retained %d of %d", len(ex.Text), ex.Truncated, ex.Bytes, total)
	}
	for _, want := range []string{"HEAD-LINE", "TAIL-LINE", "ERROR: needle in the middle", ex.Ref} {
		if !strings.Contains(ex.Text, want) {
			t.Errorf("excerpt lacks %q", want)
		}
	}
	info, err := os.Stat(store.path(ex.Ref))
	if err != nil || info.Size() != int64(total) {
		t.Fatalf("full output not retained: %v %v", info, err)
	}
	matches, err := store.Search(ex.Ref, `needle in the (middle|end)`, 5)
	if err != nil || len(matches) != 1 || !strings.Contains(matches[0].Text, "needle") {
		t.Fatalf("search: %v %v", matches, err)
	}
	if matches, err = store.Search(ex.Ref, "filler", 3); err != nil || len(matches) != 3 {
		t.Fatalf("search limit: %v %v", matches, err)
	}
}

// TestRetainSmallAndCapConfig proves X2: output within the cap comes back whole, the cap is configurable, and bad references are refused.
func TestRetainSmallAndCapConfig(t *testing.T) {
	store, err := NewStore(t.TempDir(), 2048)
	if err != nil {
		t.Fatal(err)
	}
	ex, err := store.Retain(strings.NewReader("short output\n"))
	if err != nil || ex.Truncated || ex.Text != "short output\n" || ex.Ref == "" {
		t.Fatalf("small output: %+v %v", ex, err)
	}
	big, err := store.Retain(strings.NewReader(strings.Repeat("x", 100000)))
	if err != nil || len(big.Text) > 2048 {
		t.Fatalf("configured cap ignored: %d %v", len(big.Text), err)
	}
	for _, ref := range []string{"../../etc/passwd", "out_xyz", ""} {
		if _, err := store.Search(ref, "x", 1); err == nil {
			t.Errorf("reference %q accepted", ref)
		}
	}
	if _, err := store.Search(ex.Ref, "(", 1); err == nil {
		t.Error("invalid pattern accepted")
	}
	if _, err := NewStore(t.TempDir(), 10); err == nil {
		t.Error("tiny cap accepted")
	}
}

// TestHandoffRoundTrip proves X3 storage: the handoff carries the spec list, is content addressed,
// renders as a stable block and refuses tampering or another thread.
func TestHandoffRoundTrip(t *testing.T) {
	var h Handoff
	h.ThreadID, h.BriefVersion, h.Revision = "thr_1", 3, strings.Repeat("a", 40)
	h.Plan.Revision, h.Plan.Digest = 2, "sha256:plan"
	h.PendingEdits.Summary = " M main.go | 2 +-"
	h.FailedAttempts = []thread.FailedAttempt{{Assignment: "a1", Harness: "codex", Model: "m", Reason: "stalled"}}
	h.OpenDecisions = []thread.Decision{{ID: "d1", Kind: "approval", Summary: "push?"}}
	h.Uncertain = []string{"req_9"}
	dir := t.TempDir()
	path, sum, err := WriteHandoff(dir, h)
	if err != nil || !IsHandoffFile(path) {
		t.Fatalf("write: %q %v", path, err)
	}
	if again, _, err := WriteHandoff(dir, h); err != nil || again != path {
		t.Fatalf("not idempotent: %q %v", again, err)
	}
	got, gotSum, err := ReadHandoff(path, "thr_1")
	if err != nil || gotSum != sum || got.BriefVersion != 3 || got.Uncertain[0] != "req_9" || got.FailedAttempts[0].Reason != "stalled" {
		t.Fatalf("read: %+v %v", got, err)
	}
	block := got.Block(path, sum, []string{path})
	if !block.Stable || block.Digest != sum || !strings.Contains(block.Text, "req_9") || !strings.Contains(block.Text, "sha256:plan") {
		t.Fatalf("block: %+v", block)
	}
	if _, _, err := ReadHandoff(path, "thr_2"); err == nil {
		t.Error("another thread's handoff accepted")
	}
	writeFile(t, path, "{}")
	if _, _, err := ReadHandoff(path, "thr_1"); err == nil {
		t.Error("tampered handoff accepted")
	}
	if _, _, err := ReadHandoff(filepath.Join(dir, "resume.txt"), "thr_1"); err == nil {
		t.Error("non-handoff name accepted")
	}
}
