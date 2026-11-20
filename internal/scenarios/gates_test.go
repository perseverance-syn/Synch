package scenarios

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"synch/internal/gates"
)

// checkout creates a local committed fixture for revision-bound gate evidence.
func checkout(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %s %v", args, out, err)
		}
	}
	run("init")
	run("-c", "user.name=Scenario", "-c", "user.email=scenario@local", "commit", "--allow-empty", "-m", "fixture")
	return dir
}

// fixtureExecutor runs local gate commands in the fixture checkout.
type fixtureExecutor string

// Run applies the checkout working directory to the real local executor.
func (dir fixtureExecutor) Run(ctx context.Context, check gates.Check) (int, string, error) {
	check.Workdir = string(dir)
	return (gates.ExecExecutor{}).Run(ctx, check)
}

// TestPlantedMissingRequirementIsVisible verifies a blocked check remains explicitly not run.
func TestPlantedMissingRequirementIsVisible(t *testing.T) {
	dir := checkout(t)
	data := []byte(`{"version":1,"checks":[{"id":"failure","command":["git","diff","--no-index","--exit-code","a","b"],"required":true},{"id":"planted-missing-requirement","command":["git","version"],"required":true}]}`)
	if err := os.WriteFile(filepath.Join(dir, "a"), []byte("a"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b"), []byte("b"), 0600); err != nil {
		t.Fatal(err)
	}
	results, err := gates.NewRunner(fixtureExecutor(dir)).Run(t.Context(), data, gates.RunInput{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].Status != gates.StatusFailed || results[1].Check != "planted-missing-requirement" || results[1].Status != gates.StatusNotRun || results[1].Reason == "" {
		t.Fatalf("results: %+v", results)
	}
	t.Log("partial: local gate not-run evidence shown; daemon requirement presentation not exercised")
}

// TestFailedStaticCIGateThroughFixRerunAuditAndAcceptance checks revision-bound local gate reruns.
func TestFailedStaticCIGateThroughFixRerunAuditAndAcceptance(t *testing.T) {
	dir := checkout(t)
	a, b := filepath.Join(dir, "a"), filepath.Join(dir, "b")
	if err := os.WriteFile(a, []byte("a"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte("b"), 0600); err != nil {
		t.Fatal(err)
	}
	commitFiles(t, dir, "broken")
	data := []byte(`{"version":1,"checks":[{"id":"static","command":["git","diff","--no-index","--exit-code","a","b"],"required":true}]}`)
	runner := gates.NewRunner(fixtureExecutor(dir))
	before, err := runner.Run(t.Context(), data, gates.RunInput{Dir: dir})
	if err != nil || len(before) != 1 || before[0].Status != gates.StatusFailed {
		t.Fatalf("before: %+v %v", before, err)
	}
	if err := os.WriteFile(b, []byte("a"), 0600); err != nil {
		t.Fatal(err)
	}
	commitFiles(t, dir, "fix")
	after, err := runner.Run(t.Context(), data, gates.RunInput{Dir: dir, Stored: map[string]gates.Result{"static": before[0]}})
	if err != nil || len(after) != 1 || after[0].Status != gates.StatusPassed || after[0].Revision == before[0].Revision || strings.TrimSpace(after[0].Revision) == "" {
		t.Fatalf("after: %+v %v", after, err)
	}
	t.Log("partial: local gate rerun passed; CI mocks, audit and human acceptance not exercised")
}

// commitFiles records fixture content so the tested revisions include the change.
func commitFiles(t *testing.T, dir, message string) {
	t.Helper()
	for _, args := range [][]string{{"add", "a", "b"}, {"-c", "user.name=Scenario", "-c", "user.email=scenario@local", "commit", "-m", message}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %s %v", args, out, err)
		}
	}
}
