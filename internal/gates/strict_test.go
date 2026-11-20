package gates

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- K1: whole-report validation before any pass ---

// TestK1ReportsAreValidatedWhole proves K1: trailing garbage after a JUnit
// root, a go-test stream cut before its package outcome, an unknown go-test
// action, JSON null for lint, and a wrong SARIF version are never a pass.
func TestK1ReportsAreValidatedWhole(t *testing.T) {
	const pkgPass = "{\"Action\":\"pass\",\"Package\":\"p\"}\n"
	const testPass = "{\"Action\":\"pass\",\"Package\":\"p\",\"Test\":\"T\"}\n"
	cases := []struct {
		name   string
		format ReportFormat
		data   string
		want   Status
	}{
		{"junit trailing garbage", FormatJUnitXML, `<testsuite name="s"><testcase name="a"/></testsuite><broken`, StatusError},
		{"junit trailing element", FormatJUnitXML, `<testsuite name="s"><testcase name="a"/></testsuite><x/>`, StatusError},
		{"go-test cut before package outcome", FormatGoTestJSON, testPass, StatusPartial},
		{"go-test unknown action", FormatGoTestJSON, testPass + "{\"Action\":\"mystery\",\"Package\":\"p\"}\n" + pkgPass, StatusError},
		{"lint null", FormatGolangciLint, "null", StatusError},
		{"lint object with null Issues", FormatGolangciLint, `{"Issues":null}`, StatusError},
		{"sarif wrong version", FormatSarif, `{"version":"99.0.0","runs":[]}`, StatusError},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			check := Check{ID: "c", Command: []string{"t"}, Workdir: dir, Report: &ReportSpec{Path: "r.out", Format: c.format}}
			runner := newTestRunner(&reportWriter{inner: &scriptedExecutor{}, path: filepath.Join(dir, "r.out"), data: []byte(c.data)})
			result := runner.runCheck(context.Background(), check, "rev-1", Digest(nil), nil)
			if result.Status != c.want || result.Reason == "" {
				t.Fatalf("status = %s (%q), want %s", result.Status, result.Reason, c.want)
			}
		})
	}
}

// TestK1OversizedReportIsError proves K1: a report over the size cap is an
// error even when the padded content would otherwise be a clean report.
func TestK1OversizedReportIsError(t *testing.T) {
	dir := t.TempDir()
	padded := "[]" + strings.Repeat(" ", maxReportBytes+1)
	check := Check{ID: "c", Command: []string{"t"}, Workdir: dir, Report: &ReportSpec{Path: "r.out", Format: FormatGolangciLint}}
	runner := newTestRunner(&reportWriter{inner: &scriptedExecutor{}, path: filepath.Join(dir, "r.out"), data: []byte(padded)})
	result := runner.runCheck(context.Background(), check, "rev-1", Digest(nil), nil)
	if result.Status != StatusError || !strings.Contains(result.Reason, "limit") {
		t.Fatalf("oversized report = %+v", result)
	}
}

// --- K2: golangci-lint v2.15.0 real output and the documented examples ---

// TestK2GolangciLintRealOutput proves K2 with files golangci-lint v2.15.0
// itself wrote: a clean run is passed with no items, a run with an issue is
// failed with one item.
func TestK2GolangciLintRealOutput(t *testing.T) {
	clean, err := os.ReadFile(filepath.Join("testdata", "golangci-v2.15.0-clean.json"))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseReport(FormatGolangciLint, clean)
	if err != nil || len(parsed.Items) != 0 || parsed.Unreadable != 0 {
		t.Fatalf("clean report = %+v, %v", parsed, err)
	}
	issues, err := os.ReadFile(filepath.Join("testdata", "golangci-v2.15.0-issues.json"))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err = parseReport(FormatGolangciLint, issues)
	if err != nil || len(parsed.Items) != 1 || parsed.Items[0].Status != StatusFailed {
		t.Fatalf("issues report = %+v, %v", parsed, err)
	}
	dir := t.TempDir()
	check := Check{ID: "lint", Command: []string{"t"}, Workdir: dir, Report: &ReportSpec{Path: "lint.json", Format: FormatGolangciLint}}
	for data, want := range map[string]Status{string(clean): StatusPassed, string(issues): StatusFailed} {
		runner := newTestRunner(&reportWriter{inner: &scriptedExecutor{}, path: filepath.Join(dir, "lint.json"), data: []byte(data)})
		if got := runner.runCheck(context.Background(), check, "rev-1", Digest(nil), nil).Status; got != want {
			t.Fatalf("status = %s, want %s", got, want)
		}
	}
}

// TestK2DocExamplesWriteTheirReports proves K2: the doc example uses the
// installed linter's JSON output flag and the Go example writes its report.
func TestK2DocExamplesWriteTheirReports(t *testing.T) {
	doc, err := os.ReadFile("doc.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(doc)
	for _, want := range []string{"--output.json.path", "> test-report.json"} {
		if !strings.Contains(text, want) {
			t.Fatalf("doc.go lacks %q", want)
		}
	}
	for _, banned := range []string{"--out-format", "--output-file-path"} {
		if strings.Contains(text, banned) {
			t.Fatalf("doc.go still uses %q", banned)
		}
	}
}

// --- K3: interrupts are not failures, and the process tree dies ---

// TestHelperProcess is the re-executed test binary the K3 tests launch; it does
// nothing unless GATES_HELPER names a mode.
func TestHelperProcess(t *testing.T) {
	switch os.Getenv("GATES_HELPER") {
	case "panic":
		panic("boom")
	case "sleep":
		time.Sleep(30 * time.Second)
	case "tree":
		child := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
		child.Env = append(os.Environ(), "GATES_HELPER=marker")
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if err := child.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Second)
	case "marker":
		time.Sleep(2 * time.Second)
		if err := os.WriteFile(os.Getenv("GATES_MARKER"), []byte("alive"), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

// helperCheck builds a check that re-executes this test binary in a mode.
func helperCheck(t *testing.T, mode string, timeout int64) Check {
	t.Helper()
	t.Setenv("GATES_HELPER", mode)
	return Check{ID: "h", Command: []string{os.Args[0], "-test.run=^TestHelperProcess$"}, Timeout: timeout}
}

// TestK3PanickingCommandIsError proves K3: a subprocess that panics is an
// error with a reason, not a failed check.
func TestK3PanickingCommandIsError(t *testing.T) {
	check := helperCheck(t, "panic", 60)
	result := newTestRunner(ExecExecutor{}).runCheck(context.Background(), check, "rev-1", Digest(nil), nil)
	if result.Status != StatusError || !strings.Contains(result.Reason, "panicked") {
		t.Fatalf("panic = %+v", result)
	}
}

// TestK3CancelledCommandIsError proves K3: a command killed by cancellation
// is an error with a reason, not a failed check.
func TestK3CancelledCommandIsError(t *testing.T) {
	check := helperCheck(t, "sleep", 60)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(500*time.Millisecond, cancel)
	result := newTestRunner(ExecExecutor{}).runCheck(ctx, check, "rev-1", Digest(nil), nil)
	if result.Status != StatusError || !strings.Contains(result.Reason, "interrupted") {
		t.Fatalf("cancel = %+v", result)
	}
}

// TestK3PreCancelledContextIsNotRun proves K3: a context cancelled before
// launch records not_run with a reason and starts nothing.
func TestK3PreCancelledContextIsNotRun(t *testing.T) {
	executor := &scriptedExecutor{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	results, err := newTestRunner(executor).Run(ctx, gateFile(t, Check{ID: "a", Command: []string{"a"}}), RunInput{Dir: "rev-1"})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != StatusNotRun || results[0].Reason == "" || len(executor.runs) != 0 {
		t.Fatalf("pre-cancelled = %+v, runs %v", results[0], executor.runs)
	}
}

// TestK3TimeoutKillsProcessTree proves K3: on timeout the runner returns within
// the timeout plus a small grace and a descendant never writes its marker.
func TestK3TimeoutKillsProcessTree(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "marker")
	t.Setenv("GATES_MARKER", marker)
	check := helperCheck(t, "tree", 1)
	start := time.Now()
	_, _, err := ExecExecutor{}.Run(context.Background(), check)
	elapsed := time.Since(start)
	var timeout *TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("err = %v, want TimeoutError", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Run took %s, want within the 1s timeout plus grace", elapsed)
	}
	time.Sleep(3 * time.Second)
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("descendant outlived the timeout and wrote its marker")
	}
}

// --- K4: evidence bound to the checkout and the invocation ---

// gitRepo creates a repository with one commit and returns its directory and
// the commit hash.
func gitRepo(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	for _, args := range [][]string{
		{"init", "-q"},
		{"-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "x"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	cmd := exec.Command("git", "rev-parse", "HEAD")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	return dir, strings.TrimSpace(string(out))
}

// TestK4RevisionComesFromTheCheckout proves K4: results carry the checkout's
// HEAD, and a directory that is not a repository is an error.
func TestK4RevisionComesFromTheCheckout(t *testing.T) {
	dir, head := gitRepo(t)
	results, err := NewRunner(&scriptedExecutor{}).Run(context.Background(), gateFile(t, Check{ID: "a", Command: []string{"a"}}), RunInput{Dir: dir})
	if err != nil || results[0].Revision != head {
		t.Fatalf("revision = %q, %v, want %q", results[0].Revision, err, head)
	}
	executor := &scriptedExecutor{}
	_, err = NewRunner(executor).Run(context.Background(), gateFile(t, Check{ID: "a", Command: []string{"a"}}), RunInput{Dir: t.TempDir()})
	if err == nil || len(executor.runs) != 0 {
		t.Fatalf("non-repository: err = %v, runs %v", err, executor.runs)
	}
}

// TestK4PreExistingReportIsNotCredited proves K4: a clean report left by an
// earlier run is deleted before launch, so a command that writes nothing is an
// error rather than a pass.
func TestK4PreExistingReportIsNotCredited(t *testing.T) {
	dir := t.TempDir()
	report := filepath.Join(dir, "r.xml")
	if err := os.WriteFile(report, []byte(`<testsuite name="s"><testcase name="a"/></testsuite>`), 0600); err != nil {
		t.Fatal(err)
	}
	check := Check{ID: "c", Command: []string{"t"}, Workdir: dir, Report: &ReportSpec{Path: "r.xml", Format: FormatJUnitXML}}
	result := newTestRunner(&scriptedExecutor{}).runCheck(context.Background(), check, "rev-1", Digest(nil), nil)
	if result.Status != StatusError || !strings.Contains(result.Reason, "could not be read") {
		t.Fatalf("stale report = %+v", result)
	}
}

// --- K5: reused records are validated and obey the stop rule ---

// TestK5ReusedRequiredFailureStopsLaterChecks proves K5: a reused failed
// required record stops the next check, which is not_run with the skip reason.
func TestK5ReusedRequiredFailureStopsLaterChecks(t *testing.T) {
	file := gateFile(t, Check{ID: "a", Command: []string{"a"}, Required: true}, Check{ID: "b", Command: []string{"b"}})
	first, err := newTestRunner(&scriptedExecutor{code: 1}).Run(context.Background(), file, RunInput{Dir: "rev-1"})
	if err != nil {
		t.Fatal(err)
	}
	executor := &scriptedExecutor{}
	results, err := newTestRunner(executor).Run(context.Background(), file, RunInput{Dir: "rev-1", Stored: map[string]Result{"a": first[0]}})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != StatusFailed || results[1].Status != StatusNotRun || results[1].Reason != skipReason || len(executor.runs) != 0 {
		t.Fatalf("results = %+v, runs %v", results, executor.runs)
	}
}

// TestK5InvalidStoredRecordsAreNotReused proves K5: a cached record for another
// check, with an unknown status, or without a reason reruns the check.
func TestK5InvalidStoredRecordsAreNotReused(t *testing.T) {
	file := gateFile(t, Check{ID: "a", Command: []string{"a"}})
	digest := Digest(file)
	base := Result{Check: "a", Status: StatusError, Reason: "r", Revision: "rev-1", ConfigDigest: digest}
	wrongID, noReason, badStatus := base, base, base
	wrongID.Check = "other"
	noReason.Reason = ""
	badStatus.Status = "bogus"
	for name, stored := range map[string]Result{"wrong id": wrongID, "no reason": noReason, "bad status": badStatus} {
		executor := &scriptedExecutor{}
		results, err := newTestRunner(executor).Run(context.Background(), file, RunInput{Dir: "rev-1", Stored: map[string]Result{"a": stored}})
		if err != nil || len(executor.runs) != 1 || results[0].Check != "a" || results[0].Status != StatusPassed {
			t.Fatalf("%s: results %+v, runs %v, err %v", name, results, executor.runs, err)
		}
	}
}
