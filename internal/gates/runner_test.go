package gates

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// --- Executor stub: scripts one exit code, one error and recorded runs ---

// scriptedExecutor returns a fixed outcome for every run and records the
// commands it was asked to execute.
type scriptedExecutor struct {
	code  int
	err   error
	runs  []string
	attrs map[string]string // stdout per command's first element
}

// Run records the command and answers with the scripted outcome.
func (e *scriptedExecutor) Run(_ context.Context, check Check) (int, string, error) {
	e.runs = append(e.runs, strings.Join(check.Command, " "))
	if e.err != nil {
		return 0, "", e.err
	}
	return e.code, e.attrs[check.Command[0]], nil
}

// probeExecutor answers version probes with a fixed version and records the
// check commands separately from probe commands.
type probeExecutor struct {
	version string
	runs    []string
	probes  []string
}

// isProbe reports whether the argv is a version probe: two elements ending in
// --version.
func isProbe(argv []string) bool {
	return len(argv) == 2 && argv[1] == "--version"
}

// Run answers version probes with the version and records everything it ran.
func (e *probeExecutor) Run(_ context.Context, check Check) (int, string, error) {
	if isProbe(check.Command) {
		e.probes = append(e.probes, check.Command[0])
		return 0, e.version + "\n", nil
	}
	e.runs = append(e.runs, strings.Join(check.Command, " "))
	return 0, "", nil
}

// mapExecutor scripts a per-command outcome, keyed by the joined argv.
type mapExecutor struct {
	outcomes map[string]struct {
		code int
		err  error
	}
	runs []string
}

// Run records the command and answers with its scripted outcome.
func (e *mapExecutor) Run(_ context.Context, check Check) (int, string, error) {
	e.runs = append(e.runs, strings.Join(check.Command, " "))
	outcome := e.outcomes[strings.Join(check.Command, " ")]
	return outcome.code, "", outcome.err
}

// reportWriter wraps an executor and writes the report file when a check runs,
// the way a real command would: the runner deletes stale reports before launch.
type reportWriter struct {
	inner Executor
	path  string
	data  []byte
}

// Run writes the report, then answers with the wrapped executor.
func (e *reportWriter) Run(ctx context.Context, check Check) (int, string, error) {
	if e.data != nil {
		if err := os.WriteFile(e.path, e.data, 0600); err != nil {
			return 0, "", err
		}
	}
	return e.inner.Run(ctx, check)
}

// newTestRunner builds a runner whose revision is the RunInput Dir label, so
// tests can name revisions without creating git repositories.
func newTestRunner(executor Executor) *Runner {
	runner := NewRunner(executor)
	runner.head = func(_ context.Context, dir string) (string, error) { return dir, nil }
	return runner
}

// --- G2: ordered execution, skip reasons and tool versions ---

// TestRunnerRunsInOrderAndStopsOnRequiredFailure proves G2: checks run in
// gate-file order, a failed required check stops later checks, and the later
// records are not_run with the exact contract skip reason.
func TestRunnerRunsInOrderAndStopsOnRequiredFailure(t *testing.T) {
	executor := &scriptedExecutor{code: 1}
	runner := newTestRunner(executor)
	results, err := runner.Run(context.Background(), gateFile(t,
		Check{ID: "a", Command: []string{"a"}, Required: true},
		Check{ID: "b", Command: []string{"b"}},
		Check{ID: "c", Command: []string{"c"}, Required: true},
	), RunInput{Dir: "rev-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("results = %d, want 3", len(results))
	}
	if results[0].Status != StatusFailed || results[0].Check != "a" {
		t.Fatalf("first result = %+v", results[0])
	}
	for i, id := range []string{"b", "c"} {
		result := results[i+1]
		if result.Status != StatusNotRun || result.Reason != "skipped: earlier required check failed" {
			t.Fatalf("record for %s = %+v", id, result)
		}
	}
	if len(executor.runs) != 1 || !strings.Contains(executor.runs[0], "a") {
		t.Fatalf("executed runs = %v, want only check a", executor.runs)
	}
}

// TestRunnerContinuesAfterNonRequiredFailure proves G2: a failed check that
// is not required does not stop later checks.
func TestRunnerContinuesAfterNonRequiredFailure(t *testing.T) {
	executor := &scriptedExecutor{code: 1}
	runner := newTestRunner(executor)
	results, err := runner.Run(context.Background(), gateFile(t,
		Check{ID: "a", Command: []string{"a"}},
		Check{ID: "b", Command: []string{"b"}},
	), RunInput{Dir: "rev-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(executor.runs) != 2 {
		t.Fatalf("executed runs = %v, want both checks", executor.runs)
	}
	if results[1].Status != StatusFailed {
		t.Fatalf("second result = %+v", results[1])
	}
}

// TestRunnerRecordsToolVersions proves G2: a declared version command is
// probed and its output recorded, and a failing version probe makes the
// check's result an error.
func TestRunnerRecordsToolVersions(t *testing.T) {
	executor := &probeExecutor{version: "tool v2.3.4"}
	runner := newTestRunner(executor)
	results, err := runner.Run(context.Background(), gateFile(t, Check{
		ID:             "unit",
		Command:        []string{"tool", "test"},
		VersionCommand: []string{"tool", "--version"},
	}), RunInput{Dir: "rev-1"})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].ToolVersions["tool"] != "tool v2.3.4" {
		t.Fatalf("tool versions = %+v", results[0].ToolVersions)
	}
	if len(executor.probes) != 1 || len(executor.runs) != 1 {
		t.Fatalf("probes %v runs %v", executor.probes, executor.runs)
	}

	// A failing probe surfaces as an error with its reason.
	failing := &scriptedExecutor{err: errors.New("no such file")}
	results, err = newTestRunner(failing).Run(context.Background(), gateFile(t, Check{
		ID:             "unit",
		Command:        []string{"tool", "test"},
		VersionCommand: []string{"tool", "--version"},
	}), RunInput{Dir: "rev-1"})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != StatusError || !strings.Contains(results[0].Reason, "version command failed") {
		t.Fatalf("record = %+v", results[0])
	}
}

// TestRunnerRequiredErrorDoesNotStopLaterChecks proves the documented
// reading of G2: only a failed required check stops later checks; an errored
// required check (for example a timeout) still lets the remaining checks run.
func TestRunnerRequiredErrorDoesNotStopLaterChecks(t *testing.T) {
	executor := &mapExecutor{outcomes: map[string]struct {
		code int
		err  error
	}{
		"a": {0, &TimeoutError{Seconds: 3}},
		"b": {1, nil},
	}}
	runner := newTestRunner(executor)
	results, err := runner.Run(context.Background(), gateFile(t,
		Check{ID: "a", Command: []string{"a"}, Required: true},
		Check{ID: "b", Command: []string{"b"}},
	), RunInput{Dir: "rev-1"})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != StatusError || !strings.Contains(results[0].Reason, "timed out") {
		t.Fatalf("first result = %+v", results[0])
	}
	if results[1].Status != StatusFailed {
		t.Fatalf("second result = %+v", results[1])
	}
	if len(executor.runs) != 2 {
		t.Fatalf("executed runs = %v, want both checks", executor.runs)
	}
}

// --- G4: malformed and edge-case reports can never produce passed ---

// tableReport is one G4 fixture: a report body (or a missing file), the
// command exit code and executor error, the check definition, and the status
// the runner must produce. Every want is something other than passed.
type tableReport struct {
	name    string
	check   Check
	content string
	missing bool
	code    int
	execErr error
	want    Status
}

// buildTable assembles the shared fixtures across all four formats.
func buildTable() []tableReport {
	const goodJUnit = `<testsuite name="s" tests="1"><testcase name="ok"/></testsuite>`
	const failingJUnit = `<testsuite name="s" tests="1"><testcase name="bad"><failure message="boom"/></testcase></testsuite>`
	const goodGotest = "{\"Action\":\"pass\",\"Package\":\"pkg\",\"Test\":\"TestA\"}\n{\"Action\":\"pass\",\"Package\":\"pkg\"}\n"
	const goodGolangci = "[]"
	const goodSarif = "{\"version\":\"2.1.0\",\"runs\":[]}"
	junit := func(name, content string, code int, want Status) tableReport {
		return tableReport{name: name, check: rawCheck("c", string(FormatJUnitXML), ""), content: content, code: code, want: want}
	}
	gotest := func(name, content string, code int, want Status) tableReport {
		return tableReport{name: name, check: rawCheck("c", string(FormatGoTestJSON), ""), content: content, code: code, want: want}
	}
	golangci := func(name, content string, code int, want Status) tableReport {
		return tableReport{name: name, check: rawCheck("c", string(FormatGolangciLint), ""), content: content, code: code, want: want}
	}
	sarif := func(name, content string, code int, want Status) tableReport {
		return tableReport{name: name, check: rawCheck("c", string(FormatSarif), ""), content: content, code: code, want: want}
	}
	return []tableReport{
		// junit-xml: malformed documents are errors.
		junit("junit invalid xml", "<testsuite name=\"s\"><testcase name=\"a\"", 0, StatusError),
		junit("junit empty file", "", 0, StatusError),
		junit("junit wrong root", "<builder>x</builder>", 0, StatusError),
		junit("junit root missing name", "<testsuite tests=\"1\"><testcase name=\"a\"/></testsuite>", 0, StatusError),
		junit("junit no test cases", "<testsuite name=\"s\"/>", 0, StatusError),
		junit("junit all entries unnamed", "<testsuite name=\"s\"><testcase><failure message=\"x\"/></testcase></testsuite>", 0, StatusError),
		junit("junit some entries unnamed", "<testsuite name=\"s\"><testcase name=\"ok\"/><testcase><failure message=\"x\"/></testcase></testsuite>", 0, StatusPartial),
		junit("junit failing report exit zero", failingJUnit, 0, StatusFailed),
		junit("junit failing report exit nonzero", failingJUnit, 2, StatusFailed),
		junit("junit passing report exit nonzero", goodJUnit, 3, StatusFailed),
		{name: "junit missing report file", check: rawCheck("c", string(FormatJUnitXML), ""), missing: true, want: StatusError},
		{name: "junit timeout", check: rawCheck("c", string(FormatJUnitXML), ""), execErr: &TimeoutError{Seconds: 5}, want: StatusError},

		// go-test-json: undecodable lines are unreadable entries.
		gotest("gotest one bad line", goodGotest+"nope\n", 0, StatusPartial),
		gotest("gotest all lines bad", "nope\nnope\n", 0, StatusError),
		gotest("gotest empty file", "", 0, StatusError),
		gotest("gotest missing action", "{\"Package\":\"pkg\"}\n"+goodGotest, 0, StatusPartial),
		gotest("gotest missing package", "{\"Action\":\"pass\"}\n"+goodGotest, 0, StatusPartial),
		gotest("gotest truncated final line", goodGotest+"{\"Action\":\"pa", 0, StatusPartial),
		gotest("gotest package fail exit zero", "{\"Action\":\"fail\",\"Package\":\"pkg\"}\n", 0, StatusFailed),
		gotest("gotest test fail exit zero", "{\"Action\":\"fail\",\"Package\":\"pkg\",\"Test\":\"TestA\"}\n{\"Action\":\"fail\",\"Package\":\"pkg\"}\n", 0, StatusFailed),
		gotest("gotest passing report exit nonzero", goodGotest, 1, StatusFailed),
		{name: "gotest timeout", check: rawCheck("c", string(FormatGoTestJSON), ""), execErr: &TimeoutError{Seconds: 5}, want: StatusError},

		// golangci-lint-json: the root must be an array of issue objects.
		golangci("golangci invalid json", "{", 0, StatusError),
		golangci("golangci root not an array", "{\"issues\":[]}", 0, StatusError),
		golangci("golangci all elements not objects", "[1,2]", 0, StatusError),
		golangci("golangci some elements not objects", "[{\"FromLinter\":\"l\",\"Text\":\"t\"},5]", 0, StatusPartial),
		golangci("golangci element missing linter", "[{\"Text\":\"t\"},{\"FromLinter\":\"l\",\"Text\":\"t\"}]", 0, StatusPartial),
		golangci("golangci issues exit zero", "[{\"FromLinter\":\"l\",\"Text\":\"a\"},{\"FromLinter\":\"l\",\"Text\":\"b\"}]", 0, StatusFailed),
		golangci("golangci passing report exit nonzero", goodGolangci, 1, StatusFailed),
		{name: "golangci timeout", check: rawCheck("c", string(FormatGolangciLint), ""), execErr: &TimeoutError{Seconds: 5}, want: StatusError},

		// sarif: version and runs are required; entries decode per result.
		sarif("sarif missing version", "{\"runs\":[]}", 0, StatusError),
		sarif("sarif wrong root", "[]", 0, StatusError),
		sarif("sarif missing runs", "{\"version\":\"2.1.0\"}", 0, StatusError),
		sarif("sarif invalid json", "{\"version\":", 0, StatusError),
		sarif("sarif run missing driver name",
			"{\"version\":\"2.1.0\",\"runs\":[{\"tool\":{\"driver\":{\"name\":\"\"}},\"results\":[]},{\"tool\":{\"driver\":{\"name\":\"d\"}},\"results\":[]}]}",
			0, StatusPartial),
		sarif("sarif result missing rule id",
			"{\"version\":\"2.1.0\",\"runs\":[{\"tool\":{\"driver\":{\"name\":\"d\"}},\"results\":[{\"message\":{\"text\":\"m\"}},{\"ruleId\":\"r\",\"message\":{\"text\":\"m\"}}]}]}",
			0, StatusPartial),
		sarif("sarif results exit zero",
			"{\"version\":\"2.1.0\",\"runs\":[{\"tool\":{\"driver\":{\"name\":\"d\"}},\"results\":[{\"ruleId\":\"r1\",\"message\":{\"text\":\"m\"}},{\"ruleId\":\"r2\",\"message\":{\"text\":\"m\"}}]}]}",
			0, StatusFailed),
		sarif("sarif passing report exit nonzero", goodSarif, 1, StatusFailed),
		{name: "sarif timeout", check: rawCheck("c", string(FormatSarif), ""), execErr: &TimeoutError{Seconds: 5}, want: StatusError},
	}
}

// TestMalformedAndEdgeReportsNeverPass proves G4: every malformed, truncated,
// wrongly rooted, partly readable, timed-out, missing, unknown-format or
// exit-conflicting case produces a status that is not passed, and the failed
// cases keep a reason.
func TestMalformedAndEdgeReportsNeverPass(t *testing.T) {
	table := buildTable()
	if len(table) < 25 {
		t.Fatalf("table has %d fixtures, want at least 25", len(table))
	}
	for _, fixture := range table {
		t.Run(fixture.name, func(t *testing.T) {
			dir := t.TempDir()
			fixture.check.Workdir = dir
			fixture.check.Report = &ReportSpec{Path: "report.out", Format: fixture.check.Report.Format}
			writer := &reportWriter{inner: &scriptedExecutor{code: fixture.code, err: fixture.execErr}, path: filepath.Join(dir, "report.out")}
			if !fixture.missing {
				writer.data = []byte(fixture.content)
			}
			runner := newTestRunner(writer)
			result := runner.runCheck(context.Background(), fixture.check, "rev-1", "sha256:"+strings.Repeat("0", 64), nil)
			if result.Status == StatusPassed {
				t.Fatalf("%s produced passed: %+v", fixture.name, result)
			}
			if result.Status != fixture.want {
				t.Fatalf("%s status = %s with reason %q, want %s", fixture.name, result.Status, result.Reason, fixture.want)
			}
			if result.Reason == "" {
				t.Fatalf("%s lacks a reason: %+v", fixture.name, result)
			}
		})
	}
}

// TestUnknownFormatIsError proves G4: a check assembled with an unsupported
// format is an error at run time, even though Load also rejects it.
func TestUnknownFormatIsError(t *testing.T) {
	dir := t.TempDir()
	check := Check{ID: "c", Command: []string{"t"}, Workdir: dir,
		Report: &ReportSpec{Path: "report.out", Format: "cobertura"}}
	runner := newTestRunner(&reportWriter{inner: &scriptedExecutor{code: 0}, path: filepath.Join(dir, "report.out"), data: []byte("<coverage/>")})
	result := runner.runCheck(context.Background(), check, "rev-1", Digest(nil), nil)
	if result.Status != StatusError || !strings.Contains(result.Reason, "unsupported report format") {
		t.Fatalf("record = %+v", result)
	}
}

// TestExitCodeAndReportStricterWins proves G4: exit code 0 with a failing
// report fails, exit code nonzero with a passing report fails, and matching
// thresholds pass. The exit code reason wins when both would fail.
func TestExitCodeAndReportStricterWins(t *testing.T) {
	dir := t.TempDir()
	report := filepath.Join(dir, "report.xml")
	data := []byte(`<testsuite name="s" tests="2"><testcase name="a"/><testcase name="b"><failure message="x"/></testcase></testsuite>`)
	with := func(code int) Executor {
		return &reportWriter{inner: &scriptedExecutor{code: code}, path: report, data: data}
	}
	base := Check{ID: "c", Command: []string{"t"}, Workdir: dir,
		Report: &ReportSpec{Path: "report.xml", Format: FormatJUnitXML}}

	// Exit code nonzero with a failing report fails with the exit reason.
	result := newTestRunner(with(1)).runCheck(context.Background(), base, "rev-1", Digest(nil), nil)
	if result.Status != StatusFailed || !strings.Contains(result.Reason, "exit") {
		t.Fatalf("nonzero exit with failing report = %+v", result)
	}

	// A threshold that permits the failures still fails on the nonzero exit,
	// with the exit reason (the stricter signal).
	tolerant := base
	tolerant.Threshold = rawJSON(`{"max_failures": 5}`)
	tolerant.maxAllowed = 5
	result = newTestRunner(with(1)).runCheck(context.Background(), tolerant, "rev-1", Digest(nil), nil)
	if result.Status != StatusFailed || !strings.Contains(result.Reason, "exit") {
		t.Fatalf("nonzero exit with tolerated report = %+v", result)
	}

	// Exit 0 with a failing report and no threshold fails with the report
	// reason: the default threshold allows zero failures.
	result = newTestRunner(with(0)).runCheck(context.Background(), base, "rev-1", Digest(nil), nil)
	if result.Status != StatusFailed || !strings.Contains(result.Reason, "failures") {
		t.Fatalf("exit zero with failing report = %+v", result)
	}

	// Exit 0 with failures inside the threshold passes, and the report ref
	// carries the sha256 of the report file bytes.
	permitted := base
	permitted.Threshold = rawJSON(`{"max_failures": 2}`)
	permitted.maxAllowed = 2
	result = newTestRunner(with(0)).runCheck(context.Background(), permitted, "rev-1", Digest(nil), nil)
	if result.Status != StatusPassed || result.Report == nil || result.Report.URI != report {
		t.Fatalf("exit zero with tolerated report = %+v", result)
	}
	if result.Report.Digest != digestHexFile(data) {
		t.Fatalf("report digest = %s, want %s", result.Report.Digest, digestHexFile(data))
	}
}

// digestHexFile hashes file bytes independently of the package helpers.
func digestHexFile(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", sum)
}

// TestReportRefMissingFile proves G4's missing-report row is an error.
func TestReportRefMissingFile(t *testing.T) {
	dir := t.TempDir()
	check := Check{ID: "c", Command: []string{"t"}, Workdir: dir,
		Report: &ReportSpec{Path: "absent.xml", Format: FormatJUnitXML}}
	runner := newTestRunner(&scriptedExecutor{code: 0})
	result := runner.runCheck(context.Background(), check, "rev-1", Digest(nil), nil)
	if result.Status != StatusError || !strings.Contains(result.Reason, "could not be read") {
		t.Fatalf("missing report = %+v", result)
	}
}

// --- G6: evidence reuse ---

// TestEvidenceReuseRequiresMatchingInputs proves G6: a stored result is reused
// only when revision, config_digest and tool_versions all match; any mismatch
// reruns the check.
func TestEvidenceReuseRequiresMatchingInputs(t *testing.T) {
	file := gateFile(t,
		Check{ID: "a", Command: []string{"a"}},
		Check{ID: "b", Command: []string{"b"}},
	)
	runner := newTestRunner(&scriptedExecutor{code: 0})
	first, err := runner.Run(context.Background(), file, RunInput{Dir: "rev-1"})
	if err != nil {
		t.Fatal(err)
	}
	stored := map[string]Result{"a": first[0], "b": first[1]}

	// Same inputs: nothing runs again and the stored records come back.
	same := &scriptedExecutor{code: 0}
	second, err := newTestRunner(same).Run(context.Background(), file, RunInput{Dir: "rev-1", Stored: stored})
	if err != nil {
		t.Fatal(err)
	}
	if len(same.runs) != 0 {
		t.Fatalf("reuse ran commands: %v", same.runs)
	}
	for i, result := range second {
		if !reflect.DeepEqual(result, stored[[]string{"a", "b"}[i]]) {
			t.Fatalf("reused record %d = %+v, want %+v", i, result, stored[[]string{"a", "b"}[i]])
		}
	}

	// A different revision invalidates both records.
	changed := &scriptedExecutor{code: 0}
	if _, err = newTestRunner(changed).Run(context.Background(), file, RunInput{Dir: "rev-2", Stored: stored}); err != nil {
		t.Fatal(err)
	}
	if len(changed.runs) != 2 {
		t.Fatalf("revision change reran %v, want both checks", changed.runs)
	}

	// A different gate file digest invalidates both records.
	changedDigest := &scriptedExecutor{code: 0}
	if _, err = newTestRunner(changedDigest).Run(context.Background(), gateFile(t,
		Check{ID: "a", Command: []string{"a", "--flag"}},
		Check{ID: "b", Command: []string{"b"}},
	), RunInput{Dir: "rev-1", Stored: stored}); err != nil {
		t.Fatal(err)
	}
	if len(changedDigest.runs) != 2 {
		t.Fatalf("digest change reran %v, want both checks", changedDigest.runs)
	}

	// A changed tool version invalidates the record even when the gate file
	// and revision match.
	versioned := gateFile(t, Check{ID: "a", Command: []string{"a"}, VersionCommand: []string{"tool", "--version"}})
	versionRunner := &probeExecutor{version: "tool v1"}
	if _, err := newTestRunner(versionRunner).Run(context.Background(), versioned, RunInput{Dir: "rev-1"}); err != nil {
		t.Fatal(err)
	}
	stale := map[string]Result{"a": {
		Check: "a", Status: StatusPassed, Revision: "rev-1",
		ConfigDigest: Digest(versioned), ToolVersions: map[string]string{"tool": "tool v0"},
	}}
	reRunWithStaleVersion := &probeExecutor{version: "tool v1"}
	if _, err = newTestRunner(reRunWithStaleVersion).Run(context.Background(), versioned, RunInput{Dir: "rev-1", Stored: stale}); err != nil {
		t.Fatal(err)
	}
	if len(reRunWithStaleVersion.runs) != 1 {
		t.Fatalf("stale version reuse ran %v, want the check to rerun", reRunWithStaleVersion.runs)
	}
	// The matching version is reused without running the check.
	matching := map[string]Result{"a": {
		Check: "a", Status: StatusPassed, Revision: "rev-1",
		ConfigDigest: Digest(versioned), ToolVersions: map[string]string{"tool": "tool v1"},
	}}
	reRunWithMatchingVersion := &probeExecutor{version: "tool v1"}
	if _, err = newTestRunner(reRunWithMatchingVersion).Run(context.Background(), versioned, RunInput{Dir: "rev-1", Stored: matching}); err != nil {
		t.Fatal(err)
	}
	if len(reRunWithMatchingVersion.runs) != 0 {
		t.Fatalf("matching version reran %v, want reuse", reRunWithMatchingVersion.runs)
	}
}
