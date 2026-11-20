package gates

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// --- Runner: ordered execution, evidence reuse and status computation ---

// Executor runs one check command. A nonzero exit is a result, not an error;
// a command that could not start, exceeded its declared timeout (TimeoutError)
// or was interrupted (InterruptError) returns an error.
type Executor interface {
	Run(ctx context.Context, check Check) (code int, stdout string, err error)
}

// RunInput carries the checkout the checks run against and the stored evidence
// an earlier run produced, keyed by check id. The revision is never supplied by
// the caller: it is read from the checkout at Dir (the current directory when
// empty), so evidence cannot be labelled with a revision it did not run on.
type RunInput struct {
	Dir    string
	Stored map[string]Result
}

// Runner executes a gate file's checks in order through its executor.
type Runner struct {
	executor Executor
	head     func(ctx context.Context, dir string) (string, error)
}

// NewRunner creates a runner over an executor; revisions come from git.
func NewRunner(executor Executor) *Runner {
	return &Runner{executor: executor, head: gitHead}
}

// gitHead reads the checked-out commit of the repository at dir. A directory
// that is not a repository, or has no commit, is an error.
func gitHead(ctx context.Context, dir string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git rev-parse HEAD in %q: %w", dir, err)
	}
	return strings.TrimSpace(string(out)), nil
}

// Run parses the gate file (digesting its exact bytes) and produces one
// check.result record per check, in file order. A failed required check stops
// later checks, which become not_run with the contract's skip reason.
func (r *Runner) Run(ctx context.Context, data []byte, input RunInput) ([]Result, error) {
	file, err := Load(data)
	if err != nil {
		return nil, err
	}
	// The revision lookup ignores cancellation: records for checks that were
	// never launched still need the revision they were not run against.
	revision, err := r.head(context.WithoutCancel(ctx), input.Dir)
	if err != nil {
		return nil, fmt.Errorf("revision of the checkout: %w", err)
	}
	digest := Digest(data)
	results := make([]Result, 0, len(file.Checks))
	stopped := false
	for _, check := range file.Checks {
		if stopped {
			results = append(results, notRunResult(check.ID, revision, digest, skipReason))
			continue
		}
		if ctx.Err() != nil {
			results = append(results, notRunResult(check.ID, revision, digest, "not run: "+ctx.Err().Error()))
			continue
		}
		versions, err := r.versions(ctx, check)
		if err != nil {
			results = append(results, errorResult(check, revision, digest, versions, err.Error()))
			continue
		}
		result, ok := input.Stored[check.ID]
		if !ok || !reusable(result, check.ID, revision, digest, versions) {
			result = r.runCheck(ctx, check, revision, digest, versions)
		}
		results = append(results, result)
		if check.Required && result.Status == StatusFailed {
			stopped = true
		}
	}
	return results, nil
}

// versions probes the declared version command, keyed by the tool name. A
// failed or silent probe is an error, because evidence without a tool version
// cannot be reused or trusted.
func (r *Runner) versions(ctx context.Context, check Check) (map[string]string, error) {
	if len(check.VersionCommand) == 0 {
		return nil, nil
	}
	probe := Check{ID: check.ID, Command: check.VersionCommand, Workdir: check.Workdir, Timeout: check.Timeout}
	code, stdout, err := r.executor.Run(ctx, probe)
	if err != nil {
		return nil, fmt.Errorf("version command failed: %w", err)
	}
	if code != 0 {
		return nil, fmt.Errorf("version command %q exited with code %d", check.VersionCommand[0], code)
	}
	version := strings.TrimSpace(stdout)
	if version == "" {
		return nil, fmt.Errorf("version command %q printed nothing", check.VersionCommand[0])
	}
	return map[string]string{check.VersionCommand[0]: version}, nil
}

// runCheck executes one check and computes its status from the command and
// its report. The stricter of exit code and report outcome wins, a partly
// unreadable report is partial, and a malformed or unsupported report is an
// error that can never pass.
func (r *Runner) runCheck(ctx context.Context, check Check, revision, digest string, versions map[string]string) Result {
	// A report that already exists was written by some earlier run; remove it
	// so only this invocation's output can be credited to this result.
	if check.Report != nil {
		if err := os.Remove(reportPath(check)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return errorResult(check, revision, digest, versions, fmt.Sprintf("stale report could not be removed: %v", err))
		}
	}
	code, _, err := r.executor.Run(ctx, check)
	if err != nil {
		var timeout *TimeoutError
		if errors.As(err, &timeout) {
			return errorResult(check, revision, digest, versions, timeout.Error())
		}
		var interrupt *InterruptError
		if errors.As(err, &interrupt) {
			return errorResult(check, revision, digest, versions, interrupt.Error())
		}
		return errorResult(check, revision, digest, versions, fmt.Sprintf("command could not start: %v", err))
	}
	if check.Report == nil {
		if code != 0 {
			return failedResult(check, revision, digest, versions, nil, fmt.Sprintf("command exited with code %d", code))
		}
		return passedResult(check, revision, digest, versions, nil)
	}
	uri := reportPath(check)
	data, err := readReport(uri)
	if err != nil {
		return errorResult(check, revision, digest, versions, fmt.Sprintf("report %s could not be read: %v", uri, err))
	}
	ref := reportRef(uri, data)
	parsed, parseErr := parseReport(check.Report.Format, data)
	if parseErr != nil {
		return errorResult(check, revision, digest, versions, parseErr.Error(), withReport(ref))
	}
	// A report with unreadable entries and nothing readable is malformed; a
	// report with something readable alongside them is only partial, and
	// partial outranks the exit code and the threshold.
	if parsed.Unreadable > 0 && len(parsed.Items) == 0 && parsed.Readable == 0 {
		return errorResult(check, revision, digest, versions,
			fmt.Sprintf("all %d entries in the %s report are unreadable", parsed.Unreadable, check.Report.Format), withReport(ref))
	}
	failures := 0
	for _, item := range parsed.Items {
		if item.Status == StatusFailed {
			failures++
		}
	}
	if parsed.Unreadable > 0 {
		return partialResult(check, revision, digest, versions, ref,
			fmt.Sprintf("%d of %d entries in the %s report are unreadable", parsed.Unreadable, len(parsed.Items)+parsed.Unreadable, check.Report.Format))
	}
	if code != 0 {
		return failedResult(check, revision, digest, versions, ref, fmt.Sprintf("command exited with code %d", code))
	}
	if failures > check.maxAllowed {
		return failedResult(check, revision, digest, versions, ref,
			fmt.Sprintf("report has %d failures, the threshold allows %d", failures, check.maxAllowed))
	}
	return passedResult(check, revision, digest, versions, ref)
}

// maxReportBytes caps how much of a report file is read; a larger report is an
// error rather than a truncated pass.
const maxReportBytes = 16 << 20

// readReport reads a report file up to the size cap and rejects a larger one.
func readReport(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }() // read-only handle: a close error loses nothing
	data, err := io.ReadAll(io.LimitReader(file, maxReportBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxReportBytes {
		return nil, fmt.Errorf("report is larger than the %d byte limit", maxReportBytes)
	}
	return data, nil
}

// reportPath resolves the report file against the check's workdir.
func reportPath(check Check) string {
	if filepath.IsAbs(check.Report.Path) || check.Workdir == "" {
		return check.Report.Path
	}
	return filepath.Join(check.Workdir, check.Report.Path)
}

// --- Result constructors: one per status, keeping the reason rule in one place ---

// reportOption attaches optional members to a constructed result.
type reportOption func(*Result)

// withReport attaches the report reference.
func withReport(ref *ReportRef) reportOption {
	return func(result *Result) { result.Report = ref }
}

// passedResult builds a passed record; the reason stays absent.
func passedResult(check Check, revision, digest string, versions map[string]string, ref *ReportRef) Result {
	result := Result{Check: check.ID, Status: StatusPassed, Revision: revision, ConfigDigest: digest, ToolVersions: versions, Report: ref}
	return result
}

// failedResult builds a failed record with its reason.
func failedResult(check Check, revision, digest string, versions map[string]string, ref *ReportRef, reason string) Result {
	return Result{Check: check.ID, Status: StatusFailed, Reason: reason, Revision: revision, ConfigDigest: digest, ToolVersions: versions, Report: ref}
}

// errorResult builds an error record with its reason.
func errorResult(check Check, revision, digest string, versions map[string]string, reason string, options ...reportOption) Result {
	result := Result{Check: check.ID, Status: StatusError, Reason: reason, Revision: revision, ConfigDigest: digest, ToolVersions: versions}
	for _, option := range options {
		option(&result)
	}
	return result
}

// partialResult builds a partial record with its reason and report reference.
func partialResult(check Check, revision, digest string, versions map[string]string, ref *ReportRef, reason string) Result {
	return Result{Check: check.ID, Status: StatusPartial, Reason: reason, Revision: revision, ConfigDigest: digest, ToolVersions: versions, Report: ref}
}
