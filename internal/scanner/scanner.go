package scanner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"synch/internal/findings"
)

// --- Types, errors and limits ---

const (
	Trivy    = "trivy"
	Gitleaks = "gitleaks"
)

var (
	ErrChecksumMismatch  = errors.New("checksum_mismatch")
	ErrSignature         = errors.New("signature_verification_failed")
	ErrBinaryUnreadable  = errors.New("scanner binary unreadable")
	ErrInvalidRequest    = errors.New("invalid scan request")
	ErrReport            = errors.New("invalid scanner report")
	ErrUnsupportedReport = errors.New("unsupported scanner report")
	ErrRedaction         = errors.New("redaction_failed")
	ErrExecution         = errors.New("scanner execution failed")
	ErrRetention         = errors.New("scanner report retention failed")
	ErrScratchCleanup    = errors.New("scanner scratch cleanup failed")
	ErrReportTooLarge    = errors.New("report_too_large")
)

// Variables so tests can shrink the limits; production values never change.
var (
	// maxReportBytes caps how much scanner output is read into memory.
	maxReportBytes int64 = 64 << 20
	// waitDelay bounds how long a grandchild holding stdout can block Wait.
	waitDelay = 5 * time.Second
)

// BinaryInfo identifies a pinned scanner release and executable digest.
type BinaryInfo struct {
	Version string
	SHA256  string
}

// Pin identifies the binary for the current OS and architecture. A published
// signature requires a verifier bound to trusted signature data and keys.
type Pin struct {
	Path string
	BinaryInfo
	SignaturePublished bool
	VerifySignature    func(context.Context, string) error
}

type Manifest map[string]Pin

// ScanRequest binds a scan to a caller-resolved immutable source snapshot and
// configuration. Callers supply a distinct ScanID for each execution.
type ScanRequest struct {
	ScanID, Revision, ConfigDigest string
	SourceDir, ConfigPath          string
	Threshold                      findings.Severity
}

type ScanResult struct {
	Status, Reason string
	Findings       []findings.Finding
	Report         findings.ReportRef
	Binary         BinaryInfo
}

// Redactor replaces known values with [REDACTED] in a decoded report value.
type Redactor func(string) (string, error)

// ReportStore retains only redacted bytes and returns a trusted durable URI.
type ReportStore func(context.Context, []byte) (string, error)

type ScannerDriver interface {
	Tool() string
	Verify(context.Context) (BinaryInfo, error)
	Scan(context.Context, ScanRequest) (ScanResult, error)
}

type Driver struct {
	tool        string
	pin         Pin
	scratchRoot string
	redact      Redactor
	retain      ReportStore
}

// --- Driver construction and Scan ---

// New creates a driver for a platform-specific manifest. scratchRoot must be
// a service-account-only directory, including its inherited ACL on Windows.
func New(tool string, manifest Manifest, scratchRoot string, redact Redactor, retain ReportStore) (*Driver, error) {
	if (tool != Trivy && tool != Gitleaks) || !filepath.IsAbs(scratchRoot) || redact == nil || retain == nil {
		return nil, ErrInvalidRequest
	}
	return &Driver{tool: tool, pin: manifest[tool], scratchRoot: scratchRoot, redact: redact, retain: retain}, nil
}

// Tool identifies the driver's scanner.
func (d *Driver) Tool() string { return d.tool }

// Verify checks the manifest against a private copy of the executable.
func (d *Driver) Verify(ctx context.Context) (BinaryInfo, error) {
	dir, err := os.MkdirTemp(d.scratchRoot, "scan-")
	if err != nil {
		return BinaryInfo{}, fmt.Errorf("create verification directory: %w", err)
	}
	_, info, err := d.verifiedCopy(ctx, dir)
	if os.RemoveAll(dir) != nil {
		return BinaryInfo{}, errors.Join(err, ErrScratchCleanup)
	}
	return info, err
}

// Scan executes the verified scanner and emits evidence only after redaction
// and report validation succeed.
func (d *Driver) Scan(ctx context.Context, req ScanRequest) (ScanResult, error) {
	if req.Threshold == "" {
		req.Threshold = findings.High
	}
	if req.ScanID == "" || req.Revision == "" || req.ConfigDigest == "" || !filepath.IsAbs(req.SourceDir) ||
		(req.ConfigPath != "" && !filepath.IsAbs(req.ConfigPath)) || req.Threshold.Rank() < 0 {
		return failedScan("invalid_request", ErrInvalidRequest)
	}
	dir, err := os.MkdirTemp(d.scratchRoot, "scan-")
	if err != nil {
		return failedScan("scratch_failed", fmt.Errorf("create scan directory: %w", err))
	}
	program, info, err := d.verifiedCopy(ctx, dir)
	if err != nil && os.RemoveAll(dir) != nil {
		return failedScan("scratch_cleanup_failed", errors.Join(err, ErrScratchCleanup))
	}
	switch {
	case errors.Is(err, ErrSignature):
		return failedScan("signature_verification_failed", err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ScanResult{Status: "not_run", Reason: "canceled"}, err
	case err != nil:
		return failedScan("checksum_mismatch", err)
	}
	report, entries, err := d.scanReport(ctx, dir, program, req)
	// Delete the tool's scratch output before handing any report to durable storage.
	if os.RemoveAll(dir) != nil {
		return failedScan("scratch_cleanup_failed", errors.Join(err, ErrScratchCleanup))
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return failedScan("timeout", err)
	case errors.Is(err, context.Canceled):
		return failedScan("canceled", err)
	case errors.Is(err, ErrChecksumMismatch):
		return failedScan("checksum_mismatch", err)
	case errors.Is(err, ErrReportTooLarge):
		return failedScan("report_too_large", err)
	case errors.Is(err, ErrExecution):
		return failedScan("execution_failed", err)
	case errors.Is(err, ErrRedaction):
		return failedScan("redaction_failed", err)
	case errors.Is(err, ErrUnsupportedReport):
		return failedScan("unsupported_report", err)
	case err != nil:
		return failedScan("invalid_report", err)
	}
	digest := findings.Digest(report)
	uri, err := d.retain(ctx, report)
	if err != nil || uri == "" {
		return failedScan("retention_failed", ErrRetention)
	}
	result := ScanResult{
		Status: "passed", Binary: info,
		Report: findings.ReportRef{URI: uri, Digest: digest, Redacted: true},
	}
	for _, entry := range entries {
		entry.Location.Path = scanPath(req.SourceDir, entry.Location.Path)
		if entry.Component.Type == "config" {
			entry.Component.Name = entry.Location.Path
		}
		entry.Tool = findings.Tool{Name: d.tool, Version: info.Version, BinarySHA256: info.SHA256}
		entry.Revision = req.Revision
		entry.Evidence.Report = result.Report
		entry.Provenance = []findings.Provenance{{ScanID: req.ScanID, Tool: d.tool, Report: result.Report, NativeID: entry.Rule.ID}}
		entry.Seal()
		if entry.Severity.Rank() >= req.Threshold.Rank() {
			result.Status = "failed"
		}
		result.Findings = append(result.Findings, entry)
	}
	result.Findings = findings.Group(result.Findings)
	return result, nil
}

// --- Tool invocation and safe status mapping (scanner-driver section 3) ---

// scanPath removes the checkout prefix so relocating a snapshot preserves fingerprints.
func scanPath(root, reported string) string {
	if filepath.IsAbs(reported) {
		local, err := filepath.Rel(root, reported)
		if err == nil && filepath.IsLocal(local) {
			reported = local
		}
	}
	return filepath.ToSlash(reported)
}

// scanReport runs a tool in private scratch and validates its redacted JSON.
func (d *Driver) scanReport(ctx context.Context, dir, program string, req ScanRequest) ([]byte, []findings.Finding, error) {
	path := filepath.Join(dir, "report.json")
	args := []string{"filesystem", "--scanners", "vuln,misconfig", "--format", "json", "--exit-code", "0",
		"--skip-db-update", "--skip-java-db-update", "--skip-check-update", "--offline-scan", "--cache-backend", "memory"}
	if d.tool == Gitleaks {
		args = []string{"detect", "--redact", "--no-git", "--source", req.SourceDir, "--report-format", "json", "--report-path", path, "--exit-code", "1"}
	}
	if req.ConfigPath != "" {
		args = append(args, "--config", req.ConfigPath)
	}
	if d.tool == Trivy {
		args = append(args, "--", req.SourceDir)
	}
	// Re-hash right before exec to shrink the window for swapping the staged copy.
	if actual, err := hashFile(program); err != nil || actual != strings.ToLower(d.pin.SHA256) {
		return nil, nil, ErrChecksumMismatch
	}
	cmd := exec.CommandContext(ctx, program, args...)
	cmd.Dir = dir
	cmd.WaitDelay = waitDelay
	var stdout capWriter
	if d.tool == Trivy {
		stdout.limit = maxReportBytes
		cmd.Stdout = &stdout
	}
	runErr := cmd.Run()
	// A killed or cancelled scanner is never a verdict, whatever its exit code or report says.
	if err := ctx.Err(); err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrExecution, err)
	}
	if stdout.over {
		return nil, nil, ErrReportTooLarge
	}
	if d.tool == Trivy {
		if runErr != nil {
			return nil, nil, ErrExecution
		}
		return d.trivyReport(stdout.buf.Bytes())
	}
	if runErr != nil {
		var code *exec.ExitError
		if !errors.As(runErr, &code) || code.ExitCode() != 1 {
			return nil, nil, ErrExecution
		}
	}
	output, err := readCapped(path, maxReportBytes)
	if errors.Is(err, ErrReportTooLarge) {
		return nil, nil, err
	}
	if err != nil {
		return nil, nil, ErrReport
	}
	report, entries, err := d.gitleaksReport(output)
	if err == nil && (runErr != nil) != (len(entries) > 0) {
		return nil, nil, ErrReport
	}
	return report, entries, err
}

// capWriter buffers up to limit bytes and records overflow instead of growing.
type capWriter struct {
	buf   bytes.Buffer
	limit int64
	over  bool
}

// Write buffers p, failing the pipe once the limit is exceeded.
func (w *capWriter) Write(p []byte) (int, error) {
	if int64(w.buf.Len()+len(p)) > w.limit {
		w.over = true
		return 0, ErrReportTooLarge
	}
	return w.buf.Write(p)
}

// readCapped reads a file, failing with ErrReportTooLarge past limit bytes.
func readCapped(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err := errors.Join(err, f.Close()); err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, ErrReportTooLarge
	}
	return data, nil
}

// failedScan returns a non-clean status and a diagnostic without tool output.
func failedScan(reason string, err error) (ScanResult, error) {
	return ScanResult{Status: "error", Reason: reason}, err
}
