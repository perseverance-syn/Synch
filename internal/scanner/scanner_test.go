package scanner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"synch/internal/findings"
)

// --- Driver test entry point ---

// TestDriver exercises F1–F3 through the driver with an offline subprocess and report store.
func TestDriver(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "fakescanner.exe")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./testdata/fakescanner")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build fake scanner: %v\n%s", err, output)
	}
	executable, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	pin := Pin{Path: binary, BinaryInfo: BinaryInfo{Version: "fixture-v1", SHA256: strings.TrimPrefix(findings.Digest(executable), "sha256:")}}
	t.Run("verify before executing", func(t *testing.T) { testVerification(t, pin) })
	t.Run("normalize and retain", func(t *testing.T) { testReports(t, pin) })
	t.Run("reject invalid reports", func(t *testing.T) { testInvalidReports(t, pin) })
	t.Run("hardening", func(t *testing.T) { testHardening(t, pin) })
	t.Run("accept clean reports", func(t *testing.T) { testCleanReports(t, pin) })
}

// --- Harness and helpers ---

type harness struct {
	scratch, marker, stored string
	request                 ScanRequest
}

// newHarness gives the tool private scratch and the driver a separate durable sink.
func newHarness(t *testing.T, fixture string, exit string) harness {
	t.Helper()
	root := t.TempDir()
	h := harness{
		scratch: filepath.Join(root, "scratch"),
		marker:  filepath.Join(root, "executed"),
		stored:  filepath.Join(root, "retained.json"),
		request: ScanRequest{ScanID: "scan-1", Revision: "rev-1", ConfigDigest: "config-1", SourceDir: filepath.Join(root, "source")},
	}
	for _, dir := range []string{h.scratch, h.request.SourceDir} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("SYNCH_FAKE_MARKER", h.marker)
	t.Setenv("SYNCH_FAKE_FIXTURE", fixture)
	t.Setenv("SYNCH_FAKE_EXIT", exit)
	t.Setenv("SYNCH_FAKE_SOURCE", h.request.SourceDir)
	t.Cleanup(func() {
		entries, err := os.ReadDir(h.scratch)
		if err != nil || len(entries) != 0 {
			t.Errorf("scratch not removed: %v, %v", entries, err)
		}
	})
	return h
}

// driver constructs the scanner with the supplied manifest and test storage.
func (h harness) driver(t *testing.T, tool string, manifest Manifest, redact Redactor) *Driver {
	t.Helper()
	driver, err := New(tool, manifest, h.scratch, redact, func(_ context.Context, data []byte) (string, error) {
		return "report://fixture", os.WriteFile(h.stored, data, 0600)
	})
	if err != nil {
		t.Fatal(err)
	}
	return driver
}

// redactKnown replaces the planted, broker-known secret.
func redactKnown(value string) (string, error) {
	return strings.ReplaceAll(value, "planted-<secret>", redacted), nil
}

// missing fails the test if an unexpected side effect exists.
func missing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s exists or could not be checked: %v", path, err)
	}
}

// --- Verification tests (F1) ---

// testVerification proves F1 rejects missing pins, missing and unreadable binaries,
// mismatches and missing signatures before invoking the program.
func testVerification(t *testing.T, pin Pin) {
	wrong, absent, directory, unsigned := pin, pin, pin, pin
	wrong.SHA256 = strings.Repeat("0", 64)
	absent.Path = filepath.Join(t.TempDir(), "missing.exe")
	directory.Path = t.TempDir()
	unsigned.SignaturePublished = true
	for _, tc := range []struct {
		name     string
		manifest Manifest
		want     error
	}{
		{"missing pin", nil, ErrChecksumMismatch},
		{"mismatch", Manifest{Trivy: wrong}, ErrChecksumMismatch},
		{"missing file", Manifest{Trivy: absent}, ErrBinaryUnreadable},
		{"unreadable as executable", Manifest{Trivy: directory}, ErrBinaryUnreadable},
		{"missing signature", Manifest{Trivy: unsigned}, ErrSignature},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, fixturePath(t, "trivy-v2.json"), "0")
			driver := h.driver(t, Trivy, tc.manifest, redactKnown)
			result, err := driver.Scan(t.Context(), h.request)
			if !errors.Is(err, tc.want) || result.Status != "error" {
				t.Fatalf("Scan = %+v, %v, want %v", result, err, tc.want)
			}
			missing(t, h.marker)
			missing(t, h.stored)
		})
	}
	h := newHarness(t, fixturePath(t, "trivy-v2.json"), "0")
	calls := 0
	pin.SignaturePublished = true
	pin.VerifySignature = func(_ context.Context, path string) error {
		calls++
		if filepath.Dir(path) == filepath.Dir(pin.Path) {
			t.Fatal("signature verifier received original binary path")
		}
		return nil
	}
	driver := h.driver(t, Trivy, Manifest{Trivy: pin}, redactKnown)
	info, err := driver.Verify(t.Context())
	if err != nil || info != pin.BinaryInfo || calls != 1 {
		t.Fatalf("Verify = %+v, %v, signature calls = %d", info, err, calls)
	}
	missing(t, h.marker)
	if _, err := driver.Scan(t.Context(), h.request); err != nil || calls != 2 {
		t.Fatalf("Scan did not reverify: %v, signature calls = %d", err, calls)
	}
}

// --- Report tests: normalization, redaction and status (F2–F3) ---

// testReports checks both normalized schemas, threshold semantics and durable redaction.
func testReports(t *testing.T, pin Pin) {
	for _, tc := range []struct {
		tool, fixture, exit, status string
		threshold                   findings.Severity
		count                       int
	}{
		{Trivy, "trivy-v2.json", "0", "passed", findings.Critical, 2},
		{Gitleaks, "gitleaks-v8.json", "1", "failed", findings.High, 1},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			h := newHarness(t, fixturePath(t, tc.fixture), tc.exit)
			h.request.Threshold = tc.threshold
			driver := h.driver(t, tc.tool, Manifest{tc.tool: pin}, redactKnown)
			result, err := driver.Scan(t.Context(), h.request)
			if err != nil || result.Status != tc.status || len(result.Findings) != tc.count {
				t.Fatalf("Scan = %+v, %v", result, err)
			}
			data, err := os.ReadFile(h.stored)
			if err != nil {
				t.Fatal(err)
			}
			if !result.Report.Redacted || result.Report.Digest != findings.Digest(data) {
				t.Fatalf("wrong report provenance: %+v", result.Report)
			}
			assertRedacted(t, data)
			if !bytes.Contains(data, []byte(redacted)) {
				t.Fatal("report lacks redaction marker")
			}
			for _, finding := range result.Findings {
				checkFinding(t, finding, result.Report, tc.tool, pin, h.request)
			}
			first := result.Findings[0]
			if tc.tool == Trivy {
				if first.Component.PURL != "pkg:golang/example.test/dependency@v1.2.3" || first.NativeSeverity != "HIGH" ||
					result.Findings[1].Component.Type != "config" || result.Findings[1].Location.StartLine != 4 {
					t.Fatalf("unexpected Trivy translation: %+v", result.Findings)
				}
			} else if first.Component.Type != "secret" || first.Component.Name != first.Rule.ID || first.NativeSeverity != "" ||
				first.Evidence.SnippetRedacted != redacted || first.Severity != findings.High || first.Location.Commit != redacted {
				t.Fatalf("unexpected Gitleaks translation: %+v", first)
			}
			h.request.ScanID = "scan-2"
			repeated, err := driver.Scan(t.Context(), h.request)
			if err != nil {
				t.Fatal(err)
			}
			grouped := findings.Group(append(result.Findings, repeated.Findings...))
			if len(grouped) != tc.count || len(grouped[0].Provenance) != 2 || grouped[0].Provenance[1].ScanID != "scan-2" {
				t.Fatalf("lost repeated scan provenance: %+v", grouped)
			}
		})
	}
}

// checkFinding verifies the v2 envelope payload, identity and report provenance.
func checkFinding(t *testing.T, f findings.Finding, report findings.ReportRef, tool string, pin Pin, req ScanRequest) {
	t.Helper()
	if f.FindingVersion != 2 || !regexp.MustCompile(`^[0-7][0-9A-HJKMNP-TV-Z]{25}$`).MatchString(f.ID) ||
		f.Tool.Name != tool || f.Tool.Version != pin.Version || f.Tool.BinarySHA256 != pin.SHA256 ||
		f.Revision != req.Revision || f.Evidence.Report != report ||
		!slices.Equal(f.Provenance, []findings.Provenance{{ScanID: req.ScanID, Tool: tool, Report: report, NativeID: f.Rule.ID}}) {
		t.Fatalf("invalid finding identity or provenance: %+v", f)
	}
	payload := f.Recorded()
	data, err := json.Marshal(payload)
	if err != nil || payload.Source != "scanner" {
		t.Fatalf("invalid event payload: %s, %v", data, err)
	}
	assertRedacted(t, data)
}

// assertRedacted scans serialized bytes for both the raw and JSON-escaped planted values.
func assertRedacted(t *testing.T, data []byte) {
	t.Helper()
	for _, secret := range []string{"planted-", "unknown-fixture-token"} {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatalf("secret leaked into serialized output")
		}
	}
}

// testInvalidReports covers truncated, malformed, unsupported, crashing and unredactable runs.
func testInvalidReports(t *testing.T, pin Pin) {
	for _, tc := range []struct {
		name, tool, body, exit string
		want                   error
	}{
		{"truncated", Trivy, `{"SchemaVersion":2,`, "0", ErrReport},
		{"malformed", Gitleaks, `{"findings":[]}`, "0", ErrReport},
		{"unsupported", Trivy, `{"SchemaVersion":3,"ArtifactName":"fixture","ArtifactType":"filesystem"}`, "0", ErrUnsupportedReport},
		{"trailing JSON", Trivy, `{"SchemaVersion":2,"ArtifactName":"fixture","ArtifactType":"filesystem"} {}`, "0", ErrReport},
		{"crash", Gitleaks, `[]`, "2", ErrExecution},
		{"invalid exit", Gitleaks, `[]`, "1", ErrReport},
		{"redactor failure", Gitleaks, `[{"RuleID":"test","File":"test","StartLine":1,"EndLine":1}]`, "1", ErrRedaction},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := filepath.Join(t.TempDir(), "invalid.json")
			if err := os.WriteFile(fixture, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			h := newHarness(t, fixture, tc.exit)
			redact := redactKnown
			if tc.want == ErrRedaction {
				redact = func(string) (string, error) { return "", errors.New("unknown-fixture-token") }
			}
			result, err := h.driver(t, tc.tool, Manifest{tc.tool: pin}, redact).Scan(t.Context(), h.request)
			if !errors.Is(err, tc.want) || result.Status != "error" || result.Reason == "" ||
				strings.Contains(err.Error(), "unknown-fixture-token") {
				t.Fatalf("Scan = %+v, %v, want %v", result, err, tc.want)
			}
			missing(t, h.stored)
		})
	}
}

// testCleanReports verifies F2 distinguishes native empty reports from malformed ones.
func testCleanReports(t *testing.T, pin Pin) {
	for tool, body := range map[string]string{
		Trivy:    `{"SchemaVersion":2,"ArtifactName":"fixture","ArtifactType":"filesystem"}`,
		Gitleaks: `[]`,
	} {
		t.Run(tool, func(t *testing.T) {
			fixture := filepath.Join(t.TempDir(), "clean.json")
			if err := os.WriteFile(fixture, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			h := newHarness(t, fixture, "0")
			result, err := h.driver(t, tool, Manifest{tool: pin}, redactKnown).Scan(t.Context(), h.request)
			if err != nil || result.Status != "passed" || len(result.Findings) != 0 || !result.Report.Redacted {
				t.Fatalf("clean Scan = %+v, %v", result, err)
			}
		})
	}
}

// --- Fixture paths ---

// fixturePath resolves a committed native-format report for the subprocess.
func fixturePath(t *testing.T, name string) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return path
}

// --- Hardening tests (card 505 fix 1: M1, M2, M4, M6) ---

// testHardening covers timeout-is-error (M1), report cap (M2), WaitDelay (M4) and re-hash (M6).
func testHardening(t *testing.T, pin Pin) {
	leak := fixturePath(t, "gitleaks-v8.json")
	t.Run("M1 timeout is error", func(t *testing.T) {
		for _, tool := range []string{Gitleaks, Trivy} {
			fixture, exit := leak, "1"
			if tool == Trivy {
				fixture, exit = fixturePath(t, "trivy-v2.json"), "0"
			}
			h := newHarness(t, fixture, exit)
			t.Setenv("SYNCH_FAKE_SLEEP", "10s")
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			result, err := h.driver(t, tool, Manifest{tool: pin}, redactKnown).Scan(ctx, h.request)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) || result.Status != "error" || result.Reason != "timeout" || len(result.Findings) != 0 {
				t.Fatalf("%s Scan = %+v, %v", tool, result, err)
			}
			missing(t, h.stored)
		}
	})
	t.Run("M2 report cap", func(t *testing.T) {
		old := maxReportBytes
		maxReportBytes = 1 << 10
		t.Cleanup(func() { maxReportBytes = old })
		big := filepath.Join(t.TempDir(), "big.json")
		body := `[{"RuleID":"` + strings.Repeat("a", 4<<10) + `","File":"f","StartLine":1,"EndLine":1}]`
		if err := os.WriteFile(big, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		trivyBody := `{"SchemaVersion":2,"ArtifactName":"` + strings.Repeat("a", 4<<10) + `","ArtifactType":"filesystem"}`
		trivyBig := filepath.Join(t.TempDir(), "trivy-big.json")
		if err := os.WriteFile(trivyBig, []byte(trivyBody), 0600); err != nil {
			t.Fatal(err)
		}
		for tool, tc := range map[string][2]string{Gitleaks: {big, "1"}, Trivy: {trivyBig, "0"}} {
			h := newHarness(t, tc[0], tc[1])
			result, err := h.driver(t, tool, Manifest{tool: pin}, redactKnown).Scan(t.Context(), h.request)
			if !errors.Is(err, ErrReportTooLarge) || result.Status != "error" || result.Reason != "report_too_large" {
				t.Fatalf("%s Scan = %+v, %v", tool, result, err)
			}
			missing(t, h.stored)
		}
	})
	t.Run("M4 grandchild holding stdout", func(t *testing.T) {
		old := waitDelay
		waitDelay = 300 * time.Millisecond
		t.Cleanup(func() { waitDelay = old })
		h := newHarness(t, fixturePath(t, "trivy-v2.json"), "0")
		holder := filepath.Join(t.TempDir(), "holder.exe")
		data, err := os.ReadFile(pin.Path)
		if err != nil || os.WriteFile(holder, data, 0700) != nil {
			t.Fatal("stage holder", err)
		}
		t.Setenv("SYNCH_FAKE_GRANDCHILD", holder)
		t.Setenv("SYNCH_FAKE_SLEEP", "20s")
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		result, err := h.driver(t, Trivy, Manifest{Trivy: pin}, redactKnown).Scan(ctx, h.request)
		if elapsed := time.Since(start); elapsed > 2500*time.Millisecond || result.Status != "error" || err == nil {
			t.Fatalf("Scan = %+v, %v after %v", result, err, elapsed)
		}
		time.Sleep(3 * time.Second) // let the holder exit so its temp file can be removed
	})
	t.Run("M6 staged copy swapped before exec", func(t *testing.T) {
		h := newHarness(t, leak, "1")
		d := h.driver(t, Gitleaks, Manifest{Gitleaks: pin}, redactKnown)
		dir := t.TempDir()
		program, _, err := d.verifiedCopy(t.Context(), dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(program, 0700); err != nil || os.WriteFile(program, []byte("evil"), 0700) != nil {
			t.Fatal("could not swap staged copy", err)
		}
		_, _, err = d.scanReport(t.Context(), dir, program, h.request)
		if !errors.Is(err, ErrChecksumMismatch) {
			t.Fatalf("scanReport = %v, want checksum mismatch", err)
		}
		missing(t, h.marker)
	})
}
