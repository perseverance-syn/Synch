package cli

import (
	"path/filepath"
	"testing"

	"synch/internal/evidence"
)

// TestEvidenceExportAuthenticates verifies that export uses the authenticated event API.
func TestEvidenceExportAuthenticates(t *testing.T) {
	f := newCLIFixture(t)
	id := f.createThread("evidence-create", "evidence test")
	dest := filepath.Join(t.TempDir(), "bundle")
	code, _, stderr := f.run(f.authenticated("evidence", "export", id, "--out", dest)...)
	if code != 0 {
		t.Fatalf("export: %d %s", code, stderr)
	}
	if err := evidence.Verify(dest); err != nil {
		t.Fatal(err)
	}
	f.envMap["SYNCH_TOKEN_FILE"] = filepath.Join(t.TempDir(), "missing")
	code, _, _ = f.run("--server", f.url, "evidence", "export", id, "--out", filepath.Join(t.TempDir(), "denied"))
	if code == 0 {
		t.Fatal("unauthenticated export succeeded")
	}
}
