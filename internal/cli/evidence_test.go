package cli

import (
	"archive/zip"
	"bytes"
	"path/filepath"
	"testing"

	"os"
	"strings"
	"synch/internal/api"
	"synch/internal/credentials"
	"synch/internal/eventlog"
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

func TestEvidenceExportUsesDaemonRedactor(t *testing.T) {
	red := credentials.NewRedactor()
	secret := "another-live-secret"
	if err := red.Add(secret); err != nil {
		t.Fatal(err)
	}
	f := newCLIFixture(t, func(c *api.Config) { c.EvidenceRedactor = red })
	id := f.createThread("evidence-daemon", secret)
	_, err := f.store.Append(t.Context(), []eventlog.NewEvent{{ID: eventlog.NewID(), Stream: "thread:" + id, ThreadID: id, Type: "finding.recorded", TypeVersion: 1, Actor: eventlog.Actor{Kind: "daemon", ID: "test"}, Payload: []byte(`{"text":"` + secret + `"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "bundle")
	code, _, stderr := f.run(f.authenticated("evidence", "export", id, "--out", dest)...)
	if code != 0 {
		t.Fatalf("%d %s", code, stderr)
	}
	if err := evidence.Verify(dest); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dest, "journal.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), secret) {
		t.Fatal("live secret leaked")
	}
}
func TestExtractEvidenceRejectsUnsafeArchive(t *testing.T) {
	var data bytes.Buffer
	z := zip.NewWriter(&data)
	w, err := z.Create("../escape")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write([]byte("bad")); err != nil {
		t.Fatal(err)
	}
	if err = z.Close(); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "bundle")
	if err = extractEvidence(dest, data.Bytes(), ""); err == nil {
		t.Fatal("unsafe archive accepted")
	}
	if _, err = os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("partial bundle retained: %v", err)
	}
}
