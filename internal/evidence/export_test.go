package evidence

import (
	"encoding/base64"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"synch/internal/credentials"
	"synch/internal/eventlog"
)

// TestBundle verifies redaction, direct limitations, hashes and tamper detection (E1, E2, E7).
func TestBundle(t *testing.T) {
	secret := "secret-with / chars"
	red := credentials.NewRedactor()
	if err := red.Add(secret); err != nil {
		t.Fatal(err)
	}
	events := []eventlog.Event{{NewEvent: eventlog.NewEvent{Type: "assignment.dispatched", Payload: []byte(`{"execution":"direct"}`)}}, {NewEvent: eventlog.NewEvent{Type: "finding.recorded", Payload: []byte(`{"text":"` + secret + ` ` + base64.StdEncoding.EncodeToString([]byte(secret)) + ` ` + url.QueryEscape(secret) + `"}`)}}}
	dir := filepath.Join(t.TempDir(), "bundle")
	if err := Export(dir, "thread", events, red); err != nil {
		t.Fatal(err)
	}
	if err := Verify(dir); err != nil {
		t.Fatal(err)
	}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, s := range []string{secret, base64.StdEncoding.EncodeToString([]byte(secret)), url.QueryEscape(secret)} {
			if strings.Contains(string(b), s) {
				t.Errorf("secret in %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "README.txt"))
	if err != nil || !strings.Contains(string(b), DirectLimit) {
		t.Fatal("missing direct limit", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "journal.json"), []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if Verify(dir) == nil {
		t.Fatal("tampering passed verification")
	}
}
