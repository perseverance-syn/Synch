package context

import (
	"bytes"
	"encoding/base64"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"synch/internal/credentials"
)

// TestRetainRedactsKnownSecrets proves card 401 C3 (the 602 finding): with a redactor set, a
// planted secret in plain, base64 and URL-encoded form appears neither in the retained file
// nor in the excerpt nor in Search results, for a small and a truncated output.
func TestRetainRedactsKnownSecrets(t *testing.T) {
	secret := "planted-S3CRET/value"
	planted := []string{secret, base64.StdEncoding.EncodeToString([]byte(secret)), url.QueryEscape(secret)}
	red := credentials.NewRedactor()
	if err := red.Add(secret); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store, err := NewStore(dir, 2048)
	if err != nil {
		t.Fatal(err)
	}
	store.SetRedactor(red)
	line := "FAIL token=" + strings.Join(planted, " ") + "\n"
	for _, input := range []string{line, strings.Repeat("noise\n", 4000) + line + strings.Repeat("noise\n", 4000) + line} {
		ex, err := store.Retain(strings.NewReader(input), "token")
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(dir, ex.Ref+".out"))
		if err != nil {
			t.Fatal(err)
		}
		matches, err := store.Search(ex.Ref, "token", 10)
		if err != nil || len(matches) == 0 {
			t.Fatalf("search %v %v", matches, err)
		}
		for _, f := range planted {
			if bytes.Contains(raw, []byte(f)) || strings.Contains(ex.Text, f) || strings.Contains(matches[0].Text, f) {
				t.Fatalf("form %q survived retention", f)
			}
		}
		if !strings.Contains(ex.Text, credentials.Redacted) || ex.Bytes != int64(len(raw)) {
			t.Fatalf("excerpt %q, bytes %d vs %d", ex.Text[:min(len(ex.Text), 200)], ex.Bytes, len(raw))
		}
	}
}
