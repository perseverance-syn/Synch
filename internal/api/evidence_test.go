package api

import (
	"archive/zip"
	"bytes"
	"io"
	"net/http"
	"strings"
	"synch/internal/credentials"
	"testing"
)

func TestEvidenceEndpoint(t *testing.T) {
	s, store, _ := fixture(t)
	id := create(t, s, "evidence")
	secret := "live-secret-value"
	appendThreadFact(t, store, id, "finding.recorded", 1, map[string]string{"text": secret})
	red := credentials.NewRedactor()
	if err := red.Add(secret); err != nil {
		t.Fatal(err)
	}
	s.config.EvidenceRedactor = red
	path := "/v1/threads/" + id + "/evidence"
	requireProblem(t, request(t, s, "GET", path, "", "", ""), 401, "unauthorized", "")
	got := request(t, s, "GET", path, testToken, "", "")
	if got.Code != http.StatusOK || got.Header().Get("Synch-Manifest-Digest") == "" {
		t.Fatalf("%d %s", got.Code, got.Body)
	}
	z, err := zip.NewReader(bytes.NewReader(got.Body.Bytes()), int64(got.Body.Len()))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range z.File {
		r, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), secret) {
			t.Fatal("secret leaked")
		}
	}
	requireProblem(t, request(t, s, "GET", "/v1/threads/invalid/evidence", testToken, "", ""), 404, "not_found", "")
}
