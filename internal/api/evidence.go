package api

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"synch/internal/eventlog"
	"synch/internal/evidence"
)

// getEvidence builds the bundle with the daemon's live redactor.
func (s *Server) getEvidence(w http.ResponseWriter, r *http.Request, id string) {
	if !ulidPattern.MatchString(id) || s.config.Threads == nil || s.config.EvidenceRedactor == nil {
		writeProblem(w, newProblem(404, "not_found", ""))
		return
	}
	if _, err := s.config.Threads.Status(r.Context(), id); err != nil {
		writeProblem(w, newProblem(404, "not_found", ""))
		return
	}
	var events []eventlog.Event
	var cursor int64
	for {
		page, err := s.config.Store.ReadStream("thread:"+id, cursor, 1000)
		if err != nil {
			writeProblem(w, newProblem(500, "internal", ""))
			return
		}
		events = append(events, page...)
		if len(page) < 1000 {
			break
		}
		cursor = page[len(page)-1].Seq
	}
	root, err := os.MkdirTemp("", "synch-evidence-")
	if err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	defer func() { _ = os.RemoveAll(root) }()
	dir := filepath.Join(root, "bundle")
	if err = evidence.Export(dir, id, events, s.config.EvidenceRedactor); err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	manifest, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	err = filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		name, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		out, err := zw.Create(filepath.ToSlash(name))
		if err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(out, f)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
	if closeErr := zw.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	sum := sha256.Sum256(manifest)
	w.Header().Set("Synch-Manifest-Digest", fmt.Sprintf("sha256:%x", sum))
	writeBytes(w, 200, "application/zip", buf.Bytes())
}
