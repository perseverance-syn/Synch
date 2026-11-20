package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"synch/internal/credentials"
	"synch/internal/eventlog"
)

// DirectLimit describes the integrity limit of direct host access.
const DirectLimit = "Direct-mode evidence has the integrity limits of its host account"

// Manifest lists hashes of all evidence files; its digest is in manifest.sha256.
type Manifest struct {
	Thread      string            `json:"thread"`
	Files       map[string]string `json:"files"`
	Limitations []string          `json:"limitations"`
}

// Export writes journal-derived evidence through the supplied redactor before disk.
// The output directory must not already exist, preventing accidental overwrite.
func Export(dir, thread string, events []eventlog.Event, red *credentials.Redactor) error {
	if red == nil {
		return errors.New("evidence: redactor required")
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return fmt.Errorf("create bundle: %w", err)
	}
	if err := os.Mkdir(filepath.Join(dir, "diagnostics"), 0700); err != nil {
		return err
	}
	groups := map[string][]eventlog.Event{"journal.json": {}, "findings.json": {}, "checks.json": {}, "audit.json": {}, "approvals.json": {}, "revision.json": {}, "diagnostics/process.json": {}}
	direct := false
	limitations := []string{}
	for _, e := range events {
		if e.Type == "assignment.dispatched" && strings.Contains(string(e.Payload), `"direct"`) {
			direct = true
		}
		if e.Type == "child.result_recorded" {
			var p struct {
				Limitations []string `json:"limitations"`
			}
			if json.Unmarshal(e.Payload, &p) == nil {
				limitations = append(limitations, p.Limitations...)
				for _, l := range p.Limitations {
					if strings.Contains(strings.ToLower(l), "direct") {
						direct = true
					}
				}
			}
		}
		switch e.Type {
		case "engine.output", "process.exited":
			groups["diagnostics/process.json"] = append(groups["diagnostics/process.json"], e)
		default:
			groups["journal.json"] = append(groups["journal.json"], e)
		}
		var target string
		switch e.Type {
		case "finding.recorded":
			target = "findings.json"
		case "check.result":
			target = "checks.json"
		case "audit.status", "audit.setting_recorded":
			target = "audit.json"
		case "approval.recorded", "approval.voided":
			target = "approvals.json"
		case "plan.revised", "thread.state_changed", "assignment.dispatched":
			target = "revision.json"
		}
		if target != "" {
			groups[target] = append(groups[target], e)
		}
	}
	if direct {
		limitations = append(limitations, DirectLimit)
	}
	readme := "Thread evidence. Journal and named evidence are durable records. diagnostics/ contains bounded process output; truncation flags and references are preserved in its events.\nRevision and diff are limited to journal records; no diff is exposed by the event API.\n"
	if direct {
		readme += DirectLimit + "\nActions exceeding journal coverage (recorded limitations):\n" + strings.Join(limitations, "\n") + "\n"
	}
	files := map[string][]byte{"README.txt": []byte(readme)}
	for name, group := range groups {
		b, err := json.MarshalIndent(group, "", "  ")
		if err != nil {
			return err
		}
		files[name] = append(b, '\n')
	}
	hashes := map[string]string{}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		sum, err := write(dir, name, files[name], red)
		if err != nil {
			return err
		}
		hashes[name] = sum
	}
	manifest := Manifest{Thread: red.String(thread), Files: hashes, Limitations: limitations}
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	sum, err := write(dir, "manifest.json", b, red)
	if err != nil {
		return err
	}
	_, err = write(dir, "manifest.sha256", []byte(sum+"  manifest.json\n"), red)
	return err
}

// write redacts before writing and hashes the exact bytes stored on disk.
func write(dir, name string, data []byte, red *credentials.Redactor) (string, error) {
	f, err := os.OpenFile(filepath.Join(dir, filepath.FromSlash(name)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	w := red.Writer(io.MultiWriter(f, h))
	_, err = w.Write(data)
	closeErr := w.Close()
	fileErr := f.Close()
	if err != nil {
		return "", err
	}
	if closeErr != nil {
		return "", closeErr
	}
	if fileErr != nil {
		return "", fileErr
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Verify checks the manifest digest and every listed file, rejecting unsafe paths.
func Verify(dir string) error {
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return err
	}
	digest, err := os.ReadFile(filepath.Join(dir, "manifest.sha256"))
	if err != nil {
		return err
	}
	sum := sha256.Sum256(b)
	if string(digest) != hex.EncodeToString(sum[:])+"  manifest.json\n" {
		return errors.New("evidence: manifest digest mismatch")
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	for name, want := range m.Files {
		if !filepath.IsLocal(name) {
			return errors.New("evidence: unsafe manifest path")
		}
		b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
		if err != nil {
			return err
		}
		sum := sha256.Sum256(b)
		if hex.EncodeToString(sum[:]) != want {
			return fmt.Errorf("evidence: hash mismatch for %s", name)
		}
	}
	return nil
}
