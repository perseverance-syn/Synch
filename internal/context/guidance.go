package context

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"synch/internal/engine"
)

// maxGuidanceBytes bounds one guidance file so a stray large file cannot flood a session.
const maxGuidanceBytes = 256 << 10

// Block is one piece of guidance with its origin and content digest.
type Block struct {
	// Kind is "role", "project", "target" or "handoff".
	Kind string
	// Source names where the text came from (a file path).
	Source string
	// Digest is "sha256:<hex>" of Text.
	Digest string
	Text   string
	// Stable blocks never change between sessions of the same role and project, so
	// they come first and keep the harness prompt cache valid.
	Stable bool
}

// Inputs selects the guidance for one session. An empty field loads nothing.
type Inputs struct {
	// PackDir is the role-skills pack directory holding role-skills.json.
	PackDir string
	// Role is the Synch role whose skills are loaded (builder, ops, security, ...).
	Role string
	// ProjectFile and TargetFile are absolute paths of optional guidance files.
	ProjectFile, TargetFile string
}

// Guidance loads the role, project and target blocks in that fixed order. The same
// inputs always give byte-identical blocks. A configured file that cannot be read
// is an error, never silently skipped.
func Guidance(in Inputs) ([]Block, error) {
	var blocks []Block
	if in.PackDir != "" && in.Role != "" {
		role, err := roleBlocks(in.PackDir, in.Role)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, role...)
	}
	for _, f := range []struct{ kind, path string }{{"project", in.ProjectFile}, {"target", in.TargetFile}} {
		if f.path == "" {
			continue
		}
		text, err := readBounded(f.path)
		if err != nil {
			return nil, fmt.Errorf("load %s guidance: %w", f.kind, err)
		}
		blocks = append(blocks, newBlock(f.kind, f.path, text, true))
	}
	return blocks, nil
}

// roleBlocks reads the pack manifest and returns the role's skill files in manifest order.
// A path that leaves the pack directory is refused: the pack is trusted, but a
// manifest must not be able to pull in other files.
func roleBlocks(packDir, role string) ([]Block, error) {
	raw, err := os.ReadFile(filepath.Join(packDir, "role-skills.json"))
	if err != nil {
		return nil, fmt.Errorf("read role pack: %w", err)
	}
	var manifest struct {
		Roles map[string][]string `json:"roles"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("parse role pack: %w", err)
	}
	files, ok := manifest.Roles[role]
	if !ok {
		return nil, fmt.Errorf("role pack has no role %q", role)
	}
	var blocks []Block
	for _, rel := range files {
		if !filepath.IsLocal(filepath.FromSlash(rel)) {
			return nil, fmt.Errorf("role pack path %q leaves the pack", rel)
		}
		path := filepath.Join(packDir, filepath.FromSlash(rel))
		text, err := readBounded(path)
		if err != nil {
			return nil, fmt.Errorf("load role %q skill: %w", role, err)
		}
		blocks = append(blocks, newBlock("role", filepath.ToSlash(rel), text, true))
	}
	return blocks, nil
}

// readBounded reads a whole guidance file and refuses one over maxGuidanceBytes.
func readBounded(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	raw, err := io.ReadAll(io.LimitReader(f, maxGuidanceBytes+1))
	if err != nil {
		return "", err
	}
	if len(raw) > maxGuidanceBytes {
		return "", fmt.Errorf("%s exceeds %d bytes", path, maxGuidanceBytes)
	}
	// Normalize line endings so a Windows checkout and a Linux one digest the same.
	return strings.ReplaceAll(string(raw), "\r\n", "\n"), nil
}

// newBlock builds a block and its digest.
func newBlock(kind, source, text string, stable bool) Block {
	return Block{Kind: kind, Source: source, Digest: digest([]byte(text)), Text: text, Stable: stable}
}

// digest returns the "sha256:<hex>" form used across Synch records.
func digest(raw []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(raw)) }

// Instruction renders the block with a one-line header naming its source and digest,
// so a transcript shows exactly which guidance a session was given.
func (b Block) Instruction() engine.Instruction {
	return engine.Instruction{
		Text:   fmt.Sprintf("[synch:%s source=%s digest=%s]\n%s", b.Kind, b.Source, b.Digest, b.Text),
		Stable: b.Stable,
	}
}

// Assemble returns the session instructions: every stable block in the order given,
// then the caller's stable instructions, then the caller's changing ones. Stable
// text always precedes changing text, which is what the engine contract requires.
func Assemble(blocks []Block, extra []engine.Instruction) []engine.Instruction {
	var out, changing []engine.Instruction
	for _, b := range blocks {
		if b.Stable {
			out = append(out, b.Instruction())
		} else {
			changing = append(changing, b.Instruction())
		}
	}
	for _, in := range extra {
		if in.Stable {
			out = append(out, in)
		} else {
			changing = append(changing, in)
		}
	}
	return append(out, changing...)
}

// ErrNotHandoff reports a path that is not a content-addressed handoff file.
var ErrNotHandoff = errors.New("context: not a handoff file")
