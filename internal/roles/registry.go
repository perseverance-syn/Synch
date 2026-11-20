package roles

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"synch/internal/engine"
)

// Typed load and resolve errors; callers match them with errors.Is.
var (
	// ErrUnknownRole reports a role the registry does not define.
	ErrUnknownRole = errors.New("roles: unknown role")
	// ErrMissingSkill reports a skill file that is absent or a pin naming a skill the role is not eligible for.
	ErrMissingSkill = errors.New("roles: missing skill")
	// ErrDigestMismatch reports skill text that no longer matches its pinned sha256.
	ErrDigestMismatch = errors.New("roles: skill digest mismatch")
	// ErrPathEscape reports a manifest path that leaves the pack directory.
	ErrPathEscape = errors.New("roles: skill path escapes the pack")
	// ErrBadPack reports an unreadable, unsupported or malformed pack.
	ErrBadPack = errors.New("roles: invalid role pack")
)

// maxSkillBytes bounds one skill file so a stray large file cannot flood a session.
const maxSkillBytes = 256 << 10

// definitions are the built-in role definitions: compiled in, so a worker cannot edit
// them. Profile only names the tool/policy profile; the daemon's policy decides what it allows.
var definitions = map[string]struct{ profile, instructions string }{
	"coordinator": {"coordinator", "You are the Coordinator. Plan, assign and reconcile through Synch; you never run tools or commands yourself."},
	"builder":     {"builder", "You are a Builder. Deliver the assigned change with evidence for the submitted revision."},
	"ops":         {"ops", "You are Ops. Diagnose or change infrastructure, CI and deployments under the plan the operator approves."},
	"security":    {"security", "You are Security. Triage defensive findings and verify fixes; route remediation through Synch."},
	"auditor":     {"auditor", "You are the Auditor. Independently assess the deliverable against the accepted brief and recorded evidence."},
}

// Skill is one instruction-only skill of a role: its ID (the SKILL.md front-matter name),
// the pack-relative path, the sha256 of the line-ending-normalized text, and the text.
type Skill struct {
	ID     string
	Path   string
	Digest string
	Text   string
}

// Pin identifies the exact skill text selected for an assignment: ID plus "sha256:<hex>".
type Pin struct {
	ID     string
	Digest string
}

// Role is a role definition: the instructions, the eligible skills and the profile name.
// Holding a Role grants nothing; tools, grants and approvals come from daemon policy.
type Role struct {
	Name         string
	Profile      string
	Instructions string
	Skills       []Skill
}

// Registry is a loaded, immutable role registry.
type Registry struct {
	version int
	roles   map[string]Role
}

// --- Loading: read the trusted pack, all or nothing ---

// Load reads role-skills.json and every skill file it lists under packDir. It is
// all-or-nothing: an unknown role, a missing skill or a path that escapes the pack
// returns a typed error and no registry.
func Load(packDir string) (*Registry, error) {
	raw, err := os.ReadFile(filepath.Join(packDir, "role-skills.json"))
	if err != nil {
		return nil, fmt.Errorf("%w: read manifest: %v", ErrBadPack, err)
	}
	var manifest struct {
		Version int                 `json:"version"`
		Roles   map[string][]string `json:"roles"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("%w: parse manifest: %v", ErrBadPack, err)
	}
	if manifest.Version != 1 {
		return nil, fmt.Errorf("%w: unsupported manifest version %d", ErrBadPack, manifest.Version)
	}
	root, err := filepath.EvalSymlinks(packDir)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBadPack, err)
	}
	reg := &Registry{version: manifest.Version, roles: map[string]Role{}}
	for name, files := range manifest.Roles {
		def, ok := definitions[name]
		if !ok {
			return nil, fmt.Errorf("%w: manifest lists %q", ErrUnknownRole, name)
		}
		role := Role{Name: name, Profile: def.profile, Instructions: def.instructions}
		for _, rel := range files {
			skill, err := loadSkill(root, rel)
			if err != nil {
				return nil, fmt.Errorf("role %q: %w", name, err)
			}
			role.Skills = append(role.Skills, skill)
		}
		reg.roles[name] = role
	}
	return reg, nil
}

// loadSkill reads one skill file, refusing any path (including through a symlink)
// that resolves outside the pack root. The trust boundary is the pack, not the manifest.
func loadSkill(root, rel string) (Skill, error) {
	if !filepath.IsLocal(filepath.FromSlash(rel)) {
		return Skill{}, fmt.Errorf("%w: %q", ErrPathEscape, rel)
	}
	path, err := filepath.EvalSymlinks(filepath.Join(root, filepath.FromSlash(rel)))
	if errors.Is(err, os.ErrNotExist) {
		return Skill{}, fmt.Errorf("%w: %q", ErrMissingSkill, rel)
	}
	if err != nil {
		return Skill{}, fmt.Errorf("%w: %q: %v", ErrBadPack, rel, err)
	}
	if inside, err := filepath.Rel(root, path); err != nil || !filepath.IsLocal(inside) {
		return Skill{}, fmt.Errorf("%w: %q", ErrPathEscape, rel)
	}
	f, err := os.Open(path)
	if err != nil {
		return Skill{}, fmt.Errorf("%w: %q: %v", ErrMissingSkill, rel, err)
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, maxSkillBytes+1))
	if err != nil || len(data) > maxSkillBytes || !utf8.Valid(data) {
		return Skill{}, fmt.Errorf("%w: %q is unreadable, too large or not UTF-8", ErrBadPack, rel)
	}
	// Normalize line endings so a Windows checkout and a Linux one digest the same.
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	id := skillName(text)
	if id == "" {
		return Skill{}, fmt.Errorf("%w: %q has no name in its front matter", ErrBadPack, rel)
	}
	return Skill{ID: id, Path: rel, Digest: digest(text), Text: text}, nil
}

// skillName returns the "name:" value of the SKILL.md front matter, or "".
func skillName(text string) string {
	rest, ok := strings.CutPrefix(text, "---\n")
	if !ok {
		return ""
	}
	front, _, _ := strings.Cut(rest, "\n---")
	for _, line := range strings.Split(front, "\n") {
		if v, ok := strings.CutPrefix(line, "name:"); ok {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// digest returns the "sha256:<hex>" form used by the catalogue and the adapters.
func digest(text string) string { return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(text))) }

// --- Lookup and pinning: select skills at assignment time, re-verify them later ---

// Version is the pack manifest version the registry was loaded from.
func (r *Registry) Version() int { return r.version }

// Role returns the named role, or ErrUnknownRole.
func (r *Registry) Role(name string) (Role, error) {
	role, ok := r.roles[name]
	if !ok {
		return Role{}, fmt.Errorf("%w: %q", ErrUnknownRole, name)
	}
	return role, nil
}

// Pins returns the pin of every eligible skill of the role, in manifest order. This is
// the selection made at assignment time.
func (r *Registry) Pins(name string) ([]Pin, error) {
	role, err := r.Role(name)
	if err != nil {
		return nil, err
	}
	pins := make([]Pin, 0, len(role.Skills))
	for _, s := range role.Skills {
		pins = append(pins, Pin{ID: s.ID, Digest: s.Digest})
	}
	return pins, nil
}

// Resolve returns the skills a recorded pin set names, verifying each against the
// pack. A skill the role is not eligible for is ErrMissingSkill and a changed text is
// ErrDigestMismatch: a pin is never silently upgraded, which keeps rollover and
// harness handoff on the text the assignment started with.
func (r *Registry) Resolve(name string, pins []Pin) ([]Skill, error) {
	role, err := r.Role(name)
	if err != nil {
		return nil, err
	}
	out := make([]Skill, 0, len(pins))
	for _, p := range pins {
		found := false
		for _, s := range role.Skills {
			if s.ID != p.ID {
				continue
			}
			if s.Digest != p.Digest {
				return nil, fmt.Errorf("%w: %q pinned %s, pack has %s", ErrDigestMismatch, p.ID, p.Digest, s.Digest)
			}
			out, found = append(out, s), true
		}
		if !found {
			return nil, fmt.Errorf("%w: %q is not eligible for role %q", ErrMissingSkill, p.ID, name)
		}
	}
	return out, nil
}

// Engine converts skills to the engine's pinned-skill form for StartRequest.Skills.
func Engine(skills []Skill) []engine.PinnedSkill {
	out := make([]engine.PinnedSkill, 0, len(skills))
	for _, s := range skills {
		out = append(out, engine.PinnedSkill{ID: s.ID, Digest: s.Digest, Text: s.Text})
	}
	return out
}
