package roles

import (
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writePack writes a manifest and skill files into a fresh pack directory.
func writePack(t *testing.T, manifest string, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "role-skills.json"), []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	for rel, text := range files {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// skillText builds a SKILL.md with the given name and body.
func skillText(name, body string) string { return "---\nname: " + name + "\n---\n" + body + "\n" }

// TestLoadShippedPack proves R1: the shipped pack loads, each role has its instructions,
// profile and an eligible skill with a sha256 digest.
func TestLoadShippedPack(t *testing.T) {
	reg, err := Load(filepath.Join("..", "..", "role-skills"))
	if err != nil {
		t.Fatal(err)
	}
	if reg.Version() != 1 {
		t.Fatalf("version = %d", reg.Version())
	}
	for _, name := range []string{"coordinator", "builder", "ops", "security", "auditor"} {
		role, err := reg.Role(name)
		if err != nil || role.Instructions == "" || role.Profile == "" || len(role.Skills) != 1 {
			t.Fatalf("role %s = %+v, %v", name, role, err)
		}
		if !strings.HasPrefix(role.Skills[0].Digest, "sha256:") || len(role.Skills[0].Digest) != 71 || role.Skills[0].Text == "" {
			t.Fatalf("skill %+v", role.Skills[0])
		}
	}
	if role, _ := reg.Role("builder"); role.Skills[0].ID != "synch-builder-delivery" {
		t.Fatalf("builder skill = %q", role.Skills[0].ID)
	}
	if _, err := reg.Role("wizard"); !errors.Is(err, ErrUnknownRole) {
		t.Fatalf("unknown role: %v", err)
	}
}

// TestLoadFailuresLoadNothing proves R1: an unknown role, a missing skill, a path leaving the
// pack, a symlink leaving the pack and a bad manifest each return a typed error and no registry.
func TestLoadFailuresLoadNothing(t *testing.T) {
	good := map[string]string{"skills/a/SKILL.md": skillText("a", "do a")}
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "SKILL.md"), []byte(skillText("evil", "x")), 0o600); err != nil {
		t.Fatal(err)
	}
	linked := writePack(t, `{"version":1,"roles":{"builder":["skills/link/SKILL.md"]}}`, nil)
	symlinkErr := os.MkdirAll(filepath.Join(linked, "skills"), 0o700)
	if symlinkErr == nil {
		symlinkErr = os.Symlink(outside, filepath.Join(linked, "skills", "link"))
	}
	type failure struct {
		name string
		dir  string
		want error
	}
	cases := []failure{
		{"unknown role", writePack(t, `{"version":1,"roles":{"wizard":["skills/a/SKILL.md"]}}`, good), ErrUnknownRole},
		{"missing skill", writePack(t, `{"version":1,"roles":{"builder":["skills/none/SKILL.md"]}}`, good), ErrMissingSkill},
		{"path escape", writePack(t, `{"version":1,"roles":{"builder":["../outside/SKILL.md"]}}`, good), ErrPathEscape},
		{"bad version", writePack(t, `{"version":2,"roles":{}}`, good), ErrBadPack},
		{"bad json", writePack(t, `{`, good), ErrBadPack},
		{"no name", writePack(t, `{"version":1,"roles":{"builder":["s.md"]}}`, map[string]string{"s.md": "no front matter"}), ErrBadPack},
		{"no manifest", t.TempDir(), ErrBadPack},
	}
	if symlinkErr == nil {
		cases = append(cases, failure{"symlink escape", linked, ErrPathEscape})
	}
	for _, c := range cases {
		reg, err := Load(c.dir)
		if !errors.Is(err, c.want) || reg != nil {
			t.Errorf("%s: registry %v, error %v, want %v", c.name, reg, err, c.want)
		}
	}
}

// TestPinResolveAndDrift proves R2: Pins selects ID plus sha256, Resolve returns the same
// text, a changed pack or an out-of-role pin is refused, and CRLF checkouts digest alike.
func TestPinResolveAndDrift(t *testing.T) {
	manifest := `{"version":1,"roles":{"builder":["b.md"],"ops":["o.md"]}}`
	dir := writePack(t, manifest, map[string]string{"b.md": skillText("b", "build"), "o.md": skillText("o", "ops")})
	reg, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	pins, err := reg.Pins("builder")
	if err != nil || len(pins) != 1 || pins[0].ID != "b" {
		t.Fatalf("pins = %+v, %v", pins, err)
	}
	skills, err := reg.Resolve("builder", pins)
	if err != nil || len(skills) != 1 || skills[0].Text != skillText("b", "build") {
		t.Fatalf("resolve = %+v, %v", skills, err)
	}
	if got := Engine(skills); got[0].ID != "b" || got[0].Digest != pins[0].Digest || got[0].Text != skills[0].Text {
		t.Fatalf("engine skill = %+v", got)
	}
	opsPins, _ := reg.Pins("ops")
	if _, err := reg.Resolve("builder", opsPins); !errors.Is(err, ErrMissingSkill) {
		t.Fatalf("out-of-role pin: %v", err)
	}
	if _, err := reg.Resolve("builder", []Pin{{ID: "b", Digest: "sha256:" + strings.Repeat("0", 64)}}); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("wrong digest: %v", err)
	}
	// The pack is edited after the pin: a fresh load must refuse the old pin, never upgrade it.
	if err := os.WriteFile(filepath.Join(dir, "b.md"), []byte(skillText("b", "build differently")), 0o600); err != nil {
		t.Fatal(err)
	}
	reg2, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg2.Resolve("builder", pins); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("drifted pack: %v", err)
	}
	crlf := writePack(t, manifest, map[string]string{"b.md": strings.ReplaceAll(skillText("b", "build"), "\n", "\r\n"), "o.md": skillText("o", "ops")})
	reg3, err := Load(crlf)
	if err != nil {
		t.Fatal(err)
	}
	if p3, _ := reg3.Pins("builder"); p3[0].Digest != pins[0].Digest {
		t.Fatalf("CRLF digest %s differs from %s", p3[0].Digest, pins[0].Digest)
	}
}

// TestChooseSpecialist proves R4 (decision 06): Builder, Ops and Security are chosen from the
// required outcome with a recorded reason, deterministically, and Security wins over Ops.
func TestChooseSpecialist(t *testing.T) {
	cases := []struct {
		name, brief string
		criteria    []string
		role        string
	}{
		{"builder", "Add pagination to the list endpoint", []string{"tests pass"}, "builder"},
		{"ops", "Roll out the new Jenkins pipeline", []string{"rollback plan recorded"}, "ops"},
		{"ops vm", "Resize the build VM", nil, "ops"},
		{"security", "Triage the Trivy findings", []string{"each finding has a rationale"}, "security"},
		{"security beats ops", "Verify the CVE-2026-1234 fix before the deployment", nil, "security"},
		{"no false vm match", "Rename vmware helper to hypervisor helper", nil, "builder"},
	}
	for _, c := range cases {
		got := Choose(c.brief, c.criteria)
		if got.Role != c.role || strings.TrimSpace(got.Reason) == "" {
			t.Errorf("%s: %+v, want role %s with a reason", c.name, got, c.role)
		}
		if again := Choose(c.brief, c.criteria); again != got {
			t.Errorf("%s: not deterministic: %+v then %+v", c.name, got, again)
		}
	}
}

// TestRolesCannotTouchAuthority proves R3 structurally: the package imports neither policy nor
// the thread machine, so loading a role or skill cannot change grants, rules or a thread's role.
func TestRolesCannotTouchAuthority(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range parsed.Imports {
			for _, banned := range []string{"internal/policy", "internal/thread", "internal/eventlog", "internal/children"} {
				if strings.Contains(imp.Path.Value, banned) {
					t.Errorf("%s imports %s", f, imp.Path.Value)
				}
			}
		}
	}
}
