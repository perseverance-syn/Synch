package policy

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- AC2/AC3: loader, trusted source, contract fixtures ---

// TestLoadContractExamples proves AC2: all three contracts/policy/examples load from the operator store.
func TestLoadContractExamples(t *testing.T) {
	paths, _ := filepath.Glob("../../contracts/policy/examples/*.json")
	if len(paths) != 3 {
		t.Fatalf("want 3 examples, got %d", len(paths))
	}
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		f, err := Load(data, Origin{Kind: OriginStore})
		if err != nil {
			t.Errorf("%s: %v", p, err)
			continue
		}
		for _, r := range f.Rules() {
			if !strings.HasPrefix(r.QID, "operator:store#") {
				t.Errorf("%s: qualified id %q", p, r.QID)
			}
		}
	}
}

// TestContractCases proves AC2: every rule-file case in contracts/tests/cases.json gets its expected verdict.
func TestContractCases(t *testing.T) {
	data, err := os.ReadFile("../../contracts/tests/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Bases map[string]any `json:"bases"`
		Cases []struct {
			Name, Schema, Expect string
			Data                 any
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, c := range doc.Cases {
		if c.Schema != "policy" {
			continue
		}
		n++
		raw, _ := json.Marshal(resolveBase(c.Data, doc.Bases))
		_, err := Load(raw, originOf(raw))
		if got := map[bool]string{true: "valid", false: "invalid"}[err == nil]; got != c.Expect {
			t.Errorf("%s: got %s (%v), want %s", c.Name, got, err, c.Expect)
		}
	}
	if n < 19 {
		t.Fatalf("only %d policy cases found", n)
	}
}

// resolveBase expands the fixture "$base"/"$delete" shorthand: base copy, deletions, then overrides.
func resolveBase(v any, bases map[string]any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		if b, ok := x["$base"].(string); ok {
			for k, bv := range bases[b].(map[string]any) {
				out[k] = bv
			}
		}
		if dels, ok := x["$delete"].([]any); ok {
			for _, d := range dels {
				delete(out, d.(string))
			}
		}
		for k, xv := range x {
			if k != "$base" && k != "$delete" {
				out[k] = resolveBase(xv, bases)
			}
		}
		return out
	case []any:
		for i := range x {
			x[i] = resolveBase(x[i], bases)
		}
	}
	return v
}

// TestTrustedSource proves AC2 §2: the origin, not the file, decides; mismatches are rejected, never downgraded.
func TestTrustedSource(t *testing.T) {
	file := func(src string) []byte {
		return []byte(`{"policy_version":3,"source":"` + src + `","rules":[{"id":"r","effect":"deny","actions":["x.y"]}]}`)
	}
	project := Origin{Kind: OriginProject, Project: "proj-1", Revision: rev1}
	pack := Origin{Kind: OriginPack, PackFile: "ops.json", Digest: "sha256:0123456789ab" + strings.Repeat("c", 52)}
	bad := []struct {
		src string
		o   Origin
	}{
		{"operator", project}, {"operator", pack}, {"operator", Origin{}},
		{"project", Origin{Kind: OriginStore}}, {"project", pack}, {"project", Origin{Kind: OriginProject, Project: "p#1", Revision: rev1}},
		{"project", Origin{Kind: OriginProject, Project: "p", Revision: "abc"}},
		{"skill", project}, {"role", Origin{Kind: OriginStore}}, {"skill", Origin{Kind: OriginPack, PackFile: "x", Digest: "bad"}},
	}
	for _, b := range bad {
		if _, err := Load(file(b.src), b.o); !errors.Is(err, ErrSourceMismatch) {
			t.Errorf("%s from %+v: got %v", b.src, b.o, err)
		}
	}
	for src, want := range map[string]string{
		"operator": "operator:store#r",
		"project":  "project:proj-1@" + rev1 + "#r",
		"skill":    "skill:ops.json@0123456789ab#r",
		"role":     "role:ops.json@0123456789ab#r",
	} {
		o := map[string]Origin{"operator": {Kind: OriginStore}, "project": project, "skill": pack, "role": pack}[src]
		f, err := Load(file(src), o)
		if err != nil || f.Rules()[0].QID != want || !qualifiedRe.MatchString(f.Rules()[0].QID) {
			t.Errorf("%s: %v %+v", src, err, f)
		}
	}
}

// TestStrictDecoding proves AC2: strict decoding rejects what the schema forbids but encoding/json accepts.
func TestStrictDecoding(t *testing.T) {
	for _, src := range []string{
		`{"policy_version":3,"source":"operator","rules":null}`,
		`{"policy_version":3,"source":"operator"}`,
		`{"policy_version":3,"source":"operator","rules":[],"extra":1}`,
		`{"policy_version":3,"source":"operator","Rules":[]}`,
		`{"policy_version":3,"source":"operator","rules":[],"rules":[]}`,
		`{"policy_version":3,"source":"operator","rules":[]} {}`,
		`{"policy_version":2,"source":"operator","rules":[]}`,
		`{"policy_version":3,"source":"operator","rules":[{"id":"r","effect":"deny","actions":[]}]}`,
		`{"policy_version":3,"source":"operator","rules":[{"id":"r","effect":"deny","actions":["x.*.y"]}]}`,
		`{"policy_version":3,"source":"operator","rules":[{"id":"r","effect":"deny","actions":["x"],"approval":{"max_ttl":"PT1H"}}]}`,
		`{"policy_version":3,"source":"operator","rules":[{"id":"r","effect":"deny","actions":["x"],"match":{"roles":["root"]}}]}`,
		`{"policy_version":3,"source":"operator","rules":[{"id":"r","effect":"deny","actions":["x"],"match":{"paths":["[a"]}}]}`,
		`{"policy_version":3,"source":"project","rules":[{"id":"r","effect":"allow","actions":["x"]}]}`,
		`{"policy_version":3,"source":"role","rules":[{"id":"r","effect":"allow","actions":["x"]}]}`,
	} {
		if _, err := Load([]byte(src), originOf([]byte(src))); err == nil {
			t.Errorf("accepted %s", src)
		}
	}
}

// TestDurations proves the $defs/iso_duration rules used for max_ttl.
func TestDurations(t *testing.T) {
	good := map[string]time.Duration{"PT4H": 4 * time.Hour, "PT5M": 5 * time.Minute, "P1DT2H30M": 26*time.Hour + 30*time.Minute, "P2D": 48 * time.Hour, "PT10S": 10 * time.Second}
	for s, want := range good {
		if got, err := parseDuration(s); err != nil || got != want {
			t.Errorf("%s: %v %v", s, got, err)
		}
	}
	for _, s := range []string{"P", "PT", "PT1H1H", "P1DT", "PT1M1H", "1H", "PT-1H", "P1W", "PT99999999999999999999H", "PT9999999999999H"} {
		if _, err := parseDuration(s); err == nil {
			t.Errorf("accepted %s", s)
		}
	}
}

// TestFileGrantConfersNothing proves AC3: a file carrying a grant (and a revocation) confers no authority.
func TestFileGrantConfersNothing(t *testing.T) {
	data, err := os.ReadFile("../../contracts/policy/examples/full-control-inherit-revoke.json")
	if err != nil {
		t.Fatal(err)
	}
	e, st, _ := fixture(t)
	st.files = []*File{mustLoad(t, string(data))}
	st.modes[rootID] = "full_control"
	d, err := e.Decide(context.Background(), Request{Thread: rootID, Action: "host.exec", Machine: "build-01", Identity: `BUILD-01\synch-ops`})
	if err != nil || d.Allowed || d.Effect != Allow || d.GrantID != "" {
		t.Fatalf("file grant conferred authority: %+v %v", d, err)
	}
}

// --- AC8: matchers (§7) and paths (§8) ---

// TestMatchAction proves AC8 §7: ".*" needs one or more further segments; bare "*" matches all.
func TestMatchAction(t *testing.T) {
	cases := []struct {
		p, a string
		want bool
	}{
		{"*", "git.push", true}, {"*", "merge", true},
		{"git.*", "git.push", true}, {"git.*", "git", false}, {"git.*", "gitx.push", false},
		{"host.*", "host.a.b", true}, {"git.push", "git.push", true}, {"git.push", "git.push_force", false},
	}
	for _, c := range cases {
		if matchAction(c.p, c.a) != c.want {
			t.Errorf("%s vs %s", c.p, c.a)
		}
	}
}

// TestPaths proves AC8 §8: @mediated, normalized absolute paths, case folding on Windows, relative paths refused.
func TestPaths(t *testing.T) {
	root := t.TempDir()
	deny := `{"policy_version":3,"source":"operator","rules":[
		{"id":"d","effect":"deny","actions":["git.reset_hard"],"match":{"paths":["@mediated"]}},
		{"id":"a","effect":"allow","actions":["git.*"]}]}`
	e, st, _ := fixture(t, deny)
	ti := st.threads[rootID]
	ti.Mediated = []string{filepath.Join(root, "wc")}
	st.threads[rootID] = ti
	try := func(p string) Decision {
		d, _ := e.Decide(context.Background(), Request{Thread: rootID, Action: "git.reset_hard", Machine: "m", Paths: []string{p}})
		return d
	}
	if d := try(filepath.Join(root, "wc", "sub", "..", "f")); d.Allowed || d.RuleID != "operator:store#d" {
		t.Errorf("inside mediated copy: %+v", d)
	}
	if d := try(filepath.Join(root, "wc2")); !d.Allowed {
		t.Errorf("sibling directory matched @mediated: %+v", d)
	}
	if d := try(filepath.Join("rel", "wc")); d.Allowed {
		t.Errorf("relative path allowed: %+v", d)
	}
	old := foldPaths
	foldPaths = true
	defer func() { foldPaths = old }()
	if d := try(strings.ToUpper(filepath.Join(root, "wc", "f"))); d.Allowed {
		t.Errorf("case-folded path escaped deny: %+v", d)
	}
}
