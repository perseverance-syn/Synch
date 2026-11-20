package policy

import (
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// --- Matching: actions (§7) ---

// foldPaths makes path matching case-insensitive; on by default on Windows (Q-03).
var foldPaths = runtime.GOOS == "windows"

// matchAction reports whether a rule pattern matches an exact action. "*"
// matches everything; "a.*" matches "a." followed by one or more segments.
func matchAction(pattern, action string) bool {
	if pattern == "*" {
		return true
	}
	if p, ok := strings.CutSuffix(pattern, ".*"); ok {
		return strings.HasPrefix(action, p+".") && len(action) > len(p)+1
	}
	return pattern == action
}

// --- Paths: lexical and resolved forms, containment (§8, D-14) ---

// pathForm is one absolute path in the two forms D-14 needs.
type pathForm struct {
	lex string // cleaned lexically, slash-separated, folded
	res string // symlinks resolved for the existing part, remainder cleaned, folded
	ok  bool   // resolution succeeded
}

// formOf builds both forms of p. Relative paths are refused because they
// cannot be matched safely.
func formOf(p string) (pathForm, bool) {
	p, ok := plainWindowsPath(p)
	if !ok || !filepath.IsAbs(p) {
		return pathForm{}, false
	}
	f := pathForm{lex: fold(filepath.ToSlash(filepath.Clean(p)))}
	if r, err := resolvePath(p); err == nil {
		f.res, f.ok = fold(filepath.ToSlash(r)), true
	}
	return f, true
}

// fold lower-cases a path when matching is case-insensitive.
func fold(p string) string {
	if foldPaths {
		return strings.ToLower(p)
	}
	return p
}

// maxLinkHops bounds link following so a link loop fails resolution.
const maxLinkHops = 255

// resolvePath applies D-14: walk p component by component, following every
// link in the existing part, then clean the non-existent remainder
// lexically. It walks itself instead of calling filepath.EvalSymlinks because
// EvalSymlinks does not follow Windows directory junctions, which any worker
// can create without privilege. Symlinks and junctions are followed through
// os.Readlink; any other reparse point (ModeIrregular that cannot be read)
// fails resolution, as does any error other than "does not exist". The
// existing part is finally normalized by EvalSymlinks (8.3 short names, case).
// The raw path is never cleaned first: "link/.." is the target's parent.
func resolvePath(p string) (string, error) {
	sep := string(filepath.Separator)
	split := func(s string) []string {
		return strings.FieldsFunc(s, func(r rune) bool { return r == '/' || r == filepath.Separator })
	}
	vol := filepath.VolumeName(p)
	cur, todo := vol+sep, split(p[len(vol):])
	for hops := 0; len(todo) > 0; {
		c := todo[0]
		todo = todo[1:]
		switch c {
		case ".":
			continue
		case "..": // cur has no links left, so its lexical parent is its real parent
			cur = filepath.Dir(cur)
			continue
		}
		next := filepath.Join(cur, c)
		fi, err := os.Lstat(next)
		if errors.Is(err, fs.ErrNotExist) {
			base, err := filepath.EvalSymlinks(cur)
			if err != nil {
				return "", err
			}
			return filepath.Clean(base + sep + strings.Join(append([]string{c}, todo...), sep)), nil
		}
		if err != nil {
			return "", err
		}
		if fi.Mode()&(fs.ModeSymlink|fs.ModeIrregular) == 0 {
			cur = next
			continue
		}
		if hops++; hops > maxLinkHops {
			return "", errors.New("too many links")
		}
		target, err := os.Readlink(next)
		if err != nil {
			return "", err
		}
		target, ok := plainWindowsPath(target) // a link target may use the extended spelling
		if !ok {
			return "", errors.New("link target in an unsupported namespace")
		}
		tv := filepath.VolumeName(target)
		switch {
		case tv != "": // absolute with a volume (or UNC) on Windows
		case strings.HasPrefix(target, "/") || strings.HasPrefix(target, sep): // rooted: same volume
			tv = vol
			target = vol + target
		default: // relative to the link's directory
			target = cur + sep + target
			tv = filepath.VolumeName(target)
		}
		cur, todo = tv+sep, append(split(target[len(tv):]), todo...)
	}
	return filepath.EvalSymlinks(cur)
}

// matchPath applies one rule glob to one request path (D-14). An allow rule
// matches only on the resolved form, so a path that escapes through a link or
// fails to resolve is never allowed by a path-scoped allow. A deny or ask rule
// matches on either form, so a path that lexically sits in the denied area
// stays denied even if a link points it elsewhere.
func matchPath(glob string, p pathForm, mediated []pathForm, eff Effect) bool {
	res := p.ok && matchOne(glob, p.res, mediated, true)
	if eff == Allow {
		return res
	}
	return res || matchOne(glob, p.lex, mediated, false)
}

// matchOne matches a single path form. "@mediated" means containment in any
// of the thread's managed working copies, compared in the same form.
func matchOne(glob, p string, mediated []pathForm, resolved bool) bool {
	if glob == "@mediated" {
		for _, m := range mediated {
			root := m.lex
			if resolved {
				if !m.ok {
					continue
				}
				root = m.res
			}
			if p == root || strings.HasPrefix(p, strings.TrimSuffix(root, "/")+"/") {
				return true
			}
		}
		return false
	}
	g, supported := plainGlob(glob) // a rule spelled \\?\C:\... matches ordinary paths (G4)
	if !supported {
		return false
	}
	ok, _ := path.Match(fold(filepath.ToSlash(g)), p) // patterns were validated at load
	return ok
}

// --- Evaluation: Effective = max(O, R, B) (§1) and built-in gates (§5.4) ---

// subject is everything a rule can match on for one evaluation.
type subject struct {
	action, target, host, role, mode string
	args                             []string
	paths, mediated                  []pathForm
	// creation is set for approval creation (§5.1), where host, args and paths
	// are not known yet. Those dimensions then count as matching (fail closed:
	// a constraint that might apply counts); action, target, role and mode
	// are known and must match.
	creation bool
}

// constraint is one matching ask rule or gate with its TTL limit.
type constraint struct {
	qid string
	ttl time.Duration
}

// result is the outcome of rule evaluation for one action.
type result struct {
	effect Effect
	ruleID string       // qualified id that decided the effect, "" when O defaulted to deny
	asks   []constraint // every matching ask rule (all sources) plus the gate
}

// minTTL returns the strictest TTL over the constraints, or false if none.
func (r result) minTTL() (time.Duration, bool) {
	var best time.Duration
	for i, c := range r.asks {
		if i == 0 || c.ttl < best {
			best = c.ttl
		}
	}
	return best, len(r.asks) > 0
}

// operational lists the actions that need a plan-digest approval (§5.1).
var operational = []string{"net.firewall_change", "net.network_change", "deploy.production"}

// gate returns the built-in gate for an action, if any (§5.4, Q-53, Q-54).
func gate(action string) (constraint, bool) {
	switch {
	case in(action, operational):
		return constraint{"builtin:gates#operational", 4 * time.Hour}, true
	case strings.HasPrefix(action, "deploy."):
		return constraint{"builtin:gates#deployment", 4 * time.Hour}, true
	}
	return constraint{}, false
}

// evaluate computes Effective = max(O, R, B). No matching operator rule means
// O = deny (Q-01); no matching restriction means R = allow. Restriction files
// cannot hold allow rules (checked at load), so they can never raise authority.
func evaluate(files []*File, s subject) result {
	type part struct {
		eff  Effect
		rule string
	}
	o, r, b := part{eff: Deny}, part{eff: Allow}, part{eff: Allow}
	oMatched := false
	var asks []constraint
	for _, f := range files {
		for _, rule := range f.rules {
			if !rule.matches(s) {
				continue
			}
			if rule.Effect == Ask {
				asks = append(asks, constraint{rule.QID, rule.MaxTTL})
			}
			p := &r
			if f.source == "operator" {
				p = &o
				if !oMatched { // the first operator match replaces the deny default
					oMatched = true
					*p = part{rule.Effect, rule.QID}
					continue
				}
			}
			if rule.Effect > p.eff {
				*p = part{rule.Effect, rule.QID}
			}
		}
	}
	if g, ok := gate(s.action); ok {
		b = part{Ask, g.qid}
		asks = append(asks, g)
	}
	// The deciding rule is the first part (operator, restriction, gate) at the
	// effective level; a defaulted operator deny defers to a named deny rule.
	best := o
	for _, p := range []part{r, b} {
		if p.eff > best.eff || best.rule == "" && p.eff == best.eff {
			best = p
		}
	}
	return result{effect: best.eff, ruleID: best.rule, asks: asks}
}

// matches reports whether the rule applies to the subject. Every present
// match dimension must hold; a missing dimension means "any" and an explicitly
// empty list matches nothing (D-13 for args, the same rule for every list).
func (r Rule) matches(s subject) bool {
	hit := false
	for _, p := range r.Actions {
		hit = hit || matchAction(p, s.action)
	}
	m := r.Match
	if !hit || !listHas(m.Targets, s.target) || !listHas(m.Roles, s.role) || !listHas(m.Modes, s.mode) {
		return false
	}
	if s.creation {
		// Unknown dimensions count as matching, except a provably impossible
		// one: an explicitly empty list never matches any request (G5, D-13).
		return !isEmptyList(m.Hosts) && !isEmptyList(m.Args) && !isEmptyList(m.Paths)
	}
	if !listHas(m.Hosts, s.host) {
		return false
	}
	if m.Args != nil {
		if len(m.Args) == 0 { // D-13: an explicitly empty list matches nothing
			return false
		}
		for _, a := range m.Args { // D-13: every listed arg must be present (AND)
			if !in(a, s.args) {
				return false
			}
		}
	}
	if m.Paths == nil {
		return true
	}
	for _, p := range s.paths {
		for _, g := range m.Paths {
			if matchPath(g, p, s.mediated, r.Effect) {
				return true
			}
		}
	}
	return false
}

// isEmptyList reports a present but empty list dimension, which matches nothing.
func isEmptyList(list []string) bool { return list != nil && len(list) == 0 }

// listHas applies one optional list dimension: nil means any, otherwise v must be listed.
func listHas(list []string, v string) bool {
	return list == nil || in(v, list)
}
