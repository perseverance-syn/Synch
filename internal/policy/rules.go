package policy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// --- Types: effects, sources, origins and loaded rules (rule-file.md §1–2) ---

// Effect is a rule result. The order allow < ask < deny is the comparison order.
type Effect int

const (
	Allow Effect = iota
	Ask
	Deny
)

// String returns the contract spelling of the effect.
func (e Effect) String() string {
	return [...]string{"allow", "ask", "deny"}[e]
}

// OriginKind names where a rule file physically came from. The caller sets it;
// it is never read from the file (rule-file.md §2).
type OriginKind int

const (
	// OriginStore is the daemon's operator policy store.
	OriginStore OriginKind = iota + 1
	// OriginProject is a registered project's policy path at an accepted revision.
	OriginProject
	// OriginPack is a packaged, digest-pinned role-skill pack file.
	OriginPack
)

// Origin describes where a rule file came from.
type Origin struct {
	Kind     OriginKind
	Project  string // OriginProject: project id
	Revision string // OriginProject: accepted revision (full commit id)
	PackFile string // OriginPack: pack file name
	Digest   string // OriginPack: "sha256:<64 hex>" pin of the file
}

// File is a loaded, trusted rule file. Only Load creates one, so the engine can
// rely on its source having been checked against its origin.
type File struct {
	source   string
	originID string
	rules    []Rule
}

// Source returns the verified source of the file.
func (f *File) Source() string { return f.source }

// Rules returns the file's rules with their qualified ids.
// It returns a deep copy, so no caller can change live policy (R104).
func (f *File) Rules() []Rule {
	out := make([]Rule, len(f.rules))
	for i, r := range f.rules {
		r.Actions = slices.Clone(r.Actions)
		m := &r.Match
		m.Paths, m.Targets, m.Hosts = slices.Clone(m.Paths), slices.Clone(m.Targets), slices.Clone(m.Hosts)
		m.Roles, m.Modes, m.Args = slices.Clone(m.Roles), slices.Clone(m.Modes), slices.Clone(m.Args)
		out[i] = r
	}
	return out
}

// Rule is one loaded rule.
type Rule struct {
	QID     string // <source>:<origin-id>#<rule-id>
	Effect  Effect
	Actions []string
	Match   Match
	MaxTTL  time.Duration // ask rules only
}

// Match holds the optional match dimensions. A nil slice means "any"; a
// non-nil empty slice matches nothing.
type Match struct {
	Paths   []string `json:"paths"`
	Targets []string `json:"targets"`
	Hosts   []string `json:"hosts"`
	Roles   []string `json:"roles"`
	Modes   []string `json:"modes"`
	Args    []string `json:"args"`
}

// Sentinel errors callers can test with errors.Is.
var (
	ErrInvalidFile    = errors.New("policy: invalid rule file")
	ErrSourceMismatch = errors.New("policy: declared source does not match origin")
)

// --- Wire form: strict decoding targets mirroring rule-file.schema.json ---

// fileJSON is the decoding target for a rule file.
type fileJSON struct {
	PolicyVersion json.RawMessage `json:"policy_version"`
	ID            string          `json:"id"`
	Source        string          `json:"source"`
	Description   string          `json:"description"`
	Rules         *[]ruleJSON     `json:"rules"`
	Grants        *[]Grant        `json:"grants"`
	Revocations   *[]revocationJS `json:"revocations"`
	Approvals     *[]Approval     `json:"approvals"`
}

// ruleJSON is the decoding target for one rule.
type ruleJSON struct {
	ID       string    `json:"id"`
	Effect   string    `json:"effect"`
	Actions  []string  `json:"actions"`
	Match    *Match    `json:"match"`
	Approval *struct { // required for ask, forbidden otherwise
		MaxTTL string `json:"max_ttl"`
	} `json:"approval"`
	Reason string `json:"reason"`
}

// revocationJS is the export form of a revocation (fixture only, §3).
type revocationJS struct {
	Grant     string    `json:"grant"`
	Scope     string    `json:"scope"`
	RevokedBy string    `json:"revoked_by"`
	RevokedAt time.Time `json:"revoked_at"`
}

var (
	ruleIDRe     = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)
	patternRe    = regexp.MustCompile(`^(\*|[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*(\.\*)?)$`)
	actionRe     = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$`)
	ulidRe       = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)
	sha256Re     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	revisionRe   = regexp.MustCompile(`^[0-9a-f]{40}([0-9a-f]{24})?$`)
	qualifiedRe  = regexp.MustCompile(`^(operator|project|role|skill|builtin):[^# ]+#[a-z0-9][a-z0-9._-]*$`)
	durationRe   = regexp.MustCompile(`^P(?:([0-9]+)D)?(?:T(?:([0-9]+)H)?(?:([0-9]+)M)?(?:([0-9]+)S)?)?$`)
	wireKeyRe    = regexp.MustCompile(`^[a-z_]+$`)
	roleValues   = []string{"coordinator", "builder", "ops", "security", "auditor"}
	modeValues   = []string{"sandbox", "provider", "full_control"}
	restrictions = []string{"project", "role", "skill"}
)

// --- Loader: strict decode, schema-equivalent checks, trusted source (§2, §3) ---

// Load decodes and validates a rule file, checks its declared source against
// the caller-supplied origin, and returns its rules with qualified ids. The
// grants/approvals/revocations sections are validated and then dropped: files
// never confer authority (§3). A mismatch is logged as a daemon diagnostic and
// the file contributes nothing; it is never downgraded and loaded.
func Load(data []byte, origin Origin) (*File, error) {
	f, err := load(data, origin)
	if err != nil {
		slog.Warn("policy file rejected", "origin_kind", int(origin.Kind), "err", err)
	}
	return f, err
}

// load does the work of Load without the diagnostic.
func load(data []byte, origin Origin) (*File, error) {
	if err := preScan(data); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidFile, err)
	}
	var raw fileJSON
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidFile, err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("%w: trailing data", ErrInvalidFile)
	}
	if err := validateFile(&raw); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidFile, err)
	}
	originID, err := originFor(raw.Source, origin)
	if err != nil {
		return nil, err
	}
	f := &File{source: raw.Source, originID: originID}
	for _, r := range *raw.Rules {
		rule := Rule{QID: raw.Source + ":" + originID + "#" + r.ID, Actions: r.Actions}
		rule.Effect = map[string]Effect{"allow": Allow, "ask": Ask, "deny": Deny}[r.Effect]
		if r.Match != nil {
			rule.Match = *r.Match
		}
		if r.Approval != nil {
			rule.MaxTTL, _ = parseDuration(r.Approval.MaxTTL) // validated above
		}
		f.rules = append(f.rules, rule)
	}
	return f, nil
}

// preScan walks the raw token stream and rejects what encoding/json would
// silently accept but the schema forbids: null values (no schema field allows
// null), duplicate keys, and keys that are not lower-case ASCII (Go matches
// field names case-insensitively, the schema does not).
func preScan(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return scanValue(dec)
}

// scanValue checks one JSON value read from dec, recursing into containers.
func scanValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	switch tok {
	case nil:
		return errors.New("null value")
	case json.Delim('{'):
		seen := map[string]bool{}
		for dec.More() {
			k, err := dec.Token()
			if err != nil {
				return fmt.Errorf("decode: %w", err)
			}
			key, _ := k.(string)
			if !wireKeyRe.MatchString(key) || seen[key] {
				return fmt.Errorf("bad or duplicate key %q", key)
			}
			seen[key] = true
			if err := scanValue(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token()
	case json.Delim('['):
		for dec.More() {
			if err := scanValue(dec); err != nil {
				return err
			}
		}
		_, err = dec.Token()
	}
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	return nil
}

// validateFile applies the schema checks that strict decoding cannot.
func validateFile(f *fileJSON) error {
	if err := checkVersion(f.PolicyVersion); err != nil {
		return err
	}
	if !in(f.Source, []string{"operator", "project", "role", "skill"}) {
		return fmt.Errorf("unknown source %q", f.Source)
	}
	if f.Rules == nil {
		return errors.New("rules is required")
	}
	restricted := in(f.Source, restrictions)
	ids := map[string]bool{}
	for i := range *f.Rules {
		r := &(*f.Rules)[i]
		if err := validateRule(r, restricted); err != nil {
			return fmt.Errorf("rule %d: %w", i, err)
		}
		// Local ids must be unique, or two rules share one qualified id (§2).
		if ids[r.ID] {
			return fmt.Errorf("duplicate rule id %q", r.ID)
		}
		ids[r.ID] = true
	}
	// Records are validated in their export form, then ignored (§3).
	if restricted && (nonEmpty(f.Grants) || nonEmpty(f.Revocations) || nonEmpty(f.Approvals)) {
		return errors.New("restriction files cannot carry grants, revocations or approvals")
	}
	for _, g := range deref(f.Grants) {
		if err := g.validate(); err != nil {
			return err
		}
	}
	for _, r := range deref(f.Revocations) {
		if r.Grant == "" || r.Scope != "subtree" || r.RevokedBy == "" || r.RevokedAt.IsZero() {
			return errors.New("invalid revocation")
		}
	}
	for i := range deref(f.Approvals) {
		if err := (*f.Approvals)[i].validate(); err != nil {
			return err
		}
	}
	return nil
}

// validateRule checks one rule; restricted files may only deny or ask.
func validateRule(r *ruleJSON, restricted bool) error {
	if !ruleIDRe.MatchString(r.ID) {
		return fmt.Errorf("bad id %q", r.ID)
	}
	if !in(r.Effect, []string{"allow", "ask", "deny"}) || restricted && r.Effect == "allow" {
		return fmt.Errorf("effect %q not allowed here", r.Effect)
	}
	if len(r.Actions) == 0 {
		return errors.New("actions must be non-empty")
	}
	for _, a := range r.Actions {
		if !patternRe.MatchString(a) {
			return fmt.Errorf("bad action pattern %q", a)
		}
	}
	if (r.Effect == "ask") != (r.Approval != nil) {
		return errors.New("approval is required for ask and forbidden otherwise")
	}
	if r.Approval != nil {
		if _, err := parseDuration(r.Approval.MaxTTL); err != nil {
			return err
		}
	}
	if m := r.Match; m != nil {
		for _, list := range [][]string{m.Paths, m.Targets, m.Hosts} {
			for _, s := range list {
				if s == "" {
					return errors.New("empty match entry")
				}
			}
		}
		for _, p := range m.Paths {
			// An unparsable glob would silently never match, turning a deny into nothing.
			if _, err := path.Match(p, ""); err != nil {
				return fmt.Errorf("bad path glob %q", p)
			}
			// A glob in an unsupported Windows spelling would also never match (V3).
			if _, ok := plainGlob(p); !ok {
				return fmt.Errorf("path glob %q is in an unsupported Windows namespace", p)
			}
		}
		for _, v := range m.Roles {
			if !in(v, roleValues) {
				return fmt.Errorf("unknown role %q", v)
			}
		}
		for _, v := range m.Modes {
			if !in(v, modeValues) {
				return fmt.Errorf("unknown mode %q", v)
			}
		}
	}
	return nil
}

// originFor checks the declared source against the origin and returns the
// loader-derived origin id used in qualified rule ids (§2).
func originFor(source string, o Origin) (string, error) {
	safe := func(s string) bool { return s != "" && !strings.ContainsAny(s, "# \t\r\n") }
	switch {
	case source == "operator" && o.Kind == OriginStore:
		return "store", nil
	case source == "project" && o.Kind == OriginProject && safe(o.Project) && revisionRe.MatchString(o.Revision):
		return o.Project + "@" + o.Revision, nil
	case (source == "role" || source == "skill") && o.Kind == OriginPack && safe(o.PackFile) && sha256Re.MatchString(o.Digest):
		return o.PackFile + "@" + o.Digest[len("sha256:"):len("sha256:")+12], nil
	}
	return "", fmt.Errorf("%w: source %q", ErrSourceMismatch, source)
}

// --- Helpers: durations and small list utilities ---

// parseDuration parses the strict ISO 8601 subset of $defs/iso_duration
// (days, hours, minutes, seconds; each at most once, in order). A day is 24h.
func parseDuration(s string) (time.Duration, error) {
	m := durationRe.FindStringSubmatch(s)
	// Go regexp has no lookahead, so "P" alone and a dangling "T" are rejected here.
	if m == nil || s == "P" || strings.HasSuffix(s, "T") {
		return 0, fmt.Errorf("bad duration %q", s)
	}
	var total time.Duration
	for i, unit := range []time.Duration{24 * time.Hour, time.Hour, time.Minute, time.Second} {
		if m[i+1] == "" {
			continue
		}
		n, err := strconv.ParseInt(m[i+1], 10, 64)
		if err != nil || n > int64((1<<63-1)/unit) || total+time.Duration(n)*unit < total {
			return 0, fmt.Errorf("duration %q out of range", s)
		}
		total += time.Duration(n) * unit
	}
	return total, nil
}

// maxVersionLen bounds the policy_version token. It is far above any honest
// spelling of 3 and only stops a worker-editable project file from carrying a
// megabyte-long number (G6).
const maxVersionLen = 4096

// jsonNumberRe is the JSON number grammar (RFC 8259 §6).
var jsonNumberRe = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

// checkVersion reports whether a raw JSON value is a number exactly equal to
// 3, as JSON Schema const compares numbers (3, 3.0, 0.3e1, 30e-1, 3 followed
// by any number of fractional zeros). It works on the digits in linear time:
// the value is 3 when the significant digits are exactly "3" and the decimal
// exponent cancels. Strings and other types never match; a token longer than
// maxVersionLen is rejected with its own error.
func checkVersion(raw json.RawMessage) error {
	if len(raw) > maxVersionLen {
		return fmt.Errorf("policy_version is longer than %d bytes", maxVersionLen)
	}
	m := jsonNumberRe.FindStringSubmatch(string(raw))
	if m == nil || raw[0] == '-' {
		return errors.New("policy_version must be 3")
	}
	frac := strings.TrimPrefix(m[2], ".")
	exp := 0
	if m[3] != "" {
		e, err := strconv.Atoi(m[3][1:])
		if err != nil { // an exponent beyond int range cannot give 3 within maxVersionLen digits
			return errors.New("policy_version must be 3")
		}
		exp = e
	}
	digits := strings.TrimLeft(m[1]+frac, "0")
	core := strings.TrimRight(digits, "0")
	if core != "3" || exp-len(frac)+len(digits)-len(core) != 0 {
		return errors.New("policy_version must be 3")
	}
	return nil
}

// in reports whether v is one of set.
func in(v string, set []string) bool {
	for _, s := range set {
		if s == v {
			return true
		}
	}
	return false
}

// nonEmpty reports whether an optional list is present with items.
func nonEmpty[T any](p *[]T) bool { return p != nil && len(*p) > 0 }

// deref returns the list behind an optional pointer, or nil.
func deref[T any](p *[]T) []T {
	if p == nil {
		return nil
	}
	return *p
}
