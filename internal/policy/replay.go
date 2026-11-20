package policy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"

	"synch/internal/eventlog"
)

// --- Recovery: rebuild obligations from the event log (G1, V1) ---

// ErrCorruptHistory reports a persisted event that fails its catalogue schema
// or a producer cross-check; recovery stops and every call fails closed.
var ErrCorruptHistory = errors.New("policy: corrupt policy history")

// Recover rebuilds approvals and grants from the event log. It runs once per
// engine, before the first public call does anything, so a fresh engine over
// an existing log always carries every persisted obligation; FlushPending
// then finishes them. If recovery fails, every public call returns that error
// (fail closed). An operation exists once its first event is persisted; a
// call whose first append failed returned an error and left nothing behind (G1).
func (e *Engine) Recover() error {
	e.recoverOnce.Do(func() { e.recoverErr = e.recover() })
	return e.recoverErr
}

// recover replays the whole log in global_seq order through one replay state,
// so every record is checked against the state as of that record (V1).
func (e *Engine) recover() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	r := &replay{e: e, terminal: map[string]bool{}, seenApprovals: map[string]bool{}}
	var after int64
	for {
		page, err := e.log.ReadGlobal(after, 1000)
		if err != nil {
			return fmt.Errorf("recover: %w", err)
		}
		if len(page) == 0 {
			return nil
		}
		for _, ev := range page {
			if ev.GlobalSeq <= after { // ReadGlobal returns ascending global_seq; anything else is corrupt
				return fmt.Errorf("%w: global_seq %d after %d", ErrCorruptHistory, ev.GlobalSeq, after)
			}
			after = ev.GlobalSeq
			if err := r.apply(ev); err != nil {
				return fmt.Errorf("%w: event %d (%s): %w", ErrCorruptHistory, ev.GlobalSeq, ev.Type, err)
			}
		}
	}
}

// replay is the state as of the record being checked. The engine's maps hold
// live approvals and all grants, including revoked grants. seenApprovals
// keeps approval IDs after voiding. terminal holds threads whose terminal
// thread.state_changed has already been replayed.
type replay struct {
	e             *Engine
	terminal      map[string]bool
	seenApprovals map[string]bool
}

// consumed lists the event types replay reads, with their catalogue type_version.
var consumed = map[string]int{
	"approval.recorded": 3, "approval.voided": 1, "grant.issued": 3, "grant.revoked": 2,
	"policy.decided": 2, "thread.state_changed": 4,
}

// apply validates one event against its catalogue schema and every
// applicable cross-check as of this point in the log, then folds it into the
// state. Other event types are ignored.
func (r *replay) apply(ev eventlog.Event) error {
	version, ok := consumed[ev.Type]
	if !ok {
		return nil
	}
	if ev.TypeVersion != version {
		return fmt.Errorf("type_version %d, want %d", ev.TypeVersion, version)
	}
	if !ulidRe.MatchString(ev.ThreadID) || ev.Stream != "thread:"+ev.ThreadID { // on_thread and C1
		return errors.New("stream and thread_id disagree (C1)")
	}
	if ev.Source != nil && ev.Actor.ID != ev.Source.Runner {
		return errors.New("actor.id and source.runner disagree (C4)")
	}
	switch ev.Type {
	case "approval.recorded":
		return r.approvalRecorded(ev)
	case "approval.voided":
		return r.approvalVoided(ev)
	case "grant.issued":
		return r.grantIssued(ev)
	case "grant.revoked":
		return r.grantRevoked(ev)
	case "policy.decided":
		return r.policyDecided(ev)
	default:
		return r.stateChanged(ev)
	}
}

// approvalRecorded checks approval.recorded v3: $defs/approval field by field,
// exactly one of revision or plan_digest, the operational bindings, P1, C3,
// and that the id is new.
func (r *replay) approvalRecorded(ev eventlog.Event) error {
	m, err := approvalSpec.check(ev.Payload)
	if err != nil {
		return err
	}
	if has(m, "revision") == has(m, "plan_digest") {
		return errors.New("exactly one of revision or plan_digest")
	}
	var a Approval
	wire := struct {
		*Approval
		ApprovedAt catalogueTime `json:"approved_at"`
		ExpiresAt  catalogueTime `json:"expires_at"`
	}{Approval: &a}
	if err := strictDecode(ev.Payload, &wire); err != nil {
		return err
	}
	a.ApprovedAt, a.ExpiresAt = wire.ApprovedAt.Time, wire.ExpiresAt.Time
	if err := a.validateRecord(); err != nil { // operational bindings and P1
		return err
	}
	switch {
	case a.Thread != ev.ThreadID:
		return errors.New("approval recorded on another thread (C3)")
	case r.seenApprovals[a.ID]:
		return fmt.Errorf("approval %s recorded twice", a.ID)
	}
	r.seenApprovals[a.ID] = true
	r.e.approvals[a.ID] = &approvalState{a: a}
	return nil
}

// approvalVoided checks approval.voided v1 and that it voids an approval that
// is live at this point, on that approval's thread.
func (r *replay) approvalVoided(ev eventlog.Event) error {
	m, err := voidSpec.check(ev.Payload)
	if err != nil {
		return err
	}
	id := str(m, "approval_id")
	s := r.e.approvals[id]
	switch {
	case s == nil:
		return fmt.Errorf("void of an approval that is unknown or already voided: %q", id)
	case s.a.Thread != ev.ThreadID:
		return errors.New("approval voided on another thread")
	}
	delete(r.e.approvals, id)
	return nil
}

// grantIssued checks grant.issued v3: $defs/grant field by field, C3, and
// that the id is new.
func (r *replay) grantIssued(ev eventlog.Event) error {
	if _, err := grantSpec.check(ev.Payload); err != nil {
		return err
	}
	var g Grant
	wire := struct {
		*Grant
		GrantedAt catalogueTime `json:"granted_at"`
	}{Grant: &g}
	if err := strictDecode(ev.Payload, &wire); err != nil {
		return err
	}
	g.GrantedAt = wire.GrantedAt.Time
	switch {
	case g.Thread != ev.ThreadID:
		return errors.New("grant issued on another thread (C3)")
	case r.e.findGrantLocked(g.ID) != nil: // revoked grants remain in the engine's grant list
		return fmt.Errorf("grant %s issued twice", g.ID)
	}
	r.e.grants = append(r.e.grants, &grantState{Grant: g})
	return nil
}

// grantRevoked checks grant.revoked v2: fields, the issuer form (operator:
// revoked_by present, ended_thread absent; thread_ended: the reverse; absent
// means the key is missing, so even an empty or null value is a violation),
// C6 as of this record (the grant was issued earlier; a thread_ended cause
// names the granted thread, whose terminal thread.state_changed came
// earlier), the revocation recorded on the grant's thread, and the
// commit/completion order: one commit, then at most one completion with an
// identical payload.
func (r *replay) grantRevoked(ev eventlog.Event) error {
	m, err := revokedSpec.check(ev.Payload)
	if err != nil {
		return err
	}
	cause := str(m, "cause")
	switch {
	case cause == "operator" && (!has(m, "revoked_by") || has(m, "ended_thread")):
		return errors.New("operator revocation needs revoked_by and must not carry ended_thread")
	case cause == "thread_ended" && (!has(m, "ended_thread") || has(m, "revoked_by")):
		return errors.New("thread_ended revocation needs ended_thread and must not carry revoked_by")
	}
	var p revokedPayload
	if err := strictDecode(ev.Payload, &p); err != nil {
		return err
	}
	g := r.e.findGrantLocked(p.GrantID)
	switch {
	case g == nil:
		return fmt.Errorf("revocation of a grant not issued earlier: %q (C6)", p.GrantID)
	case g.Thread != ev.ThreadID:
		return errors.New("revocation recorded on another thread")
	case cause == "thread_ended" && p.EndedThread != g.Thread:
		return errors.New("thread_ended revocation names another thread (C6)")
	case cause == "thread_ended" && !r.terminal[g.Thread]:
		return errors.New("thread_ended revocation before the thread's terminal state_changed (C6)")
	}
	settled := p.Settled
	p.Settled = false
	switch {
	case !settled && g.revoked:
		return errors.New("revocation committed twice")
	case !settled:
		g.revoked, g.payload, g.actor = true, p, ev.Actor
	case !g.revoked || g.settled || !samePayload(g.payload, p):
		return errors.New("settled:true without a single earlier commit with an identical payload")
	default:
		g.settled = true
	}
	return nil
}

// policyDecided checks policy.decided v2 field by field. For a daemon-produced
// decision it also checks the records it names as of this point: an
// approval_id names a live approval of the same thread, and a grant_id names
// an issued, unrevoked grant. Runner-ingested decisions may arrive late after
// an offline period, so they skip the live-record checks.
func (r *replay) policyDecided(ev eventlog.Event) error {
	m, err := decidedSpec.check(ev.Payload)
	if err != nil {
		return err
	}
	if ev.Source != nil {
		return nil
	}
	if id := str(m, "approval_id"); id != "" {
		if s := r.e.approvals[id]; s == nil || s.a.Thread != ev.ThreadID {
			return fmt.Errorf("decision names approval %q that is not live on this thread", id)
		}
	}
	if id := str(m, "grant_id"); id != "" {
		if g := r.e.findGrantLocked(id); g == nil || g.revoked {
			return fmt.Errorf("decision names grant %q that is not live", id)
		}
	}
	return nil
}

// stateChanged checks thread.state_changed v4 field by field, its
// conditional fields, and that exactly one transition row matches (the
// schema's oneOf). A terminal destination is remembered for C6.
func (r *replay) stateChanged(ev eventlog.Event) error {
	m, err := stateSpec.check(ev.Payload)
	if err != nil {
		return err
	}
	from, to, tr := str(m, "from"), str(m, "to"), str(m, "transition")
	switch {
	case (to == "paused") != has(m, "pause_reason"):
		return errors.New("pause_reason is required iff to=paused")
	case (from == "paused") != has(m, "from_pause_reason"):
		return errors.New("from_pause_reason is required iff from=paused")
	case (tr == "T24") != has(m, "interrupted_state"):
		return errors.New("interrupted_state is required iff transition=T24")
	}
	matches := 0
	for _, row := range transitions {
		if row.matches(tr, from, to, str(m, "pause_reason"), str(m, "from_pause_reason"), str(m, "interrupted_state")) {
			matches++
		}
	}
	if matches != 1 {
		return fmt.Errorf("transition %s %s->%s matches %d rows", tr, from, to, matches)
	}
	if in(to, terminalStates) {
		r.terminal[ev.ThreadID] = true
	}
	return nil
}

// --- Field specs: the catalogue schemas, one entry per property (V1) ---

// fkind is the JSON Schema type and format of one property.
type fkind int

const (
	fStr     fkind = iota // string, minLength 1
	fAnyStr               // any string
	fULID                 // $defs/ulid
	fSHA                  // $defs/sha256
	fRev                  // $defs/git_revision
	fTime                 // string, format date-time (RFC 3339)
	fBool                 // boolean
	fTrue                 // const true
	fEnum                 // enum of strings
	fPattern              // string matching re
	fULIDs                // array of $defs/ulid
	fStrs                 // array of strings, each minLength 1
	fActions              // array of exact actions, minItems 1, uniqueItems
	fQIDs                 // array of qualified rule ids
)

// field is one property of a payload schema.
type field struct {
	kind fkind
	req  bool
	enum []string
	re   *regexp.Regexp
}

// spec is a closed object schema (additionalProperties: false).
type spec map[string]field

var (
	threadStates   = []string{"draft", "plan_review", "queued", "running", "waiting_children", "awaiting_decision", "paused", "rollover", "verifying", "awaiting_acceptance", "completed", "failed", "cancelled"}
	nonTerminal    = threadStates[:10]
	terminalStates = []string{"completed", "failed", "cancelled"}
	pauseReasons   = []string{"operator", "harness_unavailable", "no_eligible_pair", "disk_pressure", "reconciliation", "disagreement", "mode_switch", "runner_disconnected", "daemon_recovery"}
	transitionRe   = regexp.MustCompile(`^T[0-9]{2}$`)

	approvalSpec = spec{
		"id": {kind: fStr, req: true}, "approver": {kind: fStr, req: true}, "thread": {kind: fULID, req: true},
		"revision": {kind: fRev}, "plan_digest": {kind: fSHA}, "worktree_digest": {kind: fSHA, req: true},
		"target": {kind: fStr, req: true}, "actions": {kind: fActions, req: true},
		"approved_at": {kind: fTime, req: true}, "expires_at": {kind: fTime, req: true},
		"rollback": {kind: fStr}, "irreversible_effects": {kind: fStrs}, "constraint_rules": {kind: fQIDs},
	}
	voidSpec = spec{
		"approval_id": {kind: fStr, req: true}, "reason": {kind: fEnum, req: true, enum: voidReasons},
	}
	grantSpec = spec{
		"id": {kind: fStr, req: true}, "kind": {kind: fEnum, req: true, enum: []string{"full_control"}},
		"thread": {kind: fULID, req: true}, "host": {kind: fStr, req: true}, "identity": {kind: fStr, req: true},
		"granted_by": {kind: fStr, req: true}, "granted_at": {kind: fTime, req: true}, "inherit": {kind: fTrue, req: true},
	}
	revokedSpec = spec{
		"grant_id": {kind: fStr, req: true}, "affected_threads": {kind: fULIDs, req: true}, "settled": {kind: fBool, req: true},
		"revoked_by": {kind: fStr}, "cause": {kind: fEnum, req: true, enum: []string{"operator", "thread_ended"}},
		"ended_thread": {kind: fULID},
	}
	decidedSpec = spec{
		"action": {kind: fStr, req: true}, "decision": {kind: fEnum, req: true, enum: []string{"allow", "deny", "ask"}},
		"rule_id": {kind: fPattern, re: qualifiedRe}, "machine": {kind: fStr, req: true}, "target": {kind: fAnyStr},
		"approval_id": {kind: fAnyStr}, "grant_id": {kind: fAnyStr},
		"outcome": {kind: fEnum, req: true, enum: []string{"executed", "blocked", "waiting_approval", "failed"}},
		"command": {kind: fAnyStr},
	}
	stateSpec = spec{
		"from": {kind: fEnum, req: true, enum: threadStates}, "to": {kind: fEnum, req: true, enum: threadStates},
		"transition":   {kind: fPattern, req: true, re: transitionRe},
		"pause_reason": {kind: fEnum, enum: pauseReasons}, "from_pause_reason": {kind: fEnum, enum: pauseReasons},
		"interrupted_state": {kind: fEnum, enum: []string{"running", "rollover", "verifying"}}, "detail": {kind: fAnyStr},
	}
)

// voidReasons is the approval.voided reason enum of the catalogue.
var voidReasons = []string{"diff_changed", "target_changed", "scope_changed", "expired", "revoked", "thread_ended", "used"}

// check validates a payload against the spec and returns its properties. The
// payload must be a JSON object without nulls, duplicate or non-lower-case
// keys (preScan); every key must be a declared property; every required
// property must be present; every present property must have its type and
// format. Presence is presence of the key, so an empty or null value of a
// property that must be absent is a violation, never "absent".
func (sp spec) check(raw []byte) (map[string]json.RawMessage, error) {
	if err := preScan(raw); err != nil {
		return nil, err
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return nil, errors.New("payload must be a JSON object")
	}
	for k := range m {
		if _, ok := sp[k]; !ok {
			return nil, fmt.Errorf("unknown property %q", k)
		}
	}
	for name, f := range sp {
		v, ok := m[name]
		if !ok {
			if f.req {
				return nil, fmt.Errorf("missing required property %q", name)
			}
			continue
		}
		if err := f.check(v); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
	}
	return m, nil
}

// check validates one property value.
func (f field) check(v json.RawMessage) error {
	switch f.kind {
	case fBool:
		if s := string(v); s != "true" && s != "false" {
			return errors.New("must be a boolean")
		}
		return nil
	case fTrue:
		if string(v) != "true" {
			return errors.New("must be true")
		}
		return nil
	case fULIDs, fStrs, fActions, fQIDs:
		return f.checkList(v)
	}
	var s string
	if len(v) == 0 || v[0] != '"' || json.Unmarshal(v, &s) != nil {
		return errors.New("must be a string")
	}
	return f.checkString(s)
}

// checkString validates a string value against the kind's format.
func (f field) checkString(s string) error {
	ok := true
	switch f.kind {
	case fStr:
		ok = s != ""
	case fULID:
		ok = ulidRe.MatchString(s)
	case fSHA:
		ok = sha256Re.MatchString(s)
	case fRev:
		ok = revisionRe.MatchString(s)
	case fTime:
		_, ok = parseCatalogueTime(s)
	case fEnum:
		ok = in(s, f.enum)
	case fPattern:
		ok = f.re.MatchString(s)
	}
	if !ok {
		return fmt.Errorf("invalid value %q", s)
	}
	return nil
}

// checkList validates an array value and its items.
func (f field) checkList(v json.RawMessage) error {
	var items []json.RawMessage
	if len(v) == 0 || v[0] != '[' || json.Unmarshal(v, &items) != nil {
		return errors.New("must be an array")
	}
	item := map[fkind]field{fULIDs: {kind: fULID}, fStrs: {kind: fStr}, fActions: {kind: fPattern, re: actionRe}, fQIDs: {kind: fPattern, re: qualifiedRe}}[f.kind]
	seen := map[string]bool{}
	for _, it := range items {
		if err := item.check(it); err != nil {
			return err
		}
		if f.kind == fActions {
			if seen[string(it)] {
				return errors.New("duplicate action")
			}
			seen[string(it)] = true
		}
	}
	if f.kind == fActions && len(items) == 0 {
		return errors.New("must not be empty")
	}
	return nil
}

// has reports whether the property key is present, whatever its value.
func has(m map[string]json.RawMessage, k string) bool {
	_, ok := m[k]
	return ok
}

// str returns a string property already validated by check, or "".
func str(m map[string]json.RawMessage, k string) string {
	var s string
	_ = json.Unmarshal(m[k], &s) // validated, or absent
	return s
}

// strictDecode decodes a validated payload into its struct, rejecting unknown
// fields and trailing data.
func strictDecode(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("trailing data")
	}
	return nil
}

// samePayload reports whether two revocation payloads are identical.
func samePayload(a, b revokedPayload) bool {
	return a.GrantID == b.GrantID && a.RevokedBy == b.RevokedBy && a.Cause == b.Cause &&
		a.EndedThread == b.EndedThread && a.Settled == b.Settled && slices.Equal(a.AffectedThreads, b.AffectedThreads)
}

// --- Thread transitions: the oneOf rows of thread.state_changed v4 ---

// transition is one oneOf row. Nil lists mean the row does not constrain
// that property.
type transition struct {
	t         string
	from      []string
	to        string
	pause     []string // allowed pause_reason
	fromPause []string // allowed from_pause_reason
	inter     string   // required interrupted_state (T24)
}

// matches reports whether the row accepts these property values.
func (t transition) matches(tr, from, to, pause, fromPause, inter string) bool {
	return t.t == tr && in(from, t.from) && t.to == to &&
		(t.pause == nil || in(pause, t.pause)) && (t.fromPause == nil || in(fromPause, t.fromPause)) &&
		(t.inter == "" || inter == t.inter)
}

// one returns a single-value list.
func one(s string) []string { return []string{s} }

var (
	pauseNoRecovery = pauseReasons[:8] // every pause reason except daemon_recovery
	pauseNoHarness  = []string{"operator", "no_eligible_pair", "disk_pressure", "reconciliation", "disagreement", "mode_switch", "runner_disconnected", "daemon_recovery"}

	transitions = []transition{
		{t: "T01", from: one("draft"), to: "queued"},
		{t: "T02", from: one("draft"), to: "plan_review"},
		{t: "T03", from: one("plan_review"), to: "queued"},
		{t: "T04", from: one("plan_review"), to: "draft"},
		{t: "T05", from: one("queued"), to: "running"},
		{t: "T06", from: one("queued"), to: "paused", pause: one("no_eligible_pair")},
		{t: "T07", from: one("queued"), to: "paused", pause: one("disk_pressure")},
		{t: "T08", from: one("running"), to: "waiting_children"},
		{t: "T09", from: one("waiting_children"), to: "queued"},
		{t: "T10", from: one("running"), to: "awaiting_decision"},
		{t: "T11", from: one("awaiting_decision"), to: "queued"},
		{t: "T12", from: one("awaiting_decision"), to: "failed"},
		{t: "T13", from: one("running"), to: "rollover"},
		{t: "T14", from: one("rollover"), to: "queued"},
		{t: "T15", from: one("running"), to: "paused", pause: one("harness_unavailable")},
		{t: "T16", from: one("running"), to: "paused", pause: one("reconciliation")},
		{t: "T17", from: one("running"), to: "paused", pause: one("disagreement")},
		{t: "T18", from: one("running"), to: "paused", pause: one("mode_switch")},
		{t: "T19", from: one("running"), to: "paused", pause: one("runner_disconnected")},
		{t: "T20", from: one("running"), to: "paused", pause: one("operator")},
		{t: "T21", from: one("paused"), to: "queued", fromPause: pauseNoRecovery},
		{t: "T22", from: one("paused"), to: "queued", fromPause: one("daemon_recovery")},
		{t: "T23", from: one("paused"), to: "paused", fromPause: pauseNoHarness, pause: one("harness_unavailable")},
		{t: "T24", from: one("running"), to: "paused", pause: one("daemon_recovery"), inter: "running"},
		{t: "T24", from: one("rollover"), to: "paused", pause: one("daemon_recovery"), inter: "rollover"},
		{t: "T24", from: one("verifying"), to: "paused", pause: one("daemon_recovery"), inter: "verifying"},
		{t: "T25", from: one("running"), to: "verifying"},
		{t: "T26", from: one("verifying"), to: "awaiting_acceptance"},
		{t: "T27", from: one("verifying"), to: "queued"},
		{t: "T28", from: one("awaiting_acceptance"), to: "completed"},
		{t: "T29", from: one("awaiting_acceptance"), to: "queued"},
		{t: "T30", from: nonTerminal, to: "cancelled"},
		{t: "T31", from: []string{"running", "verifying"}, to: "failed"},
		{t: "T32", from: one("paused"), to: "failed"},
		{t: "T33", from: one("running"), to: "paused", pause: one("disk_pressure")},
		{t: "T34", from: one("paused"), to: "awaiting_decision", fromPause: one("disagreement")},
		{t: "T35", from: one("rollover"), to: "paused", pause: one("reconciliation")},
		{t: "T36", from: one("paused"), to: "failed", fromPause: pauseReasons},
		{t: "T38", from: one("paused"), to: "verifying", fromPause: one("daemon_recovery")},
	}
)
