package policy

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"synch/internal/eventlog"
)

// --- V2: replay validator, table-driven from the schema specs ---

// rec is one event of a test history; payload is marshalled as JSON.
type rec struct {
	typ         string
	version     int
	thread      string
	payload     map[string]any
	runnerActor string // empty for daemon events
}

// approvalP is a baseline approval.recorded payload; each call returns a fresh copy.
func approvalP() map[string]any {
	return map[string]any{"id": "apr-1", "approver": "operator:alice", "thread": rootID, "revision": rev1,
		"worktree_digest": wd1, "target": "t1", "actions": []any{"ci.run"},
		"approved_at": "2026-11-18T10:00:00Z", "expires_at": "2026-11-18T11:00:00Z"}
}

// voidP is a baseline approval.voided payload for apr-1.
func voidP() map[string]any { return map[string]any{"approval_id": "apr-1", "reason": "revoked"} }

// grantP is a baseline grant.issued payload for grt-1 on the root thread.
func grantP() map[string]any {
	return map[string]any{"id": "grt-1", "kind": "full_control", "thread": rootID, "host": "h", "identity": "i",
		"granted_by": "operator:alice", "granted_at": "2026-11-18T09:00:00Z", "inherit": true}
}

// revokeOpP is a baseline operator-form grant.revoked payload.
func revokeOpP(settled bool) map[string]any {
	return map[string]any{"grant_id": "grt-1", "affected_threads": []any{rootID}, "settled": settled,
		"cause": "operator", "revoked_by": "operator:alice"}
}

// revokeEndP is a baseline thread_ended-form grant.revoked payload.
func revokeEndP(settled bool) map[string]any {
	return map[string]any{"grant_id": "grt-1", "affected_threads": []any{rootID}, "settled": settled,
		"cause": "thread_ended", "ended_thread": rootID}
}

// decidedP is a baseline policy.decided payload naming apr-1 and grt-1.
func decidedP() map[string]any {
	return map[string]any{"action": "ci.run", "decision": "ask", "rule_id": "operator:store#ask", "machine": "m",
		"target": "t1", "approval_id": "apr-1", "grant_id": "grt-1", "outcome": "executed"}
}

// stateP is a thread.state_changed payload from key/value pairs.
func stateP(kv ...string) map[string]any {
	m := map[string]any{}
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

// recApproval wraps an approval.recorded payload for the root thread.
func recApproval(p map[string]any) rec {
	return rec{typ: "approval.recorded", version: 3, thread: rootID, payload: p}
}

// recVoid wraps an approval.voided payload for the root thread.
func recVoid(p map[string]any) rec {
	return rec{typ: "approval.voided", version: 1, thread: rootID, payload: p}
}

// recGrant wraps a grant.issued payload for the root thread.
func recGrant(p map[string]any) rec {
	return rec{typ: "grant.issued", version: 3, thread: rootID, payload: p}
}

// recRevoke wraps a grant.revoked payload for the root thread.
func recRevoke(p map[string]any) rec {
	return rec{typ: "grant.revoked", version: 2, thread: rootID, payload: p}
}

// recDecided wraps a policy.decided payload for the root thread.
func recDecided(p map[string]any) rec {
	return rec{typ: "policy.decided", version: 2, thread: rootID, payload: p}
}

const replayRunner = "runner-1"

// runnerDecision makes a late runner decision whose references need not be live.
func runnerDecision(actor string) rec {
	r := recDecided(decidedP())
	r.runnerActor = actor
	return r
}

// recState wraps a thread.state_changed payload for the root thread.
func recState(p map[string]any) rec {
	return rec{typ: "thread.state_changed", version: 4, thread: rootID, payload: p}
}

// ended is the root thread's terminal transition.
func ended() rec {
	return recState(stateP("from", "awaiting_acceptance", "to", "completed", "transition", "T28"))
}

// recoverHistory writes the history to a fresh log and returns Recover's error.
func recoverHistory(t *testing.T, history []rec) error {
	t.Helper()
	_, st, log := fixture(t)
	for _, r := range history {
		raw, err := json.Marshal(r.payload)
		if err != nil {
			t.Fatal(err)
		}
		if r.runnerActor != "" {
			persistRunnerDecision(t, st.dbPath, r, raw)
			continue
		}
		_, err = log.Append(context.Background(), []eventlog.NewEvent{{
			ID: newULID(time.Now()), Stream: "thread:" + r.thread, Type: r.typ, TypeVersion: r.version,
			Actor: eventlog.Actor{Kind: "daemon", ID: "synchd"}, ThreadID: r.thread, Payload: raw,
		}})
		if err != nil {
			t.Fatal(err)
		}
	}
	return New(log, st, func() time.Time { return st.now }).Recover()
}

// persistRunnerDecision bypasses ingestion, which always writes matching
// identities. The fixture is the only row in a fresh log.
func persistRunnerDecision(t *testing.T, dbPath string, r rec, raw []byte) {
	t.Helper()
	ev := eventlog.Event{
		NewEvent: eventlog.NewEvent{
			ID: newULID(t0), Stream: "thread:" + r.thread, Type: r.typ, TypeVersion: r.version,
			Actor: eventlog.Actor{Kind: "runner", ID: r.runnerActor}, ThreadID: r.thread, OccurredAt: &t0, Payload: raw,
		},
		EnvelopeVersion: 2, Seq: 1, GlobalSeq: 1, RecordedAt: t0,
		Source: &eventlog.Source{Runner: replayRunner, Seq: 1},
	}
	envelope, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(context.Background(),
		"INSERT INTO events(global_seq, stream, seq, id, runner, source_seq, envelope) VALUES(?, ?, ?, ?, ?, ?, ?)",
		ev.GlobalSeq, ev.Stream, ev.Seq, ev.ID, ev.Source.Runner, ev.Source.Seq, envelope)
	if err != nil {
		t.Fatal(err)
	}
}

// baseline is a valid history whose last event is the record under test.
type baseline struct {
	name    string
	sp      spec
	history func() []rec
}

// baselines covers every consumed event type and both issuer forms.
var baselines = []baseline{
	{"approval.recorded", approvalSpec, func() []rec { return []rec{recApproval(approvalP())} }},
	{"approval.voided", voidSpec, func() []rec { return []rec{recApproval(approvalP()), recVoid(voidP())} }},
	{"grant.issued", grantSpec, func() []rec { return []rec{recGrant(grantP())} }},
	{"grant.revoked commit operator", revokedSpec, func() []rec { return []rec{recGrant(grantP()), recRevoke(revokeOpP(false))} }},
	{"grant.revoked completion operator", revokedSpec, func() []rec {
		return []rec{recGrant(grantP()), recRevoke(revokeOpP(false)), recRevoke(revokeOpP(true))}
	}},
	{"grant.revoked commit thread_ended", revokedSpec, func() []rec {
		return []rec{recGrant(grantP()), ended(), recRevoke(revokeEndP(false))}
	}},
	{"grant.revoked completion thread_ended", revokedSpec, func() []rec {
		return []rec{recGrant(grantP()), ended(), recRevoke(revokeEndP(false)), recRevoke(revokeEndP(true))}
	}},
	{"policy.decided", decidedSpec, func() []rec {
		return []rec{recApproval(approvalP()), recGrant(grantP()), recDecided(decidedP())}
	}},
	{"policy.decided runner C4 identity matches", decidedSpec, func() []rec {
		return []rec{runnerDecision(replayRunner)}
	}},
	{"thread.state_changed T15", stateSpec, func() []rec {
		return []rec{recState(stateP("from", "running", "to", "paused", "transition", "T15", "pause_reason", "harness_unavailable"))}
	}},
	{"thread.state_changed T21", stateSpec, func() []rec {
		return []rec{recState(stateP("from", "paused", "to", "queued", "transition", "T21", "from_pause_reason", "operator"))}
	}},
	{"thread.state_changed T24", stateSpec, func() []rec {
		return []rec{recState(stateP("from", "rollover", "to", "paused", "transition", "T24", "pause_reason", "daemon_recovery", "interrupted_state", "rollover"))}
	}},
	{"thread.state_changed T28", stateSpec, func() []rec { return []rec{ended()} }},
}

// mutation changes the last record of a baseline history.
type mutation struct {
	name string
	edit func(p map[string]any)
}

// fieldMutations derives, from the spec, every mutation that must fail: each
// required property missing; each property null and of the wrong type; each
// string that must be non-empty or formatted set to "" (for an optional
// property absent from the baseline this is "present but empty", which the
// schema forbids or rejects); each enum out of range; const true set false;
// each array with a bad item; and one unknown property.
func fieldMutations(sp spec) []mutation {
	names := make([]string, 0, len(sp))
	for n := range sp {
		names = append(names, n)
	}
	sort.Strings(names)
	ms := []mutation{{"unknown property", func(p map[string]any) { p["extra"] = "x" }}}
	for _, n := range names {
		f := sp[n]
		if f.req {
			ms = append(ms, mutation{n + " missing", func(p map[string]any) { delete(p, n) }})
		}
		ms = append(ms, mutation{n + " null", func(p map[string]any) { p[n] = nil }})
		wrong := any(7)
		if f.kind == fBool || f.kind == fTrue {
			wrong = "x"
		}
		ms = append(ms, mutation{n + " wrong type", func(p map[string]any) { p[n] = wrong }})
		switch f.kind {
		case fStr, fULID, fSHA, fRev, fTime, fEnum, fPattern:
			ms = append(ms, mutation{n + " empty string", func(p map[string]any) { p[n] = "" }})
		case fTrue:
			ms = append(ms, mutation{n + " false", func(p map[string]any) { p[n] = false }})
		case fULIDs, fStrs, fActions, fQIDs:
			ms = append(ms, mutation{n + " bad item", func(p map[string]any) { p[n] = []any{""} }})
		}
		if f.kind == fEnum {
			ms = append(ms, mutation{n + " out of enum", func(p map[string]any) { p[n] = "bogus" }})
		}
	}
	return ms
}

// crossCase is one cross-record violation; its history must fail closed.
type crossCase struct {
	name    string
	history func() []rec
}

// crossCases are the ordering, thread, duplicate and cross-field checks.
var crossCases = []crossCase{
	{"type_version wrong", func() []rec {
		return []rec{{typ: "approval.recorded", version: 2, thread: rootID, payload: approvalP()}}
	}},
	{"approval C3 payload thread differs", func() []rec {
		p := approvalP()
		p["thread"] = otherID
		return []rec{recApproval(p)}
	}},
	{"approval C3 recorded on another stream", func() []rec {
		return []rec{{typ: "approval.recorded", version: 3, thread: otherID, payload: approvalP()}}
	}},
	{"approval duplicate", func() []rec { return []rec{recApproval(approvalP()), recApproval(approvalP())} }},
	{"approval ID reused after void", func() []rec {
		return []rec{recApproval(approvalP()), recVoid(voidP()), recApproval(approvalP())}
	}},
	{"approval P1 expires before approved", func() []rec {
		p := approvalP()
		p["expires_at"] = "2026-11-18T09:00:00Z"
		return []rec{recApproval(p)}
	}},
	{"approval P1 expires at approved", func() []rec {
		p := approvalP()
		p["expires_at"] = p["approved_at"]
		return []rec{recApproval(p)}
	}},
	{"approval revision and plan_digest both", func() []rec {
		p := approvalP()
		p["plan_digest"] = plan1
		return []rec{recApproval(p)}
	}},
	{"approval neither revision nor plan_digest", func() []rec {
		p := approvalP()
		delete(p, "revision")
		return []rec{recApproval(p)}
	}},
	{"approval operational bound to revision", func() []rec {
		p := approvalP()
		p["actions"], p["rollback"], p["irreversible_effects"] = []any{"net.firewall_change"}, "rb", []any{}
		return []rec{recApproval(p)}
	}},
	{"approval duplicate action", func() []rec {
		p := approvalP()
		p["actions"] = []any{"ci.run", "ci.run"}
		return []rec{recApproval(p)}
	}},
	{"void before record (order reversed)", func() []rec { return []rec{recVoid(voidP()), recApproval(approvalP())} }},
	{"void twice", func() []rec { return []rec{recApproval(approvalP()), recVoid(voidP()), recVoid(voidP())} }},
	{"void on another thread", func() []rec {
		return []rec{recApproval(approvalP()), {typ: "approval.voided", version: 1, thread: otherID, payload: voidP()}}
	}},
	{"grant C3 recorded on another stream", func() []rec { return []rec{{typ: "grant.issued", version: 3, thread: otherID, payload: grantP()}} }},
	{"grant duplicate", func() []rec { return []rec{recGrant(grantP()), recGrant(grantP())} }},
	{"grant ID reused after completed revocation", func() []rec {
		return []rec{recGrant(grantP()), recRevoke(revokeOpP(false)), recRevoke(revokeOpP(true)), recGrant(grantP())}
	}},
	{"revoke before issue (order reversed)", func() []rec { return []rec{recRevoke(revokeOpP(false)), recGrant(grantP())} }},
	{"revoke on another thread", func() []rec {
		return []rec{recGrant(grantP()), {typ: "grant.revoked", version: 2, thread: otherID, payload: revokeOpP(false)}}
	}},
	{"commit twice", func() []rec {
		return []rec{recGrant(grantP()), recRevoke(revokeOpP(false)), recRevoke(revokeOpP(false))}
	}},
	{"completion without commit", func() []rec { return []rec{recGrant(grantP()), recRevoke(revokeOpP(true))} }},
	{"completion before commit (order reversed)", func() []rec {
		return []rec{recGrant(grantP()), recRevoke(revokeOpP(true)), recRevoke(revokeOpP(false))}
	}},
	{"completion twice", func() []rec {
		return []rec{recGrant(grantP()), recRevoke(revokeOpP(false)), recRevoke(revokeOpP(true)), recRevoke(revokeOpP(true))}
	}},
	{"completion payload differs", func() []rec {
		c := revokeOpP(true)
		c["affected_threads"] = []any{rootID, childID}
		return []rec{recGrant(grantP()), recRevoke(revokeOpP(false)), recRevoke(c)}
	}},
	{"completion cause differs", func() []rec {
		return []rec{recGrant(grantP()), ended(), recRevoke(revokeOpP(false)), recRevoke(revokeEndP(true))}
	}},
	{"C6 thread_ended before terminal (order reversed)", func() []rec {
		return []rec{recGrant(grantP()), recRevoke(revokeEndP(false)), ended()}
	}},
	{"C6 thread_ended with no terminal state", func() []rec { return []rec{recGrant(grantP()), recRevoke(revokeEndP(false))} }},
	{"C6 thread_ended terminal only on another thread", func() []rec {
		other := ended()
		other.thread = otherID
		return []rec{recGrant(grantP()), other, recRevoke(revokeEndP(false))}
	}},
	{"C6 thread_ended names another thread", func() []rec {
		p := revokeEndP(false)
		p["ended_thread"] = otherID
		other := ended()
		other.thread = otherID
		return []rec{recGrant(grantP()), ended(), other, recRevoke(p)}
	}},
	{"operator form with ended_thread empty", func() []rec {
		p := revokeOpP(false)
		p["ended_thread"] = ""
		return []rec{recGrant(grantP()), recRevoke(p)}
	}},
	{"operator form with ended_thread valid", func() []rec {
		p := revokeOpP(false)
		p["ended_thread"] = rootID
		return []rec{recGrant(grantP()), ended(), recRevoke(p)}
	}},
	{"operator form without revoked_by", func() []rec {
		p := revokeOpP(false)
		delete(p, "revoked_by")
		return []rec{recGrant(grantP()), recRevoke(p)}
	}},
	{"thread_ended form with revoked_by empty", func() []rec {
		p := revokeEndP(false)
		p["revoked_by"] = ""
		return []rec{recGrant(grantP()), ended(), recRevoke(p)}
	}},
	{"thread_ended form with revoked_by", func() []rec {
		p := revokeEndP(false)
		p["revoked_by"] = "operator:alice"
		return []rec{recGrant(grantP()), ended(), recRevoke(p)}
	}},
	{"thread_ended form without ended_thread", func() []rec {
		p := revokeEndP(false)
		delete(p, "ended_thread")
		return []rec{recGrant(grantP()), ended(), recRevoke(p)}
	}},
	{"decided names an unknown approval", func() []rec { return []rec{recGrant(grantP()), recDecided(decidedP())} }},
	{"runner decision C4 actor differs from source", func() []rec { return []rec{runnerDecision("runner-2")} }},
	{"decided names a voided approval", func() []rec {
		return []rec{recApproval(approvalP()), recGrant(grantP()), recVoid(voidP()), recDecided(decidedP())}
	}},
	{"decided names another thread's approval", func() []rec {
		return []rec{recApproval(approvalP()), recGrant(grantP()), {typ: "policy.decided", version: 2, thread: otherID, payload: decidedP()}}
	}},
	{"decided names an unknown grant", func() []rec { return []rec{recApproval(approvalP()), recDecided(decidedP())} }},
	{"decided names a revoked grant", func() []rec {
		return []rec{recApproval(approvalP()), recGrant(grantP()), recRevoke(revokeOpP(false)), recDecided(decidedP())}
	}},
	{"state transition does not match from", func() []rec {
		return []rec{recState(stateP("from", "draft", "to", "running", "transition", "T05"))}
	}},
	{"state unknown transition T37", func() []rec {
		return []rec{recState(stateP("from", "running", "to", "verifying", "transition", "T37"))}
	}},
	{"state paused without pause_reason", func() []rec {
		return []rec{recState(stateP("from", "running", "to", "paused", "transition", "T15"))}
	}},
	{"state pause_reason while not paused", func() []rec {
		return []rec{recState(stateP("from", "queued", "to", "running", "transition", "T05", "pause_reason", "operator"))}
	}},
	{"state pause_reason mismatches row", func() []rec {
		return []rec{recState(stateP("from", "running", "to", "paused", "transition", "T15", "pause_reason", "operator"))}
	}},
	{"state from paused without from_pause_reason", func() []rec {
		return []rec{recState(stateP("from", "paused", "to", "queued", "transition", "T21"))}
	}},
	{"state from_pause_reason mismatches row", func() []rec {
		return []rec{recState(stateP("from", "paused", "to", "queued", "transition", "T21", "from_pause_reason", "daemon_recovery"))}
	}},
	{"state T24 without interrupted_state", func() []rec {
		return []rec{recState(stateP("from", "running", "to", "paused", "transition", "T24", "pause_reason", "daemon_recovery"))}
	}},
	{"state T24 interrupted_state differs from from", func() []rec {
		return []rec{recState(stateP("from", "running", "to", "paused", "transition", "T24", "pause_reason", "daemon_recovery", "interrupted_state", "rollover"))}
	}},
	{"state interrupted_state outside T24", func() []rec {
		return []rec{recState(stateP("from", "queued", "to", "running", "transition", "T05", "interrupted_state", "running"))}
	}},
	{"state T30 from a terminal state", func() []rec {
		return []rec{recState(stateP("from", "completed", "to", "cancelled", "transition", "T30"))}
	}},
}

// TestReplayValidatorTable proves V1/V2: every baseline history recovers;
// every schema-derived field mutation of its last record and every
// cross-check violation fails closed with ErrCorruptHistory.
func TestReplayValidatorTable(t *testing.T) {
	total := 0
	for _, b := range baselines {
		total++
		if err := recoverHistory(t, b.history()); err != nil {
			t.Errorf("baseline %s: %v", b.name, err)
		}
		for _, m := range fieldMutations(b.sp) {
			total++
			h := b.history()
			m.edit(h[len(h)-1].payload)
			if err := recoverHistory(t, h); !errors.Is(err, ErrCorruptHistory) {
				t.Errorf("%s / %s: recovered (%v)", b.name, m.name, err)
			}
		}
	}
	for _, c := range crossCases {
		total++
		if err := recoverHistory(t, c.history()); !errors.Is(err, ErrCorruptHistory) {
			t.Errorf("cross-check %s: recovered (%v)", c.name, err)
		}
	}
	t.Logf("replay validator table: %d baselines, %d cross-checks, %d cases in total", len(baselines), len(crossCases), total)
}

// TestReplayCatalogueDateTime checks H2 with vectors shared with the pinned
// ajv-formats validator. Each vector names its RFC section or checker rule.
func TestReplayCatalogueDateTime(t *testing.T) {
	raw, err := os.ReadFile("testdata/date-time.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []struct {
		Name, Value, From string
		Valid             bool
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, v := range vectors {
		t.Run(v.Name, func(t *testing.T) {
			approval := approvalP()
			approval["approved_at"], approval["expires_at"] = v.Value, "9999-12-31T23:59:59Z"
			expiry := approvalP()
			expiry["approved_at"], expiry["expires_at"] = "0000-01-01T00:00:00Z", v.Value
			grant := grantP()
			grant["granted_at"] = v.Value
			for _, r := range []rec{recApproval(approval), recApproval(expiry), recGrant(grant)} {
				err := recoverHistory(t, []rec{r})
				if v.Valid && err != nil || !v.Valid && !errors.Is(err, ErrCorruptHistory) {
					t.Errorf("%s %q (%s): recover returned %v, want valid=%v", r.typ, v.Value, v.From, err, v.Valid)
				}
			}
		})
	}
}

// --- V4: Windows path validator tables ---

// dosDevices lists the device basenames reserved with or without an extension.
func dosDevices() []string {
	names := []string{"CON", "PRN", "AUX", "NUL"}
	for _, p := range []string{"COM", "LPT"} {
		for _, d := range []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "¹", "²", "³"} {
			names = append(names, p+d)
		}
	}
	return names
}

// mixedCase alternates letter case, e.g. "nUl".
func mixedCase(s string) string {
	b := []rune(strings.ToLower(s))
	for i := 1; i < len(b); i += 2 {
		b[i] = []rune(strings.ToUpper(string(b[i])))[0]
	}
	return string(b)
}

// TestWindowsDeviceNameTable checks V4 and H1 by crossing device names with
// seven spellings in local leaf, inner component and UNC positions.
func TestWindowsDeviceNameTable(t *testing.T) {
	old := windowsPaths
	windowsPaths = true
	defer func() { windowsPaths = old }()
	forms := []func(string) string{
		func(n string) string { return n },
		func(n string) string { return n + ".txt" },
		func(n string) string { return n + ":" },
		func(n string) string { return n + ":stream" },
		mixedCase,
		func(n string) string { return n + " " },
		func(n string) string { return n + " .txt" },
	}
	cases := 0
	names := append(dosDevices(), "CONIN$", "CONOUT$", "COM0", "LPT0", "CLOCK$")
	for _, n := range names {
		for i, f := range forms {
			c := f(n)
			wantGlob := false
			switch n {
			case "CONIN$", "CONOUT$":
				wantGlob = i == 1 || i == 6 // only the bare console name is reserved
			case "COM0", "LPT0", "CLOCK$":
				wantGlob = i != 2 && i != 3 && i != 5 // no colons or trailing spaces
			}
			for _, p := range []string{`\\?\C:\wc\` + c, `\\?\C:\` + c + `\x`, `\\?\UNC\srv\share\` + c} {
				cases++
				plain := strings.TrimPrefix(p, `\\?\`)
				if rest, ok := strings.CutPrefix(plain, `UNC\`); ok {
					plain = `\\` + rest
				}
				full, err := osFullPath(plain)
				wantPath := wantGlob && err == nil && full == plain
				if got, ok := plainWindowsPath(p); ok != wantPath || ok && got != plain {
					t.Errorf("path %q: got %q, %v, want accepted=%v (normalizer: %q, %v)", p, got, ok, wantPath, full, err)
				}
				if got, ok := plainGlob(p); ok != wantGlob || ok && got != plain {
					t.Errorf("glob %q: got %q, %v, want accepted=%v", p, got, ok, wantGlob)
				}
			}
		}
	}
	for _, c := range []string{"CONSOLE", "NULL.txt", "COM10", "LPT", "aux_x", "conx", "COMA"} {
		cases++
		if got, ok := plainWindowsPath(`\\?\C:\wc\` + c); !ok || got != `C:\wc\`+c {
			t.Errorf("lookalike %q: got %q %v", c, got, ok)
		}
	}
	t.Logf("windows device table: %d names x %d forms x 3 positions + lookalikes = %d cases", len(names), len(forms), cases)
}

// TestWindowsOrdinaryDeviceLookalikes checks H1 by proving the ordinary and extended
// spellings identify the same real file and both match policy.
func TestWindowsOrdinaryDeviceLookalikes(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("requires Win32 path normalization and file identity")
	}
	dir := t.TempDir()
	e, _, _ := fixture(t, op(`{"id":"files","effect":"allow","actions":["fs.write"],"match":{"paths":["`+filepath.ToSlash(dir)+`/*.txt"]}}`))
	for _, tc := range []struct {
		name   string
		legacy bool
	}{
		{name: "COM0.txt"}, {name: "LPT0.txt"}, {name: "CLOCK$.txt"},
		{name: "CONIN$.txt", legacy: true}, {name: "CONOUT$.txt", legacy: true},
		{name: "CON.txt", legacy: true}, {name: "COM1.txt", legacy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plain := filepath.Join(dir, tc.name)
			extended := `\\?\` + plain
			if full, err := osFullPath(plain); err != nil || full != plain {
				if tc.legacy {
					t.Skipf("Win32 treats this legacy spelling as an alias: %q, %v", full, err)
				}
				t.Fatalf("normalizer returned %q, %v", full, err)
			}
			if err := os.WriteFile(extended, []byte(tc.name), 0o600); err != nil {
				t.Fatal(err)
			}
			a, err := os.Stat(plain)
			if err != nil && !tc.legacy {
				t.Fatal(err)
			}
			b, err := os.Stat(extended)
			if err != nil {
				t.Fatal(err)
			}
			if a == nil || !a.Mode().IsRegular() || !os.SameFile(a, b) {
				if tc.legacy {
					t.Skip("Windows exposes distinct objects for this legacy spelling")
				}
				t.Fatal("the two spellings do not identify the same ordinary file")
			}
			for _, path := range []string{plain, extended} {
				d, err := e.Decide(context.Background(), Request{Thread: rootID, Action: "fs.write", Machine: "m", Paths: []string{path}})
				if err != nil || !d.Allowed {
					t.Errorf("Decide(%q): %+v, %v", path, d, err)
				}
			}
		})
	}
}

// TestWindowsNormalizerRoundTrip proves V3 on Windows: an accepted extended
// path's ordinary spelling is unchanged by GetFullPathNameW, and every device
// form is rejected whatever this Windows version's normalizer does with it.
func TestWindowsNormalizerRoundTrip(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("GetFullPathNameW only exists on Windows")
	}
	wc := filepath.Join(t.TempDir(), "wc")
	if err := os.MkdirAll(wc, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, c := range []string{"f", "CONSOLE", "NULL.txt", "COM10", "LPT", "aux_x", "f.txt", "a b"} {
		p := `\\?\` + wc + `\` + c
		if got, ok := plainWindowsPath(p); !ok || got != wc+`\`+c {
			t.Errorf("%q: got %q %v", p, got, ok)
		}
	}
	for _, n := range dosDevices() {
		for _, c := range []string{n, n + ":", n + ".txt"} {
			if _, ok := plainWindowsPath(`\\?\` + wc + `\` + c); ok {
				full, err := osFullPath(wc + `\` + c)
				t.Errorf("%s accepted (normalizer gave %q, %v)", c, full, err)
			}
		}
	}
}

// TestLoaderRejectsUnsupportedGlob proves V3 at load time: a rule glob in an
// unsupported Windows spelling is rejected, so a deny cannot silently stop matching.
func TestLoaderRejectsUnsupportedGlob(t *testing.T) {
	old := windowsPaths
	windowsPaths = true
	defer func() { windowsPaths = old }()
	for _, g := range []string{`\\\\?\\C:\\wc\\NUL`, `\\\\.\\C:\\wc`, `\\\\?\\C:\\wc\\f.`} {
		src := op(`{"id":"d","effect":"deny","actions":["fs.write"],"match":{"paths":["` + g + `"]}}`)
		if _, err := Load([]byte(src), Origin{Kind: OriginStore}); err == nil {
			t.Errorf("glob %s accepted", g)
		}
	}
	src := op(`{"id":"d","effect":"deny","actions":["fs.write"],"match":{"paths":["\\\\?\\C:\\wc\\*"]}}`)
	if _, err := Load([]byte(src), Origin{Kind: OriginStore}); err != nil {
		t.Errorf("wildcard glob rejected: %v", err)
	}
}
