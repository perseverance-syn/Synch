package policy

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// Regression tests for review R104 (card 104-F1). This file uses only API
// that existed at ee0efbf, so it can be run against that commit; tests for
// API added in 104-F1 live in fix_r104_api_test.go.

// --- F1: revocation and dispatch share one ordering boundary (§6) ---

// holdWriteLock takes the SQLite write lock on the event log so the next
// append blocks, and returns a release function.
func holdWriteLock(t *testing.T, path string) func() {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	return func() {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
		_ = db.Close()
	}
}

// TestRevocationOrdersInFlightDecision proves F1/§6: a decision in flight
// (past its grant check, blocked while recording) cannot return Allowed=true
// after a revocation committed.
func TestRevocationOrdersInFlightDecision(t *testing.T) {
	ctx := context.Background()
	e, st, _ := fixture(t, op(`{"id":"all","effect":"allow","actions":["*"]}`))
	st.modes[rootID] = "full_control"
	g, err := e.IssueGrant(ctx, rootID, "h", "i", "operator:alice")
	if err != nil {
		t.Fatal(err)
	}
	release := holdWriteLock(t, st.dbPath)
	type out struct {
		d   Decision
		err error
	}
	decided, revoked := make(chan out, 1), make(chan error, 1)
	go func() {
		d, err := e.Decide(ctx, Request{Thread: rootID, Action: "host.exec", Machine: "h", Identity: "i"})
		decided <- out{d, err}
	}()
	time.Sleep(200 * time.Millisecond) // the decision is now blocked appending policy.decided
	go func() { revoked <- e.Revoke(ctx, g.ID, "operator:alice") }()
	time.Sleep(200 * time.Millisecond) // give the revocation every chance to commit
	e.mu.Lock()
	committedInFlight := e.findGrantLocked(g.ID).revoked
	e.mu.Unlock()
	release()
	a := <-decided
	if err := <-revoked; err != nil {
		t.Fatal(err)
	}
	if a.err != nil || !a.d.Allowed {
		t.Fatalf("the earlier decision should complete first and be allowed: %+v %v", a.d, a.err)
	}
	if committedInFlight {
		t.Fatal("revocation committed while a decision past its grant check was in flight, which then returned Allowed=true")
	}
}

// --- F2: records are owned by the engine (§§1-3, 5.1, 5.3) ---

// TestRulesSnapshot proves F2/§2: mutating what Rules() returns cannot change live policy or audit identity.
func TestRulesSnapshot(t *testing.T) {
	e, st, _ := fixture(t, op(`{"id":"r","effect":"allow","actions":["git.push"],"match":{"targets":["origin"]}}`))
	rs := st.files[0].Rules()
	rs[0].QID = "operator:store#forged"
	rs[0].Actions[0] = "proc.exec"
	rs[0].Match.Targets[0] = "evil"
	d, err := e.Decide(context.Background(), Request{Thread: rootID, Action: "git.push", Target: "origin", Machine: "m"})
	if err != nil || !d.Allowed || d.RuleID != "operator:store#r" {
		t.Fatalf("live rule changed through Rules(): %+v %v", d, err)
	}
	if d, _ := e.Decide(context.Background(), Request{Thread: rootID, Action: "proc.exec", Target: "evil", Machine: "m"}); d.Allowed {
		t.Fatal("caller widened live policy through Rules()")
	}
}

// TestApprovalOwnsValues proves F2/§5.1: neither the input nor the returned approval aliases the live record.
func TestApprovalOwnsValues(t *testing.T) {
	ctx := context.Background()
	policy := op(`{"id":"ask","effect":"ask","actions":["ci.*"],"approval":{"max_ttl":"PT4H"}}`)
	e, st, _ := fixture(t, policy)
	in := baseApproval()
	got, err := e.CreateApproval(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	in.Actions[0] = "ci.pipeline_edit" // caller-held input slice
	st.now = t0.Add(time.Minute)
	if d, _ := e.Decide(ctx, Request{Thread: rootID, Action: "ci.pipeline_edit", Target: "t1", Machine: "m", ApprovalID: got.ID}); d.Allowed {
		t.Fatal("caller widened a live approval's actions through its input")
	}
	e2, st2, _ := fixture(t, policy)
	got2, err := e2.CreateApproval(ctx, baseApproval())
	if err != nil {
		t.Fatal(err)
	}
	*got2.Revision = rev2 // caller-held returned pointer
	st2.now = t0.Add(time.Minute)
	if d, err := e2.Decide(ctx, Request{Thread: rootID, Action: "ci.run", Target: "t1", Machine: "m", ApprovalID: got2.ID}); err != nil || !d.Allowed {
		t.Fatalf("caller changed a live approval's revision through the result: %+v %v", d, err)
	}
}

// --- F3: a void is durable (§5.3) ---

// TestVoidIsDurable proves F3/§5.3: when approval.voided cannot be recorded, use stays denied and the record is retried.
func TestVoidIsDurable(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`))
	got, err := e.CreateApproval(ctx, baseApproval())
	if err != nil {
		t.Fatal(err)
	}
	st.now = t0.Add(time.Minute)
	req := Request{Thread: rootID, Action: "ci.run", Target: "t1", Machine: "m", ApprovalID: got.ID}
	st.cur.WorktreeDigest = wd2
	dead, cancel := context.WithCancel(ctx)
	cancel()
	if d, _ := e.Decide(dead, req); d.Allowed {
		t.Fatal("allowed with a changed worktree")
	}
	st.cur.WorktreeDigest = wd1 // the condition clears, but the approval is already void
	if d, _ := e.Decide(ctx, req); d.Allowed {
		t.Fatal("voided approval usable after its record failed")
	}
	v := events(t, log, rootID, "approval.voided")
	if len(v) != 1 || v[0]["reason"] != "diff_changed" || v[0]["approval_id"] != got.ID {
		t.Fatalf("void record not retried: %v", v)
	}
	if d, _ := e.Decide(ctx, req); d.Allowed || len(events(t, log, rootID, "approval.voided")) != 1 {
		t.Fatalf("after the record: %+v", d)
	}
}

// --- F4: settle can be retried (§6) ---

// TestSettleRetry proves F4/§6: a committed revocation whose settle failed is finished by a retry, with dispatch blocked throughout.
func TestSettleRetry(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"all","effect":"allow","actions":["*"]}`))
	st.modes[rootID] = "full_control"
	g, err := e.IssueGrant(ctx, rootID, "h", "i", "operator:alice")
	if err != nil {
		t.Fatal(err)
	}
	fails := 1
	st.settle = func(context.Context, string, []string) error {
		if fails > 0 {
			fails--
			return errors.New("runner offline")
		}
		return nil
	}
	blocked := func() bool {
		d, _ := e.Decide(ctx, Request{Thread: rootID, Action: "host.exec", Machine: "h", Identity: "i"})
		return !d.Allowed
	}
	if err := e.Revoke(ctx, g.ID, "operator:alice"); err == nil {
		t.Fatal("settle failure hidden")
	}
	if !blocked() {
		t.Fatal("dispatch allowed between failed settle and retry")
	}
	if err := e.Revoke(ctx, g.ID, "operator:alice"); err != nil {
		t.Fatalf("retry refused: %v", err)
	}
	r := events(t, log, rootID, "grant.revoked")
	if len(r) != 2 || r[0]["settled"] != false || r[1]["settled"] != true {
		t.Fatalf("%v", r)
	}
	if !blocked() {
		t.Fatal("dispatch allowed after settle")
	}
}

// --- F5: creation-time constraints honor known dimensions (§5.1) ---

// TestCreationConstraintSelection proves F5/§5.1: known dimensions (role, mode, target) exclude a constraint; unknown ones (host, args, paths) count.
func TestCreationConstraintSelection(t *testing.T) {
	ctx := context.Background()
	base := op(`{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`)
	excluded := []string{
		`{"id":"aud","effect":"ask","actions":["ci.run"],"match":{"roles":["auditor"]},"approval":{"max_ttl":"PT5M"}}`,
		`{"id":"fc","effect":"ask","actions":["ci.run"],"match":{"modes":["full_control"]},"approval":{"max_ttl":"PT5M"}}`,
		`{"id":"tg","effect":"ask","actions":["ci.run"],"match":{"targets":["t2"]},"approval":{"max_ttl":"PT5M"}}`,
	}
	for _, r := range excluded {
		e, _, _ := fixture(t, base, skill(r))
		if _, err := e.CreateApproval(ctx, baseApproval()); err != nil { // builder, provider mode, target t1, one hour
			t.Errorf("inapplicable constraint %s applied: %v", r, err)
		}
	}
	counted := []string{
		`{"id":"bld","effect":"ask","actions":["ci.run"],"match":{"roles":["builder"]},"approval":{"max_ttl":"PT5M"}}`,
		`{"id":"host","effect":"ask","actions":["ci.run"],"match":{"hosts":["h9"]},"approval":{"max_ttl":"PT5M"}}`,
		`{"id":"arg","effect":"ask","actions":["ci.run"],"match":{"args":["--x"]},"approval":{"max_ttl":"PT5M"}}`,
		`{"id":"pth","effect":"ask","actions":["ci.run"],"match":{"paths":["/x/*"]},"approval":{"max_ttl":"PT5M"}}`,
	}
	for _, r := range counted {
		e, _, _ := fixture(t, base, skill(r))
		if _, err := e.CreateApproval(ctx, baseApproval()); !errors.Is(err, ErrInvalidApproval) {
			t.Errorf("constraint that might apply was ignored %s: %v", r, err)
		}
	}
}

// --- F6/F7: loader identity and numeric const (§2, schema) ---

// TestDuplicateLocalIDs proves F6/§2: two rules with one local id in one file are rejected.
func TestDuplicateLocalIDs(t *testing.T) {
	src := op(`{"id":"r","effect":"allow","actions":["a.b"]},{"id":"r","effect":"deny","actions":["c.d"]}`)
	if _, err := Load([]byte(src), Origin{Kind: OriginStore}); err == nil {
		t.Fatal("duplicate local id accepted")
	}
}

// TestPolicyVersionNumeric proves F7: policy_version follows JSON Schema const number equality.
func TestPolicyVersionNumeric(t *testing.T) {
	file := func(v string) []byte { return []byte(`{"policy_version":` + v + `,"source":"operator","rules":[]}`) }
	for _, v := range []string{"3", "3.0", "3.000", "0.3e1", "30e-1", "3E0", "3e+0"} {
		if _, err := Load(file(v), Origin{Kind: OriginStore}); err != nil {
			t.Errorf("%s rejected: %v", v, err)
		}
	}
	for _, v := range []string{`"3"`, "3.1", "-3", "2.9999999999999999999", "3e999999999", "true", "[3]", "{}"} {
		if _, err := Load(file(v), Origin{Kind: OriginStore}); err == nil {
			t.Errorf("%s accepted", v)
		}
	}
}

// --- F8: D-13 args and D-14 path containment ---

// TestArgsD13 proves F8/D-13: args [] matches nothing; several entries must all be present; missing args matches anything.
func TestArgsD13(t *testing.T) {
	ctx := context.Background()
	try := func(rule string, args ...string) bool {
		e, _, _ := fixture(t, op(rule))
		d, err := e.Decide(ctx, Request{Thread: rootID, Action: "proc.exec", Machine: "m", Args: args})
		if err != nil {
			t.Fatal(err)
		}
		return d.Allowed
	}
	empty := `{"id":"a","effect":"allow","actions":["proc.exec"],"match":{"args":[]}}`
	both := `{"id":"a","effect":"allow","actions":["proc.exec"],"match":{"args":["--x","--y"]}}`
	none := `{"id":"a","effect":"allow","actions":["proc.exec"]}`
	if try(empty) || try(empty, "--x") {
		t.Error("args [] matched")
	}
	if try(both, "--x") || !try(both, "--y", "--z", "--x") {
		t.Error("several args are not ANDed")
	}
	if !try(none) || !try(none, "--q") {
		t.Error("missing args dimension did not match anything")
	}
}

// makeLink creates a directory symlink at link pointing to target. Where the
// OS refuses symlinks (Windows without the privilege) it creates a directory
// junction instead, which needs no privilege and which D-14 must also follow.
func makeLink(target, link string) error {
	err := os.Symlink(target, link)
	if err == nil || runtime.GOOS != "windows" {
		return err
	}
	if out, jerr := exec.Command("cmd", "/c", "mklink", "/J", link, target).CombinedOutput(); jerr != nil {
		return fmt.Errorf("%v; junction: %v %s", err, jerr, out)
	}
	return nil
}

// TestPathsD14 proves F8/D-14: ".." cannot escape @mediated and a path-scoped allow never follows a link outside.
func TestPathsD14(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	wc, outside := filepath.Join(root, "wc"), filepath.Join(root, "outside")
	for _, d := range []string{wc, outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	setup := func(rules string) *Engine {
		e, st, _ := fixture(t, op(rules))
		ti := st.threads[rootID]
		ti.Mediated = []string{wc}
		st.threads[rootID] = ti
		return e
	}
	write := func(e *Engine, p string) bool {
		d, err := e.Decide(ctx, Request{Thread: rootID, Action: "fs.write", Machine: "m", Paths: []string{p}})
		if err != nil {
			t.Fatal(err)
		}
		return d.Allowed
	}
	sep := string(filepath.Separator)
	allowWC := setup(`{"id":"a","effect":"allow","actions":["fs.write"],"match":{"paths":["@mediated"]}}`)
	if !write(allowWC, filepath.Join(wc, "new", "f")) {
		t.Error("non-existent path inside the copy not allowed")
	}
	if write(allowWC, wc+sep+".."+sep+"outside"+sep+"f") {
		t.Error(".. escaped the mediated copy")
	}
	if write(allowWC, filepath.Join(root, "wc-sibling", "f")) {
		t.Error("lookalike sibling matched")
	}
	link := filepath.Join(wc, "link")
	if err := makeLink(outside, link); err != nil {
		t.Skipf("link cases not run: the OS refused both a symlink and a junction (%v)", err)
	}
	if write(allowWC, filepath.Join(link, "f")) {
		t.Error("allow followed a link out of the mediated copy")
	}
	denyWC := setup(`{"id":"all","effect":"allow","actions":["fs.write"]},{"id":"d","effect":"deny","actions":["fs.write"],"match":{"paths":["@mediated"]}}`)
	if write(denyWC, filepath.Join(link, "f")) {
		t.Error("deny on the mediated area escaped through a link")
	}
	into := filepath.Join(root, "in-link")
	if err := makeLink(wc, into); err != nil {
		t.Fatal(err)
	}
	if !write(allowWC, filepath.Join(into, "f")) {
		t.Error("link resolving into the copy was not treated as inside")
	}
}
