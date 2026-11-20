package policy

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"synch/internal/eventlog"
)

// Regression tests for review R104-2 (card 104-F2). This file uses only API
// that existed at 6276927, so it can be run against that commit.

// --- G1: a fresh engine over the same log rebuilds and finishes every obligation ---

// TestRestartRecoversObligations proves G1: after reopening the store, a new
// engine's FlushPending settles a revocation left at settled:false and
// records a void that was detected but never persisted.
func TestRestartRecoversObligations(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"all","effect":"allow","actions":["host.*"]},{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`))
	st.modes[rootID] = "full_control"
	g, err := e.IssueGrant(ctx, rootID, "h", "i", "operator:alice")
	if err != nil {
		t.Fatal(err)
	}
	keep, err := e.CreateApproval(ctx, baseApproval()) // one hour
	if err != nil {
		t.Fatal(err)
	}
	short := baseApproval()
	short.ExpiresAt = t0.Add(5 * time.Minute)
	lost, err := e.CreateApproval(ctx, short)
	if err != nil {
		t.Fatal(err)
	}
	st.settle = func(context.Context, string, []string) error { return errors.New("runner offline") }
	if err := e.Revoke(ctx, g.ID, "operator:alice"); err == nil {
		t.Fatal("settle failure hidden")
	}
	st.now = t0.Add(10 * time.Minute) // the short approval has expired
	dead, cancel := context.WithCancel(ctx)
	cancel()
	if d, _ := e.Decide(dead, Request{Thread: rootID, Action: "ci.run", Target: "t1", Machine: "m", ApprovalID: lost.ID}); d.Allowed {
		t.Fatal("expired approval allowed")
	}
	// Restart: close the store, reopen it, build a fresh engine.
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	log2, err := eventlog.Open(st.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log2.Close() })
	settles := 0
	st.settle = func(context.Context, string, []string) error { settles++; return nil }
	e2 := New(log2, st, func() time.Time { return st.now })
	if err := e2.FlushPending(ctx); err != nil {
		t.Fatal(err)
	}
	r := events(t, log2, rootID, "grant.revoked")
	if settles != 1 || len(r) != 2 || r[1]["settled"] != true {
		t.Fatalf("revocation not resumed after restart: settles=%d %v", settles, r)
	}
	v := events(t, log2, rootID, "approval.voided")
	if len(v) != 1 || v[0]["approval_id"] != lost.ID || v[0]["reason"] != "expired" {
		t.Fatalf("lost void not re-detected after restart: %v", v)
	}
	if d, err := e2.Decide(ctx, Request{Thread: rootID, Action: "ci.run", Target: "t1", Machine: "m", ApprovalID: keep.ID}); err != nil || !d.Allowed {
		t.Fatalf("persisted approval lost on restart: %+v %v", d, err)
	}
	if d, _ := e2.Decide(ctx, Request{Thread: rootID, Action: "host.exec", Machine: "h", Identity: "i"}); d.Allowed {
		t.Fatal("revoked grant usable after restart")
	}
	if err := e2.FlushPending(ctx); err != nil || len(events(t, log2, rootID, "grant.revoked")) != 2 || len(events(t, log2, rootID, "approval.voided")) != 1 {
		t.Fatalf("second flush added records: %v", err)
	}
}

// --- G2: approval voids are ordered against in-flight decisions ---

// TestApprovalVoidOrdersInFlightDecision proves G2: a decision blocked in its
// use-time state read cannot return Allowed=true after a void of its
// approval committed, whether the void is an explicit revoke or a use-time
// void by another decision.
func TestApprovalVoidOrdersInFlightDecision(t *testing.T) {
	cases := map[string]func(e *Engine, id string) error{
		"explicit revoke": func(e *Engine, id string) error { return e.RevokeApproval(context.Background(), id) },
		"use-time void by another thread": func(e *Engine, id string) error {
			_, err := e.Decide(context.Background(), Request{Thread: otherID, Action: "ci.run", Target: "t1", Machine: "m", ApprovalID: id})
			return err
		},
	}
	for name, voidIt := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			e, st, log := fixture(t, op(`{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`))
			a, err := e.CreateApproval(ctx, baseApproval())
			if err != nil {
				t.Fatal(err)
			}
			st.now = t0.Add(time.Minute)
			entered, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			st.onCurr = func() { // block only the first decision's state read
				if calls.Add(1) == 1 {
					close(entered)
					<-release
				}
			}
			type out struct {
				d   Decision
				err error
			}
			decided, voided := make(chan out, 1), make(chan error, 1)
			go func() {
				d, err := e.Decide(ctx, Request{Thread: rootID, Action: "ci.run", Target: "t1", Machine: "m", ApprovalID: a.ID})
				decided <- out{d, err}
			}()
			<-entered
			go func() { voided <- voidIt(e, a.ID) }()
			time.Sleep(200 * time.Millisecond) // give the void every chance to commit
			committedInFlight := len(events(t, log, rootID, "approval.voided")) > 0
			close(release)
			first := <-decided
			if err := <-voided; err != nil {
				t.Fatal(err)
			}
			if first.err != nil || !first.d.Allowed {
				t.Fatalf("the earlier decision should complete first and be allowed: %+v %v", first.d, first.err)
			}
			if committedInFlight {
				t.Fatal("void committed while a decision that already read the approval was in flight, which then returned Allowed=true")
			}
			if len(events(t, log, rootID, "approval.voided")) != 1 {
				t.Fatal("void not recorded after the decision")
			}
		})
	}
}

// --- G3: the settler cannot change the revocation payload ---

// TestSettlerGetsCopy proves G3: a settler that reorders its input changes
// neither the completion event nor the list a retry receives.
func TestSettlerGetsCopy(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"all","effect":"allow","actions":["*"]}`))
	g, err := e.IssueGrant(ctx, rootID, "h", "i", "operator:alice")
	if err != nil {
		t.Fatal(err)
	}
	var seen [][]string
	st.settle = func(_ context.Context, _ string, threads []string) error {
		seen = append(seen, slices.Clone(threads))
		slices.Reverse(threads) // a misbehaving settler
		if len(seen) == 1 {
			return errors.New("runner offline")
		}
		return nil
	}
	_ = e.Revoke(ctx, g.ID, "operator:alice")
	if err := e.Revoke(ctx, g.ID, "operator:alice"); err != nil {
		t.Fatal(err)
	}
	want := []string{rootID, childID, grandID}
	if len(seen) != 2 || !slices.Equal(seen[0], want) || !slices.Equal(seen[1], want) {
		t.Fatalf("settler inputs %v", seen)
	}
	r := events(t, log, rootID, "grant.revoked")
	if len(r) != 2 {
		t.Fatalf("%v", r)
	}
	for _, ev := range r {
		var got []string
		for _, x := range ev["affected_threads"].([]any) {
			got = append(got, x.(string))
		}
		if !slices.Equal(got, want) {
			t.Fatalf("affected_threads changed between commit and completion: %v", r)
		}
	}
}

// --- G4: Windows extended-length spellings compare equal ---

// TestWindowsExtendedPaths proves G4 on Windows: \\?\C:\ and \\?\UNC\
// spellings match the same managed copy as the ordinary spellings, for
// requests, mediated roots and rule globs, for allow and deny.
func TestWindowsExtendedPaths(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows path namespaces only exist on Windows")
	}
	ctx := context.Background()
	root := t.TempDir()
	wc := filepath.Join(root, "wc")
	if err := os.MkdirAll(wc, 0o755); err != nil {
		t.Fatal(err)
	}
	ext := func(p string) string { return `\\?\` + p }
	run := func(rules, mediated, action, p string) bool {
		e, st, _ := fixture(t, op(rules))
		ti := st.threads[rootID]
		ti.Mediated = []string{mediated}
		st.threads[rootID] = ti
		d, err := e.Decide(ctx, Request{Thread: rootID, Action: action, Machine: "m", Paths: []string{p}})
		if err != nil {
			t.Fatal(err)
		}
		return d.Allowed
	}
	allow := `{"id":"a","effect":"allow","actions":["fs.write"],"match":{"paths":["@mediated"]}}`
	deny := `{"id":"all","effect":"allow","actions":["fs.delete"]},{"id":"d","effect":"deny","actions":["fs.delete"],"match":{"paths":["@mediated"]}}`
	inside := filepath.Join(wc, "f")
	if run(deny, wc, "fs.delete", ext(inside)) {
		t.Error("extended request path escaped a deny on the mediated copy")
	}
	if !run(allow, wc, "fs.write", ext(inside)) {
		t.Error("extended request path not allowed inside the copy")
	}
	if !run(allow, ext(wc), "fs.write", inside) || run(deny, ext(wc), "fs.delete", inside) {
		t.Error("extended mediated root not equal to the ordinary spelling")
	}
	globDeny := `{"id":"all","effect":"allow","actions":["fs.delete"]},{"id":"d","effect":"deny","actions":["fs.delete"],"match":{"paths":["` +
		strings.ReplaceAll(ext(wc)+`\*`, `\`, `\\`) + `"]}}`
	if run(globDeny, wc, "fs.delete", inside) {
		t.Error("extended rule glob did not match the ordinary path")
	}
	if run(`{"id":"all","effect":"allow","actions":["fs.write"]}`, wc, "fs.write", `\\.\`+inside) {
		t.Error("device-namespace path was not rejected")
	}
	vol := filepath.VolumeName(wc)
	unc := `\\localhost\` + strings.TrimSuffix(vol, ":") + `$` + wc[len(vol):]
	if _, err := os.Stat(unc); err != nil {
		t.Logf("loopback UNC part not run: the admin share is not reachable (%v)", err)
		return
	}
	extUNC := `\\?\UNC\` + unc[2:]
	if !run(allow, unc, "fs.write", filepath.Join(extUNC, "f")) || run(deny, unc, "fs.delete", filepath.Join(extUNC, "f")) {
		t.Error("extended UNC path not equal to the ordinary UNC spelling")
	}
}

// --- G5: an impossible constraint contributes no TTL ---

// TestImpossibleConstraint proves G5: rules that cannot match under D-13
// (args, hosts or paths listed as []) do not limit approval creation.
func TestImpossibleConstraint(t *testing.T) {
	ctx := context.Background()
	for _, dim := range []string{"args", "hosts", "paths"} {
		e, _, _ := fixture(t,
			op(`{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`),
			skill(`{"id":"never","effect":"ask","actions":["ci.run"],"match":{"`+dim+`":[]},"approval":{"max_ttl":"PT5M"}}`))
		got, err := e.CreateApproval(ctx, baseApproval()) // one hour
		if err != nil {
			t.Errorf("%s: []: impossible constraint limited creation: %v", dim, err)
			continue
		}
		if slices.Contains(got.ConstraintRules, "skill:pack.json@aaaaaaaaaaaa#never") {
			t.Errorf("%s: []: impossible constraint listed", dim)
		}
	}
}

// --- G6: exact numeric policy_version ---

// TestPolicyVersionExact proves G6: any JSON spelling of 3 within the length
// limit loads, unequal values fail, and an over-limit token fails with a clear error.
func TestPolicyVersionExact(t *testing.T) {
	file := func(v string) []byte { return []byte(`{"policy_version":` + v + `,"source":"operator","rules":[]}`) }
	zeros := func(n int) string { return strings.Repeat("0", n) }
	for _, v := range []string{"3." + zeros(70), "3." + zeros(4000), "0.03e2", "300e-2", "0." + zeros(60) + "3e61"} {
		if _, err := Load(file(v), Origin{Kind: OriginStore}); err != nil {
			t.Errorf("%.40s… rejected: %v", v, err)
		}
	}
	for _, v := range []string{"3." + zeros(70) + "1", "3e1", "0.3", "3e99999999999999999999"} {
		if _, err := Load(file(v), Origin{Kind: OriginStore}); err == nil {
			t.Errorf("%.40s… accepted", v)
		}
	}
	_, err := Load(file("3."+zeros(5000)), Origin{Kind: OriginStore})
	if err == nil || !strings.Contains(err.Error(), "longer than") {
		t.Errorf("over-limit token: %v", err)
	}
}
