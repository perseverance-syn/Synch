package policy

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Tests for API and behaviour added in cards 104-F1 and 104-F2
// (Request.Start, G1 first-append semantics, Windows spellings, recovery
// failure). They do not compile, or test a defined behaviour, at earlier commits.

// TestStartRunsInsideBoundary proves F1 (sibling path): Start runs only for allowed actions, and its failure blocks dispatch.
func TestStartRunsInsideBoundary(t *testing.T) {
	ctx := context.Background()
	e, _, _ := fixture(t, op(`{"id":"all","effect":"allow","actions":["git.*"]}`))
	calls := 0
	start := func() error { calls++; return nil }
	if d, _ := e.Decide(ctx, Request{Thread: rootID, Action: "git.push", Machine: "m", Start: start}); !d.Allowed || calls != 1 {
		t.Fatalf("allowed: %+v calls=%d", d, calls)
	}
	if d, _ := e.Decide(ctx, Request{Thread: rootID, Action: "proc.exec", Machine: "m", Start: start}); d.Allowed || calls != 1 {
		t.Fatalf("denied action started: %+v calls=%d", d, calls)
	}
	fail := func() error { return errors.New("runner gone") }
	if d, err := e.Decide(ctx, Request{Thread: rootID, Action: "git.push", Machine: "m", Start: fail}); d.Allowed || err == nil {
		t.Fatalf("failed start reported allowed: %+v %v", d, err)
	}
}

// TestFirstAppendFailureLeavesNothing proves G1 for approvals: a failed
// explicit revoke returns an error and changes nothing; a retry records it.
func TestFirstAppendFailureLeavesNothing(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`))
	got, err := e.CreateApproval(ctx, baseApproval())
	if err != nil {
		t.Fatal(err)
	}
	dead, cancel := context.WithCancel(ctx)
	cancel()
	if err := e.RevokeApproval(dead, got.ID); err == nil {
		t.Fatal("failed record reported success")
	}
	st.now = t0.Add(time.Minute)
	req := Request{Thread: rootID, Action: "ci.run", Target: "t1", Machine: "m", ApprovalID: got.ID}
	if d, err := e.Decide(ctx, req); err != nil || !d.Allowed {
		t.Fatalf("a revoke that never committed changed the approval: %+v %v", d, err)
	}
	if err := e.RevokeApproval(ctx, got.ID); err != nil {
		t.Fatal(err)
	}
	if v := events(t, log, rootID, "approval.voided"); len(v) != 1 || v[0]["reason"] != "revoked" {
		t.Fatalf("%v", v)
	}
	if d, _ := e.Decide(ctx, req); d.Allowed {
		t.Fatal("revoked approval usable")
	}
}

// TestFirstRevocationAppendFailure proves G1 for grants: a failed commit
// record returns an error and leaves nothing behind; a retry commits,
// settles and records both events.
func TestFirstRevocationAppendFailure(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"all","effect":"allow","actions":["*"]}`))
	st.modes[rootID] = "full_control"
	g, err := e.IssueGrant(ctx, rootID, "h", "i", "operator:alice")
	if err != nil {
		t.Fatal(err)
	}
	dead, cancel := context.WithCancel(ctx)
	cancel()
	if err := e.Revoke(dead, g.ID, "operator:alice"); err == nil {
		t.Fatal("failed commit record reported success")
	}
	if len(events(t, log, rootID, "grant.revoked")) != 0 {
		t.Fatal("half-done revocation recorded")
	}
	if err := e.Revoke(ctx, g.ID, "operator:alice"); err != nil {
		t.Fatal(err)
	}
	if r := events(t, log, rootID, "grant.revoked"); len(r) != 2 || r[1]["settled"] != true || r[1]["revoked_by"] != "operator:alice" {
		t.Fatalf("%v", r)
	}
	if d, _ := e.Decide(ctx, Request{Thread: rootID, Action: "host.exec", Machine: "h", Identity: "i"}); d.Allowed {
		t.Fatal("dispatch allowed after revocation")
	}
}

// TestPlainWindowsPath proves G4's spelling rules on every OS: extended drive
// and UNC prefixes map to the ordinary spelling; other namespaces are rejected.
func TestPlainWindowsPath(t *testing.T) {
	old := windowsPaths
	windowsPaths = true
	defer func() { windowsPaths = old }()
	cases := map[string]string{
		`C:\wc\f`:             `C:\wc\f`,
		`\\?\C:\wc\f`:         `C:\wc\f`,
		`\\?\c:\wc`:           `c:\wc`,
		`\\?\UNC\srv\share\f`: `\\srv\share\f`,
		`\\?\unc\srv\share`:   `\\srv\share`,
		`\\?\C:\`:             `C:\`,
		`\\?\C:\wc\`:          `C:\wc\`,
		`\\?\C:\wc\f.txt`:     `C:\wc\f.txt`,
		`\\srv\share\f`:       `\\srv\share\f`,
		`C:\wc\f.`:            `C:\wc\f.`, // ordinary spelling: Win32 itself normalizes it
	}
	for in, want := range cases {
		if got, ok := plainWindowsPath(in); !ok || got != want {
			t.Errorf("%s: got %q %v, want %q", in, got, ok, want)
		}
	}
	if got, ok := plainGlob(`\\?\C:\wc\*.go`); !ok || got != `C:\wc\*.go` {
		t.Errorf("glob with wildcard: %q %v", got, ok)
	}
	// K4: anything the Win32 normalizer would change under the extended prefix is rejected.
	for _, in := range []string{`\\.\C:\wc`, `\\?\Volume{0}\x`, `\\?\GLOBALROOT\x`, `\\?\C:`, `\??\C:\wc`, `//?/C:/wc`,
		`\\?\C:\wc\f.`, `\\?\C:\wc\f `, `\\?\C:\wc\..\x`, `\\?\C:\wc\.\x`, `\\?\C:\wc\\x`, `\\?\C:\wc/x`,
		`\\?\C:\wc\nul`, `\\?\C:\wc\NUL.txt`, `\\?\C:\wc\com1`, `\\?\C:\wc\lpt9.log`, `\\?\C:\wc\conin$`,
		`\\?\C:\wc\a*b`, `\\?\UNC\srv`, `\\?\UNC\srv\share.\x`} {
		if _, ok := plainWindowsPath(in); ok {
			t.Errorf("%s accepted", in)
		}
	}
}

// TestRecoverFailureFailsClosed proves G1: if the log cannot be replayed,
// every call fails instead of running with partial records.
func TestRecoverFailureFailsClosed(t *testing.T) {
	_, st, log := fixture(t, op(`{"id":"all","effect":"allow","actions":["*"]}`))
	_ = log.Close()
	e := New(log, st, func() time.Time { return st.now })
	if d, err := e.Decide(context.Background(), Request{Thread: rootID, Action: "git.push", Machine: "m"}); err == nil || d.Allowed {
		t.Fatalf("decision without recovery: %+v %v", d, err)
	}
	if err := e.FlushPending(context.Background()); err == nil {
		t.Fatal("flush without recovery succeeded")
	}
}
