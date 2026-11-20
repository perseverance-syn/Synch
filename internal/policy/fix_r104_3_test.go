package policy

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"synch/internal/eventlog"
)

// Regression tests for review R104-3 (card 104-F3). This file uses only API
// that existed at 88a7768, so it can be run against that commit.

// --- K1/K2: a void is durable before the call that reports it returns ---

// raceVoid holds the log's write lock, starts first (which blocks in its void
// append and then fails because its context is cancelled), then starts second
// while that append is in flight. It returns second's error.
func raceVoid(t *testing.T, dbPath string, first func(context.Context), second func() error) error {
	t.Helper()
	release := holdWriteLock(t, dbPath)
	cctx, cancel := context.WithCancel(context.Background())
	firstDone, secondDone := make(chan struct{}), make(chan error, 1)
	go func() { first(cctx); close(firstDone) }()
	time.Sleep(200 * time.Millisecond) // first is now blocked appending its void
	go func() { secondDone <- second() }()
	time.Sleep(200 * time.Millisecond) // second arrives while first's append is in flight
	cancel()                           // first's append will fail
	time.Sleep(100 * time.Millisecond)
	release()
	<-firstDone
	return <-secondDone
}

// TestRevokeApprovalWaitsForInFlightVoid proves K2: while another path's void
// append is in flight and then fails, RevokeApproval does not report success
// unless a void is durable.
func TestRevokeApprovalWaitsForInFlightVoid(t *testing.T) {
	e, st, log := fixture(t, op(`{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`))
	a, err := e.CreateApproval(context.Background(), baseApproval())
	if err != nil {
		t.Fatal(err)
	}
	st.now = t0.Add(2 * time.Hour) // FlushPending will detect expiry and append a void
	err = raceVoid(t, st.dbPath,
		func(ctx context.Context) { _ = e.FlushPending(ctx) },
		func() error { return e.RevokeApproval(context.Background(), a.ID) })
	if n := len(events(t, log, rootID, "approval.voided")); err == nil && n != 1 {
		t.Fatalf("RevokeApproval reported success with %d durable voids", n)
	}
}

// TestRequestVoidDurableBeforeReturn proves K1: a use that detects a
// request-dependent void (target mismatch) does not return without error
// while the void is not durable, even if another use's append of the same
// void is in flight and fails.
func TestRequestVoidDurableBeforeReturn(t *testing.T) {
	e, st, log := fixture(t, op(`{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`))
	a, err := e.CreateApproval(context.Background(), baseApproval())
	if err != nil {
		t.Fatal(err)
	}
	st.now = t0.Add(time.Minute)
	wrong := Request{Thread: rootID, Action: "ci.run", Target: "t2", Machine: "m", ApprovalID: a.ID}
	err = raceVoid(t, st.dbPath,
		func(ctx context.Context) { _, _ = e.Decide(ctx, wrong) },
		func() error { _, err := e.Decide(context.Background(), wrong); return err })
	v := events(t, log, rootID, "approval.voided")
	if err == nil && (len(v) != 1 || v[0]["reason"] != "target_changed") {
		t.Fatalf("use returned without error but the void is not durable: %v", v)
	}
}

// TestRequestVoidRetried proves K1: when the void append fails, the call
// errors, the approval stays blocked in-process, and FlushPending records the
// void once the log works again.
func TestRequestVoidRetried(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`))
	a, err := e.CreateApproval(ctx, baseApproval())
	if err != nil {
		t.Fatal(err)
	}
	st.now = t0.Add(time.Minute)
	dead, cancel := context.WithCancel(ctx)
	cancel()
	if d, err := e.Decide(dead, Request{Thread: rootID, Action: "ci.run", Target: "t2", Machine: "m", ApprovalID: a.ID}); err == nil || d.Allowed {
		t.Fatalf("failed void write not reported: %+v %v", d, err)
	}
	good := Request{Thread: rootID, Action: "ci.run", Target: "t1", Machine: "m", ApprovalID: a.ID}
	if d, _ := e.Decide(ctx, good); d.Allowed {
		t.Fatal("approval usable after a request-dependent void")
	}
	if err := e.FlushPending(ctx); err != nil {
		t.Fatal(err)
	}
	if v := events(t, log, rootID, "approval.voided"); len(v) != 1 || v[0]["reason"] != "target_changed" {
		t.Fatalf("%v", v)
	}
}

// --- K3: replay validates every policy event it consumes ---

// corruptLog appends one raw policy event and returns a fresh engine over the log.
func corruptLog(t *testing.T, st *fakeState, log *eventlog.Store, typ string, version int, payload string) *Engine {
	t.Helper()
	_, err := log.Append(context.Background(), []eventlog.NewEvent{{
		ID: newULID(st.now), Stream: "thread:" + rootID, Type: typ, TypeVersion: version,
		Actor: eventlog.Actor{Kind: "daemon", ID: "synchd"}, ThreadID: rootID, Payload: json.RawMessage(payload),
	}})
	if err != nil {
		t.Fatal(err)
	}
	return New(log, st, func() time.Time { return st.now })
}

// TestReplayRejectsIncompleteVoid proves K3: an approval.voided without a
// reason makes recovery fail closed; nothing is authorized.
func TestReplayRejectsIncompleteVoid(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"all","effect":"allow","actions":["git.*"]},{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`))
	a, err := e.CreateApproval(ctx, baseApproval())
	if err != nil {
		t.Fatal(err)
	}
	e2 := corruptLog(t, st, log, "approval.voided", 1, `{"approval_id":"`+a.ID+`"}`)
	if d, err := e2.Decide(ctx, Request{Thread: rootID, Action: "git.push", Machine: "m"}); err == nil || d.Allowed {
		t.Fatalf("authorized over corrupt history: %+v %v", d, err)
	}
}

// TestReplayRejectsOrphanSettled proves K3: a settled:true revocation without
// a matching earlier commit makes recovery fail closed instead of cancelling
// the settle obligation.
func TestReplayRejectsOrphanSettled(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"all","effect":"allow","actions":["*"]}`))
	g, err := e.IssueGrant(ctx, rootID, "h", "i", "operator:alice")
	if err != nil {
		t.Fatal(err)
	}
	e2 := corruptLog(t, st, log, "grant.revoked", 2,
		`{"grant_id":"`+g.ID+`","affected_threads":["`+rootID+`"],"settled":true,"cause":"operator","revoked_by":"operator:alice"}`)
	if d, err := e2.Decide(ctx, Request{Thread: rootID, Action: "git.push", Machine: "m"}); err == nil || d.Allowed {
		t.Fatalf("authorized over an orphan completion: %+v %v", d, err)
	}
	if err := e2.FlushPending(ctx); err == nil {
		t.Fatal("flush succeeded over corrupt history")
	}
}

// --- K4: extended paths map only to the same object ---

// TestExtendedPathDistinctObjects proves K4 on Windows: files created through
// \\?\ with a literal trailing dot or space are distinct objects (os.SameFile)
// and a request naming them is blocked, never matched as the ordinary file.
func TestExtendedPathDistinctObjects(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("extended-length paths only exist on Windows")
	}
	ctx := context.Background()
	wc := filepath.Join(t.TempDir(), "wc")
	if err := os.MkdirAll(wc, 0o755); err != nil {
		t.Fatal(err)
	}
	plain := filepath.Join(wc, "f")
	if err := os.WriteFile(plain, []byte("plain"), 0o600); err != nil {
		t.Fatal(err)
	}
	plainInfo, err := os.Stat(plain)
	if err != nil {
		t.Fatal(err)
	}
	glob := strings.ReplaceAll(plain, `\`, `\\`)
	e, _, _ := fixture(t, op(`{"id":"a","effect":"allow","actions":["fs.write"],"match":{"paths":["`+glob+`"]}}`))
	write := func(p string) bool {
		d, err := e.Decide(ctx, Request{Thread: rootID, Action: "fs.write", Machine: "m", Paths: []string{p}})
		if err != nil {
			t.Fatal(err)
		}
		return d.Allowed
	}
	if !write(`\\?\` + plain) {
		t.Fatal("extended spelling of the same file not allowed")
	}
	for _, suffix := range []string{".", " "} {
		ext := `\\?\` + plain + suffix
		if err := os.WriteFile(ext, []byte("other"), 0o600); err != nil {
			t.Fatalf("create %q: %v", ext, err)
		}
		t.Cleanup(func() { _ = os.Remove(ext) }) // ordinary RemoveAll cannot delete it
		info, err := os.Stat(ext)
		if err != nil {
			t.Fatal(err)
		}
		if os.SameFile(info, plainInfo) {
			t.Fatalf("%q is the same file as %q on this host", ext, plain)
		}
		if write(ext) {
			t.Errorf("request for distinct file %q matched the allow for %q", ext, plain)
		}
	}
}
