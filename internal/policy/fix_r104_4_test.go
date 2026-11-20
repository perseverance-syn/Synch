package policy

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"synch/internal/eventlog"
)

// Regression tests for review R104-4 (card 104-F4, V5). This file uses only
// API that existed at ccba5dc, so it can be run against that commit.

// appendRaw appends one event with a raw payload to thread's stream.
func appendRaw(t *testing.T, log *eventlog.Store, thread, typ string, version int, payload string) {
	t.Helper()
	_, err := log.Append(context.Background(), []eventlog.NewEvent{{
		ID: newULID(time.Now()), Stream: "thread:" + thread, Type: typ, TypeVersion: version,
		Actor: eventlog.Actor{Kind: "daemon", ID: "synchd"}, ThreadID: thread, Payload: json.RawMessage(payload),
	}})
	if err != nil {
		t.Fatal(err)
	}
}

// --- V5 blocker 1: C6 is checked as of the revocation, not at recovery time ---

// TestReplayC6TerminalAfterRevocation proves V1/C6: a thread_ended revocation
// whose terminal thread.state_changed comes after it in the log is corrupt,
// even though the thread is terminal by the time recovery runs.
func TestReplayC6TerminalAfterRevocation(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"all","effect":"allow","actions":["*"]}`))
	g, err := e.IssueGrant(ctx, rootID, "h", "i", "operator:alice")
	if err != nil {
		t.Fatal(err)
	}
	appendRaw(t, log, rootID, "grant.revoked", 2, `{"grant_id":"`+g.ID+`","affected_threads":["`+rootID+`"],"settled":false,"cause":"thread_ended","ended_thread":"`+rootID+`"}`)
	appendRaw(t, log, rootID, "thread.state_changed", 4, `{"from":"awaiting_acceptance","to":"completed","transition":"T28"}`)
	ti := st.threads[rootID]
	ti.Terminal = true // terminal now, but not before the revocation
	st.threads[rootID] = ti
	e2 := New(log, st, func() time.Time { return st.now })
	if d, err := e2.Decide(ctx, Request{Thread: otherID, Action: "git.push", Machine: "m"}); err == nil || d.Allowed {
		t.Fatalf("recovered a revocation recorded before the thread ended: %+v %v", d, err)
	}
}

// --- V5 blocker 2: a forbidden property is present even when empty ---

// TestReplayRejectsEmptyEndedThreadOnOperatorCompletion proves V1: an
// operator-form completion carrying ended_thread (here "") is malformed and
// must not cancel the pending settlement.
func TestReplayRejectsEmptyEndedThreadOnOperatorCompletion(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"all","effect":"allow","actions":["*"]}`))
	g, err := e.IssueGrant(ctx, rootID, "h", "i", "operator:alice")
	if err != nil {
		t.Fatal(err)
	}
	st.settle = func(context.Context, string, []string) error { return errors.New("runner offline") }
	_ = e.Revoke(ctx, g.ID, "operator:alice") // commit recorded, settle failed
	r := events(t, log, rootID, "grant.revoked")
	if len(r) != 1 {
		t.Fatalf("%v", r)
	}
	affected, _ := json.Marshal(r[0]["affected_threads"])
	appendRaw(t, log, rootID, "grant.revoked", 2, `{"grant_id":"`+g.ID+`","affected_threads":`+string(affected)+
		`,"settled":true,"cause":"operator","revoked_by":"operator:alice","ended_thread":""}`)
	settles := 0
	st.settle = func(context.Context, string, []string) error { settles++; return nil }
	e2 := New(log, st, func() time.Time { return st.now })
	if err := e2.FlushPending(ctx); err == nil {
		t.Fatalf("recovered over a malformed completion (settles=%d)", settles)
	}
	if d, _ := e2.Decide(ctx, Request{Thread: otherID, Action: "git.push", Machine: "m"}); d.Allowed {
		t.Fatal("authorized over corrupt history")
	}
}

// --- V5 blocker 3: an extended DOS device alias is rejected ---

// TestExtendedDeviceAliasRejected proves V3: \\?\...\NUL: (which Win32 turns
// into \\.\NUL) is rejected before matching; on Windows a request naming it
// is blocked even under an allow-all rule.
func TestExtendedDeviceAliasRejected(t *testing.T) {
	old := windowsPaths
	windowsPaths = true
	defer func() { windowsPaths = old }()
	if p, ok := plainWindowsPath(`\\?\C:\wc\NUL:`); ok {
		t.Fatalf("accepted as %q", p)
	}
	if runtime.GOOS != "windows" {
		t.Skip("the request-level check needs Windows path semantics")
	}
	wc := filepath.Join(t.TempDir(), "wc")
	if err := os.MkdirAll(wc, 0o755); err != nil {
		t.Fatal(err)
	}
	e, _, _ := fixture(t, op(`{"id":"all","effect":"allow","actions":["fs.write"]}`))
	d, err := e.Decide(context.Background(), Request{Thread: rootID, Action: "fs.write", Machine: "m", Paths: []string{`\\?\` + wc + `\NUL:`}})
	if err != nil || d.Allowed {
		t.Fatalf("device alias allowed: %+v %v", d, err)
	}
}
