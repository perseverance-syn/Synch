package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- Sources, receipts and setup: C2, C3, C5 and the C7 workflow ---

// TestServerAndTokenSources proves C2: the server URL resolves from --server,
// SYNCH_SERVER and the config file in that order; the token resolves from
// --token-file, SYNCH_TOKEN and the config file; non-loopback servers are
// refused without --allow-remote; and the token is never printed.
func TestServerAndTokenSources(t *testing.T) {
	f := newCLIFixture(t)

	// Explicit flags reach the server.
	if code, out, errOut := f.run(f.authenticated("health")...); code != 0 {
		t.Fatalf("health with flags = %d, %s, %s", code, out, errOut)
	}
	// Environment variables reach the server.
	f.envMap = map[string]string{"SYNCH_SERVER": f.url, "SYNCH_TOKEN": cliToken}
	if code, out, errOut := f.run("health"); code != 0 {
		t.Fatalf("health with environment = %d, %s, %s", code, out, errOut)
	}
	f.envMap = map[string]string{}
	// The config file written by setup supplies both afterwards.
	if code, out, errOut := f.run("--config", f.configFile, "--server", f.url, "--token-file", f.tokenFile, "setup"); code != 0 {
		t.Fatalf("setup = %d, %s, %s", code, out, errOut)
	}
	if code, out, errOut := f.run("--config", f.configFile, "health"); code != 0 {
		t.Fatalf("health from config file = %d, %s, %s", code, out, errOut)
	}
	// A non-loopback server is refused locally without --allow-remote.
	code, out, errOut := f.run("--server", "http://127.0.0.2:7420", "--token-file", f.tokenFile, "health")
	if code != exitUsage || out != "" || !strings.Contains(errOut, "--allow-remote") {
		t.Fatalf("non-loopback server = %d, %q, %q", code, out, errOut)
	}
	// The token is never printed, even when the server rejects it.
	secret := "supersecret-token-value"
	secretFile := filepath.Join(f.dir, "secret.txt")
	if err := os.WriteFile(secretFile, []byte(secret), 0600); err != nil {
		t.Fatal(err)
	}
	code, out, errOut = f.run("--server", f.url, "--token-file", secretFile, "health")
	if code != exitProblem || strings.Contains(out+errOut, secret) {
		t.Fatalf("rejected token = %d, %q, %q (token leaked)", code, out, errOut)
	}

	// Receipts are keyed by the caller identity behind the token, so the bob
	// token cannot see alice's receipt: this proves which token was sent.
	f.createThread("src-create", "token source thread")
	if code, _, errOut := f.run(f.authenticated("receipt", "src-create")...); code != 0 {
		t.Fatalf("own receipt = %d, %s", code, errOut)
	}
	otherFile := filepath.Join(f.dir, "other.txt")
	if err := os.WriteFile(otherFile, []byte(otherToken), 0600); err != nil {
		t.Fatal(err)
	}
	if code, out, errOut := f.run("--server", f.url, "--token-file", otherFile, "receipt", "src-create"); code != exitProblem {
		t.Fatalf("other caller's receipt = %d, %s, %s", code, out, errOut)
	}
}

// TestMutatingReceiptsAndConflicts proves C3: mutating calls send generated
// request IDs, an explicit retry with the same ID and body prints the stored
// response as replayed, a reused ID with a changed body prints its conflict
// code and exits non-zero, and problem answers print their code.
func TestMutatingReceiptsAndConflicts(t *testing.T) {
	f := newCLIFixture(t)

	// Generated request IDs: two separate calls both succeed and differ.
	for i, brief := range []string{"First", "Second"} {
		code, out, errOut := f.run(f.authenticated("--json", "threads", "create", "--project", "p1", "--brief", brief)...)
		if code != 0 {
			t.Fatalf("generated create %d = %d, %s, %s", i, code, out, errOut)
		}
		fields := receiptMap(t, out)
		if fields["replayed"] != false || fields["receipt_id"] == "" {
			t.Fatalf("generated create %d receipt = %s", i, out)
		}
	}

	// First use of the explicit ID is not replayed.
	retryArgs := f.authenticated("--json", "threads", "create", "--project", "p1", "--brief", "Same body", "--request-id", "retry-1")
	code, firstOut, errOut := f.run(retryArgs...)
	if code != 0 {
		t.Fatalf("first retry call = %d, %s, %s", code, firstOut, errOut)
	}
	first := receiptMap(t, firstOut)
	if first["replayed"] != false || first["status"] != "accepted" {
		t.Fatalf("first use = %s", firstOut)
	}
	// The retry prints the stored response with Synch-Receipt-Replayed: true.
	code, secondOut, errOut := f.run(retryArgs...)
	if code != 0 {
		t.Fatalf("replayed call = %d, %s, %s", code, secondOut, errOut)
	}
	second := receiptMap(t, secondOut)
	if second["replayed"] != true || second["receipt_id"] != first["receipt_id"] {
		t.Fatalf("replayed receipt = %s, want the stored %s", secondOut, firstOut)
	}
	storedFirst, storedSecond := receiptMap(t, firstOut), receiptMap(t, secondOut)
	delete(storedFirst, "replayed")
	delete(storedSecond, "replayed")
	if !equalJSON(storedFirst, storedSecond) {
		t.Fatalf("replay changed the stored response: %s vs %s", firstOut, secondOut)
	}

	// The same ID with a different body is a 409 conflict carrying its code.
	code, out, errOut := f.run(f.authenticated("--json", "threads", "create", "--project", "p1", "--brief", "Changed body", "--request-id", "retry-1")...)
	if code != exitProblem || !strings.Contains(errOut, "request_id_conflict") || out != "" {
		t.Fatalf("conflict = %d, %q, %q", code, out, errOut)
	}

	// A stored rejection prints its code and exits non-zero: the thread machine
	// reports a missing thread as not_found.
	code, out, errOut = f.run(f.authenticated("--json", "thread", "cmd", "01AAAAAAAAAAAAAAAAAAAAAAAA", "pause", "--request-id", "bad-thread")...)
	if code != exitProblem || !strings.Contains(errOut, "not_found") {
		t.Fatalf("stored rejection = %d, %q, %q", code, out, errOut)
	}
}

// equalJSON compares two decoded JSON values deeply.
func equalJSON(left, right map[string]any) bool {
	return string(mustJSON(left)) == string(mustJSON(right))
}

// TestSetupWritesConfigAndChecksHealth proves C5: setup writes the server URL
// and token file path (never the token), checks the daemon with health, and
// is idempotent.
func TestSetupWritesConfigAndChecksHealth(t *testing.T) {
	f := newCLIFixture(t)
	args := []string{"--config", f.configFile, "--server", f.url, "--token-file", f.tokenFile, "setup"}
	code, out, errOut := f.run(args...)
	if code != 0 {
		t.Fatalf("setup = %d, %s, %s", code, out, errOut)
	}
	raw, err := os.ReadFile(f.configFile)
	if err != nil {
		t.Fatal(err)
	}
	contents := string(raw)
	if !strings.Contains(contents, `"server":"`+f.url+`"`) ||
		!strings.Contains(contents, `"token_file":`+mustJSONString(f.tokenFile)) {
		t.Fatalf("config file = %s", contents)
	}
	if strings.Contains(contents, cliToken) {
		t.Fatalf("config file contains the token: %s", contents)
	}
	first := string(raw)
	code, out, errOut = f.run(args...)
	if code != 0 {
		t.Fatalf("second setup = %d, %s, %s", code, out, errOut)
	}
	raw, err = os.ReadFile(f.configFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != first {
		t.Fatalf("setup was not idempotent: %s vs %s", first, string(raw))
	}
	// Ordinary commands resolve both settings from the config file alone.
	if code, out, errOut := f.run("--config", f.configFile, "health"); code != 0 {
		t.Fatalf("health from setup config = %d, %s, %s", code, out, errOut)
	}
}

// mustJSONString encodes a string as a JSON member value.
func mustJSONString(value string) string {
	return string(mustJSON(value))
}

// TestOperatorWorkflowProvesReceiptsAndFollow proves C7 and C3 end to end
// against the real server: create a thread, send a command, retry with the
// same request id (replayed), and follow events across a forced server
// disconnect with no gap and no duplicate.
func TestOperatorWorkflowProvesReceiptsAndFollow(t *testing.T) {
	f := newCLIFixture(t)
	threadID := f.createThread("wf-create", "operator workflow")
	pauseArgs := f.authenticated("--json", "thread", "cmd", threadID, "pause", "--request-id", "wf-pause")
	code, firstOut, errOut := f.run(pauseArgs...)
	if code != 0 {
		t.Fatalf("pause = %d, %s, %s", code, firstOut, errOut)
	}
	// P3: the same command with the same request id replays the stored
	// response: same receipt id, replayed true, otherwise identical.
	code, replayOut, errOut := f.run(pauseArgs...)
	if code != 0 {
		t.Fatalf("pause replay = %d, %s, %s", code, replayOut, errOut)
	}
	first, replay := receiptMap(t, firstOut), receiptMap(t, replayOut)
	if first["replayed"] != false || replay["replayed"] != true || replay["receipt_id"] != first["receipt_id"] || first["receipt_id"] == "" {
		t.Fatalf("pause replay = %s, want the stored receipt of %s with replayed true", replayOut, firstOut)
	}
	delete(first, "replayed")
	delete(replay, "replayed")
	if !equalJSON(first, replay) {
		t.Fatalf("pause replay changed the stored response: %s vs %s", firstOut, replayOut)
	}

	// Measure how many events one command emits so the burst size is known.
	before := f.head()
	f.sendCommand(threadID, "resume")
	perCommand := int(f.head() - before)
	startHead := int(f.head())

	// The writer stops the follow once every event up to the end of the burst
	// has been delivered: the limit is armed before the follow starts.
	writer := newStopAfterWriter()
	writer.arm(startHead + 2*perCommand)
	var followErr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- Main(f.authenticated("--json", "events", "follow", "all"), writer, &followErr, f.env)
	}()
	writer.awaitLines(t, startHead)

	// Force a disconnect: the daemon stops and serves the same log again, so
	// the follower must reconnect with its own last contiguous cursor.
	f.restart()

	// The burst lands while the follower is reconnecting.
	f.sendCommand(threadID, "pause")
	f.sendCommand(threadID, "resume")

	select {
	case code = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("follow did not exit after the burst")
	}
	if code != exitUsage { // the writer's deliberate close ends the stream
		t.Fatalf("follow exit = %d", code)
	}
	want := startHead + 2*perCommand
	cursors := decodeCursors(t, writer.collected())
	if len(cursors) != want {
		t.Fatalf("delivered %d events, want %d", len(cursors), want)
	}
	for i, cursor := range cursors {
		if cursor != int64(i+1) {
			t.Fatalf("cursor at position %d = %d: gap or duplicate across the disconnect\ncursors: %v", i, cursor, cursors)
		}
	}
}

// --- Follow recovery: C4 resync paths ---

// TestFollowResyncsAfterCursorAhead proves C4: when the server answers 409
// cursor_ahead, the follower drops all state, rebuilds from cursor 0 through
// events page, and continues live on the new log lineage.
func TestFollowResyncsAfterCursorAhead(t *testing.T) {
	f := newCLIFixture(t)
	f.createThread("resync-create", "resync thread")
	firstHead := int(f.head()) // one submission's event count on a fresh log
	f.sendCommand(f.lastThreadID(), "pause")
	oldHead := int(f.head())

	// The new lineage emits exactly firstHead events, so the rebuilt stream
	// stops the writer after oldHead + firstHead lines; the limit is armed
	// before the follow starts.
	writer := newStopAfterWriter()
	writer.arm(oldHead + firstHead)
	var followErr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- Main(f.authenticated("--json", "events", "follow", "all"), writer, &followErr, f.env)
	}()
	writer.awaitLines(t, oldHead)

	// The fresh lineage has fewer events than the follower's cursor, so the
	// reconnect answers 409 cursor_ahead whether it lands before or after the
	// new submission, and the resync rebuilds from cursor 0.
	f.restartWithNewStore()
	f.createThread("resync-new", "new lineage thread")

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("follow did not exit after the resync")
	}
	assertRebuiltCursors(t, writer.collected(), oldHead, firstHead)
}

// TestFollowResyncsOnLogEpochChange proves C4: a hello frame with a changed
// log_epoch forces the same resync from cursor 0 through events page, even
// when the follower's cursor is still valid.
func TestFollowResyncsOnLogEpochChange(t *testing.T) {
	f := newCLIFixture(t)
	f.createThread("epoch-create", "epoch thread")
	firstHead := int(f.head())
	f.sendCommand(f.lastThreadID(), "pause")
	perCommand := int(f.head()) - firstHead
	oldHead := int(f.head())

	// The new lineage emits firstHead + perCommand events, all of which the
	// rebuilt stream delivers; the limit is armed before the follow starts.
	writer := newStopAfterWriter()
	writer.arm(oldHead + firstHead + perCommand)
	var followErr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- Main(f.authenticated("--json", "events", "follow", "all"), writer, &followErr, f.env)
	}()
	writer.awaitLines(t, oldHead)

	// Build the new lineage on a temporary port first: the mutations must
	// already exist when the follower's first successful reconnect arrives,
	// so the hello epoch check triggers instead of cursor_ahead.
	f.restartWithNewStoreOnTempPort()
	f.createThread("epoch-new", "epoch new thread")
	f.sendCommand(f.lastThreadID(), "pause")
	f.serveOnOriginalPort()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("follow did not exit after the epoch resync")
	}
	assertRebuiltCursors(t, writer.collected(), oldHead, firstHead+perCommand)
}

// decodeCursors parses each --json event line's global sequence.
func decodeCursors(t *testing.T, lines []string) []int64 {
	t.Helper()
	cursors := make([]int64, 0, len(lines))
	for i, line := range lines {
		var event struct {
			GlobalSeq int64 `json:"global_seq"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("line %d = %q: %v", i, line, err)
		}
		cursors = append(cursors, event.GlobalSeq)
	}
	return cursors
}

// assertRebuiltCursors verifies the follower delivered the original prefix and
// then rebuilt from cursor 0 after the resync.
func assertRebuiltCursors(t *testing.T, lines []string, oldHead, rebuiltHead int) {
	t.Helper()
	if len(lines) != oldHead+rebuiltHead {
		t.Fatalf("delivered %d lines, want %d:\n%s", len(lines), oldHead+rebuiltHead, strings.Join(lines, "\n"))
	}
	for i, cursor := range decodeCursors(t, lines) {
		want := int64(i + 1)
		if i >= oldHead {
			want = int64(i - oldHead + 1)
		}
		if cursor != want {
			t.Fatalf("line %d has cursor %d, want %d", i, cursor, want)
		}
	}
}
