package remote

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"golang.org/x/crypto/ssh"

	"synch/internal/eventlog"
)

// TestLostDispatchReply proves decision 18 never resubmits an assignment after a lost response.
func TestLostDispatchReply(t *testing.T) {
	f := openFixture(t)
	ctx := testContext(t)
	spec := childSpec(t, "gate", filepath.Join(t.TempDir(), "never-open"))
	// Model the committed intent followed by an SSH channel that sends the request but never reads its reply.
	if err := reserve(ctx, f.driver.ledger, f.runner.id, spec.ID, spec.ThreadID); err != nil {
		t.Fatal(err)
	}
	link, err := dialSSH(ctx, f.target, f.broker)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = link.Close() }()
	sendWithoutReply(t, link, wireRequest{Operation: OpSpawn, Spec: &spec})
	waitFor(t, ctx, "runner accepting dispatch", func() bool {
		_, err := f.runner.lookupRun(spec.ThreadID, spec.ID)
		return err == nil
	})
	if err := link.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.driver.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.driver.Spawn(ctx, spec); !errors.Is(err, ErrDispatchRecorded) {
		t.Fatalf("daemon repeated a recorded dispatch: %v", err)
	}
	res, _, err := callSSH(ctx, f.driver.link.client, f.runner.id, wireRequest{Operation: OpSpawn, Spec: &spec})
	if err != nil || !errors.Is(responseError(res.Error), ErrDispatchRecorded) {
		t.Fatalf("runner repeated a recorded dispatch: %+v, %v", res, err)
	}
}

// sendWithoutReply sends the runner a complete dispatch without observing acknowledgement.
func sendWithoutReply(t *testing.T, link *ssh.Client, request wireRequest) {
	t.Helper()
	session, err := link.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	input, err := session.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := session.RequestSubsystem(subsystem); err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(input).Encode(request); err != nil {
		t.Fatal(err)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestReplayRejectsUnassignedThread proves catalogue §3 requires a daemon dispatch before ingest.
func TestReplayRejectsUnassignedThread(t *testing.T) {
	f := openFixture(t)
	ctx := testContext(t)
	id := eventlog.NewID()
	if err := reserve(ctx, f.runner.db, f.runner.id, id, testThread); err != nil {
		t.Fatal(err)
	}
	recordUsage(t, f.runner, id)
	if err := f.driver.Reconnect(ctx); !errors.Is(err, errUnassignedRecord) {
		t.Fatalf("unexpected replay result: %v", err)
	}
	status := f.driver.Status()
	if status.Connected || !status.Degraded {
		t.Fatalf("unauthorized source was not flagged: %+v", status)
	}
	if cursor, err := f.driver.journalCursor(ctx); err != nil || cursor != 0 {
		t.Fatalf("unauthorized record advanced the cursor: %d, %v", cursor, err)
	}
}
