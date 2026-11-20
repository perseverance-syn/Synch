package remote

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"synch/internal/eventlog"
	"synch/internal/supervisor"
)

// --- Reconnect and replay integrity (S4) ---

// TestDisconnectReplay proves S3 and S4 preserve runner ownership and replay the entire offline suffix once.
func TestDisconnectReplay(t *testing.T) {
	f := openFixture(t)
	ctx := testContext(t)
	if err := f.driver.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	gate := filepath.Join(t.TempDir(), "resume")
	spec := childSpec(t, "gate", gate)
	process, err := f.driver.Spawn(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "pre-disconnect output", func() bool { return strings.Contains(outputFor(t, f, spec.ID), "online") })
	before, err := f.driver.journalCursor(ctx)
	if err != nil || before < 1 {
		t.Fatalf("cursor before disconnect: %d, %v", before, err)
	}
	f.cutNetwork()
	waitFor(t, ctx, "SSH disconnect", func() bool { return !f.driver.Status().Connected })
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, "exit in the remote journal", func() bool {
		records, err := readRecords(f.runner.db, f.runner.id, before)
		if err != nil || len(records) == 0 {
			return false
		}
		last, err := decodeRecord(records[len(records)-1])
		return err == nil && last.Type == "process.exited"
	})
	if current, err := f.driver.journalCursor(ctx); err != nil || current != before {
		t.Fatalf("daemon got offline output: %d, %v", current, err)
	}
	if err := f.driver.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	exit, err := process.Wait(ctx)
	if err != nil || exit.Work != "complete" || !exit.DescendantsCleaned || !strings.Contains(outputFor(t, f, spec.ID), "offline-finished") {
		t.Fatalf("replayed exit and output: %+v, %v", exit, err)
	}
	if err := f.driver.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewDriver(f.registry, f.runner.id, f.broker, f.policy, f.log, f.ledger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if err := restarted.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	events, err := f.log.ReadStream("thread:"+testThread, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	local, err := readRecords(f.runner.db, f.runner.id, 0)
	if err != nil || len(events) != len(local) || len(events) <= int(before) {
		t.Fatalf("local/daemon lengths = %d/%d, %v", len(local), len(events), err)
	}
	for i, e := range events {
		record, err := decodeRecord(local[i])
		if err != nil || e.Source == nil || e.Source.Seq != int64(i+1) || e.ID != record.ID || e.Actor.ID != f.runner.id {
			t.Fatalf("unexpected replay mapping at %d: %+v, %v", i, e, err)
		}
	}
	if _, err := restarted.Spawn(ctx, spec); !errors.Is(err, ErrDispatchRecorded) {
		t.Fatalf("repeated dispatch after daemon restart: %v", err)
	}
	restored, err := restarted.Attach(ctx, testThread, spec.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if exit, err := restored.Wait(ctx); err != nil || exit.Work != "complete" {
		t.Fatalf("attach to durable exit: %v, %v", exit, err)
	}
}

// TestReplayGap proves S4 preserves the committed prefix and degrades a runner at the first missing sequence.
func TestReplayGap(t *testing.T) {
	f := openFixture(t)
	ctx := testContext(t)
	id := eventlog.NewID()
	if err := reserve(ctx, f.runner.db, f.runner.id, id, testThread); err != nil {
		t.Fatal(err)
	}
	if err := reserve(ctx, f.driver.ledger, f.runner.id, id, testThread); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		recordUsage(t, f.runner, id)
	}
	if _, err := f.runner.db.Exec("DELETE FROM runner_journal WHERE runner=? AND seq=2", f.runner.id); err != nil {
		t.Fatal(err)
	}
	if err := f.driver.Reconnect(ctx); !errors.Is(err, eventlog.ErrRunnerGap) {
		t.Fatalf("expected ErrRunnerGap, got %v", err)
	}
	s := f.driver.Status()
	if !s.Degraded || s.Connected || !errors.Is(s.Error, eventlog.ErrRunnerGap) {
		t.Fatalf("gap failed to degrade runner: %+v", s)
	}
	if seq, err := f.driver.journalCursor(ctx); err != nil || seq != 1 {
		t.Fatalf("committed prefix sequence = %d, %v", seq, err)
	}
}

// --- Durable process ownership (S3, decision 18) ---

// TestRunnerRestartSequence proves S3 preserves the local journal sequence and dispatch reservations across restart.
func TestRunnerRestartSequence(t *testing.T) {
	ctx := testContext(t)
	cfg := supervisor.Config{DataDir: t.TempDir()}
	runner, err := OpenRunner("restart", cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := eventlog.NewID()
	if err := reserve(ctx, runner.db, runner.id, id, testThread); err != nil {
		t.Fatal(err)
	}
	recordUsage(t, runner, id)
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
	runner, err = OpenRunner("restart", cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runner.Close() })
	if err := reserve(ctx, runner.db, runner.id, id, testThread); !errors.Is(err, ErrDispatchRecorded) {
		t.Fatalf("runner lost its durable reservation: %v", err)
	}
	if record := recordUsage(t, runner, id); record.SourceSeq != 2 {
		t.Fatalf("source sequence after restart: %d", record.SourceSeq)
	}
}

// TestProtocolOutputAfterDisconnect proves S3 keeps lossless protocol output available across SSH transport loss.
func TestProtocolOutputAfterDisconnect(t *testing.T) {
	f := openFixture(t)
	ctx := testContext(t)
	if err := f.driver.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	spec := childSpec(t, "protocol", "")
	spec.ProtocolIO = true
	process, err := f.driver.Spawn(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	stdin, stdout := process.ProtocolStreams()
	defer func() { _ = stdout.Close() }()
	sent := bytes.Repeat([]byte{0, 0xff, 1}, 17000)
	if n, err := stdin.Write(sent); err != nil || n != len(sent) {
		t.Fatalf("protocol write: %d, %v", n, err)
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	f.driver.Disconnect()
	if err := f.driver.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(stdout)
	if err != nil || !bytes.Equal(got, sent) {
		t.Fatalf("protocol output: %d bytes, %v", len(got), err)
	}
}

// recordUsage adds one valid runner-local fact for test replay.
func recordUsage(t *testing.T, r *Runner, assignment string) eventlog.RunnerRecord {
	t.Helper()
	payload := json.RawMessage(`{"assignment_id":"` + assignment + `","reported":false}`)
	record, err := r.Record(testThread, "engine.usage", 1, 0, payload)
	if err != nil {
		t.Fatal(err)
	}
	return record
}
