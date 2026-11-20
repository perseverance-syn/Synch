package eventlog

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- Fixtures: create isolated stores and deterministic events ---

// must unwraps a fixture result or fails the test at the caller.
func must[T any](t *testing.T, v T, err error) T {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// openTest opens an isolated log and registers cleanup.
func openTest(t *testing.T, r ...Redactor) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "event log.db")
	s, err := Open(path, r...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, path
}

// input constructs an event with reproducible IDs for sequencing assertions.
func input(id, thread int) NewEvent {
	tid := fmt.Sprintf("%026d", thread)
	return NewEvent{ID: fmt.Sprintf("%026d", id), ThreadID: tid, Stream: "thread:" + tid, Type: "engine.usage", TypeVersion: 1, Actor: Actor{Kind: "daemon", ID: "local"}, Payload: json.RawMessage("{}")}
}

// appendOK appends fixture events or fails at the caller.
func appendOK(t *testing.T, s *Store, b ...NewEvent) []Event {
	t.Helper()
	events, err := s.Append(t.Context(), b)
	return must(t, events, err)
}

// --- Durability: WAL, transaction rollback, and abrupt process exit ---

// TestPragmas verifies the SQLite WAL and durability settings.
func TestPragmas(t *testing.T) {
	s, _ := openTest(t)
	var journal string
	var syncMode int
	if err := s.db.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
		t.Fatalf("journal=%s err=%v", journal, err)
	}
	if err := s.db.QueryRow("PRAGMA synchronous").Scan(&syncMode); err != nil || syncMode != 2 {
		t.Fatalf("synchronous=%d err=%v", syncMode, err)
	}
}

// TestAppendRollback verifies that a bad event rolls back its whole batch and consumes no sequence.
func TestAppendRollback(t *testing.T) {
	s, _ := openTest(t)
	appendOK(t, s, input(1, 1))
	if events, err := s.Append(t.Context(), []NewEvent{input(2, 1), input(1, 2)}); err == nil || len(events) != 0 {
		t.Fatal("acknowledged failed batch")
	}
	start := time.Now()
	events := appendOK(t, s, input(2, 1), input(3, 2))
	for i, e := range events {
		if e.GlobalSeq != int64(i+2) || e.Seq != int64(2-i) || e.RecordedAt.Before(start) || e.RecordedAt.After(time.Now()) {
			t.Fatalf("unexpected envelope %+v", e)
		}
	}
	page, err := s.ReadStream(events[0].Stream, 1, 1)
	if err != nil || len(page) != 1 || page[0].ID != events[0].ID {
		t.Fatalf("page=%v err=%v", page, err)
	}
}

// TestKillAfterAck verifies that a committed event survives abrupt process termination.
func TestKillAfterAck(t *testing.T) {
	if path := os.Getenv("SYNCH_101_CRASH_DB"); path != "" {
		s, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i <= 12; i++ {
			appendOK(t, s, input(i, i%4))
			fmt.Println("ACK", i)
		}
		_, _ = bufio.NewReader(os.Stdin).ReadByte()
		return
	}
	path := filepath.Join(t.TempDir(), "crash.db")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestKillAfterAck$")
	cmd.Env = append(os.Environ(), "SYNCH_101_CRASH_DB="+path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stdin.Close() }() // Wait also closes the child's pipe.
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	scan := bufio.NewScanner(out)
	for i := 1; i <= 12; i++ {
		if !scan.Scan() || scan.Text() != fmt.Sprint("ACK ", i) {
			t.Fatalf("missing ack %d: %q", i, scan.Text())
		}
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("child exited normally")
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	events, err := s.ReadGlobal(0, 13)
	if err != nil || len(events) != 12 {
		t.Fatalf("recovered %d err=%v", len(events), err)
	}
	for i, e := range events {
		if e.GlobalSeq != int64(i+1) || e.Seq != int64(i/4+1) || e.ID != input(i+1, (i+1)%4).ID {
			t.Fatalf("recovered %+v", e)
		}
	}
}

// --- Event integrity: numbering, validation, immutability, and runner replay ---

// TestConcurrentNumbering verifies gap-free global and per-stream numbering under concurrency.
func TestConcurrentNumbering(t *testing.T) {
	s, _ := openTest(t)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := range 16 {
		wg.Go(func() {
			<-start
			for j := range 8 {
				_, err := s.Append(t.Context(), []NewEvent{input(worker*8+j, worker%4)})
				if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
	events, err := s.ReadGlobal(0, 129)
	if err != nil || len(events) != 128 {
		t.Fatalf("events=%d err=%v", len(events), err)
	}
	counts := map[string]int64{}
	for i, e := range events {
		counts[e.Stream]++
		if e.GlobalSeq != int64(i+1) || e.Seq != counts[e.Stream] {
			t.Fatalf("gap %+v", e)
		}
	}
}

// TestEnvelope verifies that persistence rejects invalid envelope and stream fields.
func TestEnvelope(t *testing.T) {
	s, _ := openTest(t)
	for _, change := range []func(*NewEvent){
		func(e *NewEvent) { e.ID = "" }, func(e *NewEvent) { e.Stream = "" }, func(e *NewEvent) { e.Type = "" }, func(e *NewEvent) { e.TypeVersion = 0 }, func(e *NewEvent) { e.Actor.ID = "" }, func(e *NewEvent) { e.Actor.Kind = "runner" }, func(e *NewEvent) { e.Payload = nil }, func(e *NewEvent) { e.Payload = []byte("[]") }, func(e *NewEvent) { e.ThreadID = "" }, func(e *NewEvent) { e.ThreadID = input(1, 2).ThreadID }, func(e *NewEvent) { e.Stream = "system" }, func(e *NewEvent) { e.Type = "runner.connection" }, func(e *NewEvent) { e.Type = "resource.pressure" },
	} {
		e := input(1, 1)
		change(&e)
		if _, err := s.Append(t.Context(), []NewEvent{e}); err == nil {
			t.Fatalf("accepted %+v", e)
		}
	}
	for i, typ := range []string{"resource.pressure", "runner.connection"} {
		e := input(i, 1)
		e.Stream = "system"
		e.ThreadID = ""
		e.Type = typ
		appendOK(t, s, e)
	}
}

// TestImmutable verifies that SQLite rejects updates and deletes of committed events.
func TestImmutable(t *testing.T) {
	s, _ := openTest(t)
	appendOK(t, s, input(1, 1))
	for _, q := range []string{"UPDATE events SET envelope='{}'", "DELETE FROM events"} {
		if _, err := s.db.Exec(q); err == nil || !strings.Contains(err.Error(), "immutable event") {
			t.Fatalf("%s: %v", q, err)
		}
	}
	page, err := s.ReadGlobal(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	page[0].Payload[0] = '!'
	again, err := s.ReadGlobal(0, 1)
	if err != nil || string(again[0].Payload) != "{}" {
		t.Fatal("read mutated stored payload", err)
	}
}

// TestRunnerReplay verifies runner journal deduplication, ordering, and gap handling.
func TestRunnerReplay(t *testing.T) {
	s, _ := openTest(t)
	e := input(1, 1)
	r := RunnerRecord{RunnerEventVersion: 2, Runner: "r", SourceSeq: 1, ID: e.ID, ThreadID: e.ThreadID, OccurredAt: time.Now().UTC().Add(-time.Hour), Type: e.Type, TypeVersion: 1, Payload: e.Payload, Redactions: 2}
	batch := []RunnerRecord{r, r}
	if _, err := s.IngestRunner(t.Context(), "other", batch); err == nil {
		t.Fatal("accepted impersonation")
	}
	result, err := s.IngestRunner(t.Context(), "r", batch)
	if err != nil || len(result) != 1 {
		t.Fatalf("replay=%v err=%v", result, err)
	}
	got := result[0]
	if *got.Source != (Source{"r", 1}) || got.Actor != (Actor{Kind: "runner", ID: "r"}) || got.ID != r.ID || got.Seq != 1 || got.GlobalSeq != 1 || !got.RecordedAt.After(r.OccurredAt) || got.Redactions != 2 {
		t.Fatalf("mapping %+v", got)
	}
	r.SourceSeq = 3
	if _, err := s.IngestRunner(t.Context(), "r", []RunnerRecord{r}); !errors.Is(err, ErrRunnerGap) {
		t.Fatalf("expected journal gap, got %v", err)
	}
	head, err := s.Head()
	if head != 1 || err != nil {
		t.Fatal(head, err)
	}
}

// --- SQLite lifecycle: migration and online backup with restore ---

// TestMigrations verifies schema initialization and rejection of a newer format.
func TestMigrations(t *testing.T) {
	for _, existing := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "v0.db")
		if existing {
			db, err := connect(path, "rwc")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec("CREATE TABLE schema_version(version INTEGER PRIMARY KEY); INSERT INTO schema_version VALUES(0)"); err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
		}
		s, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		var v int
		if err = s.db.QueryRow("SELECT max(version) FROM schema_version").Scan(&v); err != nil || v != version {
			t.Fatal(v, err)
		}
		if _, err = s.db.Exec("INSERT INTO schema_version VALUES(2)"); err != nil {
			t.Fatal(err)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err = Open(path); err == nil || !strings.Contains(err.Error(), "newer") {
			t.Fatal(err)
		}
	}
}

// TestOnlineBackupRestore verifies snapshot consistency during writes and epoch rotation on restore.
func TestOnlineBackupRestore(t *testing.T) {
	s, _ := openTest(t)
	appendOK(t, s, input(1, 1))
	backup := filepath.Join(t.TempDir(), "backup.db")
	done := make(chan struct{})
	started := make(chan struct{})
	writer := make(chan error, 1)
	go func() {
		for i := 2; ; i++ {
			_, err := s.Append(t.Context(), []NewEvent{input(i, i%4)})
			if i == 2 {
				close(started)
			}
			if err != nil {
				writer <- err
				return
			}
			select {
			case <-done:
				writer <- nil
				return
			default:
			}
		}
	}()
	<-started
	err := s.Backup(t.Context(), backup)
	close(done)
	if writeErr := <-writer; writeErr != nil {
		t.Fatal(writeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "restore.db")
	previous, err := Open(dest)
	if err != nil {
		t.Fatal(err)
	}
	appendOK(t, previous, input(999, 1))
	if err = previous.Close(); err != nil {
		t.Fatal(err)
	}
	if err = Restore(t.Context(), backup, dest); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(dest)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := restored.Close(); err != nil {
			t.Error(err)
		}
	}()
	old, err := s.LogEpoch()
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := restored.LogEpoch()
	if err != nil || old == fresh || !ulid.MatchString(fresh) {
		t.Fatal(old, fresh, err)
	}
	head, err := restored.Head()
	if err != nil || head < 2 {
		t.Fatal(head, err)
	}
	expected, err := s.ReadGlobal(0, int(head))
	if err != nil {
		t.Fatal(err)
	}
	got, err := restored.ReadGlobal(0, int(head)+1)
	if err != nil || !reflect.DeepEqual(got, expected) {
		t.Fatal("snapshot differs", err)
	}
	snapshotStore, err := Open(backup)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := snapshotStore.Close(); err != nil {
			t.Error(err)
		}
	}()
	snapshotHead, err := snapshotStore.Head()
	if err != nil || snapshotHead != head {
		t.Fatal("incomplete restore", snapshotHead, head, err)
	}
	snapshotEpoch, err := snapshotStore.LogEpoch()
	if err != nil || snapshotEpoch != old {
		t.Fatal("backup epoch changed", snapshotEpoch, old, err)
	}
	e := appendOK(t, restored, input(int(head)+1, 1))[0]
	if e.GlobalSeq != head+1 {
		t.Fatal(e)
	}
}

// --- Payload rules: redact before writing and enforce chunk limits ---

// TestRedactionOnDisk verifies that only redacted payloads are stored.
func TestRedactionOnDisk(t *testing.T) {
	secret := []byte("synthetic-secret-918374")
	s, path := openTest(t, func(p json.RawMessage) (json.RawMessage, int, error) {
		return bytes.ReplaceAll(p, secret, []byte("[REDACTED]")), bytes.Count(p, secret), nil
	})
	e := input(1, 1)
	e.Payload = []byte(`{"value":"synthetic-secret-918374"}`)
	appendOK(t, s, e)
	if _, err := s.db.Exec("PRAGMA wal_checkpoint(FULL)"); err != nil {
		t.Fatal(err)
	}
	got, err := s.ReadGlobal(0, 1)
	if err != nil || got[0].Redactions != 1 || string(got[0].Payload) != `{"value":"[REDACTED]"}` {
		t.Fatal(got, err)
	}
	for _, file := range []string{path, path + "-wal"} {
		b, err := os.ReadFile(file)
		if err != nil || len(b) == 0 || bytes.Contains(b, secret) {
			t.Fatalf("redaction scan %s len=%d err=%v", file, len(b), err)
		}
	}
}

// TestChunkBoundary verifies the engine output chunk size and overflow reference rules.
func TestChunkBoundary(t *testing.T) {
	s, _ := openTest(t)
	for i, tc := range []struct {
		chunk               string
		truncated, full, ok bool
	}{
		{strings.Repeat("a", 65536), false, false, true}, {strings.Repeat("a", 65537), false, false, false}, {strings.Repeat("é", 32768), false, false, true}, {strings.Repeat("é", 32768) + "a", true, true, false}, {"cut", true, false, false}, {"cut", true, true, true},
	} {
		fields := map[string]any{"chunk": tc.chunk, "truncated": tc.truncated}
		if tc.full {
			fields["full_output"] = map[string]string{"uri": "file:///output", "digest": "example"}
		}
		e := input(i, 1)
		e.Type = "engine.output"
		e.Payload, _ = json.Marshal(fields)
		_, err := s.Append(t.Context(), []NewEvent{e})
		if (err == nil) != tc.ok {
			t.Errorf("case %d err=%v", i, err)
		}
	}
}
