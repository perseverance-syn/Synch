package eventlog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- Write hardening: redact escaped secrets and serialize concurrent writers ---

// TestEscapedSecret verifies that JSON escapes cannot bypass payload redaction.
func TestEscapedSecret(t *testing.T) {
	secret := []byte("hunter2")
	s, _ := openTest(t, func(p json.RawMessage) (json.RawMessage, int, error) {
		return bytes.ReplaceAll(p, secret, []byte("[REDACTED]")), bytes.Count(p, secret), nil
	})
	e := input(1, 1)
	e.Payload = []byte(`{"nested":["hunter\u0032"],"number":9007199254740993}`)
	appendOK(t, s, e)
	r := RunnerRecord{RunnerEventVersion: 2, Runner: "r", SourceSeq: 1, ID: input(2, 1).ID, ThreadID: e.ThreadID, OccurredAt: time.Now(), Type: e.Type, TypeVersion: 1, Payload: e.Payload}
	if _, err := s.IngestRunner(t.Context(), "r", []RunnerRecord{r}); err != nil {
		t.Fatal(err)
	}
	events, err := s.ReadGlobal(0, 2)
	if err != nil || len(events) != 2 {
		t.Fatal(events, err)
	}
	for _, event := range events {
		var data struct {
			Nested []string
			Number json.Number
		}
		if err := json.Unmarshal(event.Payload, &data); err != nil {
			t.Fatal(err)
		}
		if event.Redactions != 1 || len(data.Nested) != 1 || data.Nested[0] != "[REDACTED]" || data.Number.String() != "9007199254740993" {
			t.Errorf("source=%v count=%d decoded payload mismatch", event.Source, event.Redactions)
		}
	}
}

// TestWriterStress verifies that concurrent daemon and runner writes succeed without SQLITE_BUSY or other errors and preserve gap-free, duplicate-free numbering.
func TestWriterStress(t *testing.T) {
	s, _ := openTest(t)
	// Bound a stalled write without making throughput part of the contract.
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 64 {
		wg.Go(func() {
			<-start
			for j := range 25 {
				if _, err := s.Append(ctx, []NewEvent{input(w*25+j, w%8)}); err != nil {
					t.Errorf("append worker %d event %d: %v", w, j, err)
					return
				}
			}
		})
	}
	for w := range 2 {
		wg.Go(func() {
			<-start
			runner := fmt.Sprintf("runner-%d", w)
			for j := range 50 {
				e := input(1600+w*50+j, w)
				r := RunnerRecord{RunnerEventVersion: 2, Runner: runner, SourceSeq: int64(j + 1), ID: e.ID, Type: e.Type, TypeVersion: 1, ThreadID: e.ThreadID, OccurredAt: time.Now(), Payload: e.Payload}
				if _, err := s.IngestRunner(ctx, runner, []RunnerRecord{r}); err != nil {
					t.Errorf("ingest %s sequence %d: %v", runner, r.SourceSeq, err)
					return
				}
			}
		})
	}
	began := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(began)
	t.Logf("1700 transactions in %v", elapsed)
	events, err := s.ReadGlobal(0, 1701)
	if err != nil || len(events) != 1700 {
		t.Fatal(len(events), err)
	}
	counts := map[string]int64{}
	for i, e := range events {
		counts[e.Stream]++
		if e.GlobalSeq != int64(i+1) || e.Seq != counts[e.Stream] {
			t.Fatalf("numbering gap at %d", i)
		}
	}
}

// --- Read and restore hardening: bounds, cancellation, and error context ---

// TestNegativeLimit verifies that replay rejects a negative limit.
func TestNegativeLimit(t *testing.T) {
	s, _ := openTest(t)
	if _, err := s.ReadGlobal(0, -1); err == nil {
		t.Fatal("negative global limit accepted")
	}
	if _, err := s.ReadStream("system", 0, -1); err == nil {
		t.Fatal("negative stream limit accepted")
	}
}

// TestRestoreCancellation verifies that cancelling a restore preserves the destination.
func TestRestoreCancellation(t *testing.T) {
	s, _ := openTest(t)
	path := filepath.Join(t.TempDir(), "snapshot")
	if err := s.Backup(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "restored")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := Restore(ctx, path, dest); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected publication: %v", err)
	}
}

// TestReadPoolWhileWriterBusy verifies that a writer transaction does not block independent replay reads.
func TestReadPoolWhileWriterBusy(t *testing.T) {
	s, _ := openTest(t)
	writer, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			t.Error(err)
		}
	}()
	if s.db.Stats().MaxOpenConnections != 1 {
		t.Fatal("writer pool is unbounded")
	}
	if _, err := s.Head(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err = s.Append(ctx, []NewEvent{input(1, 1)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued append: %v", err)
	}
}

// TestReadErrorContext verifies that a read failure reports event log operation context.
func TestReadErrorContext(t *testing.T) {
	s, _ := openTest(t)
	if _, err := s.db.Exec("DELETE FROM metadata"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LogEpoch(); !errors.Is(err, sql.ErrNoRows) || !strings.Contains(err.Error(), "read log epoch") {
		t.Fatalf("missing epoch error: %v", err)
	}
}
