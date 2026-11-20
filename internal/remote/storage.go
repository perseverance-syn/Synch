package remote

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	_ "modernc.org/sqlite"

	"synch/internal/eventlog"
)

const replayLimit = 128

// replayBytes reserves room in the wire message for the runner identity and replay head.
const replayBytes = wireMessageLimit - 1024

// openStorage opens a durable single-writer store for dispatch reservations and runner records.
func openStorage(path string) (*sql.DB, error) {
	if path == "" || path == ":memory:" {
		return nil, errors.New("remote storage requires a durable file")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open remote storage: %w", err)
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`
  PRAGMA synchronous=FULL;
  CREATE TABLE IF NOT EXISTS remote_dispatch (
   runner TEXT NOT NULL, run TEXT NOT NULL, thread TEXT NOT NULL, PRIMARY KEY (runner, run)
  );
  CREATE INDEX IF NOT EXISTS remote_dispatch_thread ON remote_dispatch(runner, thread);
  CREATE TABLE IF NOT EXISTS runner_journal (
   runner TEXT NOT NULL, seq INTEGER NOT NULL, record BLOB NOT NULL, PRIMARY KEY (runner, seq)
  );
 `)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("initialize remote storage: %w", err), db.Close())
	}
	return db, nil
}

// reserve commits an assignment identity before dispatch and prevents blind repetition.
func reserve(ctx context.Context, db *sql.DB, runner, run, thread string) error {
	res, err := db.ExecContext(ctx, "INSERT INTO remote_dispatch VALUES (?, ?, ?) ON CONFLICT DO NOTHING", runner, run, thread)
	if err != nil {
		return fmt.Errorf("persist dispatch reservation: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("check dispatch reservation: %w", err)
	}
	if n == 0 {
		return ErrDispatchRecorded
	}
	return nil
}

// dispatched checks the durable association by thread, and by run when specified.
func dispatched(ctx context.Context, db *sql.DB, runner, thread, run string) (bool, error) {
	q := "SELECT EXISTS(SELECT 1 FROM remote_dispatch WHERE runner=? AND thread=?"
	args := []any{runner, thread}
	if run != "" {
		q += " AND run=?"
		args = append(args, run)
	}
	var found bool
	if err := db.QueryRowContext(ctx, q+")", args...).Scan(&found); err != nil {
		return false, fmt.Errorf("read dispatch reservation: %w", err)
	}
	return found, nil
}

// appendRecord validates and persists the next sequence in the same transaction.
func appendRecord(db *sql.DB, runner, thread, typ string, version, redactions int, payload json.RawMessage) (eventlog.RunnerRecord, error) {
	tx, err := db.Begin()
	if err != nil {
		return eventlog.RunnerRecord{}, fmt.Errorf("begin journal append: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // Preserve whichever validation or commit result decided the append.
	record := eventlog.RunnerRecord{
		RunnerEventVersion: 2, Runner: runner, ID: eventlog.NewID(), ThreadID: thread,
		Type: typ, TypeVersion: version, OccurredAt: time.Now().UTC(), Redactions: redactions, Payload: payload,
	}
	if err := tx.QueryRow("SELECT coalesce(max(seq),0)+1 FROM runner_journal WHERE runner=?", runner).Scan(&record.SourceSeq); err != nil {
		return eventlog.RunnerRecord{}, fmt.Errorf("read next journal sequence: %w", err)
	}
	if err := validateRecord(record); err != nil {
		return eventlog.RunnerRecord{}, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return eventlog.RunnerRecord{}, fmt.Errorf("encode journal record: %w", err)
	}
	if len(raw)+1 > replayBytes {
		return eventlog.RunnerRecord{}, fmt.Errorf("%w: record exceeds replay frame", errInvalidPayload)
	}
	if _, err := tx.Exec("INSERT INTO runner_journal VALUES (?, ?, ?)", runner, record.SourceSeq, raw); err != nil {
		return eventlog.RunnerRecord{}, fmt.Errorf("write journal record: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return eventlog.RunnerRecord{}, fmt.Errorf("commit journal record: %w", err)
	}
	return record, nil
}

// readRecords reads one ordered page after the daemon's last durable runner sequence.
func readRecords(db *sql.DB, runner string, after int64) ([]json.RawMessage, error) {
	return readRecordPage(db, runner, after, math.MaxInt64)
}

// readRecordPage bounds a page to the advertised head and the wire byte budget.
func readRecordPage(db *sql.DB, runner string, after, head int64) ([]json.RawMessage, error) {
	rows, err := db.Query("SELECT record FROM runner_journal WHERE runner=? AND seq>? AND seq<=? ORDER BY seq LIMIT ?", runner, after, head, replayLimit)
	if err != nil {
		return nil, fmt.Errorf("read runner journal: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var records []json.RawMessage
	budget := replayBytes
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("read journal record: %w", err)
		}
		if len(raw)+1 > budget {
			if len(records) == 0 {
				return nil, fmt.Errorf("%w: stored record exceeds replay frame", errInvalidPayload)
			}
			break
		}
		budget -= len(raw) + 1
		records = append(records, raw)
	}
	return records, rows.Err()
}

// readReplayPage reports the current head along with the bounded page, even if the page is empty.
func readReplayPage(db *sql.DB, runner string, after int64) ([]json.RawMessage, int64, error) {
	var head int64
	if err := db.QueryRow("SELECT coalesce(max(seq),0) FROM runner_journal WHERE runner=?", runner).Scan(&head); err != nil {
		return nil, 0, fmt.Errorf("read runner journal head: %w", err)
	}
	// Capture the head first so concurrent appends cannot make an empty page look like a gap.
	records, err := readRecordPage(db, runner, after, head)
	return records, head, err
}
