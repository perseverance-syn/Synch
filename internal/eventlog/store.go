package eventlog

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// --- SQLite connections: serialize writes and isolate readers ---

type Store struct {
	db      *sql.DB // Single writer shared by Append and IngestRunner.
	readers *sql.DB
	redact  Redactor
}

// Open accepts at most one payload redactor. Redactors must be safe for concurrent calls.
func Open(path string, redactors ...Redactor) (*Store, error) {
	if len(redactors) > 1 {
		return nil, errors.New("open event log: multiple redactors")
	}
	db, err := connect(path, "rwc")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if len(redactors) == 1 {
		s.redact = redactors[0]
	}
	var mode string
	err = db.QueryRow("PRAGMA journal_mode=WAL").Scan(&mode)
	if err == nil && mode != "wal" {
		err = fmt.Errorf("unexpected journal mode %q", mode)
	}
	if err == nil {
		err = s.migrate()
	}
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open event log: %w", err)
	}
	s.readers, err = connect(path, "ro")
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open event readers: %w", err)
	}
	s.readers.SetMaxOpenConns(8)
	return s, nil
}

// connect opens SQLite with an immediate writer lock and durable synchronous commits.
func connect(path, mode string) (*sql.DB, error) {
	if path == "" {
		return nil, errors.New("empty event log path")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve event log: %w", err)
	}
	p := filepath.ToSlash(abs)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	u := url.URL{Scheme: "file", Path: p}
	q := url.Values{"mode": {mode}, "_txlock": {"immediate"}, "_pragma": {"busy_timeout(15000)", "synchronous(FULL)"}}
	u.RawQuery = q.Encode()
	return sql.Open("sqlite", u.String())
}

// Close closes both SQLite connection pools.
func (s *Store) Close() error { return errors.Join(s.readers.Close(), s.db.Close()) }

// --- Schema and epoch: initialize and migrate an event log ---

const version = 1
const initialSchema = `
 CREATE TABLE metadata(epoch TEXT NOT NULL);
 CREATE TABLE events(
 global_seq INTEGER PRIMARY KEY, stream TEXT NOT NULL, seq INTEGER NOT NULL,
 id TEXT NOT NULL UNIQUE, runner TEXT, source_seq INTEGER, envelope BLOB NOT NULL,
 UNIQUE(stream,seq), UNIQUE(runner,source_seq));
 CREATE TRIGGER immutable_update BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT,'immutable event'); END;
 CREATE TRIGGER immutable_delete BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT,'immutable event'); END;
`

// newEpoch generates a time-prefixed random ULID for replay epochs and event IDs.
func newEpoch() string {
	var raw [16]byte
	binary.BigEndian.PutUint64(raw[:8], uint64(time.Now().UnixMilli())<<16)
	rand.Read(raw[6:])
	n := new(big.Int).SetBytes(raw[:])
	base := big.NewInt(32)
	var out [26]byte
	var digit big.Int
	for i := 25; i >= 0; i-- {
		n.QuoRem(n, base, &digit)
		out[i] = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"[digit.Int64()]
	}
	return string(out[:])
}

// migrate installs the base event schema and additive component tables atomically.
func (s *Store) migrate() error {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // Commit or an earlier error determines the result.
	if _, err = tx.Exec("CREATE TABLE IF NOT EXISTS schema_version(version INTEGER PRIMARY KEY)"); err != nil {
		return fmt.Errorf("create schema version table: %w", err)
	}
	var current int
	if err = tx.QueryRow("SELECT coalesce(max(version),0) FROM schema_version").Scan(&current); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if current > version {
		return fmt.Errorf("schema version %d is newer than supported version %d", current, version)
	}
	if current == 0 {
		if _, err = tx.Exec(initialSchema); err != nil {
			return fmt.Errorf("create event schema: %w", err)
		}
		if _, err = tx.Exec("INSERT INTO metadata VALUES(?)", newEpoch()); err != nil {
			return fmt.Errorf("initialize epoch: %w", err)
		}
		if _, err = tx.Exec("INSERT INTO schema_version VALUES(1)"); err != nil {
			return fmt.Errorf("record schema version: %w", err)
		}
	}
	if err := installReceipts(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}

// --- Cursors: identify the log and its committed head ---

// LogEpoch returns the identifier clients use to detect a restored or replaced log.
func (s *Store) LogEpoch() (string, error) {
	var epoch string
	err := s.readers.QueryRow("SELECT epoch FROM metadata").Scan(&epoch)
	if err != nil {
		return "", fmt.Errorf("read log epoch: %w", err)
	}
	return epoch, nil
}

// Head returns the highest committed global event sequence.
func (s *Store) Head() (int64, error) {
	var head int64
	err := s.readers.QueryRow("SELECT coalesce(max(global_seq),0) FROM events").Scan(&head)
	if err != nil {
		return 0, fmt.Errorf("read log head: %w", err)
	}
	return head, nil
}

// --- Writes: redact, number, and persist event batches ---

// Append validates and persists a batch of events in one transaction.
func (s *Store) Append(ctx context.Context, batch []NewEvent) ([]Event, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin append: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // Commit or an earlier error determines the result.
	result := make([]Event, 0, len(batch))
	for _, input := range batch {
		e, err := s.insert(ctx, tx, Event{NewEvent: input})
		if err != nil {
			return nil, fmt.Errorf("append event: %w", err)
		}
		result = append(result, e)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit append: %w", err)
	}
	return result, nil
}

// insert redacts and numbers an envelope within the caller's write transaction.
func (s *Store) insert(ctx context.Context, tx *sql.Tx, e Event) (Event, error) {
	if err := e.validate(e.Source != nil); err != nil {
		return Event{}, err
	}
	e.Payload = append(json.RawMessage(nil), e.Payload...)
	if s.redact != nil {
		// Preserve JSON number literals exactly while normalizing string escapes.
		decoder := json.NewDecoder(bytes.NewReader(e.Payload))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			return Event{}, fmt.Errorf("decode payload for redaction: %w", err)
		}
		var canonical bytes.Buffer
		encoder := json.NewEncoder(&canonical)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(value); err != nil {
			return Event{}, fmt.Errorf("encode payload for redaction: %w", err)
		}
		p, count, err := s.redact(bytes.TrimSpace(canonical.Bytes()))
		if err != nil {
			return Event{}, fmt.Errorf("redact event: %w", err)
		}
		if count < 0 || count > int(^uint(0)>>1)-e.Redactions {
			return Event{}, errors.New("invalid redaction count")
		}
		if err := validatePayload(e.Type, p); err != nil {
			return Event{}, err
		}
		e.Payload = p
		e.Redactions += count
	}
	err := tx.QueryRowContext(ctx, `SELECT (SELECT coalesce(max(global_seq),0)+1 FROM events),
 coalesce(max(seq),0)+1 FROM events WHERE stream=?`, e.Stream).Scan(&e.GlobalSeq, &e.Seq)
	if err != nil {
		return Event{}, fmt.Errorf("assign event sequences: %w", err)
	}
	e.RecordedAt = time.Now().UTC()
	e.EnvelopeVersion = 2
	raw, err := json.Marshal(e)
	if err != nil {
		return Event{}, err
	}
	var runner, seq any
	if e.Source != nil {
		runner = e.Source.Runner
		seq = e.Source.Seq
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO events(global_seq,stream,seq,id,runner,source_seq,envelope) VALUES(?,?,?,?,?,?,?)", e.GlobalSeq, e.Stream, e.Seq, e.ID, runner, seq, raw)
	if err != nil {
		return Event{}, fmt.Errorf("insert envelope: %w", err)
	}
	return e, nil
}

// --- Reads: replay events from the requested cursor ---

// ReadStream returns committed events after the supplied stream sequence.
func (s *Store) ReadStream(stream string, afterSeq int64, limit int) ([]Event, error) {
	if limit < 0 {
		return nil, errors.New("negative event read limit")
	}
	return s.read("SELECT envelope FROM events WHERE stream=? AND seq>? ORDER BY seq LIMIT ?", stream, afterSeq, limit)
}

// ReadGlobal returns committed events after the supplied global sequence.
func (s *Store) ReadGlobal(afterGlobalSeq int64, limit int) ([]Event, error) {
	if limit < 0 {
		return nil, errors.New("negative event read limit")
	}
	return s.read("SELECT envelope FROM events WHERE global_seq>? ORDER BY global_seq LIMIT ?", afterGlobalSeq, limit)
}

// read decodes query results through the independent reader pool.
func (s *Store) read(query string, args ...any) ([]Event, error) {
	rows, err := s.readers.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("read events: %w", err)
	}
	defer func() { _ = rows.Close() }() // rows.Err reports errors after iteration.
	var result []Event
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan envelope: %w", err)
		}
		var e Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("decode stored envelope: %w", err)
		}
		result = append(result, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate envelopes: %w", err)
	}
	return result, nil
}
