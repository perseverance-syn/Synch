package eventlog

import (
	"context"
	"database/sql"
	"fmt"
)

// Transaction joins component writes to the serialized event writer.
type Transaction struct {
	writer    *sql.Tx
	store     *Store
	requestID string
}

// Write runs a component operation on the event writer and commits only on success.
func (s *Store) Write(ctx context.Context, f func(*Transaction) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin component write: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := f(&Transaction{writer: tx, store: s}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit component write: %w", err)
	}
	return nil
}

// Append persists an event within the component transaction, assigning an ID when needed.
func (tx *Transaction) Append(ctx context.Context, e NewEvent) (Event, error) {
	if e.RequestID == "" {
		e.RequestID = tx.requestID
	}
	if e.ID == "" {
		e.ID = NewID()
	}
	return tx.store.insert(ctx, tx.writer, Event{NewEvent: e})
}

// WithRequest correlates events appended through this view with the command.
func (tx *Transaction) WithRequest(id string) *Transaction {
	return &Transaction{writer: tx.writer, store: tx.store, requestID: id}
}

// RequestID returns the inherited command ID for the transaction view.
func (tx *Transaction) RequestID() string { return tx.requestID }

// ExecContext executes a component write on the same connection as the events.
func (tx *Transaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.writer.ExecContext(ctx, query, args...)
}

// QueryContext reads component records visible within the current write transaction.
func (tx *Transaction) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.writer.QueryContext(ctx, query, args...)
}

// QueryRowContext looks up one record within the current write transaction.
func (tx *Transaction) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.writer.QueryRowContext(ctx, query, args...)
}

// NewID generates a ULID using the event log's identifier format.
func NewID() string { return newEpoch() }

// installReceipts adds the receipt table to new and existing version-one logs.
func installReceipts(tx *sql.Tx) error {
	_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS receipts(
 caller_identity TEXT NOT NULL, request_id TEXT NOT NULL, receipt_id TEXT NOT NULL UNIQUE,
 payload_digest TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('accepted','rejected')),
 http_status INTEGER NOT NULL, content_type TEXT NOT NULL, body BLOB NOT NULL, record BLOB NOT NULL,
 PRIMARY KEY(caller_identity,request_id));
 CREATE TABLE IF NOT EXISTS decision_receipts(
 thread_id TEXT NOT NULL, decision_id TEXT NOT NULL, receipt_id TEXT NOT NULL UNIQUE REFERENCES receipts(receipt_id),
 PRIMARY KEY(thread_id,decision_id,receipt_id))`)
	if err != nil {
		return fmt.Errorf("install receipt schema: %w", err)
	}
	return nil
}
