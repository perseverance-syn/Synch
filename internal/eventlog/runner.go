package eventlog

import (
	"context"
	"errors"
	"fmt"
)

// ErrRunnerGap indicates that the next record would skip a runner journal sequence.
var ErrRunnerGap = errors.New("runner journal gap")

var runnerVersions = map[string]int{"engine.output": 1, "engine.usage": 1, "engine.capability_unavailable": 1, "process.exited": 1, "check.result": 1, "policy.decided": 2}

// IngestRunner commits each new record individually. On error it returns the
// committed prefix. The caller authenticates the runner and authorizes dispatch.
func (s *Store) IngestRunner(ctx context.Context, authenticated string, records []RunnerRecord) ([]Event, error) {
	if !runnerID.MatchString(authenticated) {
		return nil, fmt.Errorf("invalid runner id")
	}
	var result []Event
	for _, r := range records {
		if authenticated != r.Runner || r.SourceSeq < 1 {
			return result, fmt.Errorf("invalid runner source")
		}
		e, err := s.ingestOne(ctx, r)
		if err != nil {
			return result, fmt.Errorf("ingest runner sequence %d: %w", r.SourceSeq, err)
		}
		if e != nil {
			result = append(result, *e)
		}
	}
	return result, nil
}

// ingestOne deduplicates and commits the next runner record without allowing a gap in its journal.
func (s *Store) ingestOne(ctx context.Context, r RunnerRecord) (*Event, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin runner ingest: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // Commit or an earlier error determines the result.
	var last int64
	if err := tx.QueryRowContext(ctx, "SELECT coalesce(max(source_seq),0) FROM events WHERE runner=?", r.Runner).Scan(&last); err != nil {
		return nil, fmt.Errorf("read runner sequence: %w", err)
	}
	if r.SourceSeq <= last {
		return nil, nil
	}
	if r.SourceSeq != last+1 {
		return nil, fmt.Errorf("%w after %d", ErrRunnerGap, last)
	}
	if r.RunnerEventVersion != 2 || r.OccurredAt.IsZero() || r.Redactions < 0 || r.TypeVersion < 1 || runnerVersions[r.Type] != r.TypeVersion {
		return nil, fmt.Errorf("invalid runner record")
	}
	input := NewEvent{ID: r.ID, Stream: "thread:" + r.ThreadID, Type: r.Type, TypeVersion: r.TypeVersion, OccurredAt: &r.OccurredAt, ThreadID: r.ThreadID, Payload: r.Payload, Actor: Actor{Kind: "runner", ID: r.Runner}}
	e, err := s.insert(ctx, tx, Event{NewEvent: input, Source: &Source{Runner: r.Runner, Seq: r.SourceSeq}, Redactions: r.Redactions})
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit runner ingest: %w", err)
	}
	return &e, nil
}
