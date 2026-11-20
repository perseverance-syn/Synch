package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"synch/internal/engine"
	"synch/internal/eventlog"
	"synch/internal/supervisor"
	"synch/internal/thread"
)

const processTable = "CREATE TABLE IF NOT EXISTS daemon_processes (id TEXT PRIMARY KEY, record BLOB)"

// threadHooks defer recovery classification until the supervisor has checked
// prior runs. Normal cancellation remains tied to the live session registry.
func (d *Daemon) threadHooks() thread.Hooks {
	allow := func(context.Context, *eventlog.Transaction, thread.State) (bool, error) { return true, nil }
	return thread.Hooks{Limits: allow, Policy: allow, Pair: d.pairHook, ConfirmGone: d.confirmGone}
}

// confirmGone records the first recovery pass. Thereafter it stops the thread's
// live engine session and waits for it to settle, so a cancelled child's work is
// gone before the parent's T30 completes. The wait is bounded because this runs
// inside the event writer.
// ponytail: an adapter whose Stop appends to the log waits out the bound; cancel outside the writer if that bites.
func (d *Daemon) confirmGone(ctx context.Context, id string) (bool, error) {
	d.mu.Lock()
	if !d.confirmed {
		d.recovering[id] = true
		d.mu.Unlock()
		return false, nil
	}
	active := d.sessions[id]
	d.mu.Unlock()
	if active == nil {
		return true, nil
	}
	active.cancel()
	select {
	case <-active.done:
		return true, nil
	case <-time.After(settleWait):
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// settleWait bounds how long T30 waits for a session to stop.
const settleWait = 5 * time.Second

// recoverProcesses probes recorded identities and only then resumes thread recovery.
func (d *Daemon) recoverProcesses(ctx context.Context) error {
	var records []supervisor.RunRecord
	err := d.store.Write(ctx, func(tx *eventlog.Transaction) error {
		if _, err := tx.ExecContext(ctx, processTable); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, "SELECT record FROM daemon_processes WHERE record IS NOT NULL ORDER BY id")
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var raw []byte
			var record supervisor.RunRecord
			if err := rows.Scan(&raw); err != nil {
				return err
			}
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
			records = append(records, record)
		}
		return rows.Err()
	})
	if err != nil {
		return err
	}
	recovered, err := d.supervisor.Recover(records)
	if err != nil {
		return err
	}
	if len(recovered.Unmanaged) != 0 || len(recovered.Unverified) != 0 {
		return fmt.Errorf("cleanup of prior processes could not be confirmed (%d unmanaged, %d unverified)", len(recovered.Unmanaged), len(recovered.Unverified))
	}
	d.confirmed = true
	for id := range d.recovering {
		if err := d.classifyDispatch(ctx, id); err != nil {
			return fmt.Errorf("classify interrupted dispatch for %s: %w", id, err)
		}
		if err := d.threads.Recover(ctx, id); err != nil {
			return fmt.Errorf("resume recovery for %s: %w", id, err)
		}
	}
	return nil
}

// saveRun records identity for restart and preserves a concurrent exit marker.
func (d *Daemon) saveRun(ctx context.Context, record supervisor.RunRecord) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return d.store.Write(ctx, func(tx *eventlog.Transaction) error {
		_, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO daemon_processes(id, record) VALUES(?, ?)", record.ID, raw)
		return err
	})
}

type engineSink struct{ daemon *Daemon }
type processSink struct{ daemon *Daemon }

// Persist writes adapter facts before they are delivered to clients or sessions.
func (s engineSink) Persist(fact engine.Fact) error {
	return s.daemon.record(fact.ThreadID, fact.Type, fact.Payload, "")
}

// Emit converts supervisor facts into durable daemon events.
func (s processSink) Emit(fact supervisor.Fact) error {
	switch {
	case fact.Pressure != nil:
		s.daemon.pressure.Store(fact.Pressure.On)
		return s.daemon.record("", fact.Type, map[string]any{
			"on": fact.Pressure.On, "free_bytes": fact.Pressure.Free, "threshold_bytes": fact.Pressure.Threshold,
		}, "")
	case fact.Output != nil:
		return s.daemon.record(fact.ThreadID, fact.Type, map[string]any{
			"assignment_id": fact.RunID, "channel": fact.Output.Stream, "chunk": fact.Output.Chunk,
			"truncated": fact.Output.Truncated, "full_output": fact.Output.FullOutput,
		}, "")
	case fact.Exit != nil:
		return s.daemon.record(fact.ThreadID, fact.Type, map[string]any{
			"assignment_id": fact.RunID, "execution_outcome": fact.Exit.ExecutionOutcome, "work": fact.Exit.Work,
			"exit_code": fact.Exit.ExitCode, "descendants_cleaned": fact.Exit.DescendantsCleaned,
		}, fact.RunID)
	default:
		return errors.New("supervisor emitted an empty fact")
	}
}

// record persists a fact and any exit marker in one transaction.
func (d *Daemon) record(id, kind string, payload any, endedRun string) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	stream := "system"
	if id != "" {
		stream = "thread:" + id
	}
	ctx := context.Background()
	return d.store.Write(ctx, func(tx *eventlog.Transaction) error {
		_, err := tx.Append(ctx, eventlog.NewEvent{
			Stream: stream, ThreadID: id, Type: kind, TypeVersion: 1,
			Actor: eventlog.Actor{Kind: "daemon", ID: "synchd"}, Payload: raw,
		})
		if err != nil || endedRun == "" {
			return err
		}
		_, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO daemon_processes(id, record) VALUES(?, NULL)", endedRun)
		return err
	})
}

// classifyDispatch makes a coordinator's interrupted dispatch recoverable without an
// operator. A coordinator takes T05 without any process and parks with T08 right
// after, so a crash between the two (T24 -> paused(daemon_recovery)) leaves an
// assignment that no process.exited ever classified, and G.4 would hold it paused.
// Nothing ran, so the dispatch is recorded as resumable (T22 -> queued). The parent
// then resumes: with a child recorded (Facts.Awaited) it parks on that child, else
// it delegates again, and no second child is created for the same dispatch.
func (d *Daemon) classifyDispatch(ctx context.Context, id string) error {
	state, err := d.threads.Status(ctx, id)
	if err != nil {
		return err
	}
	assignment := state.Facts.Assignment
	if state.Facts.ManagedParent != "" || state.Interrupted != "running" || assignment == "" || state.Work[assignment] != "" {
		return nil
	}
	return d.record(id, "process.exited", map[string]any{
		"assignment_id": assignment, "execution_outcome": "lost_on_reboot", "work": "resumable",
	}, "")
}
