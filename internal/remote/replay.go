package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"synch/internal/eventlog"
)

var errUnassignedRecord = errors.New("runner record references an unassigned thread")

// ErrRunnerHeadRegressed means the runner journal is behind the daemon's durable cursor.
var ErrRunnerHeadRegressed = errors.New("runner journal head regressed")

// --- Durable cursor and ordered replay (event catalogue §3) ---

// Replay drains the currently connected runner's journal into the daemon log.
func (d *Driver) Replay(ctx context.Context) error {
	link, err := d.connection(OpReplay, false)
	if err != nil {
		return err
	}
	if err := d.syncJournal(ctx, link); err != nil {
		if ctx.Err() == nil {
			d.drop(link, err)
		}
		return err
	}
	return nil
}

// recoverJournal derives the cursor from committed events, then ingests the remote backlog.
func (d *Driver) recoverJournal(ctx context.Context, link *runnerLink) error {
	d.replayMu.Lock()
	defer d.replayMu.Unlock()
	cursor, err := d.journalCursor(ctx)
	if err != nil {
		return err
	}
	link.cursor = cursor
	return d.pullAndRecord(ctx, link, true)
}

// journalCursor finds the last durable source sequence using the event log's public reads.
func (d *Driver) journalCursor(ctx context.Context) (int64, error) {
	head, err := d.log.Head()
	if err != nil {
		return 0, err
	}
	var global, source int64
	for global < head {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		page, err := d.log.ReadGlobal(global, replayLimit)
		if err != nil {
			return 0, err
		}
		if len(page) == 0 {
			break
		}
		for _, event := range page {
			global = event.GlobalSeq
			if event.Source != nil && event.Source.Runner == d.runner {
				source = event.Source.Seq
			}
		}
	}
	return source, nil
}

// syncJournal serializes live pulls against reconnect, attach, and manual replay.
func (d *Driver) syncJournal(ctx context.Context, link *runnerLink) error {
	d.replayMu.Lock()
	defer d.replayMu.Unlock()
	if err := link.ctx.Err(); err != nil {
		return err
	}
	return d.pullAndRecord(ctx, link, false)
}

// pullAndRecord persists the inclusive replay range after ingest, including a committed prefix on failure.
func (d *Driver) pullAndRecord(ctx context.Context, link *runnerLink, connecting bool) error {
	start := link.cursor
	pullErr := d.pull(ctx, link)
	if start == link.cursor && (!connecting || pullErr != nil) {
		return pullErr
	}
	state := "connected"
	if pullErr != nil {
		state = "disconnected"
	}
	var first, last int64
	if start < link.cursor {
		first, last = start+1, link.cursor
	}
	// A cancelled pull may have committed a prefix that still needs an audit event.
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), wireTimeout)
	defer cancel()
	auditErr := d.recordConnection(auditCtx, state, first, last)
	if auditErr != nil {
		return errors.Join(pullErr, auditErr)
	}
	return pullErr
}

// recordConnection appends a daemon-owned system event with the catalogue's replay fields.
func (d *Driver) recordConnection(ctx context.Context, state string, first, last int64) error {
	payload, err := json.Marshal(struct {
		Runner string `json:"runner"`
		State  string `json:"state"`
		First  int64  `json:"replayed_from_seq,omitempty"`
		Last   int64  `json:"replayed_to_seq,omitempty"`
	}{d.runner, state, first, last})
	if err != nil {
		return fmt.Errorf("encode runner connection: %w", err)
	}
	_, err = d.log.Append(ctx, []eventlog.NewEvent{{
		ID: eventlog.NewID(), Stream: "system", Type: "runner.connection", TypeVersion: 1,
		Actor: eventlog.Actor{Kind: "daemon", ID: "daemon"}, Payload: payload,
	}})
	if err != nil {
		return fmt.Errorf("record runner connection: %w", err)
	}
	return nil
}

// pull authenticates records and delegates ordering and deduplication to IngestRunner.
func (d *Driver) pull(ctx context.Context, link *runnerLink) error {
	for {
		res, _, err := callSSH(ctx, link.client, d.runner, wireRequest{Operation: OpReplay, After: link.cursor})
		if err != nil {
			return err
		}
		if err := responseError(res.Error); err != nil {
			return err
		}
		if res.Head == nil || *res.Head < 0 {
			return errInvalidRecord
		}
		if *res.Head < link.cursor {
			return fmt.Errorf("%w: head %d is below cursor %d", ErrRunnerHeadRegressed, *res.Head, link.cursor)
		}
		previous := link.cursor
		for _, raw := range res.Records {
			record, err := decodeRecord(raw)
			if err != nil {
				return err
			}
			if record.Runner != d.runner {
				return errRunnerIdentity
			}
			if record.SourceSeq > *res.Head {
				return errInvalidRecord
			}
			found, err := dispatched(ctx, d.ledger, d.runner, record.ThreadID, "")
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("%w at source sequence %d", errUnassignedRecord, record.SourceSeq)
			}
			events, err := d.log.IngestRunner(ctx, d.runner, []eventlog.RunnerRecord{record})
			if err != nil {
				return err
			}
			// Concurrent ingest can already have committed the same record. IngestRunner establishes continuity either way.
			link.cursor = max(link.cursor, record.SourceSeq)
			for _, event := range events {
				d.publish(event)
			}
		}
		if link.cursor == *res.Head {
			return nil
		}
		if link.cursor == previous {
			return fmt.Errorf("%w after %d", eventlog.ErrRunnerGap, link.cursor)
		}
	}
}

// --- Live journal delivery and connection health ---

// follow detects connection loss and keeps live process consumers current with the durable log.
func (d *Driver) follow(link *runnerLink) {
	go d.watchTransport(link)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-link.ctx.Done():
			return
		case <-ticker.C:
			if err := d.syncJournal(link.ctx, link); err != nil {
				d.drop(link, err)
				return
			}
		}
	}
}

// watchTransport drops admission when an idle SSH transport closes.
func (d *Driver) watchTransport(link *runnerLink) {
	_ = link.client.Wait()
	d.drop(link, errOffline)
}

// degrades identifies replay integrity violations that block runner admission.
func degrades(err error) bool {
	return errors.Is(err, eventlog.ErrRunnerGap) || errors.Is(err, ErrRunnerHeadRegressed) || errors.Is(err, errRunnerIdentity) ||
		errors.Is(err, errUnassignedRecord) || errors.Is(err, errInvalidRecord) || errors.Is(err, errInvalidPayload)
}
