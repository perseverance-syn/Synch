package policy

import (
	"context"
	"encoding/json"
	"fmt"

	"synch/internal/eventlog"
)

// Pending holds a prepared policy change across the caller's event transaction.
// Its owner must defer Abort immediately, append once, and call Commit only
// after that append's transaction commits. It is not safe for concurrent use.
type Pending struct {
	event   *eventlog.NewEvent
	publish func()
	release func()
	finish  func() error
}

// Append joins the policy event to a caller-owned transaction, such as a receipt.
func (p *Pending) Append(ctx context.Context, tx *eventlog.Transaction) error {
	if p.event == nil {
		return nil
	}
	if _, err := tx.Append(ctx, *p.event); err != nil {
		return fmt.Errorf("record %s: %w", p.event.Type, err)
	}
	return nil
}

// Commit publishes the persisted change, releases its locks, and finishes any
// post-commit settlement. A settlement error leaves a durable obligation.
func (p *Pending) Commit() error {
	if p.release == nil {
		return nil
	}
	if p.publish != nil {
		p.publish()
	}
	p.Abort()
	if p.finish != nil {
		return p.finish()
	}
	return nil
}

// Abort releases the locks without publishing a change. It is safe after Commit.
func (p *Pending) Abort() {
	if p.release != nil {
		p.release()
		p.release = nil
	}
}

// persist applies the prepare protocol when no caller owns a transaction.
func (e *Engine) persist(ctx context.Context, p *Pending) error {
	defer p.Abort()
	if p.event != nil {
		if err := e.log.Write(ctx, func(tx *eventlog.Transaction) error {
			return p.Append(ctx, tx)
		}); err != nil {
			return err
		}
	}
	return p.Commit()
}

// newEvent encodes one thread policy event for standalone and receipt writes.
func (e *Engine) newEvent(thread, typ string, version int, actor eventlog.Actor, payload any) (*eventlog.NewEvent, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", typ, err)
	}
	return &eventlog.NewEvent{
		ID: newULID(e.now()), Stream: "thread:" + thread, Type: typ, TypeVersion: version,
		Actor: actor, ThreadID: thread, Payload: raw,
	}, nil
}
