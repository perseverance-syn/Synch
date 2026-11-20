package policy

import (
	"errors"
	"testing"

	"synch/internal/eventlog"
)

// TestPreparedApprovalRollback proves AC7: an aborted write cannot authorize a later decision.
func TestPreparedApprovalRollback(t *testing.T) {
	ctx := t.Context()
	e, _, log := fixture(t, op(`{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`))
	approval, pending, err := e.PrepareCreateApproval(baseApproval())
	if err != nil {
		t.Fatal(err)
	}
	defer pending.Abort()
	rollback := errors.New("roll back receipt")
	err = log.Write(ctx, func(tx *eventlog.Transaction) error {
		if err := pending.Append(ctx, tx); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("write: %v", err)
	}
	pending.Abort()
	got, err := e.Decide(ctx, Request{Thread: rootID, Action: "ci.run", Target: "t1", Machine: "m", ApprovalID: approval.ID})
	if err != nil || got.Allowed || len(events(t, log, rootID, "approval.recorded")) != 0 {
		t.Fatalf("rolled-back approval: %+v, %v", got, err)
	}
}
