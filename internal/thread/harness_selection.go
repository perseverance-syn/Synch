package thread

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"synch/internal/eventlog"
)

// HarnessSelection is the durable operator choice exposed by Status for routing.
type HarnessSelection struct {
	Harness string `json:"harness"`
	Model   string `json:"model"`
}

// loadHarnessSelection derives D-16 status from the latest accepted receipt on this thread.
func loadHarnessSelection(ctx context.Context, tx *eventlog.Transaction, s *State) error {
	s.SelectedHarness = nil
	s.Facts.SelectedPair = false
	var raw []byte
	// The accepted event's per-thread sequence gives a total order, including same-millisecond receipts.
	err := tx.QueryRowContext(ctx, `SELECT record FROM receipts WHERE status='accepted'
  AND json_extract(record,'$.thread_id')=? AND json_extract(record,'$.command')='select_harness'
  ORDER BY json_extract(record,'$.event_seq') DESC LIMIT 1`, s.ID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var receipt struct {
		EventSeq int64            `json:"event_seq"`
		Result   HarnessSelection `json:"result"`
	}
	if json.Unmarshal(raw, &receipt) != nil || receipt.EventSeq < 1 || receipt.Result.Harness == "" || receipt.Result.Model == "" {
		return nil
	}
	s.SelectedHarness = &receipt.Result
	if s.Phase == "paused" && s.Reason == "harness_unavailable" {
		var entered int64
		// A choice from before this failure is visible for routing, but the pause needs a replacement.
		err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM events WHERE stream=?
   AND json_extract(envelope,'$.type')='thread.state_changed' AND json_extract(envelope,'$.type_version')=4
   AND json_extract(envelope,'$.payload.transition') IN ('T15','T23')`, "thread:"+s.ID).Scan(&entered)
		if err != nil {
			return err
		}
		s.Facts.SelectedPair = receipt.EventSeq > entered
	} else {
		s.Facts.SelectedPair = true
	}
	return nil
}
