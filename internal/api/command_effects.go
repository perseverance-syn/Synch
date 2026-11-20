package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"synch/internal/eventlog"
	"synch/internal/queue"
	"synch/internal/thread"
)

// recordAcceptance appends the human decision before the machine loads its transition evidence.
func recordAcceptance(ctx context.Context, tx *eventlog.Transaction, machine *thread.Machine, caller, id, action string, body map[string]any) error {
	decision, transition := "accepted", "T28"
	if action == "reject" {
		decision, transition = "rejected", "T29"
	}
	revision := body["revision"].(string)
	payload := map[string]string{"decision": decision, "revision": revision, "operator": caller}
	if note, ok := body["note"].(string); ok {
		payload["note"] = note
	}
	// Both writes share the receipt savepoint, so a rejected transition rolls back its evidence.
	if err := appendOperatorEvidence(ctx, tx, caller, id, "acceptance.recorded", 1, payload); err != nil {
		return err
	}
	return machine.ApplyTx(ctx, tx, id, thread.Command{Transition: transition, Revision: revision})
}

// recordResolution uses the originating event's operation and the authenticated operator evidence.
func (s *Server) recordResolution(ctx context.Context, tx *eventlog.Transaction, caller, id string, body map[string]any) error {
	requestID := body["external_request_id"].(string)
	operation, err := s.externalOperation(ctx, id, requestID)
	if err != nil {
		return err
	}
	payload := map[string]string{
		"external_request_id": requestID, "operation": operation, "resolution": body["resolution"].(string),
		"via": "operator", "operator": caller, "note": body["note"].(string),
	}
	return appendOperatorEvidence(ctx, tx, caller, id, "external_request.resolved", 4, payload)
}

var externalOperationValue = oneOf("create_from_base", "start", "stop", "delete", "snapshot", "restore")

// externalOperation finds an intent on this thread through the event log's public read API.
func (s *Server) externalOperation(ctx context.Context, threadID, requestID string) (string, error) {
	var after int64
	// The receipt owns the writer. The WAL reader sees all previously committed intents without nesting a write.
	for ctx.Err() == nil {
		events, err := s.config.Store.ReadStream("thread:"+threadID, after, pageMax)
		if err != nil {
			return "", fmt.Errorf("read external request history: %w", err)
		}
		for _, e := range events {
			if e.Type != "external_request.persisted" || e.TypeVersion != 3 {
				continue
			}
			var intent struct {
				ID        string `json:"external_request_id"`
				Operation string `json:"operation"`
			}
			if err := json.Unmarshal(e.Payload, &intent); err != nil {
				return "", fmt.Errorf("decode persisted external request: %w", err)
			}
			if intent.ID != requestID {
				continue
			}
			if !externalOperationValue(intent.Operation) {
				return "", errors.New("invalid persisted external request operation")
			}
			return intent.Operation, nil
		}
		if len(events) < pageMax {
			return "", &queue.Rejection{Code: "not_found", Detail: "external request not found on this thread"}
		}
		after = events[len(events)-1].Seq
	}
	return "", ctx.Err()
}

// appendOperatorEvidence writes one catalogue event correlated to the enclosing receipt request.
func appendOperatorEvidence(ctx context.Context, tx *eventlog.Transaction, caller, id, kind string, version int, payload map[string]string) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s evidence: %w", kind, err)
	}
	_, err = tx.Append(ctx, eventlog.NewEvent{
		Stream: "thread:" + id, ThreadID: id, Type: kind, TypeVersion: version,
		Actor: eventlog.Actor{Kind: "operator", ID: caller}, Payload: raw,
	})
	if err != nil {
		return fmt.Errorf("append %s evidence: %w", kind, err)
	}
	return nil
}
