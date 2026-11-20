package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"synch/internal/eventlog"
	"synch/internal/thread"
	"time"
)

// Decision describes the accepted command. Commit runs after request.accepted,
// while both events and the receipt can still roll back.
type Decision struct {
	ThreadID   string
	HTTPStatus int
	Commit     func(context.Context, *eventlog.Transaction) (any, error)
}
type Decide func(context.Context, *eventlog.Transaction, map[string]any) (Decision, error)
type Rejection struct{ Code, Detail string }

// Error returns the deterministic rejection's problem code.
func (r *Rejection) Error() string { return r.Code }

var rejectionStatus = map[string]int{"validation_failed": 400, "policy_denied": 403, "grant_required": 403, "not_owner": 403, "approval_required": 403, "approval_stale": 403, "not_found": 404, "invalid_transition": 409, "disk_pressure": 422}

// containsNull finds explicit null members after their payload identity has been computed.
func containsNull(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case map[string]any:
		for _, item := range x {
			if containsNull(item) {
				return true
			}
		}
	case []any:
		for _, item := range x {
			if containsNull(item) {
				return true
			}
		}
	}
	return false
}

// Execute replays a receipt or applies the first request and records its acceptance in one transaction.
func Execute(ctx context.Context, s *eventlog.Store, r Request, decide Decide) (Response, error) {
	return execute(ctx, s, r, func(ctx context.Context, tx *eventlog.Transaction, response Response, body map[string]any) (Response, error) {
		tx = tx.WithRequest(r.ID)
		command := r.Command
		if command == "" {
			command, _ = body["command"].(string)
		}
		if command == "" {
			command = r.Route
		}
		decisionID, validDecide := acceptedDecisionInput(r, command, body)
		if decisionID != "" {
			command = "decide"
		}
		pair, validSelection := harnessSelectionInput(r, command, body)
		if pair != nil {
			command = "select_harness"
		}
		record := map[string]any{"request_id": r.ID, "receipt_id": response.ReceiptID, "payload_digest": response.PayloadDigest, "command": command, "created_at": time.Now().UTC()}
		// A deterministic rejection must roll back command writes while retaining its receipt.
		_, err := tx.ExecContext(ctx, "SAVEPOINT command")
		if err != nil {
			return Response{}, err
		}
		var decision Decision
		switch {
		case body != nil && containsNull(body):
			err = &Rejection{"validation_failed", "command fields cannot be null"}
		case !validDecide:
			err = &Rejection{"validation_failed", "invalid decide command"}
		case !validSelection:
			err = &Rejection{"validation_failed", "invalid select_harness command"}
		case decide == nil:
			return Response{}, errors.New("missing command handler")
		default:
			decision, err = decide(ctx, tx, body)
		}
		if err == nil {
			if decisionID != "" && decision.ThreadID != r.PathParams["thread_id"] {
				return Response{}, errors.New("decide handler returned a different thread")
			}
			if pair != nil && decision.ThreadID != r.PathParams["thread_id"] {
				return Response{}, errors.New("select_harness handler returned a different thread")
			}
			response.HTTPStatus = decision.HTTPStatus
			response.Status = "accepted"
			response.ContentType = "application/json"
			payload, encodeErr := json.Marshal(map[string]string{"request_id": r.ID, "receipt_id": response.ReceiptID, "payload_digest": response.PayloadDigest, "command": command})
			if encodeErr != nil {
				return Response{}, encodeErr
			}
			e, appendErr := tx.Append(ctx, eventlog.NewEvent{Stream: "thread:" + decision.ThreadID, ThreadID: decision.ThreadID, RequestID: r.ID, Type: "request.accepted", TypeVersion: 1, Actor: eventlog.Actor{Kind: "client", ID: r.Caller}, Payload: payload})
			if appendErr != nil {
				return Response{}, appendErr
			}
			record["thread_id"] = decision.ThreadID
			record["event_seq"] = e.Seq
			if decision.Commit != nil {
				record["result"], err = decision.Commit(ctx, tx)
			}
		}
		if err != nil {
			switch {
			case errors.Is(err, thread.ErrTransition):
				err = &Rejection{Code: "invalid_transition"}
			case errors.Is(err, thread.ErrMissing):
				err = &Rejection{Code: "not_found"}
			}
			var reject *Rejection
			if !errors.As(err, &reject) {
				return Response{}, err
			}
			if _, err := tx.ExecContext(ctx, "ROLLBACK TO command"); err != nil {
				return Response{}, err
			}
			status, ok := rejectionStatus[reject.Code]
			if !ok {
				return Response{}, fmt.Errorf("unknown rejection: %w", reject)
			}
			delete(record, "event_seq")
			delete(record, "result")
			response.HTTPStatus = status
			response.Status = "rejected"
			response.ContentType = "application/problem+json"
			problem := map[string]any{"type": "https://synch.local/problems/" + reject.Code, "title": reject.Code, "status": status, "code": reject.Code, "receipt_id": response.ReceiptID}
			if reject.Detail != "" {
				problem["detail"] = reject.Detail
			}
			record["problem"] = problem
			response.Body, err = json.Marshal(problem)
			if err != nil {
				return Response{}, err
			}
		}
		if response.Status == "accepted" && pair != nil {
			record["result"] = pair
		}
		if _, err := tx.ExecContext(ctx, "RELEASE command"); err != nil {
			return Response{}, err
		}
		record["status"] = response.Status
		record["http_status"] = response.HTTPStatus
		response.Record, err = json.Marshal(record)
		if err != nil {
			return Response{}, err
		}
		if response.Status == "accepted" {
			response.Body = response.Record
			if decisionID != "" {
				response.decisionID = decisionID
				response.decisionThread = decision.ThreadID
			}
		}
		return response, nil
	})
}

// --- Command evidence: bind accepted receipts to admitted payloads ---

// harnessSelectionInput preserves the admitted pair as the accepted receipt's result for D-16.
func harnessSelectionInput(r Request, command string, body map[string]any) (map[string]string, bool) {
	if command != "select_harness" && body["command"] != "select_harness" {
		return nil, true
	}
	harness, _ := body["harness"].(string)
	model, _ := body["model"].(string)
	if r.Method != "POST" || r.Route != "/v1/threads/{thread_id}/commands" || r.PathParams["thread_id"] == "" ||
		len(body) != 3 || body["command"] != "select_harness" || model == "" {
		return nil, false
	}
	switch harness {
	case "codex", "claude-code", "kimi-code":
		return map[string]string{"harness": harness, "model": model}, true
	default:
		return nil, false
	}
}

// acceptedDecisionInput extracts D-15 receipt evidence from the admitted request.
func acceptedDecisionInput(r Request, command string, body map[string]any) (string, bool) {
	if command != "decide" && body["command"] != "decide" {
		return "", true
	}
	id, _ := body["decision_id"].(string)
	choice, _ := body["choice"].(string)
	if r.Method != "POST" || r.Route != "/v1/threads/{thread_id}/commands" || r.PathParams["thread_id"] == "" ||
		body["command"] != "decide" || id == "" || choice == "" {
		return "", false
	}
	for key, v := range body {
		switch key {
		case "command", "decision_id", "choice":
		case "note":
			if _, ok := v.(string); !ok {
				return "", false
			}
		default:
			return "", false
		}
	}
	return id, true
}

// --- Receipt lookup: scope records to the authenticated caller ---

// Lookup returns the receipt record scoped to the caller identity and request ID.
func Lookup(ctx context.Context, s *eventlog.Store, caller, id string) (json.RawMessage, error) {
	var record json.RawMessage
	err := s.Write(ctx, func(tx *eventlog.Transaction) error {
		return tx.QueryRowContext(ctx, "SELECT record FROM receipts WHERE caller_identity=? AND request_id=?", caller, id).Scan(&record)
	})
	return record, err
}
