package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"synch/internal/eventlog"
	"synch/internal/policy"
	"synch/internal/queue"
	"synch/internal/thread"
)

// executePolicyMutation takes locks before opening the receipt writer and
// publishes the change only after a first acceptance commits.
func (s *Server) executePolicyMutation(ctx context.Context, req queue.Request, op string) (queue.Response, error) {
	body, err := queue.Admit(req.Route, req.Body)
	if err != nil {
		return queue.Response{}, err
	}
	decision, pending, prepareErr := s.preparePolicyMutation(ctx, req, op, body)
	if pending != nil {
		defer pending.Abort()
	}

	// Defer preparation errors to the first-attempt callback so a replay or
	// payload conflict takes priority over changes to current policy state.
	response, err := queue.Execute(ctx, s.config.Store, req, func(ctx context.Context, tx *eventlog.Transaction, _ map[string]any) (queue.Decision, error) {
		if prepareErr != nil {
			return queue.Decision{}, prepareErr
		}
		if id := req.PathParams["thread_id"]; id != "" && ulidPattern.MatchString(id) && s.config.Threads != nil {
			if err := s.config.Threads.Control(ctx, tx, id, "export", nil); err != nil {
				return queue.Decision{}, err
			}
		}
		return decision, nil
	})
	if err != nil {
		return queue.Response{}, err
	}
	if pending != nil && response.Status == "accepted" && !response.Replayed {
		if err := pending.Commit(); err != nil {
			slog.Error("settle accepted grant revocation", "receipt_id", response.ReceiptID, "error", err)
		}
	}
	return response, nil
}

// preparePolicyMutation validates an admitted body and stages its policy event.
func (s *Server) preparePolicyMutation(ctx context.Context, req queue.Request, op string, body map[string]any) (queue.Decision, *policy.Pending, error) {
	id := req.PathParams["thread_id"]
	var pending *policy.Pending
	var result any
	var err error
	status := 201
	if op != "revokeGrant" {
		if !ulidPattern.MatchString(id) {
			return queue.Decision{}, nil, invalid("invalid thread_id")
		}
		if s.config.Threads == nil {
			return queue.Decision{}, nil, errors.New("thread machine is unavailable")
		}
	}
	switch op {
	case "createApproval":
		if !validApproval(body) {
			return queue.Decision{}, nil, invalid("invalid approval")
		}
		var input, approval policy.Approval
		// The admitted body contains only client-supplied approval fields.
		raw, marshalErr := json.Marshal(body)
		if marshalErr != nil {
			return queue.Decision{}, nil, marshalErr
		}
		if err := json.Unmarshal(raw, &input); err != nil {
			return queue.Decision{}, nil, invalid("invalid approval")
		}
		input.Approver, input.Thread = req.Caller, id
		approval, pending, err = s.config.Policy.PrepareCreateApproval(input)
		result = map[string]string{"approval_id": approval.ID}
	case "createGrant":
		if !createGrantBody.valid(body) {
			return queue.Decision{}, nil, invalid("invalid grant")
		}
		var grant policy.Grant
		grant, pending, err = s.config.Policy.PrepareIssueGrant(id, body["host"].(string), body["identity"].(string), req.Caller)
		result = map[string]string{"grant_id": grant.ID}
	case "revokeGrant":
		status = 202
		grantID := req.PathParams["grant_id"]
		id, pending, err = s.config.Policy.PrepareRevoke(context.WithoutCancel(ctx), grantID, req.Caller)
		result = map[string]string{"grant_id": grantID}
	}
	switch {
	case errors.Is(err, policy.ErrInvalidRequest), errors.Is(err, policy.ErrInvalidApproval):
		return queue.Decision{}, nil, invalid(err.Error())
	case errors.Is(err, policy.ErrUnknownGrant), errors.Is(err, thread.ErrMissing):
		return queue.Decision{}, nil, &queue.Rejection{Code: "not_found"}
	case err != nil:
		return queue.Decision{}, nil, err
	}
	return queue.Decision{ThreadID: id, HTTPStatus: status, Commit: func(ctx context.Context, tx *eventlog.Transaction) (any, error) {
		return result, pending.Append(ctx, tx)
	}}, pending, nil
}
