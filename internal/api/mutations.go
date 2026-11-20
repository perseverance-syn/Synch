package api

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"synch/internal/eventlog"
	"synch/internal/queue"
	"synch/internal/thread"
)

// Authority handles policy decisions within the queue transaction. Its
// Decision.Commit performs the accepted effect after request.accepted is appended.
type Authority interface {
	Approve(ctx context.Context, tx *eventlog.Transaction, caller, threadID string, body map[string]any) (queue.Decision, error)
	Grant(ctx context.Context, tx *eventlog.Transaction, caller, threadID string, body map[string]any) (queue.Decision, error)
	Revoke(ctx context.Context, tx *eventlog.Transaction, caller, grantID string) (queue.Decision, error)
}

const (
	requestIDHeader = "Synch-Request-Id"
	receiptIDHeader = "Synch-Receipt-Id"
	replayedHeader  = "Synch-Receipt-Replayed"
)

// mutate validates the request ID before reading the body, then delegates admission and replay to queue.
func (s *Server) mutate(w http.ResponseWriter, r *http.Request, caller string, entry route, params map[string]string) {
	ids := r.Header.Values(requestIDHeader)
	switch {
	case len(ids) == 0:
		writeProblem(w, newProblem(400, "request_id_required", ""))
		return
	case len(ids) != 1 || !requestIDPattern.MatchString(ids[0]):
		writeProblem(w, newProblem(400, "request_id_invalid", ""))
		return
	}
	raw, err := s.readCommandBody(w, r, entry.operation)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			// Close the connection rather than draining a body refused from Content-Length.
			w.Header().Set("Connection", "close")
			writeProblem(w, newProblem(http.StatusRequestEntityTooLarge, "body_too_large", ""))
		} else {
			writeProblem(w, newProblem(400, "malformed_body", "malformed_json"))
		}
		return
	}
	command := entry.operation
	if command == "sendThreadCommand" {
		// An admitted but invalid command still needs a schema-valid receipt command.
		admitted, err := queue.Admit(entry.path, raw)
		if err != nil {
			writeMutationError(w, err)
			return
		}
		if action, _ := admitted["command"].(string); threadCommands[action].fields != nil {
			command = action
		}
	}
	req := queue.Request{Caller: caller, ID: ids[0], Method: r.Method, Route: entry.path, PathParams: params, Body: raw, Command: command}
	response, err := s.executeMutation(r.Context(), req, entry.operation)
	if err != nil {
		writeMutationError(w, err)
		return
	}
	w.Header().Set(receiptIDHeader, response.ReceiptID)
	w.Header().Set(replayedHeader, strconv.FormatBool(response.Replayed))
	writeBytes(w, response.HTTPStatus, response.ContentType, response.Body)
}

// readCommandBody refuses oversized bodies before admission and bounds reads of unknown length.
func (s *Server) readCommandBody(w http.ResponseWriter, r *http.Request, operation string) ([]byte, error) {
	limit := s.config.MaxBodyBytes
	if r.ContentLength > limit {
		return nil, &http.MaxBytesError{Limit: limit}
	}
	body := http.MaxBytesReader(w, r.Body, limit)
	if operation == "revokeGrant" {
		// Revocation admits only an empty body. Count incoming bytes without retaining them.
		size, err := io.Copy(io.Discard, body)
		if size > 0 {
			return []byte{0}, err
		}
		return nil, err
	}
	return io.ReadAll(body)
}

// executeMutation commits the receipt and completes any deferred mode signal before responding.
func (s *Server) executeMutation(ctx context.Context, req queue.Request, operation string) (queue.Response, error) {
	if s.config.Policy != nil && (operation == "createApproval" || operation == "createGrant" || operation == "revokeGrant") {
		return s.executePolicyMutation(ctx, req, operation)
	}
	if operation == "sendThreadCommand" {
		// Keep post-commit updates in receipt order and let same-process retries see them complete.
		s.commandMu.Lock()
		defer s.commandMu.Unlock()
	}
	var afterCommit func(context.Context) error
	response, err := queue.Execute(ctx, s.config.Store, req, func(ctx context.Context, tx *eventlog.Transaction, body map[string]any) (queue.Decision, error) {
		return s.decide(ctx, tx, req.Caller, operation, req.PathParams, body, &afterCommit)
	})
	if err != nil {
		return queue.Response{}, err
	}
	// A deferred signal is only scheduled by a first request that reached its handler.
	if afterCommit != nil && response.Status == "accepted" && !response.Replayed {
		if err := afterCommit(context.WithoutCancel(ctx)); err != nil {
			slog.Error("apply accepted thread command", "receipt_id", response.ReceiptID, "error", err)
		}
	}
	return response, nil
}

// writeMutationError maps queue admission, conflict, and rollback errors without receipt headers.
func writeMutationError(w http.ResponseWriter, err error) {
	var detail queue.Detail
	var conflict *queue.Conflict
	switch {
	case errors.As(err, &detail):
		writeProblem(w, newProblem(400, detail.Code(), string(detail)))
	case errors.As(err, &conflict):
		p := newProblem(409, "request_id_conflict", "")
		p.StoredPayloadDigest, p.ReceiptID = conflict.StoredPayloadDigest, conflict.ReceiptID
		writeProblem(w, p)
	default:
		writeProblem(w, newProblem(500, "internal", ""))
	}
}

// getReceipt returns only the receipt owned by this caller identity.
func (s *Server) getReceipt(w http.ResponseWriter, r *http.Request, caller, id string) {
	if !requestIDPattern.MatchString(id) {
		writeProblem(w, newProblem(404, "not_found", ""))
		return
	}
	record, err := queue.Lookup(r.Context(), s.config.Store, caller, id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		writeProblem(w, newProblem(404, "not_found", ""))
	case err != nil:
		writeProblem(w, newProblem(500, "internal", ""))
	default:
		w.Header().Set(receiptIDHeader, stringFieldFromRecord(record, "receipt_id"))
		w.Header().Set(replayedHeader, "false")
		writeBytes(w, 200, "application/json", record)
	}
}

// --- Command dispatch: validate admitted bodies only on the first attempt ---

// decide routes validated thread and authority operations within the queue writer.
func (s *Server) decide(ctx context.Context, tx *eventlog.Transaction, caller, op string, params map[string]string, body map[string]any, after *func(context.Context) error) (queue.Decision, error) {
	id := params["thread_id"]
	if id != "" && !ulidPattern.MatchString(id) {
		return queue.Decision{}, invalid("invalid thread_id")
	}
	if op != "revokeGrant" && s.config.Threads == nil {
		return queue.Decision{}, errors.New("thread machine is unavailable")
	}
	switch op {
	case "createThread":
		return s.createThread(body)
	case "sendThreadCommand":
		return s.threadCommand(ctx, tx, caller, id, body, after)
	case "createApproval":
		if !validApproval(body) {
			return queue.Decision{}, invalid("invalid approval")
		}
	case "createGrant":
		if !createGrantBody.valid(body) {
			return queue.Decision{}, invalid("invalid grant")
		}
	}
	return s.authorityDecision(ctx, tx, caller, op, params, body)
}

// invalid returns a deterministic rejection for an admitted body outside its schema.
func invalid(detail string) error { return &queue.Rejection{Code: "validation_failed", Detail: detail} }

// authorityDecision hands policy validation and application to the injected authority.
func (s *Server) authorityDecision(ctx context.Context, tx *eventlog.Transaction, caller, op string, params map[string]string, body map[string]any) (queue.Decision, error) {
	a := s.config.Authority
	if a == nil {
		return queue.Decision{}, errors.New("authority handler is unavailable")
	}
	id := params["thread_id"]
	var decision queue.Decision
	var err error
	status := 201
	switch op {
	case "createApproval", "createGrant":
		// Control's export path loads the thread in the existing writer without changing its state.
		if err := s.config.Threads.Control(ctx, tx, id, "export", nil); err != nil {
			return queue.Decision{}, err
		}
		if op == "createApproval" {
			decision, err = a.Approve(ctx, tx, caller, id, body)
		} else {
			decision, err = a.Grant(ctx, tx, caller, id, body)
		}
		decision.ThreadID = id
	case "revokeGrant":
		status = 202
		decision, err = a.Revoke(ctx, tx, caller, params["grant_id"])
	default:
		return queue.Decision{}, errors.New("unhandled authority operation")
	}
	if err != nil {
		return queue.Decision{}, err
	}
	if !ulidPattern.MatchString(decision.ThreadID) {
		return queue.Decision{}, errors.New("authority returned invalid thread ID")
	}
	decision.HTTPStatus = status
	return decision, nil
}

// threadCommand builds an atomic transition or schedules a deferred mode signal after commit.
func (s *Server) threadCommand(ctx context.Context, tx *eventlog.Transaction, caller, id string, body map[string]any, after *func(context.Context) error) (queue.Decision, error) {
	action, _ := body["command"].(string)
	spec, ok := threadCommands[action]
	if !ok || !spec.valid(body) {
		return queue.Decision{}, invalid("invalid thread command")
	}
	machine := s.config.Threads
	decision := queue.Decision{ThreadID: id, HTTPStatus: 202}
	switch action {
	case "pause", "resume", "cancel", "decide":
		decision.Commit = func(ctx context.Context, tx *eventlog.Transaction) (any, error) {
			err := machine.Control(ctx, tx, id, action, nil)
			if action == "decide" {
				return body, err
			}
			return map[string]any{}, err
		}
	case "resolve_external_request":
		decision.Commit = func(ctx context.Context, tx *eventlog.Transaction) (any, error) {
			err := machine.Control(ctx, tx, id, action, func() error {
				return s.recordResolution(ctx, tx, caller, id, body)
			})
			return body, err
		}
	case "accept", "reject":
		decision.Commit = func(ctx context.Context, tx *eventlog.Transaction) (any, error) {
			return body, recordAcceptance(ctx, tx, machine, caller, id, action, body)
		}
	case "approve_plan":
		command := thread.Command{Transition: "T03", PlanDigest: body["plan_digest"].(string)}
		decision.Commit = func(ctx context.Context, tx *eventlog.Transaction) (any, error) {
			return body, machine.ApplyTx(ctx, tx, id, command)
		}
	case "select_harness", "change_mode":
		if err := machine.Control(ctx, tx, id, "export", nil); err != nil {
			return queue.Decision{}, err
		}
		// The queue writes the selected pair, which the thread machine reads during catch-up.
		if action == "select_harness" {
			break
		}
		decision.Commit = func(context.Context, *eventlog.Transaction) (any, error) { return body, nil }
		*after = func(ctx context.Context) error {
			return machine.Update(ctx, id, func(facts *thread.Facts) error {
				facts.SwitchToSandbox = body["to"] == "sandbox"
				return nil
			})
		}
	}
	return decision, nil
}
