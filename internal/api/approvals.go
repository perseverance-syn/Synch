package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"synch/internal/eventlog"
	"synch/internal/policy"
	"synch/internal/queue"
	"synch/internal/thread"
	"time"
)

// --- Event scan: every read endpoint here is a projection of the durable log ---

// scanGlobal walks the log in global order and hands the wanted event types to visit.
// ponytail: full scan per request; add a projection table if the log outgrows this.
func (s *Server) scanGlobal(types []string, visit func(eventlog.Event)) error {
	var after int64
	for {
		page, err := s.config.Store.ReadGlobal(after, pageMax)
		if err != nil {
			return err
		}
		for _, e := range page {
			if slices.Contains(types, e.Type) {
				visit(e)
			}
		}
		if len(page) < pageMax {
			return nil
		}
		after = page[len(page)-1].GlobalSeq
	}
}

// --- Approval requests: a policy.decided event with outcome waiting_approval ---

// approvalDecision is how a request ended.
type approvalDecision struct {
	Outcome    string    `json:"outcome"` // approved, rejected or changes_requested
	Actor      string    `json:"actor"`
	At         time.Time `json:"at"`
	Reason     string    `json:"reason,omitempty"`
	ApprovalID string    `json:"approval_id,omitempty"`
}

// approvalRequest is identified by the id of its policy.decided event.
type approvalRequest struct {
	RequestID   string            `json:"request_id"`
	ThreadID    string            `json:"thread_id"`
	State       string            `json:"state"` // pending or decided
	Action      string            `json:"action"`
	Target      string            `json:"target,omitempty"`
	PlanDigest  string            `json:"plan_digest,omitempty"`
	RuleID      string            `json:"rule_id,omitempty"`
	RequestedAt time.Time         `json:"requested_at"`
	Decision    *approvalDecision `json:"decision,omitempty"`
}

// approvalRequests projects every request in log order. A request is decided by the first later
// approval.recorded on its thread that covers its action and target, or by an approval.declined
// that names it.
func (s *Server) approvalRequests(ctx context.Context, withPlan bool) ([]approvalRequest, error) {
	var out []approvalRequest
	byID := map[string]int{}
	err := s.scanGlobal([]string{"policy.decided", "approval.recorded", "approval.declined"}, func(e eventlog.Event) {
		switch e.Type {
		case "policy.decided":
			var p struct {
				Action  string `json:"action"`
				Target  string `json:"target"`
				RuleID  string `json:"rule_id"`
				Outcome string `json:"outcome"`
			}
			if json.Unmarshal(e.Payload, &p) == nil && p.Outcome == "waiting_approval" {
				byID[e.ID] = len(out)
				out = append(out, approvalRequest{RequestID: e.ID, ThreadID: e.ThreadID, State: "pending", Action: p.Action,
					Target: p.Target, RuleID: p.RuleID, RequestedAt: e.RecordedAt})
			}
		case "approval.recorded":
			var a policy.Approval
			if json.Unmarshal(e.Payload, &a) != nil {
				return
			}
			for i := range out {
				r := &out[i]
				if r.State == "pending" && r.ThreadID == e.ThreadID && (r.Target == "" || r.Target == a.Target) && slices.Contains(a.Actions, r.Action) {
					r.State, r.Decision = "decided", &approvalDecision{Outcome: "approved", Actor: a.Approver, At: a.ApprovedAt, ApprovalID: a.ID}
				}
			}
		case "approval.declined":
			var d map[string]string
			if json.Unmarshal(e.Payload, &d) != nil {
				return
			}
			if i, ok := byID[d["request_id"]]; ok && out[i].State == "pending" {
				out[i].State, out[i].Decision = "decided", &approvalDecision{Outcome: d["decision"], Actor: d["operator"], At: e.RecordedAt, Reason: d["reason"]}
			}
		}
	})
	if err != nil {
		return nil, err
	}
	if !withPlan {
		return out, nil // inside a receipt writer the thread machine cannot open its own transaction
	}
	plans := map[string]string{}
	for i := range out {
		id := out[i].ThreadID
		plan, seen := plans[id]
		if !seen && s.config.Threads != nil {
			if st, err := s.config.Threads.Status(ctx, id); err == nil {
				plan = st.Facts.PlanDigest
			}
			plans[id] = plan
		}
		out[i].PlanDigest = plan
	}
	return out, nil
}

// listApprovals serves GET /v1/approvals?state=pending|decided&thread=.
func (s *Server) listApprovals(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	state, id := q.Get("state"), q.Get("thread")
	if (state != "" && state != "pending" && state != "decided") || (id != "" && !ulidPattern.MatchString(id)) {
		writeProblem(w, newProblem(400, "validation_failed", "invalid state or thread filter"))
		return
	}
	all, err := s.approvalRequests(r.Context(), true)
	if err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	list := []approvalRequest{}
	for _, req := range all {
		if (state == "" || req.State == state) && (id == "" || req.ThreadID == id) {
			list = append(list, req)
		}
	}
	writeJSON(w, map[string]any{"approvals": list})
}

var declineBody = schema{[]string{"reason"}, map[string]checker{"reason": nonempty}}

// declineApproval records approval.declined for a pending request. A rejection only denies the
// waiting action, so the thread keeps its state. A change request also raises the thread's
// material-risk fact (existing T10 path): a running thread goes to awaiting_decision, where the
// operator's decide command (T11) sends it back to the queue.
func (s *Server) declineApproval(ctx context.Context, caller, op, requestID string, body map[string]any, after *func(context.Context) error) (queue.Decision, error) {
	if !declineBody.valid(body) {
		return queue.Decision{}, invalid("invalid approval decision")
	}
	all, err := s.approvalRequests(ctx, false)
	if err != nil {
		return queue.Decision{}, err
	}
	i := slices.IndexFunc(all, func(r approvalRequest) bool { return r.RequestID == requestID })
	if i < 0 {
		return queue.Decision{}, &queue.Rejection{Code: "not_found", Detail: "approval request not found"}
	}
	req := all[i]
	if req.State != "pending" {
		return queue.Decision{}, &queue.Rejection{Code: "invalid_transition", Detail: "approval request already decided"}
	}
	decision := "rejected"
	if op == "requestApprovalChanges" {
		decision = "changes_requested"
		id := req.ThreadID
		*after = func(ctx context.Context) error {
			set := func(v bool) error {
				return s.config.Threads.Update(ctx, id, func(f *thread.Facts) error { f.Material = v; return nil })
			}
			// T10 applies only to a running thread; any other phase just keeps the record.
			// Clear the fact afterwards so the later T11 does not re-enter T10.
			if err := set(true); err != nil {
				return err
			}
			err := s.config.Threads.Apply(ctx, id, thread.Command{Transition: "T10"})
			if errors.Is(err, thread.ErrTransition) {
				err = nil
			}
			return errors.Join(err, set(false))
		}
	}
	payload := map[string]string{"request_id": requestID, "decision": decision, "reason": body["reason"].(string), "operator": caller}
	return queue.Decision{ThreadID: req.ThreadID, HTTPStatus: 201, Commit: func(ctx context.Context, tx *eventlog.Transaction) (any, error) {
		return map[string]string{"request_id": requestID, "decision": decision},
			appendOperatorEvidence(ctx, tx, caller, req.ThreadID, "approval.declined", 1, payload)
	}}, nil
}

// --- Checkpoint references: a rollback "vmsnap:<driver>/<endpoint>/<vm>/<snapshot>" ---

const checkpointPrefix = "vmsnap:"

// CheckpointRef names a VM snapshot used as an approval's rollback. Each part is URL-path escaped.
type CheckpointRef struct{ Driver, Endpoint, VM, Snapshot string }

// ParseCheckpointRef reads a rollback value. isRef is false for a plain rollback command.
func ParseCheckpointRef(rollback string) (ref CheckpointRef, isRef bool, err error) {
	rest, isRef := strings.CutPrefix(rollback, checkpointPrefix)
	if !isRef {
		return ref, false, nil
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 4 {
		return ref, true, errors.New("malformed checkpoint reference")
	}
	for i, p := range parts {
		v, uerr := url.PathUnescape(p)
		if uerr != nil || v == "" {
			return ref, true, errors.New("malformed checkpoint reference")
		}
		parts[i] = v
	}
	return CheckpointRef{parts[0], parts[1], parts[2], parts[3]}, true, nil
}

// checkCheckpoint admits an approval only when a checkpoint rollback verifies through its VM driver.
// The refusal is the stored rejection policy_denied (403) with detail "checkpoint_unverified: ...".
func (s *Server) checkCheckpoint(ctx context.Context, threadID string, body map[string]any) error {
	rollback, _ := body["rollback"].(string)
	ref, isRef, err := ParseCheckpointRef(rollback)
	if !isRef {
		return nil
	}
	refuse := func(why string) error {
		return &queue.Rejection{Code: "policy_denied", Detail: "checkpoint_unverified: " + why}
	}
	if err != nil {
		return refuse(err.Error())
	}
	if s.config.VerifyCheckpoint == nil {
		return refuse("no VM driver can verify checkpoints")
	}
	if err := s.config.VerifyCheckpoint(ctx, threadID, ref); err != nil {
		slog.Warn("checkpoint verification refused", "thread", threadID, "error", err)
		return refuse("snapshot not verified on its owning VM")
	}
	return nil
}
