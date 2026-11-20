package claudecode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"synch/internal/engine"
	"synch/internal/engine/claudecode/stdio"
)

const permissionTimeout = 5 * time.Second

// PermissionRequest represents a Claude tool prompt forwarded to the daemon policy receiver.
// The adapter treats a closed or unresponsive policy receiver as unavailable.
type PermissionRequest struct {
	Context      context.Context
	AssignmentID string
	ThreadID     string
	SessionRef   string
	ToolName     string
	ToolUseID    string
	Input        json.RawMessage
	reply        chan bool
}

// Reply records the first policy answer without blocking after cancellation.
func (p PermissionRequest) Reply(allow bool) {
	select {
	case p.reply <- allow:
	default:
	}
}

// permissionRequest matches the Anthropic SDK can_use_tool request payload.
type permissionRequest struct {
	Subtype   string
	Tool      string          `json:"tool_name"`
	ToolUseID string          `json:"tool_use_id"`
	Input     json.RawMessage `json:"input"`
}

// answerPermission explicitly denies invalid or unanswered requests with a usable correlation ID.
func (s *session) answerPermission(id string, raw json.RawMessage) error {
	if id == "" {
		return errors.New("missing usable ID in Claude Code permission request")
	}
	var request permissionRequest
	err := json.Unmarshal(raw, &request)
	result := stdio.Fields{"behavior": "deny", "message": "Synch did not approve this tool call"}
	s.mu.Lock()
	ready, ref := s.initialized && s.active, s.ref
	s.mu.Unlock()
	if err == nil && request.Subtype == "can_use_tool" && request.Tool != "" &&
		bytes.HasPrefix(bytes.TrimSpace(request.Input), []byte("{")) &&
		ready && s.wire.Require(engine.InterceptionOp) == nil && s.decidePermission(ref, request) {
		// Return the original input, never a mutation by the policy receiver.
		result = stdio.Fields{"behavior": "allow", "updatedInput": request.Input}
	}
	return s.wire.Write(s.wire.Context(), stdio.Fields{"type": "control_response", "response": stdio.Fields{
		"subtype": "success", "request_id": id, "response": result,
	}})
}

// decidePermission waits within a bounded budget while the daemon owns policy execution.
func (s *session) decidePermission(ref string, request permissionRequest) bool {
	if s.policy == nil {
		return false
	}
	return s.awaitDecision(ref, request, permissionTimeout)
}

// awaitDecision treats a missing answer or expired budget as a refusal without launching any workers.
func (s *session) awaitDecision(ref string, request permissionRequest, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(s.wire.Context(), timeout)
	defer cancel()
	query := PermissionRequest{
		Context: ctx, AssignmentID: s.assignmentID, ThreadID: s.threadID, SessionRef: ref,
		ToolName: request.Tool, ToolUseID: request.ToolUseID, Input: bytes.Clone(request.Input),
		reply: make(chan bool, 1),
	}
	if !deliverPermission(ctx, s.policy, query) {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case allow := <-query.reply:
		return allow && ctx.Err() == nil
	}
}

// deliverPermission rejects shutdown of the daemon receiver without panicking across the adapter boundary.
func deliverPermission(ctx context.Context, policy chan<- PermissionRequest, query PermissionRequest) (delivered bool) {
	// A send to a closed receiver is the only panic possible in this handoff.
	defer func() {
		if recover() != nil {
			delivered = false
		}
	}()
	select {
	case <-ctx.Done():
		return false
	case policy <- query:
		return true
	}
}
