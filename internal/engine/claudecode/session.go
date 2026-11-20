package claudecode

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"synch/internal/engine"
	"synch/internal/engine/claudecode/stdio"
)

type session struct {
	wire                   *stdio.Session
	policy                 chan<- PermissionRequest
	assignmentID, threadID string
	calls                  stdio.Calls
	sendLane               chan struct{}
	initial                string

	mu           sync.Mutex
	ref          string
	initialized  bool
	active       bool
	lastResult   string
	interrupting bool
	usage        engine.Usage
}

var _ engine.Session = (*session)(nil)

// --- Session operations: submit turns and expose persisted results ---

// Ref returns the native session ID supplied by Claude Code or selected by Resume.
func (s *session) Ref() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ref
}

// Send writes a follow-up on the existing stream and serializes first-send injection after resume.
func (s *session) Send(ctx context.Context, msg engine.Message) error {
	if err := s.wire.Require(engine.SendOp); err != nil {
		return err
	}
	select {
	case s.sendLane <- struct{}{}:
		defer func() { <-s.sendLane }()
	case <-ctx.Done():
		return ctx.Err()
	case <-s.wire.Context().Done():
		return errors.New("session closed by Claude Code")
	}
	text := msg.Text
	if s.initial != "" {
		text = s.initial + "\n\n" + text
	}
	if err := s.send(ctx, text); err != nil {
		return err
	}
	s.initial = ""
	return nil
}

// send encodes one streaming user message and marks its turn before a result can arrive.
func (s *session) send(ctx context.Context, text string) error {
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		return errors.New("a Claude Code turn is already active")
	}
	s.active = true
	ref := s.ref
	s.mu.Unlock()
	frame := stdio.Fields{"type": "user", "session_id": ref, "parent_tool_use_id": nil,
		"message": stdio.Fields{"role": "user", "content": []stdio.Fields{{"type": "text", "text": text}}}}
	if err := s.wire.Write(ctx, frame); err != nil {
		s.mu.Lock()
		s.active = false
		s.mu.Unlock()
		return err
	}
	return nil
}

// Events exposes Claude messages, tool observations and the supervised exit.
func (s *session) Events() (<-chan engine.EngineEvent, error) { return s.wire.Events() }

// Interrupt awaits the SDK interrupt receipt without closing the session.
func (s *session) Interrupt(ctx context.Context) error {
	if err := s.wire.Require(engine.InterruptOp); err != nil {
		return err
	}
	select {
	case s.sendLane <- struct{}{}:
		defer func() { <-s.sendLane }()
	case <-ctx.Done():
		return ctx.Err()
	case <-s.wire.Context().Done():
		return errors.New("session closed by Claude Code")
	}
	s.mu.Lock()
	if !s.active {
		s.mu.Unlock()
		return nil
	}
	s.interrupting = true
	s.mu.Unlock()
	if err := s.control(ctx, "interrupt"); err != nil {
		s.mu.Lock()
		s.interrupting = false
		s.mu.Unlock()
		return err
	}
	return nil
}

// Stop ends the supervised process independently of any stream consumer.
func (s *session) Stop(ctx context.Context) error { return s.wire.Stop(ctx) }

// Usage persists and returns the cumulative reported input and output totals.
func (s *session) Usage(ctx context.Context) (engine.Usage, error) {
	s.mu.Lock()
	usage := s.usage
	s.mu.Unlock()
	return s.wire.ReportUsage(ctx, usage)
}

// --- Control exchange: match lifecycle acknowledgments by request ID ---

// control performs a correlated Claude SDK initialize or interrupt request.
func (s *session) control(ctx context.Context, subtype string) error {
	request := stdio.Fields{"subtype": subtype}
	if subtype == "initialize" {
		request["hooks"] = nil
	}
	return s.calls.Call(ctx, s.wire, func(id string) stdio.Fields {
		return stdio.Fields{"type": "control_request", "request_id": id, "request": request}
	}, nil)
}

// controlResponse routes a Claude SDK control reply by its original request identifier.
func (s *session) controlResponse(raw json.RawMessage) error {
	var response struct {
		Subtype   string
		RequestID string `json:"request_id"`
		Response  json.RawMessage
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	switch response.Subtype {
	case "success":
		return s.calls.Reply(response.RequestID, response.Response, nil)
	case "error":
		return s.calls.Reply(response.RequestID, nil, errors.New("control request rejected by Claude Code"))
	default:
		return errors.New("invalid Claude Code control response")
	}
}
