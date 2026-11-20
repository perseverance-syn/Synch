package kimi

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"synch/internal/engine"
	"synch/internal/engine/claudecode/stdio"
)

type session struct {
	wire  *stdio.Session
	calls stdio.Calls
	lane  chan struct{}

	mu     sync.Mutex
	ref    string
	active bool
}

var _ engine.Session = (*session)(nil)

// Ref returns the identifier assigned by ACP session/new.
func (s *session) Ref() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ref
}

// Send submits a follow-up to the same ACP session and returns before its result arrives.
func (s *session) Send(ctx context.Context, msg engine.Message) error {
	if err := s.wire.Require(engine.SendOp); err != nil {
		return err
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()
	return s.prompt(ctx, msg.Text)
}

// Events exposes persisted ACP updates and the supervised terminal result.
func (s *session) Events() (<-chan engine.EngineEvent, error) { return s.wire.Events() }

// Interrupt notifies Kimi to cancel the active prompt. Its result ends the turn.
func (s *session) Interrupt(ctx context.Context) error {
	if err := s.wire.Require(engine.InterruptOp); err != nil {
		return err
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()
	s.mu.Lock()
	active := s.active
	s.mu.Unlock()
	if !active {
		return nil
	}
	return s.wire.Write(ctx, stdio.Fields{"jsonrpc": "2.0", "method": "session/cancel",
		"params": stdio.Fields{"sessionId": s.Ref()}})
}

// Stop ends the process and its sole ACP session through the supervisor.
func (s *session) Stop(ctx context.Context) error { return s.wire.Stop(ctx) }

// Usage returns an explicit unreported value because Kimi ACP supplies no token counts.
func (s *session) Usage(ctx context.Context) (engine.Usage, error) {
	return s.wire.ReportUsage(ctx, engine.Usage{})
}

// acquire orders outgoing prompt and cancel commands without blocking shutdown.
func (s *session) acquire(ctx context.Context) error {
	select {
	case s.lane <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.wire.Context().Done():
		return errors.New("session closed by Kimi")
	}
}

// release frees the outgoing prompt slot.
func (s *session) release() { <-s.lane }

// prompt reserves the current turn before writing the ACP session/prompt request.
func (s *session) prompt(ctx context.Context, text string) error {
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		return errors.New("a Kimi prompt is already active")
	}
	s.active = true
	s.mu.Unlock()
	params := stdio.Fields{"sessionId": s.Ref(),
		"prompt": []stdio.Fields{{"type": "text", "text": text}}}
	err := s.calls.Request(ctx, s.wire, request("session/prompt", params), s.completePrompt)
	if err != nil {
		s.mu.Lock()
		s.active = false
		s.mu.Unlock()
	}
	return err
}

// completePrompt ends a turn only after its correlated ACP result is read.
func (s *session) completePrompt(raw json.RawMessage, replyErr error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if replyErr != nil {
		return replyErr
	}
	var result struct{ StopReason string }
	if err := json.Unmarshal(raw, &result); err != nil {
		return err
	}
	switch result.StopReason {
	case "end_turn", "max_tokens", "max_turn_requests", "refusal", "cancelled":
		s.active = false
		return nil
	default:
		return errors.New("unknown Kimi ACP prompt stop reason")
	}
}
