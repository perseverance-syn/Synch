package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"synch/internal/engine"
)

// --- Session setup and commands (engine-adapter sections 1 and 3) ---

const eventBuffer = 128

type session struct {
	adapter                *Adapter
	transport              Transport
	assignmentID, threadID string
	events                 chan engine.EngineEvent
	done                   chan struct{}
	writes                 chan fields
	lane                   chan struct{}
	closeOnce              sync.Once
	abortOnce              sync.Once
	stopping               atomic.Bool

	mu                       sync.Mutex
	failure                  error
	ref, turnID, completedID string
	starting                 bool
	usage                    engine.Usage
	items                    map[string]itemState
	nextID                   uint64
	pending                  *request
}

var _ engine.Session = (*session)(nil)

// newSession starts independent pipe readers and writers before the first request.
func newSession(a *Adapter, req engine.StartRequest, t Transport) *session {
	s := &session{adapter: a, transport: t, assignmentID: req.AssignmentID, threadID: req.ThreadID,
		events: make(chan engine.EngineEvent, eventBuffer+1), done: make(chan struct{}),
		writes: make(chan fields), lane: make(chan struct{}, 1), items: make(map[string]itemState)}
	go s.writeLoop()
	go s.readLoop()
	return s
}

// initialize performs the connection handshake and opens the selected thread.
func (s *session) initialize(ctx context.Context, req engine.StartRequest, ref, prompt string) error {
	client := fields{"name": "synch", "title": "Synch", "version": s.adapter.caps.AdapterVersion}
	if err := s.call(ctx, "initialize", fields{"clientInfo": client}, nil); err != nil {
		return err
	}
	if err := s.enqueue(ctx, fields{"method": "initialized", "params": fields{}}); err != nil {
		return err
	}
	method := "thread/start"
	params := fields{}
	if req.Workdir != "" {
		params["cwd"] = req.Workdir
	}
	if req.Model != "" {
		params["model"] = req.Model
	}
	if ref != "" {
		method = "thread/resume"
		params["threadId"] = ref
		params["excludeTurns"] = true
	}
	err := s.call(ctx, method, params, func(raw json.RawMessage) error {
		id, err := resultID(raw, "thread")
		if err != nil {
			return err
		}
		if ref != "" && ref != id {
			return errors.New("resume returned a different thread")
		}
		s.mu.Lock()
		s.ref = id
		s.mu.Unlock()
		return nil
	})
	if err != nil || ref != "" {
		return err
	}
	return s.startTurn(ctx, prompt)
}

// Ref returns the Codex thread ID used for native resume.
func (s *session) Ref() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ref
}

// Send starts a follow-up turn on the live Codex thread.
func (s *session) Send(ctx context.Context, msg engine.Message) error {
	if err := s.require(engine.SendOp); err != nil {
		return err
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()
	return s.startTurn(ctx, msg.Text)
}

// Events gates access to the ordered stream and the terminal process result.
func (s *session) Events() (<-chan engine.EngineEvent, error) {
	if err := s.require(engine.StreamOp); err != nil {
		return nil, err
	}
	return s.events, nil
}

// Interrupt cancels the active Codex turn and leaves the session alive.
func (s *session) Interrupt(ctx context.Context) error {
	if err := s.require(engine.InterruptOp); err != nil {
		return err
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()
	s.mu.Lock()
	ref, turn := s.ref, s.turnID
	s.mu.Unlock()
	if turn == "" {
		return nil
	}
	return s.call(ctx, "turn/interrupt", fields{"threadId": ref, "turnId": turn}, nil)
}

// Stop unsubscribes the engine session, then asks the supervisor to terminate its tree.
func (s *session) Stop(ctx context.Context) error {
	if err := s.require(engine.StopOp); err != nil {
		return err
	}
	if s.stopping.Load() {
		return s.awaitExit(ctx)
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()
	if s.stopping.Load() {
		return s.awaitExit(ctx)
	}
	err := s.call(ctx, "thread/unsubscribe", fields{"threadId": s.Ref()}, s.confirmUnsubscribe)
	if err != nil {
		s.fail(err)
		// A failed unsubscribe cannot leave a process running.
		s.abort()
		return s.awaitExit(ctx)
	}
	if err := s.transport.Cancel(ctx); err != nil {
		s.fail(harnessFailure("stop Codex process", err))
		s.abort()
	}
	return s.awaitExit(ctx)
}

// confirmUnsubscribe recognizes all documented states in which this connection is detached.
func (s *session) confirmUnsubscribe(raw json.RawMessage) error {
	var result struct{ Status string }
	if err := json.Unmarshal(raw, &result); err != nil {
		return err
	}
	switch result.Status {
	case "unsubscribed", "notSubscribed", "notLoaded":
		s.stopping.Store(true)
		return nil
	default:
		return errors.New("unknown thread unsubscribe status")
	}
}

// Usage returns the last cumulative report and persists it before returning it.
func (s *session) Usage(ctx context.Context) (engine.Usage, error) {
	if err := s.require(engine.UsageOp); err != nil {
		return engine.Usage{}, err
	}
	if err := ctx.Err(); err != nil {
		return engine.Usage{}, err
	}
	s.mu.Lock()
	usage := s.usage
	s.mu.Unlock()
	payload := engine.UsageReport{AssignmentID: s.assignmentID, Reported: usage.Reported}
	if usage.Reported {
		payload.InputTokens, payload.OutputTokens = &usage.InputTokens, &usage.OutputTokens
	}
	if err := s.persist("engine.usage", payload); err != nil {
		return engine.Usage{}, err
	}
	return usage, nil
}

// --- Turn state and operation serialization (engine-adapter sections 1 and 3) ---

// startTurn correlates the RPC result with notifications that may arrive before it.
func (s *session) startTurn(ctx context.Context, text string) error {
	s.mu.Lock()
	if s.turnID != "" {
		s.mu.Unlock()
		return errors.New("a Codex turn is already active")
	}
	s.starting = true
	ref := s.ref
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.starting = false
		s.mu.Unlock()
	}()
	params := fields{"threadId": ref, "input": []fields{{"type": "text", "text": text}}}
	return s.call(ctx, "turn/start", params, func(raw json.RawMessage) error {
		id, err := resultID(raw, "turn")
		if err != nil {
			return err
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.turnID != "" && s.turnID != id {
			return errors.New("turn response disagrees with turn notification")
		}
		if s.completedID != id {
			s.turnID = id
		}
		return nil
	})
}

// require records a denial before returning an unavailable operation error.
func (s *session) require(op string) error {
	return s.adapter.caps.Require(s.adapter.sink, s.assignmentID, s.threadID, op)
}

// acquire gives one caller the request slot while honoring its context.
func (s *session) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.lane <- struct{}{}:
		select {
		case <-s.done:
			s.release()
			return s.closedError()
		default:
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release frees the request slot for the next session operation.
func (s *session) release() { <-s.lane }

// awaitExit waits for the process result to have been persisted by the supervisor.
func (s *session) awaitExit(ctx context.Context) error {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.failure
	case <-ctx.Done():
		return ctx.Err()
	}
}

// persist hands the next fact to the sink synchronously before returning.
func (s *session) persist(kind string, payload any) error {
	if err := s.adapter.sink.Persist(engine.Fact{Type: kind, ThreadID: s.threadID, Payload: payload}); err != nil {
		return fmt.Errorf("persist %s: %w", kind, err)
	}
	return nil
}
