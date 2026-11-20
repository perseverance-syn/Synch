package codex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"synch/internal/engine"
)

// --- Request encoding and cancellation (D-01) ---

type fields map[string]any

type envelope struct {
	ID     json.RawMessage
	Method string
	Params json.RawMessage
	Result json.RawMessage
	Error  *struct {
		Code int
	}
}

type request struct {
	id     uint64
	accept func(json.RawMessage) error
	result chan error
}

// call exchanges one request. A cancelled in-flight exchange closes the session
// because the server may already have acted on the request.
func (s *session) call(ctx context.Context, method string, params fields, accept func(json.RawMessage) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.nextID++
	r := &request{id: s.nextID, accept: accept, result: make(chan error, 1)}
	s.pending = r
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.pending = nil
		s.mu.Unlock()
	}()
	if err := s.enqueue(ctx, fields{"method": method, "id": r.id, "params": params}); err != nil {
		return err
	}
	select {
	case err := <-r.result:
		return err
	case <-s.done:
		// The reader may have accepted a reply just before seeing EOF.
		select {
		case err := <-r.result:
			return err
		default:
			return s.closedError()
		}
	case <-ctx.Done():
		s.fail(harnessFailure(method, errors.New("request outcome is unknown after cancellation")))
		s.abort()
		return ctx.Err()
	}
}

// enqueue submits an outbound message without blocking a caller past its deadline.
func (s *session) enqueue(ctx context.Context, msg fields) error {
	select {
	case <-s.done:
		return s.closedError()
	case <-ctx.Done():
		return ctx.Err()
	case s.writes <- msg:
		return nil
	}
}

// writeLoop serializes complete JSONL writes independently of read progress.
func (s *session) writeLoop() {
	encoder := json.NewEncoder(s.transport)
	for {
		select {
		case <-s.done:
			return
		case msg := <-s.writes:
			if err := encoder.Encode(msg); err != nil {
				s.fail(harnessFailure("write Codex protocol", err))
				s.abort()
				return
			}
			s.transport.Progress()
		}
	}
}

// --- Inbound frame handling and process disposition (D-17) ---

// readLoop decodes one complete line at a time and emits the exit after process persistence.
func (s *session) readLoop() {
	reader := bufio.NewScanner(s.transport)
	reader.Buffer(make([]byte, 32<<10), 16<<20)
	var readErr error
	for reader.Scan() {
		s.transport.Progress()
		if readErr = s.frame(reader.Bytes()); readErr != nil {
			s.fail(readErr)
			s.abort()
			break
		}
	}
	if readErr == nil && !s.stopping.Load() {
		err := reader.Err()
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		s.fail(harnessFailure("read Codex protocol", err))
	}
	s.finish()
}

// frame dispatches Codex requests, notifications and correlated replies in wire order.
func (s *session) frame(raw []byte) error {
	var msg envelope
	if err := json.Unmarshal(raw, &msg); err != nil {
		return harnessFailure("decode Codex frame", err)
	}
	switch {
	case msg.Method != "" && len(msg.ID) != 0:
		// Tool mediation belongs to the daemon. Reject unsupported server requests without executing them.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := s.enqueue(ctx, fields{"id": msg.ID, "error": fields{"code": -32601, "message": "Client method unavailable"}})
		if err != nil {
			return &engine.HarnessError{Operation: "reject Codex request", Cause: err}
		}
		return nil
	case msg.Method != "":
		return s.notification(msg.Method, msg.Params)
	case len(msg.ID) != 0:
		return s.reply(msg)
	default:
		return harnessFailure("decode Codex frame", errors.New("missing method and response ID"))
	}
}

// reply accepts only the outstanding ID and applies state changes before the next frame.
func (s *session) reply(msg envelope) error {
	var id uint64
	if err := json.Unmarshal(msg.ID, &id); err != nil || id == 0 {
		return harnessFailure("decode Codex response ID", errors.New("invalid response ID"))
	}
	s.mu.Lock()
	r := s.pending
	if r != nil && r.id == id {
		s.pending = nil
	}
	s.mu.Unlock()
	if r == nil || r.id != id {
		return nil
	}
	if msg.Error != nil {
		if len(msg.Result) != 0 {
			return harnessFailure("decode Codex response", errors.New("response contains result and error"))
		}
		err := fmt.Errorf("server rejected request (code %d)", msg.Error.Code)
		r.result <- harnessFailure("Codex RPC", err)
		return nil
	}
	if !bytes.HasPrefix(bytes.TrimSpace(msg.Result), []byte("{")) {
		return harnessFailure("decode Codex response", errors.New("missing result object"))
	}
	if r.accept != nil {
		if err := r.accept(msg.Result); err != nil {
			return harnessFailure("decode Codex result", err)
		}
	}
	r.result <- nil
	return nil
}

// resultID extracts the mandatory id from a thread or turn response.
func resultID(raw json.RawMessage, key string) (string, error) {
	var result map[string]struct{ ID string }
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", err
	}
	if result[key].ID == "" {
		return "", fmt.Errorf("missing %s ID", key)
	}
	return result[key].ID, nil
}

// --- Transport shutdown and terminal event (D-17) ---

// fail retains the first terminal failure for callers and the exit event.
func (s *session) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure == nil {
		s.failure = err
	}
}

// closedError returns the recorded cause or a typed error for later operations.
func (s *session) closedError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure != nil {
		return s.failure
	}
	return harnessFailure("Codex session", errors.New("session closed"))
}

// abort requests cleanup asynchronously so request cancellation can return even with blocked I/O.
func (s *session) abort() {
	s.abortOnce.Do(func() {
		go func() {
			_ = cancelProcess(s.transport)
			s.closeTransport()
		}()
	})
}

// closeTransport releases concurrent I/O and closes both caller-owned handles once.
func (s *session) closeTransport() { s.closeOnce.Do(func() { _ = s.transport.Close() }) }

// finish waits for a persisted process exit, cancelling if EOF leaves the process running.
func (s *session) finish() {
	defer close(s.done)
	defer close(s.events)
	defer s.closeTransport()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	info, err := s.transport.Wait(ctx)
	cancel()
	if err != nil || !info.DescendantsCleaned {
		_ = cancelProcess(s.transport)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		info, err = s.transport.Wait(ctx)
		cancel()
	}
	if err != nil || !info.DescendantsCleaned {
		s.fail(&engine.HarnessError{Operation: "confirm Codex process exit", Cause: errors.Join(err, errors.New("process exit or cleanup unconfirmed"))})
		info.ExitCode = -1
	}
	s.mu.Lock()
	failure := s.failure
	s.mu.Unlock()
	// The extra channel slot is reserved for the terminal event.
	s.events <- engine.EngineEvent{Channel: "exit", ExitCode: info.ExitCode, Err: failure}
}
