package stdio

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"synch/internal/engine"
)

const eventBuffer = 128
const eventRetry = 5 * time.Millisecond
const ioTimeout = 10 * time.Second

// Fields is a JSON object used for small, named wire envelopes.
type Fields map[string]any

// Session owns the raw transport and provides capability-gated publication.
type Session struct {
	ctx       context.Context
	cancel    context.CancelFunc
	transport Transport
	caps      engine.CapabilitySet
	sink      engine.Sink
	req       engine.StartRequest
	writes    chan write
	events    chan engine.EngineEvent
	done      chan struct{}

	mu      sync.Mutex
	failure error
}

type write struct {
	value any
	done  chan error
}

// NewSession allocates transport state before the adapter starts protocol handling.
func NewSession(t Transport, caps engine.CapabilitySet, sink engine.Sink, req engine.StartRequest) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	return &Session{ctx: ctx, cancel: cancel, transport: t, caps: caps, sink: sink, req: req,
		writes: make(chan write), events: make(chan engine.EngineEvent, eventBuffer+2), done: make(chan struct{})}
}

// Start dispatches incoming frames serially and allows reverse requests without concurrent pipe writes.
func (s *Session) Start(frame func([]byte) error) {
	go s.run(frame)
}

// Context expires at shutdown so protocol handlers can abandon pending work.
func (s *Session) Context() context.Context { return s.ctx }

// Require records unavailable operations through the engine gate.
func (s *Session) Require(op string) error {
	return s.caps.Require(s.sink, s.req.AssignmentID, s.req.ThreadID, op)
}

// Events gates access to persisted output and the terminal process event.
func (s *Session) Events() (<-chan engine.EngineEvent, error) {
	if err := s.Require(engine.StreamOp); err != nil {
		return nil, err
	}
	return s.events, nil
}

// Output persists UTF-8 chunks in wire order before publishing them.
func (s *Session) Output(channel, chunk string) error {
	for len(chunk) > 0 {
		end := min(len(chunk), engine.OutputCap)
		for end < len(chunk) && !utf8.RuneStart(chunk[end]) {
			end--
		}
		part := chunk[:end]
		chunk = chunk[end:]
		if err := s.persist("engine.output", engine.Output{AssignmentID: s.req.AssignmentID, Channel: channel, Chunk: part}); err != nil {
			return err
		}
		if s.caps.Ops[engine.StreamOp].Status != engine.Qualified {
			continue
		}
		if err := s.publish(engine.EngineEvent{Channel: channel, Chunk: part}); err != nil {
			return err
		}
	}
	return nil
}

// publish backpressures the protocol reader and retains the last persisted chunk during Stop.
func (s *Session) publish(event engine.EngineEvent) error {
	if len(s.events) >= eventBuffer {
		// The receive-only API exposes no drain notification. Recheck occupancy only
		// while saturated, leaving slots for the pending chunk and terminal event.
		tick := time.NewTicker(eventRetry)
		defer tick.Stop()
		for len(s.events) >= eventBuffer && s.ctx.Err() == nil {
			select {
			case <-tick.C:
			case <-s.ctx.Done():
			}
		}
	}
	// Only the read loop publishes output. The terminal event follows after it joins.
	s.events <- event
	return s.ctx.Err()
}

// ReportUsage persists the current report, including explicit absence, before returning it.
func (s *Session) ReportUsage(ctx context.Context, usage engine.Usage) (engine.Usage, error) {
	if err := s.Require(engine.UsageOp); err != nil {
		return engine.Usage{}, err
	}
	if err := ctx.Err(); err != nil {
		return engine.Usage{}, err
	}
	report := engine.UsageReport{AssignmentID: s.req.AssignmentID, Reported: usage.Reported}
	if usage.Reported {
		report.InputTokens, report.OutputTokens = &usage.InputTokens, &usage.OutputTokens
	}
	if err := s.persist("engine.usage", report); err != nil {
		return engine.Usage{}, err
	}
	return usage, nil
}

// persist synchronously records a fact under the current thread.
func (s *Session) persist(kind string, payload any) error {
	if err := s.sink.Persist(engine.Fact{Type: kind, ThreadID: s.req.ThreadID, Payload: payload}); err != nil {
		return fmt.Errorf("persist %s: %w", kind, err)
	}
	return nil
}

// Write sends one frame with a bounded deadline. A timed-out write already in flight aborts the session.
func (s *Session) Write(ctx context.Context, frame any) error {
	ctx, cancel := context.WithTimeout(ctx, ioTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	request := write{value: frame, done: make(chan error, 1)}
	select {
	case <-s.ctx.Done():
		return s.closedError()
	case <-ctx.Done():
		return ctx.Err()
	case s.writes <- request:
	}
	select {
	case err := <-request.done:
		return err
	case <-ctx.Done():
		// An accepted write has a worker and will resolve even after session shutdown.
		// Only a caller deadline with a live session makes its wire state ambiguous.
		if s.ctx.Err() == nil {
			s.Fail("write protocol frame", ctx.Err())
		}
		return ctx.Err()
	}
}

// Stop always begins cleanup, even if the capability is unqualified or the caller is canceled.
func (s *Session) Stop(ctx context.Context) error {
	s.cancel()
	return errors.Join(s.Wait(ctx), s.Require(engine.StopOp))
}

// Wait reports the final cleanup outcome and does not depend on any event reader.
func (s *Session) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.failure
	}
}

// Fail preserves the first failure and initiates cleanup without waiting on protocol work.
func (s *Session) Fail(op string, err error) {
	s.record(op, err)
	s.cancel()
}

// record retains the causal failure without initiating a change in transport state.
func (s *Session) record(op string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure == nil {
		s.failure = &engine.HarnessError{Operation: s.caps.Adapter + " " + op, Cause: err}
	}
}

// closedError returns the protocol failure or indicates orderly shutdown.
func (s *Session) closedError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failure != nil {
		return s.failure
	}
	return errors.New("engine session closed")
}

// --- Pipe workers and supervised shutdown (D-17) ---

// run terminates the process out of band, joins workers, and publishes exit after supervisor confirmation.
func (s *Session) run(frame func([]byte) error) {
	defer close(s.done)
	defer close(s.events)
	defer s.cancel()
	readDone := make(chan struct{})
	writeDone := make(chan struct{})
	go s.readLoop(frame, readDone)
	go s.writeLoop(writeDone)

	select {
	case <-s.ctx.Done():
	case <-readDone:
	}
	forced := s.ctx.Err() != nil
	s.cancel()
	// A native pipe close can wait on an in-flight syscall. Terminate the child first
	// so its pipe ends release blocked I/O without needing a client or stream reader.
	if forced {
		_ = cancelProcess(s.transport)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	info, err := s.transport.Wait(ctx)
	cancel()
	var cancelErr error
	if err != nil || !info.DescendantsCleaned {
		cancelErr = cancelProcess(s.transport)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		info, err = s.transport.Wait(ctx)
		cancel()
	}
	_ = s.transport.Close()
	<-readDone
	<-writeDone
	if err != nil || !info.DescendantsCleaned {
		s.record("confirm process exit", errors.Join(errors.New("process cleanup unconfirmed"), cancelErr, err))
		return
	}
	if !forced && (info.ExitCode != 0 || info.ExecutionOutcome != "exited") {
		s.record("process exit", fmt.Errorf("outcome %q, status %d", info.ExecutionOutcome, info.ExitCode))
	}
	if s.caps.Ops[engine.StreamOp].Status == engine.Qualified {
		s.mu.Lock()
		failure := s.failure
		s.mu.Unlock()
		s.events <- engine.EngineEvent{Channel: "exit", ExitCode: info.ExitCode, Err: failure}
	}
}

// readLoop decodes bounded UTF-8 protocol lines without forwarding raw stdout to the event stream.
func (s *Session) readLoop(frame func([]byte) error, done chan<- struct{}) {
	defer close(done)
	reader := bufio.NewScanner(s.transport)
	reader.Buffer(make([]byte, 32<<10), 16<<20)
	for s.ctx.Err() == nil && reader.Scan() {
		s.transport.Progress()
		if !utf8.Valid(reader.Bytes()) {
			s.Fail("read protocol frame", errors.New("invalid UTF-8"))
			return
		}
		if err := frame(reader.Bytes()); err != nil {
			if s.ctx.Err() == nil {
				s.Fail("handle protocol frame", err)
			}
			return
		}
	}
	if err := reader.Err(); err != nil && s.ctx.Err() == nil {
		s.Fail("read protocol frame", err)
	}
}

// writeLoop serializes frame writes while an independent worker keeps reading stdout.
func (s *Session) writeLoop(done chan<- struct{}) {
	defer close(done)
	encoder := json.NewEncoder(s.transport)
	for {
		select {
		case <-s.ctx.Done():
			return
		case request := <-s.writes:
			err := encoder.Encode(request.value)
			if err != nil && s.ctx.Err() == nil {
				s.Fail("write protocol frame", err)
			}
			request.done <- err
			if err != nil {
				return
			}
			s.transport.Progress()
		}
	}
}
