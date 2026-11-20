package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"synch/internal/eventlog"
	"synch/internal/execdriver"
	"synch/internal/supervisor"
)

type remoteProc struct {
	driver      *Driver
	thread, id  string
	protocol    bool
	mu          sync.Mutex
	finished    bool
	done        chan struct{}
	result      execdriver.ExitInfo
	subscribers map[chan execdriver.OutputChunk]struct{}
	afterExit   func()
}

var _ execdriver.Proc = (*remoteProc)(nil)

// newRemoteProc tracks live delivery and completion separately from the SSH connection.
func newRemoteProc(d *Driver, thread, id string, protocol bool) *remoteProc {
	return &remoteProc{driver: d, thread: thread, id: id, protocol: protocol, done: make(chan struct{}), subscribers: make(map[chan execdriver.OutputChunk]struct{})}
}

// ID returns the durable dispatch identifier.
func (p *remoteProc) ID() string { return p.id }

// Subscribe delivers future ingested output, closing the subscription if its consumer falls behind.
func (p *remoteProc) Subscribe(buffer int) (<-chan execdriver.OutputChunk, func()) {
	ch := make(chan execdriver.OutputChunk, max(0, buffer))
	p.mu.Lock()
	if p.finished {
		close(ch)
	} else {
		p.subscribers[ch] = struct{}{}
	}
	p.mu.Unlock()
	return ch, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if _, exists := p.subscribers[ch]; exists {
			close(ch)
			delete(p.subscribers, ch)
		}
	}
}

// Cancel requests runner-side process cancellation over the active connection.
func (p *remoteProc) Cancel(ctx context.Context) error {
	select {
	case <-p.done:
		return nil
	default:
	}
	_, err := p.call(ctx, OpCancel, wireRequest{})
	return err
}

// Progress reports activity on the runner without requiring a new approval.
func (p *remoteProc) Progress() {
	_, _ = p.call(context.Background(), OpProgress, wireRequest{})
}

// Wait waits for an ingested exit, including one that arrives after reconnection.
func (p *remoteProc) Wait(ctx context.Context) (execdriver.ExitInfo, error) {
	// A completed result remains available even after its driver shuts down.
	select {
	case <-p.done:
		return p.result, nil
	default:
	}
	select {
	case <-p.done:
		return p.result, nil
	case <-p.driver.shutdown:
		return execdriver.ExitInfo{}, errOffline
	case <-ctx.Done():
		return execdriver.ExitInfo{}, ctx.Err()
	}
}

// ProtocolStreams returns lossless stdout and a writer for process stdin when requested.
func (p *remoteProc) ProtocolStreams() (io.WriteCloser, io.ReadCloser) {
	if !p.protocol {
		return nil, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &remoteInput{proc: p}, &remoteOutput{proc: p, ctx: ctx, cancel: cancel}
}

// call submits a process operation using the current connection rather than its original one.
func (p *remoteProc) call(ctx context.Context, op string, req wireRequest) (wireResponse, error) {
	link, err := p.driver.connection(op, false)
	if err != nil {
		return wireResponse{}, err
	}
	req.Operation, req.Thread, req.Run = op, p.thread, p.id
	return p.driver.call(ctx, link, req)
}

// onExit registers cleanup even if the exit raced and was ingested before dispatch returned.
func (p *remoteProc) onExit(cleanup func()) {
	p.mu.Lock()
	if !p.finished {
		p.afterExit = cleanup
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	cleanup()
}

// --- Delivery follows successful ingest (execution-driver §2) ---

type deliveredFact struct {
	Assignment string           `json:"assignment_id"`
	Channel    string           `json:"channel"`
	Chunk      string           `json:"chunk"`
	Full       *artifactPayload `json:"full_output"`
	Truncated  bool             `json:"truncated"`
	Outcome    string           `json:"execution_outcome"`
	Work       string           `json:"work"`
	Code       int              `json:"exit_code"`
	Cleaned    bool             `json:"descendants_cleaned"`
}

// delivery parses the committed copy so redacted output is what subscribers see.
func delivery(e eventlog.Event) (deliveredFact, bool) {
	var fact deliveredFact
	if e.Type != "engine.output" && e.Type != "process.exited" {
		return fact, false
	}
	err := json.Unmarshal(e.Payload, &fact)
	return fact, err == nil
}

// publish routes a durable runner event to its matching assignment.
func (d *Driver) publish(event eventlog.Event) {
	fact, ok := delivery(event)
	if !ok {
		return
	}
	d.mu.Lock()
	proc := d.procs[fact.Assignment]
	d.mu.Unlock()
	if proc != nil && proc.thread == event.ThreadID {
		proc.receive(event.Type, fact)
	}
}

// receive applies output or a final exit, ignoring facts after the exit.
func (p *remoteProc) receive(eventType string, fact deliveredFact) {
	p.mu.Lock()
	if p.finished {
		p.mu.Unlock()
		return
	}
	if eventType == "process.exited" {
		p.result = execdriver.ExitInfo{ExecutionOutcome: fact.Outcome, Work: fact.Work, ExitCode: fact.Code, DescendantsCleaned: fact.Cleaned}
		p.finished = true
		close(p.done)
		for ch := range p.subscribers {
			close(ch)
			delete(p.subscribers, ch)
		}
		cleanup := p.afterExit
		p.afterExit = nil
		p.mu.Unlock()
		if cleanup != nil {
			cleanup()
		}
		return
	}
	output := execdriver.OutputChunk{Stream: fact.Channel, Chunk: fact.Chunk, Truncated: fact.Truncated}
	if fact.Full != nil {
		output.FullOutput = &supervisor.Artifact{URI: fact.Full.URI, Digest: fact.Full.Digest}
	}
	for ch := range p.subscribers {
		select {
		case ch <- output:
		default:
			close(ch)
			delete(p.subscribers, ch)
		}
	}
	p.mu.Unlock()
}

// Attach looks up an already reserved assignment without resubmitting it.
func (d *Driver) Attach(ctx context.Context, thread, id string, protocol bool) (execdriver.Proc, error) {
	if !ulid.MatchString(id) || !ulid.MatchString(thread) {
		return nil, ErrOperationDenied
	}
	found, err := dispatched(ctx, d.ledger, d.runner, thread, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrOperationDenied
	}
	d.replayMu.Lock()
	defer d.replayMu.Unlock()
	d.mu.Lock()
	proc := d.procs[id]
	if proc == nil {
		proc = newRemoteProc(d, thread, id, protocol)
		d.procs[id] = proc
	}
	d.mu.Unlock()
	// Holding replayMu closes the race between reading the catch-up window and subscribing to new commits.
	var cursor int64
	for {
		page, err := d.log.ReadStream("thread:"+thread, cursor, replayLimit)
		if err != nil {
			return nil, fmt.Errorf("attach to process events: %w", err)
		}
		for _, e := range page {
			cursor = e.Seq
			if e.Type != "process.exited" || e.Source == nil || e.Source.Runner != d.runner {
				continue
			}
			fact, ok := delivery(e)
			if ok && fact.Assignment == id {
				proc.receive(e.Type, fact)
				return proc, nil
			}
		}
		if len(page) < replayLimit {
			return proc, nil
		}
	}
}

// --- Lossless protocol I/O, independent of the raw SSH session ---

type remoteInput struct{ proc *remoteProc }

// Write sends bounded chunks once and returns the bytes the runner acknowledged.
func (p *remoteInput) Write(data []byte) (int, error) {
	var total int
	for len(data) != 0 {
		chunk := data[:min(len(data), protocolChunk)]
		res, err := p.proc.call(context.Background(), OpProtocol, wireRequest{Protocol: "write", Data: chunk})
		if res.Written < 0 || res.Written > len(chunk) {
			return total, io.ErrShortWrite
		}
		total += res.Written
		if err != nil {
			return total, err
		}
		if res.Written != len(chunk) {
			return total, io.ErrShortWrite
		}
		data = data[len(chunk):]
	}
	return total, nil
}

// Close closes process stdin on the owning runner.
func (p *remoteInput) Close() error {
	_, err := p.proc.call(context.Background(), OpProtocol, wireRequest{Protocol: "close"})
	return err
}

type remoteOutput struct {
	proc   *remoteProc
	ctx    context.Context
	cancel context.CancelFunc
	offset int64
}

// Read retrieves stdout by byte offset, waiting through changes in SSH transport.
func (p *remoteOutput) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		if p.ctx.Err() != nil {
			return 0, io.ErrClosedPipe
		}
		res, err := p.proc.call(p.ctx, OpProtocol, wireRequest{Protocol: "read", Offset: p.offset})
		if err == nil {
			if len(res.Data) != 0 {
				n := copy(data, res.Data)
				p.offset += int64(n)
				return n, nil
			}
			if res.EOF {
				return 0, io.EOF
			}
		} else if !errors.Is(err, errOffline) && p.proc.driver.Status().Connected {
			return 0, err
		}
		select {
		case <-ticker.C:
		case <-p.ctx.Done():
			return 0, io.ErrClosedPipe
		case <-p.proc.driver.shutdown:
			return 0, errOffline
		}
	}
}

// Close detaches this protocol reader without stopping the runner's capture.
func (p *remoteOutput) Close() error { p.cancel(); return nil }
