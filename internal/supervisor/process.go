package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

type Proc struct {
	s           *Supervisor
	spec        Spec
	parent      *Proc
	cmd         *exec.Cmd
	stdin       io.WriteCloser
	protocolOut *os.File
	tree        processTree
	readers     sync.WaitGroup
	pipes       []*os.File
	done        chan struct{}
	wake        chan struct{}
	// Supervisor mutex protects closing, reason and last.
	closing   bool
	reason    string
	last      time.Time
	mu        sync.Mutex
	subs      map[chan OutputChunk]struct{}
	outputErr error
	finished  bool
	info      ExitInfo
	err       error
}

// --- Process handle and activity ---

// ID returns the caller-supplied run identifier.
func (p *Proc) ID() string { return p.spec.ID }

// PID returns the process identifier assigned by the operating system.
func (p *Proc) PID() int { return p.cmd.Process.Pid }

// WriteInput writes bytes to the child's standard input.
func (p *Proc) WriteInput(data []byte) (int, error) { return p.stdin.Write(data) }

// CloseInput closes standard input so a child reading it can observe EOF.
func (p *Proc) CloseInput() error { return p.stdin.Close() }

// ProtocolStreams returns the caller-owned streams, or two nils in capture mode.
// The caller closes both streams and periodically calls Progress during protocol activity.
func (p *Proc) ProtocolStreams() (io.WriteCloser, io.ReadCloser) {
	if p.protocolOut == nil {
		return nil, nil
	}
	return p.stdin, p.protocolOut
}

// Record returns the identity and work inputs captured at spawn for durable recovery.
func (p *Proc) Record() RunRecord {
	w := p.spec.Work
	if w.CheckpointRevision != nil {
		v := *w.CheckpointRevision
		w.CheckpointRevision = &v
	}
	return RunRecord{ID: p.ID(), ThreadID: p.spec.ThreadID, PID: p.PID(), CreationTime: p.tree.creation, Work: w}
}

// Progress records activity for the idle timeout shared by tool progress and output.
func (p *Proc) Progress() { p.s.mu.Lock(); p.last = time.Now(); p.s.mu.Unlock() }

// request latches the first stop reason and wakes the lifecycle goroutine without blocking.
func (p *Proc) request(reason string) {
	p.s.mu.Lock()
	defer p.s.mu.Unlock()
	if !p.closing {
		p.closing = true
		p.reason = reason
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}
}

// Cancel requests cancellation and waits for completion up to the caller's context deadline.
func (p *Proc) Cancel(ctx context.Context) error {
	p.request("cancelled")
	_, err := p.Wait(ctx)
	return err
}

// Wait returns the terminal result or the context error if the caller stops waiting.
func (p *Proc) Wait(ctx context.Context) (ExitInfo, error) {
	select {
	case <-p.done:
		return p.info, p.err
	case <-ctx.Done():
		return ExitInfo{}, ctx.Err()
	}
}

// Subscribe delivers future persisted chunks and returns an unsubscribe function. It disconnects lagging subscribers.
func (p *Proc) Subscribe(buffer int) (<-chan OutputChunk, func()) {
	ch := make(chan OutputChunk, max(1, buffer))
	p.mu.Lock()
	if p.finished || p.outputErr != nil {
		close(ch)
	} else {
		p.subs[ch] = struct{}{}
	}
	p.mu.Unlock()
	return ch, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if _, ok := p.subs[ch]; ok {
			close(ch)
			delete(p.subs, ch)
		}
	}
}

// --- Watchdog, child cancellation and terminal fact (execution-driver ?2, T30) ---

// run supervises one process from launch through tree cleanup and terminal event emission.
func (p *Proc) run(ctx context.Context) {
	waited := make(chan error, 1)
	go func() { waited <- p.cmd.Wait() }()
	timeout := p.spec.StallTimeout
	t := time.NewTimer(timeout)
	defer t.Stop()
	var waitErr error
	exited := false
watch:
	for {
		select {
		case waitErr = <-waited:
			exited = true
			break watch
		case <-ctx.Done():
			p.request("cancelled")
			break watch
		case <-p.wake:
			break watch
		case <-t.C:
			p.s.mu.Lock()
			left := time.Until(p.last.Add(timeout))
			if left <= 0 && !p.closing {
				p.closing = true
				p.reason = "stalled"
			}
			closing := p.closing
			p.s.mu.Unlock()
			if closing {
				break watch
			}
			t.Reset(left)
		}
	}
	p.s.mu.Lock()
	p.closing = true
	reason := p.reason
	var children []*Proc
	for _, child := range p.s.runs {
		if child.parent == p {
			children = append(children, child)
		}
	}
	p.s.mu.Unlock()
	// Wait for children after releasing the registry lock so they can remove themselves.
	cleaned := true
	for _, child := range children {
		child.request("cancelled")
	}
	for _, child := range children {
		<-child.done
		cleaned = cleaned && child.info.DescendantsCleaned
		p.err = errors.Join(p.err, child.err)
	}
	// Give the process a bounded chance to exit after interrupt before terminating its tree.
	if !exited {
		p.tree.interrupt()
		grace := time.NewTimer(p.s.cfg.Grace)
		select {
		case waitErr = <-waited:
			exited = true
		case <-grace.C:
		}
		grace.Stop()
	}
	treeErr := p.tree.clean()
	cleaned = cleaned && treeErr == nil
	if !exited {
		waitErr = <-waited
	}
	drainErr := p.finishReaders()
	cleaned = cleaned && drainErr == nil
	code := p.cmd.ProcessState.ExitCode()
	if reason == "" {
		reason = "exited"
		if code < 0 {
			reason = "killed"
		}
	}
	if reason == "cancelled" && !cleaned {
		// T30 requires proof of tree cleanup before reporting cancellation.
		reason = "killed"
	}
	p.info = ExitInfo{reason, Classify(reason == "exited" && code == 0, p.spec.Work), cleaned, code, ""}
	if drainErr != nil {
		p.info.CleanupReason = drainErr.Error()
	}
	p.mu.Lock()
	p.finished = true
	for ch := range p.subs {
		close(ch)
		delete(p.subs, ch)
	}
	p.err = errors.Join(p.err, treeErr, drainErr, p.outputErr)
	p.mu.Unlock()
	var exitErr *exec.ExitError
	if waitErr != nil && !errors.As(waitErr, &exitErr) {
		p.err = errors.Join(p.err, waitErr)
	}
	if err := p.s.sink.Emit(Fact{Type: "process.exited", RunID: p.ID(), ThreadID: p.spec.ThreadID, Exit: &p.info}); err != nil {
		p.err = errors.Join(p.err, fmt.Errorf("persist process exit: %w", err))
	}
	p.s.mu.Lock()
	delete(p.s.runs, p.ID())
	close(p.done)
	p.s.mu.Unlock()
}

var ErrDrainTimeout = errors.New("output pipe remained open after process exit")

// --- Bounded output draining after process exit (102-F1 G4) ---

// finishReaders allows a bounded drain after exit, then closes pipes a descendant still holds open.
func (p *Proc) finishReaders() error {
	done := make(chan struct{})
	go func() { p.readers.Wait(); close(done) }()
	timer := time.NewTimer(p.s.cfg.DrainTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
	}
	// Closing the readers interrupts blocked reads even if an escaped descendant retains a writer.
	for _, pipe := range p.pipes {
		_ = pipe.Close()
	}
	<-done
	return ErrDrainTimeout
}
