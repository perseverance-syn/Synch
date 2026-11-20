package fixturetest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"synch/internal/engine"
	"synch/internal/execdriver"
)

// --- Fixture harness: replay NDJSON over the Direct driver boundary ---

type step struct {
	Send   json.RawMessage
	Expect json.RawMessage
	Raw    string
	Mark   string
	Hold   bool
	Wait   bool
	Repeat int
}

// Replay reports progress and verifies the outcome of a fixture transcript.
type Replay struct {
	peer  net.Conn
	marks chan string
	next  chan struct{}
	done  chan struct{}
	err   error
}

// NewReplay connects fixture stdio without starting a harness or opening a login session.
func NewReplay(t *testing.T, path string) (*Replay, *Proc) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	cwdJSON, err := json.Marshal(cwd)
	if err != nil {
		t.Fatal(err)
	}
	data = bytes.ReplaceAll(data, []byte(`"$cwd"`), cwdJSON)
	client, peer := net.Pipe()
	if err := peer.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	r := &Replay{peer: peer, marks: make(chan string, 8), next: make(chan struct{}), done: make(chan struct{})}
	t.Cleanup(func() {
		_ = client.Close()
		_ = peer.Close()
		close(r.next)
		<-r.done
	})
	go r.run(data)
	p := &Proc{in: client, out: client, exited: r.done, Cancelled: make(chan struct{})}
	return r, p
}

// run replays each step in order and lets the test inspect results after simulated exit.
func (r *Replay) run(data []byte) {
	defer close(r.done)
	defer func() { _ = r.peer.Close() }()
	scanner := bufio.NewScanner(bytes.NewReader(data))
	read := bufio.NewReader(r.peer)
	for line := 1; scanner.Scan(); line++ {
		var s step
		if err := json.Unmarshal(scanner.Bytes(), &s); err != nil {
			r.err = fmt.Errorf("transcript step %d: %w", line, err)
			return
		}
		if err := r.apply(s, read); err != nil {
			r.err = fmt.Errorf("transcript step %d: %w", line, err)
			return
		}
	}
	r.err = scanner.Err()
}

// apply writes a harness frame, checks an adapter frame, or marks a fixture Checkpoint.
func (r *Replay) apply(s step, read *bufio.Reader) error {
	switch {
	case s.Mark != "":
		r.marks <- s.Mark
	case s.Wait:
		<-r.next
	case s.Hold:
		_, err := read.ReadByte()
		if !errors.Is(err, io.EOF) {
			return fmt.Errorf("await stdin close: %v", err)
		}
	case len(s.Expect) != 0:
		line, err := read.ReadBytes('\n')
		if err != nil {
			return err
		}
		var want, got any
		if err := json.Unmarshal(s.Expect, &want); err != nil {
			return err
		}
		if err := json.Unmarshal(line, &got); err != nil {
			return err
		}
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("got %s, want %s", bytes.TrimSpace(line), s.Expect)
		}
	default:
		frame := string(s.Send)
		if s.Raw != "" {
			frame = s.Raw
		}
		for i := range max(1, s.Repeat) {
			line := strings.ReplaceAll(frame, "$index", strconv.Itoa(i))
			if _, err := io.WriteString(r.peer, line+"\n"); err != nil {
				return err
			}
		}
	}
	return nil
}

// Advance releases a fixture step after the caller has observed the previous state.
func (r *Replay) Advance(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case r.next <- struct{}{}:
	case <-r.done:
		t.Fatalf("transcript ended before the next step: %v", r.err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// Checkpoint waits until the harness has observed the expected protocol state.
func (r *Replay) Checkpoint(t *testing.T, ctx context.Context, want string) {
	t.Helper()
	select {
	case got := <-r.marks:
		if got != want {
			t.Fatalf("transcript Checkpoint = %q, want %q", got, want)
		}
	case <-r.done:
		t.Fatalf("transcript ended before %q: %v", want, r.err)
	case <-ctx.Done():
		t.Fatalf("await transcript Checkpoint %q: %v", want, ctx.Err())
	}
}

// Finish verifies that all transcript steps ran successfully.
func (r *Replay) Finish(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-r.done:
		if r.err != nil {
			t.Fatal(r.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// FinishInterrupted checks that Stop closed the fixture pipe while it was writing.
func (r *Replay) FinishInterrupted(t *testing.T, ctx context.Context) {
	t.Helper()
	select {
	case <-r.done:
		if !errors.Is(r.err, io.ErrClosedPipe) {
			t.Fatalf("interrupted transcript: got %v, want closed pipe", r.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// --- Supervised process and synchronous sink fixtures ---

type Driver struct {
	execdriver.ExecutionDriver
	Proc    *Proc
	Version []byte
	Specs   []execdriver.ProcSpec
}

// Kind identifies the fixture target as Direct.
func (*Driver) Kind() string { return execdriver.KindDirect }

// Describe identifies the fixture target as operator host access.
func (*Driver) Describe() execdriver.DriverInfo {
	return execdriver.DriverInfo{Label: "Host access"}
}

// Spawn records the actual launch spec and returns version text or the replay pipes.
func (d *Driver) Spawn(_ context.Context, spec execdriver.ProcSpec) (execdriver.Proc, error) {
	d.Specs = append(d.Specs, spec)
	if slices.Equal(spec.Args, []string{"--version"}) {
		return &Proc{in: discardCloser{}, out: io.NopCloser(bytes.NewReader(d.Version))}, nil
	}
	if d.Proc == nil {
		return nil, errors.New("unexpected fixture process launch")
	}
	return d.Proc, nil
}

type discardCloser struct{}

// Write discards version-probe stdin.
func (discardCloser) Write(p []byte) (int, error) { return len(p), nil }

// Close finishes version-probe stdin.
func (discardCloser) Close() error { return nil }

type Proc struct {
	execdriver.Proc
	in        io.WriteCloser
	out       io.ReadCloser
	exited    <-chan struct{}
	Cancelled chan struct{}
	once      sync.Once
}

// ProtocolStreams supplies the exact replay streams to the launcher.
func (p *Proc) ProtocolStreams() (io.WriteCloser, io.ReadCloser) { return p.in, p.out }

// Progress accepts the harness heartbeat without starting a watchdog.
func (*Proc) Progress() {}

// Cancel simulates process cancellation closing the executable's ends of its pipes.
func (p *Proc) Cancel(context.Context) error {
	p.once.Do(func() {
		if p.Cancelled != nil {
			_ = p.in.Close()
			close(p.Cancelled)
		}
	})
	return nil
}

// Wait acts as the supervisor persistence barrier once the fixture peer has exited.
func (p *Proc) Wait(ctx context.Context) (execdriver.ExitInfo, error) {
	if p.exited != nil {
		select {
		case <-p.exited:
		case <-ctx.Done():
			return execdriver.ExitInfo{}, ctx.Err()
		}
	}
	return execdriver.ExitInfo{ExecutionOutcome: "exited", DescendantsCleaned: true}, nil
}

type Sink struct {
	mu    sync.Mutex
	facts []engine.Fact
}

// Persist records a fact before returning to the adapter.
func (s *Sink) Persist(fact engine.Fact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.facts = append(s.facts, fact)
	return nil
}

// OfType returns a synchronized snapshot of facts of one catalogue type.
func (s *Sink) OfType(kind string) []engine.Fact {
	s.mu.Lock()
	defer s.mu.Unlock()
	var found []engine.Fact
	for _, fact := range s.facts {
		if fact.Type == kind {
			found = append(found, fact)
		}
	}
	return found
}
