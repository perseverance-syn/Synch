package execdriver

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"synch/internal/supervisor"
)

// --- Supervised raw stdio (D-17, AC6b) ---

// TestProtocolRoundTrip verifies AC6b lossless stdout, backpressure and one exit fact.
func TestProtocolRoundTrip(t *testing.T) {
	driver, sink := newIntegrationDriver(t)
	spec := helperSpec(t, "protocol")
	spec.ProtocolIO = true
	proc, err := driver.Spawn(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	in, out := proc.ProtocolStreams()
	defer func() { _ = out.Close() }()
	defer func() { _ = in.Close() }()
	payload := strings.Repeat("{\"id\":1,\"result\":{\"text\":\"hello 界\"}}\n", 40000)
	written := make(chan error, 1)
	go func() {
		_, err := io.WriteString(in, payload)
		written <- err
		_ = in.Close()
	}()
	select {
	case err := <-written:
		t.Fatalf("writer finished before stdout was drained: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	type readResult struct {
		data []byte
		err  error
	}
	result := make(chan readResult, 1)
	go func() {
		data, err := io.ReadAll(out)
		result <- readResult{data, err}
	}()
	select {
	case got := <-result:
		want := "{\"method\":\"fixture/ready\"}\n" + payload
		if got.err != nil || string(got.data) != want {
			t.Fatalf("stdout changed: got %d bytes, want %d, error %v", len(got.data), len(want), got.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out draining protocol stdout")
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	info := waitForExit(t, proc)
	if info.ExitCode != 0 || !info.DescendantsCleaned || sink.output(spec.ID) != "" {
		t.Fatalf("unexpected exit or protocol stdout fact: %+v", info)
	}
	sink.checkOnlyExit(t, spec, info)
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, fact := range sink.facts {
		if fact.RunID == spec.ID && fact.Output != nil && fact.Output.Stream == "stderr" && fact.Output.Chunk == "diagnostic\n" {
			return
		}
	}
	t.Fatal("diagnostic was not persisted")
}

// TestProtocolProgress verifies AC6b extends the watchdog during silent protocol activity.
func TestProtocolProgress(t *testing.T) {
	driver, sink := newIntegrationDriver(t)
	spec := helperSpec(t, "idle")
	spec.ProtocolIO = true
	spec.StallTimeout = 400 * time.Millisecond
	proc, err := driver.Spawn(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	in, out := proc.ProtocolStreams()
	defer func() { _ = in.Close(); _ = out.Close() }()
	done := make(chan ExitInfo, 1)
	go func() {
		info, _ := proc.Wait(t.Context())
		done <- info
	}()
	tick := time.NewTicker(40 * time.Millisecond)
	defer tick.Stop()
	for range 20 {
		select {
		case info := <-done:
			t.Fatalf("protocol process stalled despite Progress: %+v", info)
		case <-tick.C:
			proc.Progress()
		}
	}
	select {
	case info := <-done:
		if info.ExecutionOutcome != "stalled" || !info.DescendantsCleaned {
			t.Fatalf("unexpected watchdog outcome: %+v", info)
		}
		sink.checkOnlyExit(t, spec, info)
	case <-time.After(5 * time.Second):
		t.Fatal("protocol process did not stall after Progress stopped")
	}
}

// --- Real process lifecycle (AC4 and AC6) ---

// TestStalledProcessEmitsOneExit verifies AC4's stall outcome and single terminal fact.
func TestStalledProcessEmitsOneExit(t *testing.T) {
	driver, sink := newIntegrationDriver(t)
	spec := helperSpec(t, "idle")
	spec.StallTimeout = 200 * time.Millisecond
	proc, err := driver.Spawn(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	info := waitForExit(t, proc)
	if info.ExecutionOutcome != "stalled" || info.Work != "lost" || !info.DescendantsCleaned {
		t.Fatalf("unexpected stall result: %+v", info)
	}
	sink.checkOnlyExit(t, spec, info)
}

// TestOutputContinuesAfterUnsubscribe verifies AC4's persist-before-forward ordering and AC6's detached lifetime.
func TestOutputContinuesAfterUnsubscribe(t *testing.T) {
	driver, sink := newIntegrationDriver(t)
	gate := filepath.Join(t.TempDir(), "gate")
	spec := helperSpec(t, "output", gate)
	request, cancel := context.WithCancel(t.Context())
	defer cancel()
	proc, err := driver.Spawn(request, spec)
	if err != nil {
		t.Fatal(err)
	}
	chunks, detach := proc.Subscribe(8)
	defer detach()
	_, detachOther := proc.Subscribe(8)
	defer detachOther()
	openGate(t, gate+".before")
	before := "before disconnect\n"
	var output strings.Builder
	for !strings.Contains(output.String(), before) {
		select {
		case chunk, ok := <-chunks:
			if !ok {
				t.Fatal("output ended before the client detached")
			}
			output.WriteString(chunk.Chunk)
			if !strings.Contains(sink.output(spec.ID), output.String()) {
				t.Fatalf("forwarded output %q before the sink recorded it", output.String())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for output before detaching")
		}
	}
	detach()
	detachOther()
	cancel()
	openGate(t, gate+".after")
	info := waitForExit(t, proc)
	if info.ExecutionOutcome != "exited" || info.Work != "complete" || !info.DescendantsCleaned {
		t.Fatalf("unexpected exit after detaching: %+v", info)
	}
	if got := sink.output(spec.ID); got != before+"after disconnect\n" {
		t.Fatalf("output recorded across disconnect = %q", got)
	}
	sink.checkOnlyExit(t, spec, info)
}

// --- Synchronous sink and integration fixtures ---

type factSink struct {
	mu    sync.Mutex
	facts []supervisor.Fact
}

// Emit records a fact synchronously so tests can verify when the supervisor forwards output.
func (s *factSink) Emit(fact supervisor.Fact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.facts = append(s.facts, fact)
	return nil
}

// output returns the stdout chunks accepted by the sink for the named run.
func (s *factSink) output(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result strings.Builder
	for _, fact := range s.facts {
		if fact.RunID == id && fact.Output != nil && fact.Output.Stream == "stdout" {
			result.WriteString(fact.Output.Chunk)
		}
	}
	return result.String()
}

// checkOnlyExit verifies the sink accepted one terminal fact with the wait result and correct scope.
func (s *factSink) checkOnlyExit(t *testing.T, spec ProcSpec, want ExitInfo) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, fact := range s.facts {
		if fact.Type != "process.exited" {
			continue
		}
		count++
		if fact.Exit == nil || *fact.Exit != want || fact.RunID != spec.ID || fact.ThreadID != spec.ThreadID {
			t.Fatalf("unexpected terminal fact: %v, exit %+v", fact, fact.Exit)
		}
	}
	if count != 1 {
		t.Fatalf("got %d process.exited facts, want exactly one", count)
	}
}

// newIntegrationDriver creates a local driver with a real supervisor and an active grant.
func newIntegrationDriver(t *testing.T) (*Direct, *factSink) {
	t.Helper()
	sink := &factSink{}
	s, err := supervisor.New(supervisor.Config{
		DataDir:   t.TempDir(),
		Grace:     100 * time.Millisecond,
		Threshold: func(uint64) uint64 { return 0 },
	}, sink)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	driver, err := NewDirect(s, grantFunc(func(context.Context, string, string, string) (bool, error) { return true, nil }))
	if err != nil {
		t.Fatal(err)
	}
	return driver, sink
}

// waitForExit bounds the wait for a supervised test process.
func waitForExit(t *testing.T, proc Proc) ExitInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	info, err := proc.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

// --- Child executable and synchronization ---

// helperSpec launches the test binary in child mode using the real driver entry point.
func helperSpec(t *testing.T, mode string, args ...string) ProcSpec {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return ProcSpec{
		ID: "01ARZ3NDEKTSV4RRFFQ69G5FAV", ThreadID: "01ARZ3NDEKTSV4RRFFQ69G5FAW",
		Path: path, Args: append([]string{"-test.run=^TestExecutionChild$", "--", mode}, args...),
	}
}

// TestExecutionChild provides the real process fixtures for AC4 and AC6.
func TestExecutionChild(t *testing.T) {
	if len(os.Args) < 4 || os.Args[2] != "--" {
		return
	}
	switch os.Args[3] {
	case "protocol":
		if _, err := fmt.Fprintln(os.Stderr, "diagnostic"); err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprintln(os.Stdout, "{\"method\":\"fixture/ready\"}"); err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(os.Stdout, os.Stdin); err != nil {
			t.Fatal(err)
		}
	case "idle":
		// Bound test fixtures even if a broken cleanup implementation leaves one running.
		time.Sleep(30 * time.Second)
	case "tree":
		grandchild := helperSpec(t, "idle")
		child := exec.Command(grandchild.Path, grandchild.Args...)
		if err := child.Start(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(os.Args[4], fmt.Appendf(nil, "%d %d", os.Getpid(), child.Process.Pid), 0600); err != nil {
			t.Fatal(err)
		}
		openGate(t, os.Args[4]+".started")
		if err := child.Wait(); err != nil {
			t.Fatal(err)
		}
	case "output":
		gate := os.Args[4]
		waitForGate(t, gate+".before")
		fmt.Println("before disconnect")
		waitForGate(t, gate+".after")
		fmt.Println("after disconnect")
	}
	// Bypass the test runner's own PASS line on application stdout.
	os.Exit(0)
}

// openGate releases a child waiting for the named file.
func openGate(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
}

// waitForGate waits for a file handshake from another test process.
func waitForGate(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}
