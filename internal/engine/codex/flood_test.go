package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"synch/internal/engine"
)

// --- Flood transcripts and fixture helpers ---

// eventsBeforeFlood is the number of events the live transcript yields
// before a flood inserted after afterStart (two deltas, a tool call and its
// result).
const eventsBeforeFlood = 4

// floodTranscript builds a transcript from testdata/live.transcript (1-based
// line numbers): the lines in head, then count agent-message deltas for item
// "flood" in turn_one, then the lines in tail.
func floodTranscript(t *testing.T, count int, head, tail []int) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/live.transcript")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n")
	var out []string
	for _, n := range head {
		out = append(out, lines[n-1])
	}
	for i := 0; i < count; i++ {
		out = append(out, fmt.Sprintf(`< {"method":"item/agentMessage/delta","params":{"threadId":"thr_live","turnId":"turn_one","itemId":"flood","delta":"flood %d"}}`, i))
	}
	for _, n := range tail {
		out = append(out, lines[n-1])
	}
	return strings.Join(out, "\n")
}

// span returns the 1-based line numbers from first to last inclusive.
func span(first, last int) []int {
	var out []int
	for n := first; n <= last; n++ {
		out = append(out, n)
	}
	return out
}

// afterStart is the live transcript up to and including the turn/start reply
// (line 19), with turn/completed (line 18) held back so a later flood still
// belongs to the active turn.
func afterStart() []int { return append(span(1, 17), 19) }

// unsubscribeAs4 renumbers the live transcript's unsubscribe exchange (id 6)
// to id 4, the next request id when Stop follows Start directly.
func unsubscribeAs4(transcript string) string { return strings.ReplaceAll(transcript, `"id":6`, `"id":4`) }

// replayFlood connects a synthetic JSONL peer for an in-memory transcript and
// signals every server write, so tests can tell how far the peer got.
func replayFlood(t *testing.T, transcript string, writes chan struct{}, allowEarlyClose bool) *fixtureLauncher {
	t.Helper()
	client, server := net.Pipe()
	transport := &fixtureTransport{Conn: client, server: server, exited: make(chan struct{})}
	finished := make(chan error, 1)
	go func() { finished <- exchangeFloodLines(server, transcript, writes) }()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		err := <-finished
		expectedClose := allowEarlyClose && errors.Is(err, net.ErrClosed)
		if err != nil && !expectedClose {
			t.Error(err)
		}
	})
	return &fixtureLauncher{version: "codex-cli 0.169.0", transport: transport}
}

// exchangeFloodLines mirrors exchangeTranscript for in-memory transcripts and
// signals the test after every server write completes.
func exchangeFloodLines(server net.Conn, transcript string, writes chan struct{}) error {
	reader := bufio.NewReader(server)
	for _, text := range strings.Split(strings.ReplaceAll(transcript, "\r\n", "\n"), "\n") {
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}
		if text[0] == '<' {
			if _, err := fmt.Fprintln(server, text[2:]); err != nil {
				return fmt.Errorf("write fixture line: %w", errors.Join(net.ErrClosed, err))
			}
			if writes != nil {
				writes <- struct{}{}
			}
			continue
		}
		actual, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("read fixture line: %w", errors.Join(net.ErrClosed, err))
		}
		var got, want any
		if err := json.Unmarshal([]byte(actual), &got); err != nil {
			return fmt.Errorf("decode client line: %w", err)
		}
		if err := json.Unmarshal([]byte(text[2:]), &want); err != nil {
			return fmt.Errorf("decode fixture line: %w", err)
		}
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("line: got %s want %s", actual, text[2:])
		}
	}
	return nil
}

// awaitGoroutines waits for the goroutine count to return to the baseline,
// failing when goroutines linger past the settle window (K4).
func awaitGoroutines(t *testing.T, baseline int) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if runtime.NumGoroutine() <= baseline {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("goroutines: %d > baseline %d", runtime.NumGoroutine(), baseline)
}

// awaitWrites drains n write signals with a timeout.
func awaitWrites(t *testing.T, writes chan struct{}, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-writes:
		case <-time.After(15 * time.Second):
			t.Fatalf("only %d of %d peer writes arrived", i, n)
		}
	}
}

// awaitFull waits until the session's stream holds eventBuffer events, the
// point at which the reader stops handing events over (backpressure).
func awaitFull(t *testing.T, s *session) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for len(s.events) < eventBuffer {
		if time.Now().After(deadline) {
			t.Fatalf("stream holds %d events, want %d", len(s.events), eventBuffer)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// drain reads every remaining event of a closing stream and returns them.
func drain(t *testing.T, s *session) []engine.EngineEvent {
	t.Helper()
	var out []engine.EngineEvent
	timeout := time.After(10 * time.Second)
	for {
		select {
		case ev, ok := <-s.events:
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-timeout:
			t.Fatal("stream did not close")
		}
	}
}

// floodPersisted checks that every flood delta reached the sink, whether or
// not it was handed to the consumer.
func floodPersisted(t *testing.T, sink *recordingSink, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		if !sink.contains(engine.Output{AssignmentID: "assignment", Channel: "message", Chunk: fmt.Sprintf("flood %d", i)}) {
			t.Fatalf("flood %d was not persisted", i)
		}
	}
}

// --- K1: shutdown independent of event consumption ---

// TestStopCompletesWithSaturatedStream proves K1 and K4: with the stream full
// and the reader held by backpressure, Stop completes (the unsubscribe reply
// is processed and the transport reaped) without the consumer reading; every
// flood delta is still persisted, the exit event is last, and no goroutine
// is left behind.
func TestStopCompletesWithSaturatedStream(t *testing.T) {
	baseline := runtime.NumGoroutine()
	const flood = 300
	sink := &recordingSink{}
	launcher := replayFlood(t, unsubscribeAs4(floodTranscript(t, flood, afterStart(), []int{18, 26, 27})), nil, false)
	handle, err := qualifiedAdapter(t, launcher, sink).Start(context.Background(), fixtureRequest())
	if err != nil {
		t.Fatalf("Start = %v", err)
	}
	s := handle.(*session)
	awaitFull(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("Stop with a saturated stream = %v", err)
	}
	if !launcher.transport.cancelled() {
		t.Fatal("transport was not reaped")
	}
	floodPersisted(t, sink, flood)
	events := drain(t, s)
	if len(events) > eventBuffer+1 || events[len(events)-1].Channel != "exit" {
		t.Fatalf("stream held %d events ending in %+v, want at most %d ending in exit", len(events), events[len(events)-1], eventBuffer+1)
	}
	awaitGoroutines(t, baseline)
}

// TestCancelledStopClosesSaturatedSession proves K1 and K4: when the peer
// never answers the unsubscribe, cancelling Stop closes the session (done
// closed, transport reaped) without the consumer reading.
func TestCancelledStopClosesSaturatedSession(t *testing.T) {
	baseline := runtime.NumGoroutine()
	launcher := replayFlood(t, unsubscribeAs4(floodTranscript(t, 300, afterStart(), []int{18, 26})), nil, true)
	handle, err := qualifiedAdapter(t, launcher, &recordingSink{}).Start(context.Background(), fixtureRequest())
	if err != nil {
		t.Fatalf("Start = %v", err)
	}
	s := handle.(*session)
	awaitFull(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() { stopped <- s.Stop(ctx) }()
	time.Sleep(200 * time.Millisecond) // let Stop send the unanswered unsubscribe
	cancel()
	select {
	case err := <-stopped:
		// Either result is acceptable here; what matters is that it returned.
		t.Logf("cancelled Stop = %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled Stop did not return")
	}
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		t.Fatal("session did not close after the cancelled Stop")
	}
	if !launcher.transport.cancelled() {
		t.Fatal("transport was not reaped after the cancelled Stop")
	}
	if events := drain(t, s); events[len(events)-1].Channel != "exit" {
		t.Fatal("exit event missing after the cancelled Stop")
	}
	awaitGoroutines(t, baseline)
}

// TestTransportExitClosesFullSession proves K1 for the reviewer's exit case:
// the reader reaches EOF with the stream holding eventBuffer undelivered
// events, and the session still closes and queues its exit event without the
// consumer reading, because outputs never take the slot kept for the exit.
func TestTransportExitClosesFullSession(t *testing.T) {
	baseline := runtime.NumGoroutine()
	launcher := replayFlood(t, floodTranscript(t, eventBuffer-eventsBeforeFlood, afterStart(), []int{18}), nil, true)
	handle, err := qualifiedAdapter(t, launcher, &recordingSink{}).Start(context.Background(), fixtureRequest())
	if err != nil {
		t.Fatalf("Start = %v", err)
	}
	s := handle.(*session)
	awaitFull(t, s)
	time.Sleep(100 * time.Millisecond) // the peer writes turn/completed and ends
	_ = launcher.transport.server.Close()
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		t.Fatal("transport exit did not close the session with a full stream")
	}
	events := drain(t, s)
	if len(events) != eventBuffer+1 || events[eventBuffer].Channel != "exit" || events[eventBuffer].Err == nil {
		t.Fatalf("got %d events, last %+v; want %d outputs then an exit with the EOF failure", len(events), events[len(events)-1], eventBuffer)
	}
	awaitGoroutines(t, baseline)
}

// --- K2: startup cancellation independent of delivery ---

// TestCancelStartReturnsBeforeHandle proves K2 and K4: the peer floods deltas
// before replying to turn/start, so the reader is held by backpressure and
// the caller has no handle to drain; cancelling Start returns the context
// error promptly, reaps the transport and leaves no goroutine behind.
func TestCancelStartReturnsBeforeHandle(t *testing.T) {
	baseline := runtime.NumGoroutine()
	writes := make(chan struct{}, 4096)
	launcher := replayFlood(t, floodTranscript(t, 300, span(1, 9), span(10, 19)), writes, true)
	adapter := qualifiedAdapter(t, launcher, &recordingSink{})
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan error, 1)
	go func() { _, err := adapter.Start(ctx, fixtureRequest()); started <- err }()

	// Five server lines precede the flood; once eventBuffer deltas have been
	// written the stream is full and the reader is held.
	awaitWrites(t, writes, 5+eventBuffer)
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-started:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Start = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after cancellation")
	}
	if !launcher.transport.cancelled() {
		t.Fatal("transport was not reaped after the cancellation")
	}
	awaitGoroutines(t, baseline)
}

// --- K3: concurrent Stop callers agree ---

// TestConcurrentStopConsistentResult proves K3: eight concurrent Stop callers
// on an idle resumed session all get the same successful result, for each of
// the three accepted unsubscribe statuses.
func TestConcurrentStopConsistentResult(t *testing.T) {
	for _, status := range []string{"unsubscribed", "notSubscribed", "notLoaded"} {
		t.Run(status, func(t *testing.T) {
			baseline := runtime.NumGoroutine()
			transcript := strings.Replace(mustResumeTranscript(t),
				`{"id":3,"result":{"status":"unsubscribed"}}`,
				`{"id":3,"result":{"status":"`+status+`"}}`, 1)
			launcher := replayFlood(t, transcript, nil, false)
			adapter := qualifiedAdapter(t, launcher, &recordingSink{})
			session, err := adapter.Resume(context.Background(), "thr_saved", fixtureRequest())
			if err != nil {
				t.Fatalf("Resume = %v", err)
			}
			const callers = 8
			results := make(chan error, callers)
			for i := 0; i < callers; i++ {
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					results <- session.Stop(ctx)
				}()
			}
			for i := 0; i < callers; i++ {
				if err := <-results; err != nil {
					t.Fatalf("concurrent Stop %d = %v, want the successful stop", i, err)
				}
			}
			awaitGoroutines(t, baseline)
		})
	}
}

// mustResumeTranscript returns the resume transcript's text.
func mustResumeTranscript(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/resume.transcript")
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
