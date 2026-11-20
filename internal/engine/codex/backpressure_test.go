package codex

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"synch/internal/engine"
)

// TestSlowConsumerBlocksDelivery proves E1: a consumer that pauses 5 s while
// 1000 deltas arrive delays delivery through backpressure instead of failing
// the session, loses nothing, and leaves the session alive.
func TestSlowConsumerBlocksDelivery(t *testing.T) {
	sink := &recordingSink{}
	// A hand-built session drives the real notification and output path with
	// its full event buffer, without a transport whose reads would race.
	s := &session{adapter: qualifiedAdapter(t, &fixtureLauncher{}, sink),
		assignmentID: "assignment", threadID: "thread",
		events: make(chan engine.EngineEvent, eventBuffer+1), done: make(chan struct{}),
		writes: make(chan fields), lane: make(chan struct{}, 1), items: map[string]itemState{}}
	s.mu.Lock()
	s.ref, s.turnID = "thr_slow", "turn_slow"
	s.mu.Unlock()

	const deltas = 1000
	produced := make(chan error, 1)
	go func() {
		for i := 0; i < deltas; i++ {
			delta := fmt.Sprintf("delta %d", i)
			raw, err := json.Marshal(notification{ThreadID: "thr_slow", TurnID: "turn_slow", ItemID: "item", Delta: &delta})
			if err != nil {
				produced <- err
				return
			}
			if err := s.notification("item/agentMessage/delta", raw); err != nil {
				produced <- err
				return
			}
		}
		produced <- nil
	}()

	// The consumer sleeps far longer than the removed fixed lag while the
	// producer fills the buffer and then blocks on delivery.
	time.Sleep(5 * time.Second)
	for i := 0; i < deltas; i++ {
		event := <-s.events
		if want := fmt.Sprintf("delta %d", i); event.Channel != "message" || event.Chunk != want {
			t.Fatalf("event %d = %+v, want %q", i, event, want)
		}
	}
	if err := <-produced; err != nil {
		t.Fatalf("producer failed during backpressure: %v", err)
	}
	if got := sink.count(); got != deltas {
		t.Fatalf("sink persisted %d outputs, want %d", got, deltas)
	}
	s.mu.Lock()
	failure := s.failure
	s.mu.Unlock()
	if failure != nil {
		t.Fatalf("session recorded a failure: %v", failure)
	}
	// The session still delivers after the slow-consumer episode.
	if err := s.output("message", "after pause"); err != nil {
		t.Fatalf("output after the pause = %v", err)
	}
	if event := <-s.events; event.Chunk != "after pause" {
		t.Fatalf("post-pause event = %+v", event)
	}
}
