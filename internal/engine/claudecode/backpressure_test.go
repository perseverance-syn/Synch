package claudecode_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"synch/internal/engine"
	"synch/internal/engine/claudecode/stdio/fixturetest"
)

const burstEvents = 1024

// TestStreamBackpressure proves O1 a two-second pause preserves a burst and the same live session.
func TestStreamBackpressure(t *testing.T) {
	ctx := fixturetest.Context(t)
	s, events, replay, proc, sink := startBurst(t, ctx)
	fixturetest.WaitForBackpressure(t, ctx, events, proc, sink)
	select {
	case <-proc.Cancelled:
		t.Fatal("slow consumer caused process cancellation")
	case <-time.After(2 * time.Second):
	}
	fixturetest.WaitForBackpressure(t, ctx, events, proc, sink)
	for i := range burstEvents {
		select {
		case event := <-events:
			if event.Channel != "message" || event.Chunk != "burst-"+strconv.Itoa(i) {
				t.Fatalf("burst event %d: %+v", i, event)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	replay.Checkpoint(t, ctx, "burst-complete")
	if got := len(sink.OfType("engine.output")); got != burstEvents {
		t.Fatalf("persisted burst = %d, want %d", got, burstEvents)
	}
	if err := s.Send(ctx, engine.Message{Text: "follow-up"}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		if event.Channel != "message" || event.Chunk != "still alive" {
			t.Fatalf("follow-up event: %+v", event)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	replay.Finish(t, ctx)
	fixturetest.CheckExit(t, ctx, s, "")
}

// TestStopWithStalledConsumer proves O1 Stop returns while publication is blocked and retains persisted output.
func TestStopWithStalledConsumer(t *testing.T) {
	ctx := fixturetest.Context(t)
	s, events, replay, proc, sink := startBurst(t, ctx)
	queued := fixturetest.WaitForBackpressure(t, ctx, events, proc, sink)
	stop, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := s.Stop(stop); err != nil {
		t.Fatal(err)
	}
	replay.FinishInterrupted(t, ctx)
	if persisted := len(sink.OfType("engine.output")); persisted != queued+1 {
		t.Fatalf("persisted = %d, want %d including the blocked chunk", persisted, queued+1)
	}
	fixturetest.CheckEvents(t, ctx, s, sink, repeatChannel("message", queued+1))
	fixturetest.CheckExit(t, ctx, s, "")
}

// startBurst opens the real adapter using fixture pipes and returns its idle event subscription.
func startBurst(t *testing.T, ctx context.Context) (engine.Session, <-chan engine.EngineEvent, *fixturetest.Replay, *fixturetest.Proc, *fixturetest.Sink) {
	t.Helper()
	replay, proc := fixturetest.NewReplay(t, "testdata/saturation.jsonl")
	sink := &fixturetest.Sink{}
	a, _ := claudeAdapter(t, proc, sink, nil, "version-in.txt", true)
	s, err := a.Start(ctx, fixturetest.StartOnly())
	if err != nil {
		t.Fatal(err)
	}
	events, err := s.Events()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Stop(ctx) })
	return s, events, replay, proc, sink
}

// repeatChannel builds the expected event channels for a uniform burst.
func repeatChannel(channel string, count int) []string {
	result := make([]string, count)
	for i := range result {
		result[i] = channel
	}
	return result
}
