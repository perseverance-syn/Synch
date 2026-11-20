package remote

import (
	"errors"
	"testing"

	"synch/internal/eventlog"
)

// TestRunnerConnectionReplayRange proves catalogue §3 records only the committed inclusive replay range.
func TestRunnerConnectionReplayRange(t *testing.T) {
	f := openFixture(t)
	ctx := testContext(t)
	id := eventlog.NewID()
	if err := reserve(ctx, f.runner.db, f.runner.id, id, testThread); err != nil {
		t.Fatal(err)
	}
	if err := reserve(ctx, f.driver.ledger, f.runner.id, id, testThread); err != nil {
		t.Fatal(err)
	}
	recordUsage(t, f.runner, id)
	if err := f.driver.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	f.driver.Disconnect()
	for range 2 {
		recordUsage(t, f.runner, id)
	}
	if err := f.driver.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.driver.Replay(ctx); err != nil {
		t.Fatal(err)
	}
	f.driver.Disconnect()
	for range 3 {
		recordUsage(t, f.runner, id)
	}
	if _, err := f.runner.db.Exec("DELETE FROM runner_journal WHERE runner=? AND seq=5", f.runner.id); err != nil {
		t.Fatal(err)
	}
	if err := f.driver.Reconnect(ctx); !errors.Is(err, eventlog.ErrRunnerGap) {
		t.Fatalf("want replay gap, got %v", err)
	}
	events, err := f.log.ReadStream("system", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		state       string
		first, last int64
	}{
		{"connected", 1, 1},
		{"connected", 2, 3},
		{"disconnected", 4, 4},
	}
	if len(events) != len(want) {
		t.Fatalf("got %d audit events, want %d", len(events), len(want))
	}
	threadEvents, err := f.log.ReadStream("thread:"+testThread, 0, 10)
	if err != nil || len(threadEvents) != 4 {
		t.Fatalf("got %d replayed events, want 4: %v", len(threadEvents), err)
	}
	for i, event := range events {
		if event.Type != "runner.connection" || event.TypeVersion != 1 || event.Stream != "system" ||
			event.Source != nil || event.ThreadID != "" || event.Actor.Kind != "daemon" || event.Actor.ID == "" {
			t.Fatalf("unexpected audit envelope: %+v", event)
		}
		var payload struct {
			Runner string `json:"runner"`
			State  string `json:"state"`
			First  *int64 `json:"replayed_from_seq"`
			Last   *int64 `json:"replayed_to_seq"`
		}
		expected := want[i]
		if err := decodeStrict(event.Payload, &payload); err != nil || payload.Runner != f.runner.id ||
			payload.State != expected.state || payload.First == nil || *payload.First != expected.first ||
			payload.Last == nil || *payload.Last != expected.last {
			t.Fatalf("range %d: %s, %v", i, event.Payload, err)
		}
		if event.GlobalSeq <= threadEvents[expected.last-1].GlobalSeq {
			t.Fatalf("range %d was recorded before its final replayed event", i)
		}
	}
}
