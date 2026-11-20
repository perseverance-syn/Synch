package remote

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"synch/internal/eventlog"
)

// TestReplayRejectsRegressedRunnerHead proves O1 detects a restarted journal below the daemon cursor.
func TestReplayRejectsRegressedRunnerHead(t *testing.T) {
	f := openFixture(t)
	ctx := testContext(t)
	id := eventlog.NewID()
	if err := reserve(ctx, f.runner.db, f.runner.id, id, testThread); err != nil {
		t.Fatal(err)
	}
	if err := reserve(ctx, f.driver.ledger, f.runner.id, id, testThread); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		recordUsage(t, f.runner, id)
	}
	if err := f.driver.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	f.driver.Disconnect()
	if _, err := f.runner.db.Exec("DELETE FROM runner_journal WHERE runner=?", f.runner.id); err != nil {
		t.Fatal(err)
	}
	if record := recordUsage(t, f.runner, id); record.SourceSeq != 1 {
		t.Fatalf("restarted journal sequence: %d", record.SourceSeq)
	}
	if err := f.driver.Reconnect(ctx); !errors.Is(err, ErrRunnerHeadRegressed) {
		t.Fatalf("want journal regression, got %v", err)
	}
	if status := f.driver.Status(); status.Connected || !status.Degraded || !errors.Is(status.Error, ErrRunnerHeadRegressed) {
		t.Fatalf("want degraded runner, got %+v", status)
	}
	if cursor, err := f.driver.journalCursor(ctx); err != nil || cursor != 2 {
		t.Fatalf("durable cursor = %d, %v", cursor, err)
	}
}

// TestReplayPagesFitResponseLimit proves O3 still replays a backlog larger than one response.
func TestReplayPagesFitResponseLimit(t *testing.T) {
	f := openFixture(t)
	ctx := testContext(t)
	id := eventlog.NewID()
	if err := reserve(ctx, f.runner.db, f.runner.id, id, testThread); err != nil {
		t.Fatal(err)
	}
	if err := reserve(ctx, f.driver.ledger, f.runner.id, id, testThread); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]string{"assignment_id": id, "channel": "stdout", "chunk": strings.Repeat("<", 65536)})
	if err != nil {
		t.Fatal(err)
	}
	for range 4 {
		if _, err := f.runner.Record(testThread, "engine.output", 1, 0, payload); err != nil {
			t.Fatal(err)
		}
	}
	page, head, err := readReplayPage(f.runner.db, f.runner.id, 0)
	if err != nil || len(page) != 2 || head != 4 {
		t.Fatalf("first replay page = %d of %d, %v", len(page), head, err)
	}
	if err := f.driver.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if cursor, err := f.driver.journalCursor(ctx); err != nil || cursor != head {
		t.Fatalf("durable cursor = %d, %v", cursor, err)
	}
}
