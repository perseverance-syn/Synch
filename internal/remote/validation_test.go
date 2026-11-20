package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"synch/internal/eventlog"
	"synch/internal/supervisor"
)

const testThread = "01ARZ3NDEKTSV4RRFFQ69G5FAV"

// TestJournalValidation proves S3 accepts the six catalogue payloads and refuses invalid records before acknowledgement.
func TestJournalValidation(t *testing.T) {
	runner, err := OpenRunner("journal", supervisor.Config{DataDir: t.TempDir()}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runner.Close() }()
	if err := reserve(context.Background(), runner.db, runner.id, eventlog.NewID(), testThread); err != nil {
		t.Fatal(err)
	}
	assignment := `"assignment_id":"` + testThread + `"`
	tests := []struct {
		typ, payload, required, badKey, badValue string
		version                                  int
	}{
		{"engine.output", `{` + assignment + `,"channel":"stdout","chunk":""}`, "chunk", "channel", `"invalid"`, 1},
		{"engine.usage", `{` + assignment + `,"reported":true,"input_tokens":0}`, "reported", "type", `"invalid"`, 1},
		{"engine.capability_unavailable", `{` + assignment + `,"harness":"","operation":"resume"}`, "operation", "type", `"invalid"`, 1},
		{"process.exited", `{` + assignment + `,"execution_outcome":"exited","work":"complete"}`, "work", "execution_outcome", `"invalid"`, 1},
		{"check.result", `{"check":"lint","status":"passed","revision":"head"}`, "revision", "status", `"invalid"`, 1},
		{"policy.decided", `{"action":"remote.spawn","machine":"host","decision":"allow","outcome":"executed"}`, "machine", "decision", `"invalid"`, 2},
	}
	for i, tc := range tests {
		t.Run(tc.typ, func(t *testing.T) {
			typ, payload, version := tc.typ, tc.payload, tc.version
			check := func(name, eventType, mutation string) {
				t.Helper()
				if record, err := runner.Record(testThread, eventType, version, 0, json.RawMessage(mutation)); err == nil || record.SourceSeq != 0 {
					t.Errorf("%s acknowledged invalid record at %d: %v", name, record.SourceSeq, err)
				}
			}
			check("missing required", typ, mutatePayload(t, payload, tc.required, ""))
			check("unknown field", typ, mutatePayload(t, payload, "unexpected", "true"))
			if tc.badKey == "type" {
				// These two payloads have no enum member. Mutate the runner envelope's event type enum.
				check("bad event enum", "engine.unknown", payload)
			} else {
				check("bad payload enum", typ, mutatePayload(t, payload, tc.badKey, tc.badValue))
			}
			check("explicit null", typ, mutatePayload(t, payload, tc.required, "null"))
			record, err := runner.Record(testThread, typ, version, 0, json.RawMessage(payload))
			if err != nil || record.SourceSeq != int64(i+1) {
				t.Fatalf("baseline at sequence %d: %+v, %v", i+1, record, err)
			}
		})
	}
	for _, invalid := range []struct{ typ, payload string }{
		{"engine.output", `{` + assignment + `,"channel":"stdout","chunk":"","truncated":true}`},
		{"engine.output", `{` + assignment + `,"channel":"stdout","chunk":"","full_output":{"uri":"file://evidence","digest":"invalid"}}`},
		{"engine.usage", `{` + assignment + `,"reported":false,"input_tokens":0}`},
		{"engine.usage", `{` + assignment + `,"reported":true,"elapsed_ms":-1}`},
		{"check.result", `{"check":"lint","status":"failed","revision":"head"}`},
		{"check.result", `{"check":"lint","status":"passed","revision":"head","tool_versions":{"lint":null}}`},
	} {
		if record, err := runner.Record(testThread, invalid.typ, 1, 0, json.RawMessage(invalid.payload)); err == nil || record.SourceSeq != 0 {
			t.Errorf("conditional rule or format was accepted for %s", invalid.typ)
		}
	}
	records, err := readRecords(runner.db, runner.id, 0)
	if err != nil || len(records) != len(tests) {
		t.Fatalf("journal size %d, want %d: %v", len(records), len(tests), err)
	}
}

// TestRunnerRecordEnvelope proves S3 treats the runner shape as a closed, versioned envelope.
func TestRunnerRecordEnvelope(t *testing.T) {
	valid := fmt.Sprintf(`{"runner_event_version":2,"runner":"a-b","source_seq":1,"id":%q,"type":"engine.usage","type_version":1,"occurred_at":"2026-11-19T12:00:00Z","thread_id":%q,"payload":{"assignment_id":%q,"reported":false}}`, testThread, testThread, testThread)
	if _, err := decodeRecord([]byte(valid)); err != nil {
		t.Fatal(err)
	}
	for key, bad := range map[string]string{
		"global_seq": "1", "runner_event_version": "1", "runner": `"bad/slash"`, "source_seq": "0", "id": `"bad"`, "type_version": "2",
		"occurred_at": `"2026-11-19"`, "thread_id": `"bad"`, "redactions": "null", "payload": "null",
	} {
		if _, err := decodeRecord([]byte(mutatePayload(t, valid, key, bad))); err == nil {
			t.Errorf("accepted invalid %s", key)
		}
	}
}

// mutatePayload changes or removes one exact member while preserving the other JSON fields.
func mutatePayload(t *testing.T, raw, key, value string) string {
	t.Helper()
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		t.Fatal(err)
	}
	if value == "" {
		delete(obj, key)
	} else {
		obj[key] = json.RawMessage(value)
	}
	data, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
