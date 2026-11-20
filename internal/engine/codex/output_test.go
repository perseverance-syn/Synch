package codex

import (
	"strings"
	"testing"
	"unicode/utf8"

	"synch/internal/engine"
)

// TestOutputChunkLimit verifies AC5 applies Q-39 at UTF-8 boundaries without losing bytes.
func TestOutputChunkLimit(t *testing.T) {
	sink := &recordingSink{}
	s := &session{adapter: qualifiedAdapter(t, &fixtureLauncher{}, sink),
		assignmentID: "assignment", threadID: "thread", events: make(chan engine.EngineEvent, eventBuffer+1)}
	text := strings.Repeat("界", engine.OutputCap)
	if err := s.output("message", text); err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	close(s.events)
	for event := range s.events {
		if len(event.Chunk) > engine.OutputCap || !utf8.ValidString(event.Chunk) ||
			!sink.contains(engine.Output{AssignmentID: "assignment", Channel: "message", Chunk: event.Chunk}) {
			t.Fatal("invalid or unpersisted output chunk")
		}
		got.WriteString(event.Chunk)
	}
	if got.String() != text {
		t.Fatal("chunking changed the output")
	}
}
