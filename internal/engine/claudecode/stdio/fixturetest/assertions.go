package fixturetest

import (
	"context"
	"strings"
	"testing"
	"time"

	"synch/internal/engine"
)

// --- Setup: build adapters over recorded versions, launch specs and transcripts ---

// Context bounds protocol and exit waits so a deadlock fails the fixture.
func Context(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// Assignment provides stable and changing instructions plus one digest-pinned skill.
func Assignment() engine.StartRequest {
	req := StartOnly()
	req.Instructions = []engine.Instruction{{Text: "changing"}, {Text: "stable", Stable: true}}
	req.Skills = []engine.PinnedSkill{{ID: "example", Text: "hello",
		Digest: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"}}
	return req
}

// StartOnly provides the minimum fixture assignment and thread scope.
func StartOnly() engine.StartRequest {
	return engine.StartRequest{AssignmentID: "assignment-1", ThreadID: "thread-1", Workdir: ".",
		Instructions: []engine.Instruction{{Text: "start"}}}
}

// Qualification enables only fixture operation paths without changing the shipped declaration.
func Qualification(caps engine.CapabilitySet, qualified bool) *engine.CapabilitySet {
	if !qualified {
		return nil
	}
	for op := range caps.Ops {
		caps.Ops[op] = engine.Capability{Status: engine.Qualified, Evidence: "fixture-only"}
	}
	return &caps
}

// --- Assertions: check publication order and usage against journal facts ---

// WaitForBackpressure verifies publication is blocked on the first persisted chunk beyond the queue.
func WaitForBackpressure(t *testing.T, ctx context.Context, events <-chan engine.EngineEvent, proc *Proc, sink *Sink) int {
	t.Helper()
	const queueSize = 128
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-proc.Cancelled:
			t.Fatal("slow consumer caused process cancellation")
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-tick.C:
			if len(events) == queueSize && len(sink.OfType("engine.output")) == queueSize+1 {
				return queueSize
			}
		}
	}
}

// CheckEvents checks sequence and observes matching sink state at the instant each event is received.
func CheckEvents(t *testing.T, ctx context.Context, s engine.Session, sink *Sink, channels []string) {
	t.Helper()
	events, err := s.Events()
	if err != nil {
		t.Fatal(err)
	}
	for i, channel := range channels {
		select {
		case got, ok := <-events:
			facts := sink.OfType("engine.output")
			if !ok || got.Channel != channel || len(facts) <= i {
				t.Fatalf("event %d = %+v with %d persisted facts, want %q", i, got, len(facts), channel)
			}
			fact, valid := facts[i].Payload.(engine.Output)
			if !valid || fact.Channel != channel || fact.Chunk != got.Chunk || facts[i].ThreadID != "thread-1" {
				t.Fatalf("event %d does not match its persisted fact", i)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

// CheckUsageFact verifies the returned usage has a corresponding catalogue report.
func CheckUsageFact(t *testing.T, sink *Sink, reported bool) {
	t.Helper()
	facts := sink.OfType("engine.usage")
	if len(facts) != 1 {
		t.Fatalf("expected one persisted usage fact, got %d", len(facts))
	}
	usage, ok := facts[0].Payload.(engine.UsageReport)
	if !ok || usage.Reported != reported || (usage.InputTokens != nil) != reported || (usage.OutputTokens != nil) != reported {
		t.Fatalf("unexpected persisted usage: %+v", facts[0])
	}
}

// CheckExit consumes the terminal event and verifies the stream closes immediately afterwards.
func CheckExit(t *testing.T, ctx context.Context, s engine.Session, wantError string) {
	t.Helper()
	events, err := s.Events()
	if err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case event, ok := <-events:
			if !ok {
				t.Fatal("stream closed without a terminal event")
			}
			if event.Channel != "exit" || (event.Err == nil) != (wantError == "") ||
				(event.Err != nil && !strings.Contains(event.Err.Error(), wantError)) {
				t.Fatalf("unexpected exit event: %+v", event)
			}
			select {
			case event, open := <-events:
				if open {
					t.Fatalf("event after exit: %+v", event)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			return
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
