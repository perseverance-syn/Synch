package kimi_test

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"synch/internal/engine"
	"synch/internal/engine/claudecode/stdio/fixturetest"
	"synch/internal/engine/kimi"
)

// --- ACP lifecycle and parser regressions (A2) ---

// TestKimiACPTranscript proves A2 negotiating, opening, prompting, cancelling and continuing over ACP, and A3 persistence.
func TestKimiACPTranscript(t *testing.T) {
	ctx := fixturetest.Context(t)
	r, proc := fixturetest.NewReplay(t, "testdata/live.jsonl")
	sink := &fixturetest.Sink{}
	a, driver := kimiAdapter(t, proc, sink, true)
	req := fixturetest.Assignment()
	req.Model = "test-model"
	req.MCPEndpoint = "http://localhost:12345/mcp"
	s, err := a.Start(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if s.Ref() != "kimi-1" || len(driver.Specs) != 1 || !driver.Specs[0].ProtocolIO ||
		!slices.Equal(driver.Specs[0].Args, []string{"acp"}) {
		t.Fatal("Kimi did not open the expected supervised ACP session")
	}
	r.Checkpoint(t, ctx, "interrupt")
	if err := s.Interrupt(ctx); err != nil {
		t.Fatal(err)
	}
	r.Checkpoint(t, ctx, "next")
	if err := s.Send(ctx, engine.Message{Text: "follow-up"}); err != nil {
		t.Fatal(err)
	}
	r.Checkpoint(t, ctx, "complete")
	fixturetest.CheckEvents(t, ctx, s, sink, []string{"message", "tool_call", "tool_result", "message"})
	usage, err := s.Usage(ctx)
	if err != nil || usage != (engine.Usage{}) {
		t.Fatalf("Kimi Usage() = %+v, %v", usage, err)
	}
	fixturetest.CheckUsageFact(t, sink, false)
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	r.Finish(t, ctx)
	fixturetest.CheckExit(t, ctx, s, "")
}

// TestKimiMalformedFrame proves R3 malformed JSON shuts down the engine without publishing protocol data.
func TestKimiMalformedFrame(t *testing.T) {
	ctx := fixturetest.Context(t)
	r, proc := fixturetest.NewReplay(t, "testdata/malformed.jsonl")
	sink := &fixturetest.Sink{}
	a, _ := kimiAdapter(t, proc, sink, true)
	req := fixturetest.StartOnly()
	req.Instructions = nil
	s, err := a.Start(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	r.Advance(t, ctx)
	fixturetest.CheckExit(t, ctx, s, "unexpected end of JSON input")
	r.Finish(t, ctx)
	if len(sink.OfType("engine.output")) != 0 {
		t.Fatal("malformed protocol reached the event sink")
	}
}

// TestKimiOutOfOrderFrames proves R3 premature updates, foreign sessions and unmatched replies do not change live state.
func TestKimiOutOfOrderFrames(t *testing.T) {
	ctx := fixturetest.Context(t)
	r, proc := fixturetest.NewReplay(t, "testdata/out-of-order.jsonl")
	sink := &fixturetest.Sink{}
	a, _ := kimiAdapter(t, proc, sink, true)
	s, err := a.Start(ctx, fixturetest.StartOnly())
	if err != nil {
		t.Fatal(err)
	}
	r.Checkpoint(t, ctx, "unmatched-result")
	fixturetest.CheckEvents(t, ctx, s, sink, []string{"message"})
	if err := s.Send(ctx, engine.Message{Text: "too early"}); err == nil || !strings.Contains(err.Error(), "already active") {
		t.Fatalf("Send after unmatched reply = %v", err)
	}
	r.Advance(t, ctx)
	r.Checkpoint(t, ctx, "matched-result")
	if err := s.Send(ctx, engine.Message{Text: "follow-up"}); err != nil {
		t.Fatal(err)
	}
	events, err := s.Events()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-events:
		if got.Channel != "message" || got.Chunk != "follow-up response" {
			t.Fatalf("follow-up: %+v", got)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	r.Finish(t, ctx)
	fixturetest.CheckExit(t, ctx, s, "")
	facts := sink.OfType("engine.output")
	if len(facts) != 2 || facts[0].Payload.(engine.Output).Chunk != "current session" {
		t.Fatalf("unexpected journal output: %+v", facts)
	}
}

// --- Admission and bounded process teardown (A3) ---

// TestKimiStopWithStalledConsumer proves O1 the Kimi session can stop without draining its stream.
func TestKimiStopWithStalledConsumer(t *testing.T) {
	ctx := fixturetest.Context(t)
	r, proc := fixturetest.NewReplay(t, "testdata/saturation.jsonl")
	sink := &fixturetest.Sink{}
	a, _ := kimiAdapter(t, proc, sink, true)
	s, err := a.Start(ctx, fixturetest.StartOnly())
	if err != nil {
		t.Fatal(err)
	}
	events, err := s.Events()
	if err != nil {
		t.Fatal(err)
	}
	queued := fixturetest.WaitForBackpressure(t, ctx, events, proc, sink)
	stop, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := s.Stop(stop); err != nil {
		t.Fatal(err)
	}
	r.FinishInterrupted(t, ctx)
	if len(events) != queued+2 {
		t.Fatal("stopped stream lost queued output, the pending chunk, or the terminal event")
	}
}

// TestAdmissionGates proves default declarations prevent launch and brokered credentials are rejected.
func TestAdmissionGates(t *testing.T) {
	ctx := fixturetest.Context(t)
	sink := &fixturetest.Sink{}
	adapter, driver := kimiAdapter(t, nil, sink, false)
	qualified, qualifiedDriver := kimiAdapter(t, nil, sink, true)
	caps, err := adapter.Capabilities(ctx)
	if err != nil || len(caps.Ops) != 9 {
		t.Fatalf("Capabilities() = %+v, %v", caps, err)
	}
	for op, declaration := range caps.Ops {
		if declaration.Status != engine.Untested {
			t.Fatalf("operation %s unexpectedly has status %s", op, declaration.Status)
		}
	}
	if _, err := adapter.Start(ctx, fixturetest.Assignment()); !errors.Is(err, engine.ErrUnavailable) || len(driver.Specs) != 0 ||
		len(sink.OfType("engine.capability_unavailable")) != 1 {
		t.Fatalf("unqualified Start() = %v", err)
	}
	req := fixturetest.Assignment()
	req.Env = map[string]string{"aws_session_token": "fixture"}
	if _, err := qualified.Start(ctx, req); err == nil || !strings.Contains(err.Error(), "cannot be brokered") || len(qualifiedDriver.Specs) != 0 {
		t.Fatalf("admission with a brokered credential = %v", err)
	}
}

// kimiAdapter connects the real Kimi launcher to simulated Direct protocol streams.
func kimiAdapter(t *testing.T, proc *fixturetest.Proc, sink *fixturetest.Sink, qualified bool) (*kimi.Adapter, *fixturetest.Driver) {
	t.Helper()
	driver := &fixturetest.Driver{Proc: proc}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	launcher, err := kimi.NewDirectLauncher(driver, binary)
	if err != nil {
		t.Fatal(err)
	}
	a, err := kimi.New(launcher, sink, fixturetest.Qualification(kimi.Declaration(), qualified))
	if err != nil {
		t.Fatal(err)
	}
	return a, driver
}
