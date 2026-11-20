package claudecode_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"synch/internal/engine"
	"synch/internal/engine/claudecode"
	"synch/internal/engine/claudecode/stdio/fixturetest"
)

// --- A1: Claude Code lifecycle, protocol recovery rules and policy ---

// TestClaudeCodeTranscript proves A1 permission handling, resume-independent turns and usage, and A3 ordered persistence.
func TestClaudeCodeTranscript(t *testing.T) {
	ctx := fixturetest.Context(t)
	r, proc := fixturetest.NewReplay(t, "testdata/live.jsonl")
	sink := &fixturetest.Sink{}
	policy := make(chan claudecode.PermissionRequest)
	a, driver := claudeAdapter(t, proc, sink, policy, "version-in.txt", true)
	req := fixturetest.Assignment()
	req.Model = "fixture-model"
	req.MCPEndpoint = "http://localhost:12345/mcp"
	req.Env = map[string]string{"FIXTURE_REGION": "local"}
	s, err := a.Start(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range []string{"Read", "Write", "AskUserQuestion"} {
		select {
		case p := <-policy:
			if p.ToolName != tool || p.ThreadID != req.ThreadID || p.AssignmentID != req.AssignmentID || p.SessionRef != "claude-1" {
				t.Fatalf("unexpected policy request: %+v", p)
			}
			if tool != "AskUserQuestion" {
				p.Reply(tool == "Read")
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	r.Checkpoint(t, ctx, "interrupt")
	if err := s.Interrupt(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Send(ctx, engine.Message{Text: "follow-up"}); err != nil {
		t.Fatal(err)
	}
	r.Checkpoint(t, ctx, "complete")
	fixturetest.CheckEvents(t, ctx, s, sink, []string{"message", "tool_call", "tool_result", "message"})
	usage, err := s.Usage(ctx)
	if err != nil || usage != (engine.Usage{Reported: true, InputTokens: 7, OutputTokens: 10}) {
		t.Fatalf("Usage() = %+v, %v", usage, err)
	}
	fixturetest.CheckUsageFact(t, sink, true)
	if err := s.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	r.Finish(t, ctx)
	fixturetest.CheckExit(t, ctx, s, "")
	if len(driver.Specs) != 2 || !driver.Specs[0].ProtocolIO || !driver.Specs[1].ProtocolIO ||
		driver.Specs[0].Path != driver.Specs[1].Path || driver.Specs[0].ID == driver.Specs[1].ID ||
		!slices.Contains(driver.Specs[1].Env, "FIXTURE_REGION=local") || slices.Contains(driver.Specs[0].Env, "FIXTURE_REGION=local") ||
		driver.Specs[1].ThreadID != req.ThreadID ||
		!slices.Equal(driver.Specs[1].Args, []string{"-p", "--input-format", "stream-json", "--output-format", "stream-json",
			"--verbose", "--permission-prompt-tool", "stdio", "--model=fixture-model",
			"--mcp-config", `{"mcpServers":{"synch":{"type":"http","url":"http://localhost:12345/mcp"}}}`}) {
		t.Fatal("Claude probe and session did not use the expected supervised protocol launches")
	}
}

// TestClaudeResumeTranscript proves A1 native resume and skill injection on the first follow-up without an extra turn.
func TestClaudeResumeTranscript(t *testing.T) {
	ctx := fixturetest.Context(t)
	r, proc := fixturetest.NewReplay(t, "testdata/resume.jsonl")
	sink := &fixturetest.Sink{}
	a, driver := claudeAdapter(t, proc, sink, nil, "version-in.txt", true)
	s, err := a.Resume(ctx, "resume-1", fixturetest.Assignment())
	if err != nil {
		t.Fatal(err)
	}
	if s.Ref() != "resume-1" || len(driver.Specs) != 2 || !slices.Contains(driver.Specs[1].Args, "--resume=resume-1") {
		t.Fatal("native resume did not bind the requested reference")
	}
	if err := s.Send(ctx, engine.Message{Text: "follow-up"}); err != nil {
		t.Fatal(err)
	}
	fixturetest.CheckEvents(t, ctx, s, sink, []string{"message"})
	fixturetest.CheckExit(t, ctx, s, "")
	r.Finish(t, ctx)
}

// TestClaudeMalformedTranscript proves A1 and A3 malformed stdout causes supervised cleanup and is never published.
func TestClaudeMalformedTranscript(t *testing.T) {
	ctx := fixturetest.Context(t)
	r, proc := fixturetest.NewReplay(t, "testdata/malformed.jsonl")
	sink := &fixturetest.Sink{}
	a, _ := claudeAdapter(t, proc, sink, nil, "version-in.txt", true)
	s, err := a.Start(ctx, fixturetest.StartOnly())
	if err != nil {
		t.Fatal(err)
	}
	r.Advance(t, ctx)
	fixturetest.CheckExit(t, ctx, s, "handle protocol frame")
	r.Finish(t, ctx)
	if len(sink.OfType("engine.output")) != 0 {
		t.Fatal("malformed protocol reached the event sink")
	}
	select {
	case <-proc.Cancelled:
	default:
		t.Fatal("malformed protocol did not request process cancellation")
	}
}

// TestClaudeVersionGate proves A1 rejects an unsupported installed version before starting the harness session.
func TestClaudeVersionGate(t *testing.T) {
	a, driver := claudeAdapter(t, nil, &fixturetest.Sink{}, nil, "version-out.txt", true)
	_, err := a.Start(fixturetest.Context(t), fixturetest.Assignment())
	var harnessErr *engine.HarnessError
	if !errors.As(err, &harnessErr) || len(driver.Specs) != 1 || !slices.Equal(driver.Specs[0].Args, []string{"--version"}) {
		t.Fatalf("version gate returned %v with %d invocations", err, len(driver.Specs))
	}
}

// TestStopDuringPermission proves A3 cleanup cancels a pending policy wait even with an unqualified Stop operation.
func TestStopDuringPermission(t *testing.T) {
	ctx := fixturetest.Context(t)
	r, proc := fixturetest.NewReplay(t, "testdata/pending-permission.jsonl")
	sink := &fixturetest.Sink{}
	policy := make(chan claudecode.PermissionRequest)
	_, driver := claudeAdapter(t, proc, sink, policy, "version-in.txt", true)
	caps := fixturetest.Qualification(claudecode.Declaration(), true)
	caps.Ops[engine.StopOp] = engine.Capability{Status: engine.Untested}
	a := claudeWithDriver(t, driver, sink, policy, caps)
	s, err := a.Start(ctx, fixturetest.StartOnly())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case pending := <-policy:
		if err := s.Stop(ctx); !errors.Is(err, engine.ErrUnavailable) {
			t.Fatalf("Stop() = %v, want capability denial after cleanup", err)
		}
		if pending.Context.Err() == nil {
			t.Fatal("shutdown left the permission request live")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	r.Finish(t, ctx)
	fixturetest.CheckExit(t, ctx, s, "")
}

// TestAdmissionGates proves default declarations prevent launch and brokered credentials are rejected.
func TestAdmissionGates(t *testing.T) {
	ctx := fixturetest.Context(t)
	sink := &fixturetest.Sink{}
	adapter, driver := claudeAdapter(t, nil, sink, nil, "version-in.txt", false)
	qualified, qualifiedDriver := claudeAdapter(t, nil, sink, nil, "version-in.txt", true)
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
