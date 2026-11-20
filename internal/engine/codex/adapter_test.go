package codex

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"synch/internal/engine"
)

const testDigest = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"

// --- Session transcripts (AC3 through AC5) ---

// TestLiveTranscript verifies AC1/AC3/AC4/AC5 for start, live send, stream, interrupt, usage and stop.
func TestLiveTranscript(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	sink := &recordingSink{}
	launcher := replay(t, "live")
	adapter := qualifiedAdapter(t, launcher, sink)
	session, err := adapter.Start(ctx, fixtureRequest())
	if err != nil || session.Ref() != "thr_live" {
		t.Fatalf("Start() = %v, %v", session, err)
	}
	stream, err := session.Events()
	if err != nil {
		t.Fatal(err)
	}
	for _, channel := range []string{"message", "message", "tool_call", "tool_result"} {
		event := nextEvent(t, ctx, stream)
		if event.Channel != channel || !sink.contains(engine.Output{AssignmentID: "assignment", Channel: channel, Chunk: event.Chunk}) {
			t.Fatalf("output absent from sink or out of order: %+v", event)
		}
	}
	usage, err := session.Usage(ctx)
	if err != nil || usage != (engine.Usage{Reported: false}) {
		t.Fatalf("usage before notification = %+v, %v", usage, err)
	}
	if !sink.contains(engine.UsageReport{AssignmentID: "assignment"}) {
		t.Fatal("unreported usage was not persisted")
	}
	if err := session.Send(ctx, engine.Message{Text: "follow-up"}); err != nil {
		t.Fatal(err)
	}
	if err := session.Interrupt(ctx); err != nil {
		t.Fatal(err)
	}
	usage, err = session.Usage(ctx)
	if err != nil || usage != (engine.Usage{Reported: true, InputTokens: 12, OutputTokens: 3}) {
		t.Fatalf("usage after notification = %+v, %v", usage, err)
	}
	if !sink.contains(engine.UsageReport{AssignmentID: "assignment", Reported: true, InputTokens: &usage.InputTokens, OutputTokens: &usage.OutputTokens}) {
		t.Fatal("reported usage was not persisted")
	}
	if err := session.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	exit := nextEvent(t, ctx, stream)
	if exit.Channel != "exit" || exit.Err != nil || !launcher.transport.cancelled() {
		t.Fatalf("terminal event = %+v", exit)
	}
	if _, more := <-stream; more {
		t.Fatal("stream did not close after the exit")
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	var messages []string
	for _, fact := range sink.facts {
		if output, ok := fact.Payload.(engine.Output); ok && output.Channel == "message" {
			messages = append(messages, output.Chunk)
		}
	}
	if strings.Join(messages, "") != "hello world" {
		t.Fatalf("message output included stale or duplicate data: %q", messages)
	}
}

// TestResumeTranscript verifies AC3 native_resume restores only the referenced engine thread.
func TestResumeTranscript(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	adapter := qualifiedAdapter(t, replay(t, "resume"), &recordingSink{})
	session, err := adapter.Resume(ctx, "thr_saved", fixtureRequest())
	if err != nil || session.Ref() != "thr_saved" {
		t.Fatalf("Resume() = %v, %v", session, err)
	}
	if err := session.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestMalformedTranscript verifies AC3/AC5 surface bad notifications as typed terminal errors.
func TestMalformedTranscript(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	sink := &recordingSink{}
	launcher := replay(t, "malformed")
	adapter := qualifiedAdapter(t, launcher, sink)
	session, err := adapter.Start(ctx, fixtureRequest())
	if err != nil {
		t.Fatal(err)
	}
	stream, err := session.Events()
	if err != nil {
		t.Fatal(err)
	}
	exit := nextEvent(t, ctx, stream)
	if exit.Channel != "exit" || !launcher.transport.cancelled() {
		t.Fatalf("malformed stream was not terminated: %+v", exit)
	}
	requireHarnessError(t, exit.Err)
	if sink.count() != 0 {
		t.Fatal("malformed payload reached the sink")
	}
}

// TestPersistFailure verifies AC5 never publishes an output the sink refused.
func TestPersistFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	refused := errors.New("sink refused output")
	sink := &recordingSink{err: refused}
	launcher := replay(t, "live", true)
	adapter := qualifiedAdapter(t, launcher, sink)
	_, err := adapter.Start(ctx, fixtureRequest())
	if !errors.Is(err, refused) || !launcher.transport.cancelled() {
		t.Fatalf("failed persistence did not stop startup: %v", err)
	}
}

// --- Capability, version and credential boundaries (AC2, AC4, AC5, AC7) ---

// TestCapabilityGate verifies AC2 records each denial before touching the harness.
func TestCapabilityGate(t *testing.T) {
	launcher := &fixtureLauncher{}
	sink := &recordingSink{}
	adapter, err := New(launcher, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	req := fixtureRequest()
	decl, err := adapter.Capabilities(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for op, capability := range decl.Ops {
		if capability.Status != engine.Untested {
			t.Fatalf("shipped operation %s has status %s", op, capability.Status)
		}
	}
	if decl.SkillDelivery["injection"].Status != engine.Qualified || decl.SkillDelivery["native"].Status != engine.Unavailable {
		t.Fatalf("skill delivery = %+v", decl.SkillDelivery)
	}
	decl.Ops[engine.StartOp] = engine.Capability{Status: engine.Qualified}
	_, err = adapter.Start(t.Context(), req)
	assertDenied(t, sink, engine.StartOp, err)
	_, err = adapter.Resume(t.Context(), "thr_saved", req)
	assertDenied(t, sink, engine.ResumeOp, err)
	s := &session{adapter: adapter, assignmentID: req.AssignmentID, threadID: req.ThreadID}
	assertDenied(t, sink, engine.SendOp, s.Send(t.Context(), engine.Message{}))
	_, err = s.Events()
	assertDenied(t, sink, engine.StreamOp, err)
	assertDenied(t, sink, engine.InterruptOp, s.Interrupt(t.Context()))
	assertDenied(t, sink, engine.StopOp, s.Stop(t.Context()))
	_, err = s.Usage(t.Context())
	assertDenied(t, sink, engine.UsageOp, err)
	if launcher.probes != 0 || launcher.launches != 0 || sink.count() != 7 {
		t.Fatal("capability gate probed the harness or skipped an event")
	}
}

// TestVersionFixtures verifies AC7 rejects mismatches before launching App Server.
func TestVersionFixtures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		allowed bool
	}{{"version-in.txt", true}, {"version-out.txt", false}} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := os.ReadFile("testdata/" + tc.name)
			if err != nil {
				t.Fatal(err)
			}
			unexpectedLaunch := errors.New("fixture launch boundary")
			launcher := &fixtureLauncher{version: string(output), launchErr: unexpectedLaunch}
			adapter := qualifiedAdapter(t, launcher, &recordingSink{})
			_, err = adapter.Start(t.Context(), fixtureRequest())
			requireHarnessError(t, err)
			if errors.Is(err, unexpectedLaunch) != tc.allowed || (launcher.launches == 1) != tc.allowed || launcher.probes != 1 {
				t.Fatalf("unexpected version decision: %v, probes %d, launches %d", err, launcher.probes, launcher.launches)
			}
		})
	}
}

// TestCredentialsAndSkillDigest verifies AC4/AC5 reject invalid inputs before starting the harness.
func TestCredentialsAndSkillDigest(t *testing.T) {
	t.Setenv("AWS_SECRET_ACCESS_KEY", "inherited-fixture-value")
	env, err := processEnv(map[string]string{"EXAMPLE_REGION": "fixture"})
	if err != nil || strings.Contains(strings.Join(env, "\n"), "inherited-fixture-value") {
		t.Fatalf("unexpected inherited environment or error: %v", err)
	}
	launcher := &fixtureLauncher{}
	adapter := qualifiedAdapter(t, launcher, &recordingSink{})
	for _, key := range []string{"AWS_ACCESS_KEY_ID", "aws_session_token", "GOOGLE_APPLICATION_CREDENTIALS", "AZURE_CLIENT_SECRET", "OPENAI_API_KEY"} {
		req := fixtureRequest()
		req.Env = map[string]string{key: "fixture-value"}
		_, err := adapter.Start(t.Context(), req)
		if err == nil || !strings.Contains(err.Error(), key) || strings.Contains(err.Error(), "fixture-value") {
			t.Fatalf("launcher credential check for %s = %v", key, err)
		}
	}
	req := fixtureRequest()
	req.Skills[0].Digest = strings.Repeat("0", 64)
	if _, err := adapter.Start(t.Context(), req); err == nil {
		t.Fatal("start accepted altered skill content")
	}
	if launcher.probes != 0 || launcher.launches != 0 {
		t.Fatal("invalid input reached the launcher")
	}
}

// --- Fixture inputs and synchronous sink assertions ---

// fixtureRequest returns new input for each case so tests can change fields independently.
func fixtureRequest() engine.StartRequest {
	return engine.StartRequest{ThreadID: "thread", AssignmentID: "assignment", Workdir: "/workspace", Model: "example-model",
		Instructions: []engine.Instruction{{Text: "Changing assignment"}, {Stable: true, Text: "Stable brief"}},
		Skills:       []engine.PinnedSkill{{ID: "review", Digest: testDigest, Text: "hello"}}}
}

// qualifiedAdapter injects qualification only for fixture runs.
func qualifiedAdapter(t *testing.T, l Launcher, sink engine.Sink) *Adapter {
	t.Helper()
	caps := Declaration()
	for op := range caps.Ops {
		caps.Ops[op] = engine.Capability{Status: engine.Qualified, Evidence: "fixture"}
	}
	adapter, err := New(l, sink, &caps)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

// assertDenied verifies the operation error and the already-persisted denial.
func assertDenied(t *testing.T, sink *recordingSink, operation string, err error) {
	t.Helper()
	if !errors.Is(err, engine.ErrUnavailable) || !sink.contains(engine.CapabilityUnavailable{
		AssignmentID: "assignment", Harness: "codex", Operation: operation,
	}) {
		t.Fatalf("denial of %s = %v", operation, err)
	}
}

// requireHarnessError verifies the daemon can classify the returned error.
func requireHarnessError(t *testing.T, err error) {
	t.Helper()
	if _, ok := errors.AsType[*engine.HarnessError](err); !ok {
		t.Fatalf("expected typed harness error, got %v", err)
	}
}

// nextEvent reads one event with a deadline and fails on an early close.
func nextEvent(t *testing.T, ctx context.Context, stream <-chan engine.EngineEvent) engine.EngineEvent {
	t.Helper()
	select {
	case event, ok := <-stream:
		if !ok {
			t.Fatal("stream closed before next event")
		}
		return event
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return engine.EngineEvent{}
	}
}

type recordingSink struct {
	mu    sync.Mutex
	facts []engine.Fact
	err   error
}

// Persist records accepted facts synchronously, or simulates a write failure.
func (s *recordingSink) Persist(fact engine.Fact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.facts = append(s.facts, fact)
	return nil
}

// contains checks that a fact was already persisted.
func (s *recordingSink) contains(payload any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, fact := range s.facts {
		if fact.ThreadID == "thread" && reflect.DeepEqual(payload, fact.Payload) {
			return true
		}
	}
	return false
}

// count returns the number of persisted facts.
func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.facts)
}
