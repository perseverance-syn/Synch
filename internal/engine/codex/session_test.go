package codex

import (
	"errors"
	"testing"

	"synch/internal/engine"
)

// partiallyQualifiedAdapter qualifies every operation except the named one.
func partiallyQualifiedAdapter(t *testing.T, l Launcher, sink engine.Sink, missing string) *Adapter {
	t.Helper()
	caps := Declaration()
	for op := range caps.Ops {
		if op != missing {
			caps.Ops[op] = engine.Capability{Status: engine.Qualified, Evidence: "fixture"}
		}
	}
	adapter, err := New(l, sink, &caps)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

// TestStartRequiresRoutingCapabilities proves E2: Start refuses unless start,
// send, stream and stop are all qualified, recording the first denial and
// never reaching the harness.
func TestStartRequiresRoutingCapabilities(t *testing.T) {
	for _, missing := range []string{engine.StartOp, engine.SendOp, engine.StreamOp, engine.StopOp} {
		t.Run(missing, func(t *testing.T) {
			launcher := &fixtureLauncher{}
			sink := &recordingSink{}
			adapter := partiallyQualifiedAdapter(t, launcher, sink, missing)
			session, err := adapter.Start(t.Context(), fixtureRequest())
			if session != nil || !errors.Is(err, engine.ErrUnavailable) {
				t.Fatalf("start with unqualified %s = %+v, %v", missing, session, err)
			}
			if !sink.contains(engine.CapabilityUnavailable{AssignmentID: "assignment", Harness: "codex", Operation: missing}) {
				t.Fatalf("denial of %s was not recorded", missing)
			}
			if launcher.probes != 0 || launcher.launches != 0 {
				t.Fatal("an unroutable start reached the harness")
			}
		})
	}
}

// TestStopAlwaysCallableOnStartedSession proves E2: Stop is not
// capability-gated on a started session, so a caller can always stop it.
func TestStopAlwaysCallableOnStartedSession(t *testing.T) {
	ctx := t.Context()
	sink := &recordingSink{}
	launcher := replay(t, "resume")
	// The adapter's stop operation is unqualified, so a gated Stop would refuse.
	adapter := partiallyQualifiedAdapter(t, launcher, sink, engine.StopOp)
	session, err := adapter.Resume(ctx, "thr_saved", fixtureRequest())
	if err != nil || session.Ref() != "thr_saved" {
		t.Fatalf("Resume() = %v, %v", session, err)
	}
	if err := session.Stop(ctx); err != nil {
		t.Fatalf("stop of a started session = %v", err)
	}
	if sink.contains(engine.CapabilityUnavailable{AssignmentID: "assignment", Harness: "codex", Operation: engine.StopOp}) {
		t.Fatal("stop of a started session recorded a capability denial")
	}
	if !launcher.transport.cancelled() {
		t.Fatal("stop of a started session did not cancel the process")
	}
}

// TestCapabilityDenialWithoutIDs proves E4: a capability denial for a request
// with empty assignment or thread IDs returns a typed validation error and
// persists nothing invalid.
func TestCapabilityDenialWithoutIDs(t *testing.T) {
	launcher := &fixtureLauncher{}
	sink := &recordingSink{}
	// The shipped declaration qualifies nothing, so every open attempts a denial.
	adapter, err := New(launcher, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range []engine.StartRequest{
		{ThreadID: "thread"},
		{AssignmentID: "assignment"},
		{},
	} {
		session, err := adapter.Start(t.Context(), req)
		if _, ok := errors.AsType[*engine.InvalidRequestError](err); !ok ||
			!errors.Is(err, engine.ErrInvalidRequest) || sink.count() != 0 {
			t.Fatalf("start without IDs = %+v, %v, facts %d", session, err, sink.count())
		}
		_, err = adapter.Resume(t.Context(), "thr_saved", req)
		if _, ok := errors.AsType[*engine.InvalidRequestError](err); !ok ||
			!errors.Is(err, engine.ErrInvalidRequest) || sink.count() != 0 {
			t.Fatalf("resume without IDs = %v, facts %d", err, sink.count())
		}
	}
}
