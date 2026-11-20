package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"synch/internal/engine"
	"synch/internal/eventlog"
)

// TestNewFailsClosedOnPolicyRecovery proves AC1 stops before a listener can start.
func TestNewFailsClosedOnPolicyRecovery(t *testing.T) {
	cfg, _ := gateConfig(t)
	store, err := eventlog.Open(filepath.Join(cfg.DataDir, "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	id := eventlog.NewID()
	_, writeErr := store.Append(t.Context(), []eventlog.NewEvent{{
		ID: eventlog.NewID(), Stream: "thread:" + id, ThreadID: id, Type: "grant.issued", TypeVersion: 3,
		Actor: eventlog.Actor{Kind: "operator", ID: "operator:gate"}, Payload: json.RawMessage(`{}`),
	}})
	if err := errors.Join(writeErr, store.Close()); err != nil {
		t.Fatal(err)
	}
	d, err := New(cfg)
	if d != nil || err == nil || !strings.Contains(err.Error(), "recover policy engine") {
		t.Fatalf("opened corrupt policy history: daemon=%v err=%v", d != nil, err)
	}
}

// TestDefaultCodexCapabilityGate proves AC2 never launches the shipped, unqualified Codex.
func TestDefaultCodexCapabilityGate(t *testing.T) {
	cfg, _ := gateConfig(t)
	cfg.Adapters = nil
	cfg.CodexBinary = filepath.Join(cfg.DataDir, "must-not-be-executed")
	d, base := serveGate(t, cfg)
	id := childOf(t, d, gateThread(t, base, "unqualified"))
	err := d.StartAssignment(t.Context(), engine.StartRequest{ThreadID: id, Workdir: t.TempDir()})
	if !errors.Is(err, ErrNoEligiblePair) {
		t.Fatalf("unqualified start returned %v", err)
	}
}

// unqualifiedAdapter declares an untested injected adapter for the routing gate.
type unqualifiedAdapter struct{ *fakeAdapter }

// Capabilities leaves every operation untested so routing must refuse the adapter.
func (a unqualifiedAdapter) Capabilities(context.Context) (engine.CapabilitySet, error) {
	caps := fakeCaps()
	for op := range caps.Ops {
		caps.Ops[op] = engine.Capability{Status: engine.Untested}
	}
	return caps, nil
}

// TestInjectedUnqualifiedAdapterPauses proves injection does not bypass the capability gate.
func TestInjectedUnqualifiedAdapterPauses(t *testing.T) {
	cfg, _ := gateConfig(t)
	starts := make(chan *fakeSession, 1)
	cfg.Adapters = []func(engine.Sink) (engine.EngineAdapter, error){func(sink engine.Sink) (engine.EngineAdapter, error) {
		return unqualifiedAdapter{&fakeAdapter{sink: sink, next: starts}}, nil
	}}
	d, base := serveGate(t, cfg)
	id := childOf(t, d, gateThread(t, base, "injected-unqualified"))
	err := d.StartAssignment(t.Context(), engine.StartRequest{ThreadID: id, Workdir: t.TempDir()})
	if !errors.Is(err, ErrNoEligiblePair) {
		t.Fatalf("unqualified start returned %v", err)
	}
	if len(starts) != 0 {
		t.Fatal("unqualified adapter launched")
	}
}
