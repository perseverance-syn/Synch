package daemon

import (
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

// TestDefaultCodexCapabilityGate proves AC2 denies the shipped unqualified start.
func TestDefaultCodexCapabilityGate(t *testing.T) {
	cfg, _ := gateConfig(t)
	cfg.adapter = nil
	cfg.CodexBinary = filepath.Join(cfg.DataDir, "must-not-be-executed")
	d, base := serveGate(t, cfg)
	id := gateThread(t, base, "unqualified")
	err := d.StartAssignment(t.Context(), engine.StartRequest{ThreadID: id})
	if !errors.Is(err, engine.ErrUnavailable) {
		t.Fatalf("unqualified start returned %v", err)
	}
}
