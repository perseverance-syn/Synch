package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"synch/internal/api"
	"synch/internal/engine"
	"synch/internal/eventlog"
	"synch/internal/thread"
)

type fakeAdapter struct {
	sink engine.Sink
	next chan *fakeSession
}

// --- Process fixture: isolate the runtime and inject qualified fake sessions ---

type fakeSession struct {
	engine.Session
	sink   engine.Sink
	req    engine.StartRequest
	events chan engine.EngineEvent
}

// Capabilities qualifies only the start and stream operations used by these gates.
func (*fakeAdapter) Capabilities(context.Context) (engine.CapabilitySet, error) {
	return engine.CapabilitySet{Ops: map[string]engine.Capability{
		engine.StartOp: {Status: engine.Qualified}, engine.StreamOp: {Status: engine.Qualified},
	}}, nil
}

// Start captures the request and exposes a controllable stream.
func (f *fakeAdapter) Start(_ context.Context, req engine.StartRequest) (engine.Session, error) {
	s := &fakeSession{sink: f.sink, req: req, events: make(chan engine.EngineEvent, 16)}
	f.next <- s
	return s, nil
}

// Ref identifies the fake session.
func (f *fakeSession) Ref() string { return f.req.AssignmentID }

// Events exposes output in the same order it was persisted.
func (f *fakeSession) Events() (<-chan engine.EngineEvent, error) { return f.events, nil }

// Stop settles the in-memory session.
func (*fakeSession) Stop(context.Context) error { return nil }

// output persists and delivers one adapter output event.
func (f *fakeSession) output(t *testing.T, message string) {
	t.Helper()
	err := f.sink.Persist(engine.Fact{Type: "engine.output", ThreadID: f.req.ThreadID,
		Payload: engine.Output{AssignmentID: f.req.AssignmentID, Channel: "message", Chunk: message}})
	if err != nil {
		t.Fatal(err)
	}
	f.events <- engine.EngineEvent{Channel: "message", Chunk: message}
}

// --- HTTP fixture: drive commands and streams through the real listener ---

// gateConfig supplies an isolated token, listener and fake.
func gateConfig(t *testing.T) (Config, <-chan *fakeSession) {
	t.Helper()
	tokens := api.NewMemoryTokens()
	tokens.Set("gate-token", "operator:gate")
	starts := make(chan *fakeSession, 8)
	dir := t.TempDir()
	writeGateFile(t, dir, "policy.json", `{"policy_version":3,"source":"operator","rules":[
		{"id":"dispatch","effect":"allow","actions":["assign.dispatch"]}]}`)
	return Config{
		DataDir: dir, Listen: "127.0.0.1:0", Tokens: tokens, PolicyStore: filepath.Join(dir, "policy.json"),
		adapter: func(sink engine.Sink) (engine.EngineAdapter, error) {
			return &fakeAdapter{sink: sink, next: starts}, nil
		},
		freeSpace: func(string) (uint64, uint64, error) { return 80 << 30, 100 << 30, nil },
	}, starts
}

// serveGate starts the actual loopback listener and checks its shutdown result.
func serveGate(t *testing.T, cfg Config) (*Daemon, string) {
	t.Helper()
	d, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- d.Run(t.Context()) }()
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	var address string
	waitGate(t, func() bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.listenAddr != nil {
			address = "http://" + d.listenAddr.String()
		}
		return address != ""
	})
	return d, address
}

// waitGate bounds asynchronous assertions without racing session shutdown.
func waitGate(t *testing.T, ready func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal("daemon gate condition did not arrive")
		}
	}
}

// gateHTTP calls the daemon with the gate token and returns the wire response.
func gateHTTP(t *testing.T, method, url, id, body, cursor string) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer gate-token")
	if id != "" {
		req.Header.Set("Synch-Request-Id", id)
	}
	if cursor != "" {
		req.Header.Set("Last-Event-ID", cursor)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = res.Body.Close() })
	return res
}

// gateMutation checks an accepted thread command and decodes its receipt.
func gateMutation(t *testing.T, url, id, body string, status int) map[string]any {
	t.Helper()
	res := gateHTTP(t, "POST", url, id, body, "")
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil || res.StatusCode != status {
		t.Fatalf("mutation %s: %d %s (%v)", id, res.StatusCode, raw, err)
	}
	var record map[string]any
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	return record
}

// gateThread creates a draft through the authenticated API and queue.
func gateThread(t *testing.T, base, id string) string {
	t.Helper()
	return gateMutation(t, base+"/v1/threads", id, `{"project_id":"local","brief":"Gate scenario","audit":"off"}`, 201)["thread_id"].(string)
}

// readGateFrame parses one network SSE frame including its cursor.
func readGateFrame(t *testing.T, r *bufio.Scanner) (string, string, []byte) {
	t.Helper()
	var kind, id string
	var body []byte
	for r.Scan() {
		line := r.Text()
		if line == "" {
			if kind != "" {
				return kind, id, body
			}
			continue
		}
		field, value, _ := strings.Cut(line, ": ")
		switch field {
		case "event":
			kind = value
		case "id":
			id = value
		case "data":
			body = []byte(value)
		}
	}
	t.Fatalf("SSE ended: %v", r.Err())
	return "", "", nil
}

// expectGateEvents verifies every cursor and event ID against persisted history.
func expectGateEvents(t *testing.T, in *bufio.Scanner, events []eventlog.Event) {
	t.Helper()
	for _, want := range events {
		kind, cursor, raw := readGateFrame(t, in)
		var got eventlog.Event
		if err := json.Unmarshal(raw, &got); err != nil || cursor != strconv.FormatInt(want.Seq, 10) || got.ID != want.ID || kind != want.Type {
			t.Fatalf("cursor %d mismatch: %s %s %s (%v)", want.Seq, kind, cursor, raw, err)
		}
	}
}

// gateHistory reads the durable stream that the SSE output must match.
func gateHistory(t *testing.T, d *Daemon, id string) []eventlog.Event {
	t.Helper()
	history, err := d.store.ReadStream("thread:"+id, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i, event := range history {
		if event.Seq != int64(i+1) {
			t.Fatalf("gap in event log at %d", i+1)
		}
	}
	return history
}

// --- Phase 1 scenarios: disconnect, recover, and continue a checkpoint ---

// TestGateDisconnectAndRestart proves AC1 and AC4 through an actual SSE connection.
func TestGateDisconnectAndRestart(t *testing.T) {
	cfg, starts := gateConfig(t)
	d, base := serveGate(t, cfg)
	id := gateThread(t, base, "disconnect")
	if err := d.StartAssignment(t.Context(), engine.StartRequest{ThreadID: id}); err != nil {
		t.Fatal(err)
	}
	session := <-starts
	session.output(t, "before disconnect")
	history := gateHistory(t, d, id)
	path := "/v1/streams/thread:" + id + "/events"
	open := func(server, cursor string) (*http.Response, *bufio.Scanner) {
		t.Helper()
		res := gateHTTP(t, "GET", server+path, "", "", cursor)
		in := bufio.NewScanner(res.Body)
		if kind, _, _ := readGateFrame(t, in); res.StatusCode != 200 || kind != "hello" {
			t.Fatalf("SSE handshake: %d %s", res.StatusCode, kind)
		}
		return res, in
	}
	first, in := open(base, "")
	expectGateEvents(t, in, history)
	if err := first.Body.Close(); err != nil {
		t.Fatal(err)
	}
	cursor := strconv.Itoa(len(history))
	session.output(t, "while disconnected")
	_, in = open(base, cursor)
	session.output(t, "while reconnected")
	continued := gateHistory(t, d, id)
	expectGateEvents(t, in, continued[len(history):])
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, base = serveGate(t, cfg)
	recovered := gateHistory(t, d, id)
	_, in = open(base, "")
	expectGateEvents(t, in, recovered)
	if len(recovered) <= len(continued) {
		t.Fatal("recovery did not append state evidence")
	}
	for i, event := range continued {
		if recovered[i].ID != event.ID {
			t.Fatal("restart rewrote existing history")
		}
	}
	state, err := d.threads.Status(t.Context(), id)
	if err != nil || state.Phase == "running" || !strings.Contains(string(recovered[len(continued)].Payload), `"transition":"T24"`) {
		t.Fatalf("recovered state %+v, %v, evidence %s", state, err, recovered[len(continued)].Payload)
	}
}

// finishGateReview records a passing check and accepts that revision through HTTP.
func finishGateReview(t *testing.T, d *Daemon, base, id, revision string) {
	t.Helper()
	if err := d.threads.Update(t.Context(), id, func(f *thread.Facts) error {
		f.Revision, f.EvaluatedRevision = revision, revision
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.record(id, "check.result", map[string]string{"check": "gate", "status": "passed", "revision": revision}, ""); err != nil {
		t.Fatal(err)
	}
	if err := d.record(id, "audit.status", map[string]string{"status": "pass", "revision": revision}, ""); err != nil {
		t.Fatal(err)
	}
	if err := d.threads.Apply(t.Context(), id, thread.Command{Transition: "T26"}); err != nil {
		t.Fatal(err)
	}
	gateMutation(t, base+"/v1/threads/"+id+"/commands", "accept-"+id, fmt.Sprintf(`{"command":"accept","revision":%q}`, revision), 202)
}

// TestGateRolloverContinues proves AC5 checkpoints and starts a new session.
func TestGateRolloverContinues(t *testing.T) {
	cfg, starts := gateConfig(t)
	d, base := serveGate(t, cfg)
	id := gateThread(t, base, "rollover")
	req := engine.StartRequest{ThreadID: id}
	if err := d.StartAssignment(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	first := <-starts
	first.output(t, "working")
	revision := strings.Repeat("a", 40)
	cp := thread.Checkpoint{ThreadID: id, Revision: revision, Handoff: []string{"resume.txt"}}
	if err := d.Rollover(t.Context(), cp, req); err != nil {
		t.Fatal(err)
	}
	second := <-starts
	if second.Ref() == first.Ref() || len(second.req.Instructions) == 0 || !strings.Contains(second.req.Instructions[0].Text, "resume.txt") {
		t.Fatalf("invalid continuation: %+v", second.req)
	}
	second.output(t, "continued")
	second.events <- engine.EngineEvent{Channel: "exit"}
	waitGate(t, func() bool {
		s, err := d.threads.Status(t.Context(), id)
		return err == nil && s.Phase == "verifying"
	})
	finishGateReview(t, d, base, id, revision)
	s, err := d.threads.Status(t.Context(), id)
	if err != nil || s.Phase != "completed" || s.Checkpoint.URI == "" {
		t.Fatalf("rollover completion: %+v, %v", s, err)
	}
}
