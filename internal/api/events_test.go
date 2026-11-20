package api

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"synch/internal/eventlog"
	"testing"
	"time"
)

// appendEvents writes one batch to the same persistence API used by daemon producers.
func appendEvents(ctx context.Context, store *eventlog.Store, stream string, count int) error {
	events := make([]eventlog.NewEvent, count)
	for i := range events {
		events[i] = testEvent(stream)
	}
	_, err := store.Append(ctx, events)
	return err
}

// testEvent builds an envelope in the requested test stream.
func testEvent(stream string) eventlog.NewEvent {
	eventType, threadID := "test.recorded", strings.TrimPrefix(stream, "thread:")
	if stream == "system" {
		eventType, threadID = "resource.pressure", ""
	}
	return eventlog.NewEvent{ID: eventlog.NewID(), Stream: stream, ThreadID: threadID,
		Type: eventType, TypeVersion: 1, Actor: eventlog.Actor{Kind: "daemon", ID: "test"}, Payload: []byte(`{"value":"event"}`)}
}

// seed writes persisted events before a client connects.
func seed(t *testing.T, store *eventlog.Store, stream string, count int) {
	t.Helper()
	if err := appendEvents(t.Context(), store, stream, count); err != nil {
		t.Fatal(err)
	}
}

// --- Paging and health: expose bounded history and lineage before starting an SSE stream ---

type eventPage struct {
	logPosition
	Events []eventlog.Event `json:"events"`
}

// TestEventPages proves AC6 cursor bounds, pagination defaults, explicit limits, and stream-specific cursors.
func TestEventPages(t *testing.T) {
	s, store, _ := fixture(t)
	id := eventlog.NewID()
	seed(t, store, "thread:"+id, 2)
	seed(t, store, "system", 205)
	path := "/v1/streams/all/events/page"
	first := request(t, s, "GET", path, testToken, "", "")
	p := decode[eventPage](t, first.Body.Bytes())
	epoch, err := store.LogEpoch()
	if err != nil {
		t.Fatal(err)
	}
	if first.Code != 200 || p.Head != 207 || p.LogEpoch != epoch || len(p.Events) != 200 {
		t.Fatalf("page: %d head %+v count %d", first.Code, p.logPosition, len(p.Events))
	}
	for i, event := range p.Events {
		if event.GlobalSeq != int64(i+1) {
			t.Fatalf("global cursor %d at index %d", event.GlobalSeq, i)
		}
	}
	tail := request(t, s, "GET", path+"?after=200&limit=1000", testToken, "", "")
	p = decode[eventPage](t, tail.Body.Bytes())
	if tail.Code != 200 || len(p.Events) != 7 || p.Events[0].GlobalSeq != 201 || p.Events[6].GlobalSeq != 207 {
		t.Fatalf("tail: %d %s", tail.Code, tail.Body)
	}
	system := request(t, s, "GET", "/v1/streams/system/events/page?after=1&limit=1", testToken, "", "")
	p = decode[eventPage](t, system.Body.Bytes())
	if system.Code != 200 || p.Head != 205 || len(p.Events) != 1 || p.Events[0].Seq != 2 || p.Events[0].GlobalSeq != 4 {
		t.Fatalf("system: %d %s", system.Code, system.Body)
	}
	for _, query := range []string{"?after=-1", "?limit=0", "?limit=1001"} {
		got := request(t, s, "GET", path+query, testToken, "", "")
		if got.Code != 400 {
			t.Fatalf("%s: %d %s", query, got.Code, got.Body)
		}
	}
	threadPath := "/v1/streams/thread:" + id + "/events/page"
	got := request(t, s, "GET", threadPath+"?after=2", testToken, "", "")
	p = decode[eventPage](t, got.Body.Bytes())
	if got.Code != 200 || p.Head != 2 || p.Events == nil || len(p.Events) != 0 {
		t.Fatalf("at head: %d %s", got.Code, got.Body)
	}
	ahead := requireProblem(t, request(t, s, "GET", threadPath+"?after=3", testToken, "", ""), 409, "cursor_ahead", "")
	if ahead.Head == nil || *ahead.Head != 2 || ahead.LogEpoch != epoch {
		t.Fatalf("ahead: %+v", ahead)
	}
}

// TestHealth proves AC8 reports persisted head, epoch, and disk pressure.
func TestHealth(t *testing.T) {
	s, store, _ := fixture(t)
	seed(t, store, "system", 2)
	epoch, err := store.LogEpoch()
	if err != nil {
		t.Fatal(err)
	}
	s.config.DiskPressure = func() bool { return true }
	w := request(t, s, "GET", "/v1/health", testToken, "", "")
	body := decode[map[string]any](t, w.Body.Bytes())
	if w.Code != 200 || body["api_version"] != "3.0.0" || body["log_epoch"] != epoch || body["head_global_seq"] != float64(2) ||
		body["status"] != "degraded" || body["disk_pressure"] != true {
		t.Fatalf("health: %d %s", w.Code, w.Body)
	}
}

// --- SSE fixtures: read complete frames over a real HTTP connection ---

type frame struct {
	name, id, comment string
	data              []byte
}

// connect opens an authenticated SSE request with an optional Last-Event-ID.
func connect(t *testing.T, srv *httptest.Server, path, last string) (*http.Response, *bufio.Reader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	r, err := http.NewRequestWithContext(ctx, "GET", srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+testToken)
	r.Host = "127.0.0.1"
	if last != "" {
		r.Header.Set("Last-Event-ID", last)
	}
	response, err := srv.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })
	return response, bufio.NewReader(response.Body)
}

// nextFrame parses one complete SSE frame, preserving the presence of cursor fields.
func nextFrame(t *testing.T, reader *bufio.Reader) frame {
	t.Helper()
	var f frame
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		line = strings.TrimSuffix(line, "\n")
		if line == "" {
			return f
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			f.name = value
		case "id":
			f.id = value
		case "data":
			f.data = []byte(value)
		case "":
			f.comment = value
		default:
			t.Fatalf("unknown SSE field %q", field)
		}
	}
}

// requireHello asserts the first frame contains only the lineage metadata.
func requireHello(t *testing.T, response *http.Response, in *bufio.Reader, head int64) logPosition {
	t.Helper()
	if response.StatusCode != 200 || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("SSE headers: %d %v", response.StatusCode, response.Header)
	}
	hello := nextFrame(t, in)
	position := decode[logPosition](t, hello.data)
	if hello.name != "hello" || hello.id != "" || position.Head != head || !ulidPattern.MatchString(position.LogEpoch) {
		t.Fatalf("hello: %+v %+v", hello, position)
	}
	return position
}

// requireCursor reads the next event and verifies the wire ID matches the persisted in-order cursor.
func requireCursor(t *testing.T, in *bufio.Reader, stream string, want int64) {
	t.Helper()
	f := nextFrame(t, in)
	for f.comment != "" {
		f = nextFrame(t, in)
	}
	e := decode[eventlog.Event](t, f.data)
	if f.name != e.Type || f.id != strconv.FormatInt(want, 10) || cursorOf(stream, e) != want {
		t.Fatalf("wanted cursor %d: %+v %+v", want, f, e)
	}
}

// observedWriter runs a test hook immediately after the selected frame is flushed.
type observedWriter struct {
	http.ResponseWriter
	recent  []byte
	onFrame func([]byte)
}

// Write captures the current frame without changing the bytes sent to the socket.
func (w *observedWriter) Write(data []byte) (int, error) {
	w.recent = append(w.recent, data...)
	return w.ResponseWriter.Write(data)
}

// FlushError flushes the socket before synchronizing with the test producer.
func (w *observedWriter) FlushError() error {
	err := http.NewResponseController(w.ResponseWriter).Flush()
	w.onFrame(w.recent)
	w.recent = w.recent[:0]
	return err
}

// Flush satisfies http.Flusher for the test decorator.
func (w *observedWriter) Flush() { _ = w.FlushError() }

// Unwrap exposes network deadlines on the underlying response.
func (w *observedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// --- SSE contracts: race the switch, force overflow, and resume across disconnects ---

// TestSSEReplayToLive proves AC5 delivers appends at the replay boundary without a gap or duplicate.
func TestSSEReplayToLive(t *testing.T) {
	s, store, _ := fixture(t)
	threadStream := "thread:" + eventlog.NewID()
	seed(t, store, threadStream, 1)
	seed(t, store, "system", 3)
	written := make(chan struct{})
	appended := make(chan error, 1)
	go func() {
		select {
		case <-written:
			appended <- appendEvents(t.Context(), store, threadStream, 3)
		case <-t.Context().Done():
		}
	}()
	network := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.ServeHTTP(&observedWriter{ResponseWriter: w, onFrame: func(data []byte) {
			if bytes.HasPrefix(data, []byte("id: 4\n")) {
				close(written)
				if err := <-appended; err != nil {
					t.Error(err)
				}
			}
		}}, r)
	}))
	t.Cleanup(network.Close)
	res, in := connect(t, network, "/v1/streams/all/events?after=999", "2")
	requireHello(t, res, in, 4)
	for id := int64(3); id <= 7; id++ {
		requireCursor(t, in, "all", id)
	}
}

// TestSSEHeartbeatAndCursorAhead proves AC5 idle heartbeats and cursor-ahead resync responses.
func TestSSEHeartbeatAndCursorAhead(t *testing.T) {
	s, store, _ := fixture(t)
	seed(t, store, "system", 1)
	s.config.Heartbeat = 5 * time.Millisecond
	ahead := requireProblem(t, request(t, s, "GET", "/v1/streams/system/events?after=2", testToken, "", ""), 409, "cursor_ahead", "")
	if ahead.Head == nil || *ahead.Head != 1 || ahead.LogEpoch == "" {
		t.Fatalf("cursor ahead: %+v", ahead)
	}
	requireProblem(t, request(t, s, "GET", "/v1/streams/system/events?after=18446744073709551616", testToken, "", ""), 409, "cursor_ahead", "")
	network := httptest.NewServer(s)
	t.Cleanup(network.Close)
	res, in := connect(t, network, "/v1/streams/system/events?after=0", "1")
	requireHello(t, res, in, 1)
	heartbeat := nextFrame(t, in)
	if heartbeat.comment == "" || heartbeat.id != "" || heartbeat.name != "" {
		t.Fatalf("heartbeat: %+v", heartbeat)
	}
}

// TestSSEOutlivesHTTPReadTimeout proves H4 keeps an authenticated stream open after ordinary request reads would time out.
func TestSSEOutlivesHTTPReadTimeout(t *testing.T) {
	s, _, _ := fixture(t)
	s.config.Heartbeat = 400 * time.Millisecond
	network := httptest.NewUnstartedServer(s)
	network.Config = s.HTTPServer()
	network.Config.ReadTimeout = 200 * time.Millisecond
	network.Start()
	t.Cleanup(network.Close)
	res, in := connect(t, network, "/v1/streams/system/events", "")
	requireHello(t, res, in, 0)
	if frame := nextFrame(t, in); frame.comment == "" {
		t.Fatalf("expected heartbeat after read timeout: %+v", frame)
	}
}

// TestSSEThreadCursor proves AC5 thread streams use thread sequences and default to full replay.
func TestSSEThreadCursor(t *testing.T) {
	s, store, _ := fixture(t)
	stream := "thread:" + eventlog.NewID()
	seed(t, store, "system", 3)
	seed(t, store, stream, 2)
	network := httptest.NewServer(s)
	t.Cleanup(network.Close)
	res, in := connect(t, network, "/v1/streams/"+stream+"/events", "")
	requireHello(t, res, in, 2)
	requireCursor(t, in, stream, 1)
	requireCursor(t, in, stream, 2)
}

// TestSSEOverflow proves AC5 a blocked writer's bounded live backlog produces overflow and closes without a cursor.
func TestSSEOverflow(t *testing.T) {
	s, store, _ := fixture(t)
	s.config.LiveBuffer = 2
	blocked := make(chan struct{})
	release := make(chan struct{})
	network := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.ServeHTTP(&observedWriter{ResponseWriter: w, onFrame: func(data []byte) {
			if bytes.HasPrefix(data, []byte("id: 1\n")) {
				close(blocked)
				select {
				case <-release:
				case <-r.Context().Done():
				}
			}
		}}, r)
	}))
	t.Cleanup(network.Close)
	res, in := connect(t, network, "/v1/streams/system/events", "")
	requireHello(t, res, in, 0)
	seed(t, store, "system", 1)
	requireCursor(t, in, "system", 1)
	<-blocked
	// Always let the server unwind even if the append fails.
	err := appendEvents(t.Context(), store, "system", 3)
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	overflow := nextFrame(t, in)
	if overflow.name != "overflow" || overflow.id != "" || string(overflow.data) != "{}" {
		t.Fatalf("overflow: %+v", overflow)
	}
	if _, err := in.ReadByte(); err != io.EOF {
		t.Fatalf("after overflow: %v", err)
	}
}

// TestSSEKillReconnectAndRestore proves AC7 resumes from a real disconnect and exposes a changed restore epoch.
func TestSSEKillReconnectAndRestore(t *testing.T) {
	s, store, tokens := fixture(t)
	seed(t, store, "system", 4)
	backup := filepath.Join(t.TempDir(), "backup.db")
	if err := store.Backup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	network := httptest.NewServer(s)
	t.Cleanup(network.Close)
	path := "/v1/streams/system/events"
	response, in := connect(t, network, path, "")
	before := requireHello(t, response, in, 4)
	requireCursor(t, in, "system", 1)
	requireCursor(t, in, "system", 2)

	started := make(chan error, 1)
	killed := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- store.Write(t.Context(), func(tx *eventlog.Transaction) error {
			_, err := tx.Append(t.Context(), testEvent("system"))
			started <- err
			if err != nil {
				return err
			}
			// Keep the transaction open across the disconnect, then commit the full batch.
			select {
			case <-killed:
			case <-t.Context().Done():
				return t.Context().Err()
			}
			for range 3 {
				if _, err := tx.Append(t.Context(), testEvent("system")); err != nil {
					return err
				}
			}
			return nil
		})
	}()
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err)
		}
	case err := <-done:
		t.Fatalf("append never started: %v", err)
	}
	err := response.Body.Close()
	close(killed)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	response, in = connect(t, network, path, "2")
	after := requireHello(t, response, in, 8)
	if before.LogEpoch != after.LogEpoch {
		t.Fatalf("reconnect epoch: %s -> %s", before.LogEpoch, after.LogEpoch)
	}
	for id := int64(3); id <= 8; id++ {
		requireCursor(t, in, "system", id)
	}
	seed(t, store, "system", 1)
	requireCursor(t, in, "system", 9)

	restoredPath := filepath.Join(t.TempDir(), "restored.db")
	if err := eventlog.Restore(t.Context(), backup, restoredPath); err != nil {
		t.Fatal(err)
	}
	restored, err := eventlog.Open(restoredPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := restored.Close(); err != nil {
			t.Error(err)
		}
	})
	restoredAPI := New(Config{Store: restored, Tokens: tokens})
	restoredServer := httptest.NewServer(restoredAPI)
	t.Cleanup(restoredServer.Close)
	restoredResponse, restoredIn := connect(t, restoredServer, path, "2")
	newEpoch := requireHello(t, restoredResponse, restoredIn, 4)
	if before.LogEpoch == newEpoch.LogEpoch {
		t.Fatalf("restore reused epoch %s", before.LogEpoch)
	}
	requireCursor(t, restoredIn, "system", 3)
	requireCursor(t, restoredIn, "system", 4)
	rebuilt := request(t, restoredAPI, "GET", path+"/page", testToken, "", "")
	page := decode[eventPage](t, rebuilt.Body.Bytes())
	if rebuilt.Code != 200 || page.logPosition != newEpoch || len(page.Events) != 4 {
		t.Fatalf("resync page: %d %s", rebuilt.Code, rebuilt.Body)
	}
	for i, event := range page.Events {
		if event.Seq != int64(i+1) {
			t.Fatalf("resync cursor %d at index %d", event.Seq, i)
		}
	}
	p := requireProblem(t, request(t, restoredAPI, "GET", path+"?after=9", testToken, "", ""), 409, "cursor_ahead", "")
	if p.Head == nil || *p.Head != 4 || p.LogEpoch != newEpoch.LogEpoch {
		t.Fatalf("restored cursor error: %+v", p)
	}
}
