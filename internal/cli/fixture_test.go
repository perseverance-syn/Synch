package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"synch/internal/api"
	"synch/internal/eventlog"
	"synch/internal/queue"
	"synch/internal/thread"
)

// --- Fixture: the real API server, identities and command helpers ---

// fixture identities and tokens; receipts are keyed by the caller identity.
const (
	cliToken    = "cli-token-alice"
	cliCaller   = "operator:alice"
	otherToken  = "cli-token-bob"
	otherCaller = "operator:bob"
	// fixtureULID is a schema-valid thread ID for authority stubs.
	fixtureULID = "01JD3Q8Z7Y6X5W4V3T2S1R0QPN"
)

// cliFixture runs the real internal/api server in-process on a loopback port
// with a real event store, thread machine, in-memory token verifier, and a
// stub authority, so Main exercises the daemon's actual HTTP surface.
type cliFixture struct {
	t           *testing.T
	url         string
	port        string
	dir         string
	store       *eventlog.Store
	machine     *thread.Machine
	server      *http.Server
	tempServer  *http.Server
	tokens      *api.MemoryTokens
	tokenFile   string
	configFile  string
	envMap      map[string]string
	lastCreated string
}

// newCLIFixture starts the API server and writes the token file.
func newCLIFixture(t *testing.T, configure ...func(*api.Config)) *cliFixture {
	t.Helper()
	dir := t.TempDir()
	store, err := eventlog.Open(filepath.Join(dir, "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	// Fixture threads have no supervised process, so terminal commands settle.
	machine, err := thread.Open(t.Context(), store, thread.Hooks{
		ConfirmGone: func(context.Context, string) (bool, error) { return true, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	tokens := api.NewMemoryTokens()
	tokens.Set(cliToken, cliCaller)
	tokens.Set(otherToken, otherCaller)
	listener, err := api.Listen(context.Background(), "127.0.0.1", "0")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	cfg := api.Config{Store: store, Threads: machine, Tokens: tokens, Authority: acceptAuthority{}, Port: port}
	for _, option := range configure {
		option(&cfg)
	}
	server := api.New(cfg).HTTPServer()
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	tokenFile := filepath.Join(dir, "token.txt")
	if err := os.WriteFile(tokenFile, []byte(cliToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	f := &cliFixture{t: t, url: "http://127.0.0.1:" + port, port: port, dir: dir,
		store: store, machine: machine, server: server, tokens: tokens,
		tokenFile: tokenFile, configFile: filepath.Join(dir, "config.json"), envMap: map[string]string{}}
	// The cleanup closes whichever store the fixture currently holds, because
	// tests swap the store mid-run.
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	return f
}

// env resolves the fixture's environment variables for Main.
func (f *cliFixture) env(key string) string { return f.envMap[key] }

// run executes one Main invocation and captures both output streams.
func (f *cliFixture) run(args ...string) (int, string, string) {
	f.t.Helper()
	var stdout, stderr bytes.Buffer
	code := Main(args, &stdout, &stderr, f.env)
	return code, stdout.String(), stderr.String()
}

// authenticated returns the standard flag prefix addressing this fixture.
func (f *cliFixture) authenticated(args ...string) []string {
	return append([]string{"--server", f.url, "--token-file", f.tokenFile}, args...)
}

// head returns the log's global head cursor through the CLI itself.
func (f *cliFixture) head() int64 {
	f.t.Helper()
	code, out, errOut := f.run(f.authenticated("--json", "health")...)
	if code != 0 {
		f.t.Fatalf("health = %d, %s, %s", code, out, errOut)
	}
	var health struct {
		HeadGlobalSeq int64 `json:"head_global_seq"`
	}
	if err := json.Unmarshal([]byte(out), &health); err != nil {
		f.t.Fatalf("decode health %q: %v", out, err)
	}
	return health.HeadGlobalSeq
}

// receiptMap decodes a --json mutation envelope printed by the receipt printer.
func receiptMap(t *testing.T, stdout string) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(stdout)), &value); err != nil {
		t.Fatalf("decode receipt %q: %v", stdout, err)
	}
	return value
}

// createThread submits one thread and returns its ID.
func (f *cliFixture) createThread(requestID, brief string) string {
	f.t.Helper()
	code, out, errOut := f.run(f.authenticated("--json", "threads", "create", "--project", "p1", "--brief", brief, "--request-id", requestID)...)
	if code != 0 {
		f.t.Fatalf("threads create = %d, %s, %s", code, out, errOut)
	}
	fields := receiptMap(f.t, out)
	result, _ := fields["result"].(map[string]any)
	id, _ := result["thread_id"].(string)
	if id == "" {
		f.t.Fatalf("receipt lacks thread_id: %s", out)
	}
	f.lastCreated = id
	return id
}

// lastThreadID returns the most recently created thread's ID.
func (f *cliFixture) lastThreadID() string { return f.lastCreated }

// sendCommand submits one thread command with a fresh request ID.
func (f *cliFixture) sendCommand(threadID, command string) {
	f.t.Helper()
	code, out, errOut := f.run(f.authenticated("--json", "thread", "cmd", threadID, command, "--request-id", "cmd-"+command+"-"+briefID())...)
	if code != 0 {
		f.t.Fatalf("thread cmd %s = %d, %s, %s", command, code, out, errOut)
	}
}

// briefID builds a unique suffix for generated request IDs.
func briefID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// restart stops the server and serves the same store on the same address,
// forcing connected clients to reconnect without changing the log lineage.
func (f *cliFixture) restart() {
	f.t.Helper()
	if err := f.server.Close(); err != nil {
		f.t.Fatal(err)
	}
	f.serveOnOriginalPort()
}

// restartWithNewStore swaps in a fresh event store and machine and serves
// them on the same address: a new log lineage with a smaller head makes the
// follower's first reconnect answer 409 cursor_ahead.
func (f *cliFixture) restartWithNewStore() {
	f.t.Helper()
	f.swapStore()
	f.serveOnOriginalPort()
}

// restartWithNewStoreOnTempPort swaps in a fresh event store and serves it on
// a temporary port, so mutations can be issued before the original address
// comes back and the follower's reconnect sees the finished new lineage.
func (f *cliFixture) restartWithNewStoreOnTempPort() {
	f.t.Helper()
	f.swapStore()
	listener, err := api.Listen(context.Background(), "127.0.0.1", "0")
	if err != nil {
		f.t.Fatal(err)
	}
	tempPort := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	server := api.New(api.Config{Store: f.store, Threads: f.machine, Tokens: f.tokens, Authority: acceptAuthority{}, Port: tempPort}).HTTPServer()
	f.tempServer = server
	go func() { _ = server.Serve(listener) }()
	f.url = "http://127.0.0.1:" + tempPort
}

// serveOnOriginalPort stops any temporary server and serves the current store
// on the fixture's original address with the original configuration. The port
// is bound again with retries, since closed listeners can linger briefly.
func (f *cliFixture) serveOnOriginalPort() {
	f.t.Helper()
	if f.tempServer != nil {
		if err := f.tempServer.Close(); err != nil {
			f.t.Error(err)
		}
		f.tempServer = nil
	}
	f.url = "http://127.0.0.1:" + f.port
	var listener net.Listener
	var err error
	for attempt := 0; attempt < 50; attempt++ {
		listener, err = api.Listen(context.Background(), "127.0.0.1", f.port)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		f.t.Fatalf("rebind %s: %v", f.port, err)
	}
	server := api.New(api.Config{Store: f.store, Threads: f.machine, Tokens: f.tokens, Authority: acceptAuthority{}, Port: f.port}).HTTPServer()
	f.server = server
	go func() { _ = server.Serve(listener) }()
}

// swapStore closes the server and store and opens a fresh lineage without
// serving it yet.
func (f *cliFixture) swapStore() {
	f.t.Helper()
	if err := f.server.Close(); err != nil {
		f.t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		f.t.Fatal(err)
	}
	newStore, err := eventlog.Open(filepath.Join(f.dir, fmt.Sprintf("events-%d.db", time.Now().UnixNano())))
	if err != nil {
		f.t.Fatal(err)
	}
	f.store = newStore
	newMachine, err := thread.Open(context.Background(), newStore, thread.Hooks{
		ConfirmGone: func(context.Context, string) (bool, error) { return true, nil },
	})
	if err != nil {
		f.t.Fatal(err)
	}
	f.machine = newMachine
}

// --- Output capture: deterministic ends for streaming commands ---

// stopAfterWriter collects output lines and, once a limit is armed, fails the
// write that reaches it so a streaming command ends deterministically.
type stopAfterWriter struct {
	mu      sync.Mutex
	lines   []string
	written chan string
	limit   int
}

// newStopAfterWriter starts an unlimited writer that announces every line.
func newStopAfterWriter() *stopAfterWriter {
	return &stopAfterWriter{written: make(chan string, 4096)}
}

// Write records the line and fails once the armed limit has been reached.
func (w *stopAfterWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	line := strings.TrimRight(string(p), "\n")
	w.lines = append(w.lines, line)
	select {
	case w.written <- line:
	default:
	}
	if w.limit > 0 && len(w.lines) >= w.limit {
		return 0, errors.New("output closed")
	}
	return len(p), nil
}

// arm sets the total line count after which the writer fails.
func (w *stopAfterWriter) arm(limit int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.limit = limit
}

// collected returns the recorded lines under the lock.
func (w *stopAfterWriter) collected() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string{}, w.lines...)
}

// awaitLines waits for n recorded lines with a timeout.
func (w *stopAfterWriter) awaitLines(t *testing.T, n int) {
	t.Helper()
	for len(w.collected()) < n {
		select {
		case <-w.written:
		case <-time.After(15 * time.Second):
			t.Fatalf("only %d of %d lines arrived", len(w.collected()), n)
		}
	}
}

// acceptAuthority accepts every authority operation, mirroring a configured
// policy layer, so approvals, grants and revocations reach their 201/202.
type acceptAuthority struct{}

// Approve accepts the approval on the given thread.
func (acceptAuthority) Approve(_ context.Context, _ *eventlog.Transaction, _ string, threadID string, _ map[string]any) (queue.Decision, error) {
	return queue.Decision{ThreadID: threadID, HTTPStatus: 201}, nil
}

// Grant accepts the grant on the given thread.
func (acceptAuthority) Grant(_ context.Context, _ *eventlog.Transaction, _ string, threadID string, _ map[string]any) (queue.Decision, error) {
	return queue.Decision{ThreadID: threadID, HTTPStatus: 201}, nil
}

// Revoke accepts the revocation and reports a schema-valid thread ID.
func (acceptAuthority) Revoke(_ context.Context, _ *eventlog.Transaction, _ string, _ string) (queue.Decision, error) {
	return queue.Decision{ThreadID: fixtureULID, HTTPStatus: 202}, nil
}
