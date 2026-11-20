package cli

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// emptyEnv resolves no environment variables.
func emptyEnv(string) string { return "" }

// sseStub serves scripted SSE passes: each connection gets the next pass's
// frames, then the connection closes. It records every Last-Event-ID header
// so tests can prove the resume cursor.
type sseStub struct {
	mu     sync.Mutex
	passes [][]string
	seen   []string
	server *httptest.Server
}

// newSSEStub starts a stub server that replays one scripted pass per connect.
func newSSEStub(t *testing.T, passes ...[]string) *sseStub {
	t.Helper()
	s := &sseStub{passes: passes}
	s.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		index := len(s.seen)
		s.seen = append(s.seen, r.Header.Get("Last-Event-ID"))
		var frames []string
		if index < len(s.passes) {
			frames = s.passes[index]
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for _, frame := range frames {
			if _, err := fmt.Fprint(w, frame); err != nil {
				return
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	}))
	t.Cleanup(s.server.Close)
	return s
}

// lastSeenIDs returns one recorded Last-Event-ID header per connection.
func (s *sseStub) lastSeenIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string{}, s.seen...)
}

// TestFollowDiscardsOutOfOrderFrame proves C4: a frame whose cursor is not the
// next contiguous cursor is discarded without delivery, the follower
// reconnects with its own last contiguous cursor, and the stream completes.
func TestFollowDiscardsOutOfOrderFrame(t *testing.T) {
	hello := "event: hello\ndata: {\"log_epoch\":\"E1\",\"head\":10}\n\n"
	stub := newSSEStub(t,
		[]string{hello,
			"id: 1\nevent: request.accepted\ndata: {\"seq\":1}\n\n",
			"id: 3\nevent: request.accepted\ndata: {\"seq\":3}\n\n"},
		[]string{hello,
			"id: 2\nevent: request.accepted\ndata: {\"seq\":2}\n\n"})
	var stdout, stderr strings.Builder
	code := Main([]string{"--server", stub.server.URL, "--token-file", stubTokenFile(t), "--json",
		"events", "follow", "all", "--until", "2"}, &stdout, &stderr, emptyEnv)
	if code != exitOK {
		t.Fatalf("follow = %d, %s, %s", code, stdout.String(), stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "{\"seq\":1}\n{\"seq\":2}" {
		t.Fatalf("delivered %q, want the in-order frames without the discarded cursor 3", got)
	}
	seen := stub.lastSeenIDs()
	if len(seen) != 2 || seen[0] != "" || seen[1] != "1" {
		t.Fatalf("resume headers = %q, want [\"\", \"1\"]", seen)
	}
}

// TestFollowReconnectsAfterOverflow proves C4: an overflow frame closes the
// stream and the follower reconnects with its own cursor, never skipping.
func TestFollowReconnectsAfterOverflow(t *testing.T) {
	hello := "event: hello\ndata: {\"log_epoch\":\"E1\",\"head\":10}\n\n"
	stub := newSSEStub(t,
		[]string{hello,
			"id: 1\nevent: request.accepted\ndata: {\"seq\":1}\n\n",
			"event: overflow\ndata: {}\n\n"},
		[]string{hello,
			"id: 2\nevent: request.accepted\ndata: {\"seq\":2}\n\n"})
	var stdout, stderr strings.Builder
	code := Main([]string{"--server", stub.server.URL, "--token-file", stubTokenFile(t), "--json",
		"events", "follow", "all", "--until", "2"}, &stdout, &stderr, emptyEnv)
	if code != exitOK {
		t.Fatalf("follow = %d, %s, %s", code, stdout.String(), stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "{\"seq\":1}\n{\"seq\":2}" {
		t.Fatalf("delivered %q, want both events across the overflow", got)
	}
	seen := stub.lastSeenIDs()
	if len(seen) != 2 || seen[0] != "" || seen[1] != "1" {
		t.Fatalf("resume headers = %q, want [\"\", \"1\"]", seen)
	}
}

// stubTokenFile writes the token file the stub's caller uses.
func stubTokenFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token.txt")
	if err := os.WriteFile(path, []byte("stub-token"), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
