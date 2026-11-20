package api

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// unreadBody detects any read of an oversized body with a known length.
type unreadBody struct{ t *testing.T }

// Read fails if the adapter reads a body it can refuse from Content-Length.
func (b unreadBody) Read([]byte) (int, error) {
	b.t.Fatal("oversized body was read")
	return 0, nil
}

// TestLargeBodyRefusedBeforeUpload proves H1 responds to a 64 MiB announcement while the client still holds the body.
func TestLargeBodyRefusedBeforeUpload(t *testing.T) {
	s, _, _ := fixture(t)
	network := httptest.NewServer(s)
	t.Cleanup(network.Close)
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", network.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conn, "POST /v1/threads HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer %s\r\nSynch-Request-Id: too-big-wire\r\nContent-Length: %d\r\n\r\n", testToken, 64<<20); err != nil {
		t.Fatal(err)
	}
	res, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("server waited for the oversized upload: %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusRequestEntityTooLarge || decode[problem](t, data).Code != "body_too_large" || !res.Close {
		t.Fatalf("oversized upload: %s, headers %v, body %s", res.Status, res.Header, data)
	}
}

// TestLargeRequestBody proves H1 rejects a declared 64 MiB body without reading or storing it.
func TestLargeRequestBody(t *testing.T) {
	s, store, _ := fixture(t)
	if s.config.MaxBodyBytes != 1<<20 {
		t.Fatalf("default body limit = %d", s.config.MaxBodyBytes)
	}
	r := httptest.NewRequest(http.MethodPost, "http://localhost/v1/threads", unreadBody{t})
	r.ContentLength = 64 << 20
	r.Header.Set("Authorization", "Bearer "+testToken)
	r.Header.Set(requestIDHeader, "too-big")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	requireProblem(t, w, http.StatusRequestEntityTooLarge, "body_too_large", "")
	if head, err := store.Head(); err != nil || head != 0 {
		t.Fatalf("store head = %d, error = %v", head, err)
	}
	requireProblem(t, request(t, s, "GET", "/v1/receipts/too-big", testToken, "", ""), 404, "not_found", "")
}

// TestUnknownLengthBodyLimit proves H1 bounds chunked bodies and accepts the configured limit inclusively.
func TestUnknownLengthBodyLimit(t *testing.T) {
	s, _, _ := fixture(t)
	valid := `{"project_id":"p","brief":"x"}`
	s.config.MaxBodyBytes = int64(len(valid))
	for _, path := range []string{"/v1/threads", "/v1/grants/grant/revoke"} {
		r := httptest.NewRequest(http.MethodPost, "http://localhost"+path, strings.NewReader(valid+" "))
		r.ContentLength = -1
		r.TransferEncoding = []string{"chunked"}
		r.Header.Set("Authorization", "Bearer "+testToken)
		r.Header.Set(requestIDHeader, "bounded")
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		requireProblem(t, w, http.StatusRequestEntityTooLarge, "body_too_large", "")
	}
	w := request(t, s, http.MethodPost, "/v1/threads", testToken, "bounded", valid)
	if w.Code != http.StatusCreated || w.Header().Get(replayedHeader) != "false" {
		t.Fatalf("exact-limit request: %d %s", w.Code, w.Body)
	}
}
