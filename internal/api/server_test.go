package api

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestLoopback proves AC1 rejects a non-loopback bind before opening a socket.
func TestLoopback(t *testing.T) {
	if DefaultPort != "7420" {
		t.Fatalf("default port = %s", DefaultPort)
	}
	for _, host := range []string{"0.0.0.0", "::", "192.0.2.1", "example.test"} {
		var typed *NonLoopbackError
		l, err := Listen(context.Background(), host, "0")
		if err == nil {
			_ = l.Close()
			t.Fatalf("bound %q", host)
		}
		if !errors.As(err, &typed) {
			t.Fatalf("bind %q: %T %v", host, err, err)
		}
	}
	for _, host := range []string{"", "127.0.0.1", "::1"} {
		l, err := Listen(context.Background(), host, "0")
		if err != nil {
			t.Fatalf("bind %q: %v", host, err)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestHostValidation proves H2 rejects foreign authorities before calling the token verifier.
func TestHostValidation(t *testing.T) {
	s, _, _ := fixture(t)
	s.config.Port = "8123"
	for _, host := range []string{"127.0.0.1", "127.0.0.1:8123", "[::1]", "[::1]:8123", "localhost", "LOCALHOST:8123"} {
		r := httptest.NewRequest(http.MethodGet, "http://"+host+"/v1/health", nil)
		r.Header.Set("Authorization", "Bearer "+testToken)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("host %s: %d %s", host, w.Code, w.Body)
		}
	}
	s.config.Tokens = unexpectedVerifier{t: t}
	for _, host := range []string{"evil.example", "evil.example:8123", "localhost.evil.example", "127.0.0.1:7420", "[::1]:", "::1", "localhost."} {
		r := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
		r.Host = host
		r.Header.Set("Authorization", "Bearer "+testToken)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		requireProblem(t, w, http.StatusMisdirectedRequest, "misdirected_request", "")
		if challenge := w.Header().Get("WWW-Authenticate"); challenge != "" {
			t.Fatalf("host %s leaked an authentication challenge: %q", host, challenge)
		}
	}
}

type unexpectedVerifier struct{ t *testing.T }

// VerifyToken fails if Host rejection reaches authentication.
func (v unexpectedVerifier) VerifyToken(context.Context, string) (string, bool) {
	v.t.Fatal("Host check reached authentication")
	return "", false
}

// TestHTTPServerRejectsSlowHeader proves H4 supplies defaults and bounds incomplete headers on a real socket.
func TestHTTPServerRejectsSlowHeader(t *testing.T) {
	server := New(Config{Port: "0"}).HTTPServer()
	if server.Addr != "127.0.0.1:0" || server.ReadHeaderTimeout != 2*time.Second ||
		server.ReadTimeout != 10*time.Second || server.IdleTimeout != 60*time.Second || server.MaxHeaderBytes != 16<<10 {
		t.Fatalf("unexpected HTTP defaults: %s %s %s %s %d", server.Addr, server.ReadHeaderTimeout, server.ReadTimeout, server.IdleTimeout, server.MaxHeaderBytes)
	}
	listener, err := Listen(t.Context(), "", "0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	})
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(6 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprint(conn, "GET /v1/health HTTP/1.1\r\nHost: localhost\r\nX-Incomplete: "); err != nil {
		t.Fatal(err)
	}
	res, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err == nil {
		defer func() { _ = res.Body.Close() }()
		if res.StatusCode != http.StatusBadRequest && res.StatusCode != http.StatusRequestTimeout {
			t.Fatalf("unexpected response to incomplete headers: %s", res.Status)
		}
	} else if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("server did not close the slow request: %v", err)
	}
}

// TestAuthenticationFirst proves AC2 rejects unauthenticated requests before routing or admission.
func TestAuthenticationFirst(t *testing.T) {
	s := New(Config{Tokens: NewMemoryTokens()})
	for _, token := range []string{"", "Bearer invalid"} {
		for _, path := range []string{"/v1/threads", "/unknown"} {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, path, nil)
			r.Host = "127.0.0.1"
			r.Header.Set("Authorization", token)
			s.ServeHTTP(w, r)
			if w.Code != 401 || w.Header().Get("Content-Type") != "application/problem+json" {
				t.Fatalf("%s: %d %s", path, w.Code, w.Body)
			}
		}
	}
}
