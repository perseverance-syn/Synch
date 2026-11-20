package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"synch/internal/eventlog"
	"synch/internal/policy"
	"synch/internal/thread"
	"time"
	"unicode/utf8"
)

const (
	DefaultPort = "7420"
	apiVersion  = "3.0.0"

	// DefaultMaxBodyBytes bounds mutating HTTP request bodies to 1 MiB.
	DefaultMaxBodyBytes = 1 << 20
)

// Config supplies the daemon's shared store, thread machine, and deferred policy integrations.
type Config struct {
	Store     *eventlog.Store
	Threads   *thread.Machine
	Tokens    TokenVerifier
	Authority Authority
	// Policy prepares authority changes before opening the receipt writer.
	// It takes precedence over Authority on the policy endpoints.
	Policy       *policy.Engine
	DiskPressure func() bool
	// ChangeMode admits and settles execution-mode commands when configured.
	ChangeMode ChangeMode
	// WaitingFor, when set, names what a queued thread is waiting on from the operator
	// (empty when nothing). It is a derived read-only detail, never a machine state.
	WaitingFor   func(ctx context.Context, thread string) string
	Heartbeat    time.Duration
	PollInterval time.Duration
	LiveBuffer   int
	// Port sets the numeric loopback port accepted in a Host header. Empty uses DefaultPort.
	Port string
	// MaxBodyBytes bounds mutating request bodies. Values below 1 use DefaultMaxBodyBytes.
	MaxBodyBytes int64
}

// Server dispatches the authenticated Synch v1 HTTP surface.
type Server struct {
	config    Config
	commandMu sync.Mutex
}

// New configures an HTTP handler with production admission and SSE defaults.
func New(cfg Config) *Server {
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 15 * time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 25 * time.Millisecond
	}
	if cfg.LiveBuffer <= 0 {
		cfg.LiveBuffer = 256
	}
	if cfg.Port == "" {
		cfg.Port = DefaultPort
	}
	if cfg.MaxBodyBytes < 1 {
		cfg.MaxBodyBytes = DefaultMaxBodyBytes
	}
	return &Server{config: cfg}
}

// --- Transport: bind loopback and bound ordinary HTTP request reads ---

// HTTPServer wraps the handler for the daemon with a 2 s header timeout, a 10 s
// request read timeout, a 60 s idle timeout, and a 16 KiB header limit. Use
// Listen and Serve for IPv6. SSE clears the read deadline after its first frame.
func (s *Server) HTTPServer() *http.Server {
	return &http.Server{
		Addr:              net.JoinHostPort("127.0.0.1", s.config.Port),
		Handler:           s,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}

// NonLoopbackError identifies a refused HTTP bind address.
type NonLoopbackError struct{ Host string }

// Error prints the refused host without resolving it.
func (e *NonLoopbackError) Error() string {
	return fmt.Sprintf("local API refuses non-loopback host %q", e.Host)
}

// Listen opens a TCP listener only on the contract's IPv4 or IPv6 loopback.
func Listen(ctx context.Context, host, port string) (net.Listener, error) {
	if host == "" {
		host = "127.0.0.1"
	}
	if host == "[::1]" {
		host = "::1"
	}
	addr, err := netip.ParseAddr(host)
	if err != nil || (addr.String() != "127.0.0.1" && addr.String() != "::1") {
		return nil, &NonLoopbackError{Host: host}
	}
	if port == "" {
		port = DefaultPort
	}
	return (&net.ListenConfig{}).Listen(ctx, "tcp", net.JoinHostPort(host, port))
}

// --- Routes: verify the authority and caller before dispatch ---

// route binds an OpenAPI operation to its exact method and path template.
type route struct {
	method, path, operation string
}

var routes = []route{
	{"GET", "/v1/health", "getHealth"},
	{"GET", "/v1/threads", "listThreads"},
	{"POST", "/v1/threads", "createThread"},
	{"GET", "/v1/threads/{thread_id}", "getThread"},
	{"POST", "/v1/threads/{thread_id}/commands", "sendThreadCommand"},
	{"POST", "/v1/threads/{thread_id}/approvals", "createApproval"},
	{"POST", "/v1/threads/{thread_id}/grants", "createGrant"},
	{"POST", "/v1/grants/{grant_id}/revoke", "revokeGrant"},
	{"GET", "/v1/receipts/{request_id}", "getReceipt"},
	{"GET", "/v1/streams/{stream}/events", "streamEvents"},
	{"GET", "/v1/streams/{stream}/events/page", "pageEvents"},
}

// matchPath decodes each segment after splitting so path parameters retain encoded slashes.
func matchPath(template, escaped string) (map[string]string, bool) {
	expected := strings.Split(template, "/")
	actual := strings.Split(escaped, "/")
	if len(expected) != len(actual) {
		return nil, false
	}
	params := make(map[string]string)
	for i, segment := range actual {
		decoded, err := url.PathUnescape(segment)
		if err != nil || !utf8.ValidString(decoded) {
			return nil, false
		}
		name := expected[i]
		if strings.HasPrefix(name, "{") && decoded != "" {
			params[name[1:len(name)-1]] = decoded
		} else if decoded != name || strings.HasPrefix(name, "{") {
			return nil, false
		}
	}
	return params, true
}

// allowsHost checks the HTTP authority against loopback spellings and the configured port.
func (s *Server) allowsHost(host string) bool {
	switch strings.ToLower(host) {
	case "127.0.0.1", "[::1]", "localhost",
		"127.0.0.1:" + s.config.Port, "[::1]:" + s.config.Port, "localhost:" + s.config.Port:
		return true
	default:
		return false
	}
}

// ServeHTTP rejects foreign Host values before checking caller identity or routing.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.allowsHost(r.Host) {
		writeProblem(w, newProblem(http.StatusMisdirectedRequest, "misdirected_request", ""))
		return
	}
	caller := callerFor(r, s.config.Tokens)
	if caller == "" {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeProblem(w, newProblem(401, "unauthorized", ""))
		return
	}
	var allowed []string
	for _, entry := range routes {
		params, match := matchPath(entry.path, r.URL.EscapedPath())
		if !match {
			continue
		}
		if r.Method == entry.method {
			s.serveRoute(w, r, caller, entry, params)
			return
		}
		allowed = append(allowed, entry.method)
	}
	if len(allowed) != 0 {
		w.Header().Set("Allow", strings.Join(allowed, ", "))
		writeProblem(w, newProblem(http.StatusMethodNotAllowed, "method_not_allowed", ""))
		return
	}
	writeProblem(w, newProblem(404, "not_found", ""))
}

// serveRoute forwards an authenticated operation to its contract handler.
func (s *Server) serveRoute(w http.ResponseWriter, r *http.Request, caller string, entry route, params map[string]string) {
	if s.config.Store == nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	if entry.method == http.MethodPost {
		s.mutate(w, r, caller, entry, params)
		return
	}
	switch entry.operation {
	case "getHealth":
		s.health(w)
	case "listThreads":
		if s.config.Threads == nil {
			writeProblem(w, newProblem(500, "internal", ""))
			return
		}
		s.listThreads(w, r)
	case "getThread":
		if s.config.Threads == nil {
			writeProblem(w, newProblem(500, "internal", ""))
			return
		}
		s.getThread(w, r, params["thread_id"])
	case "getReceipt":
		s.getReceipt(w, r, caller, params["request_id"])
	case "streamEvents", "pageEvents":
		s.serveEvents(w, r, params["stream"], entry.operation == "streamEvents")
	}
}

// --- Responses: write the same RFC 9457 shape across the HTTP surface ---

type problem struct {
	Type                string `json:"type"`
	Title               string `json:"title"`
	Status              int    `json:"status"`
	Code                string `json:"code"`
	Detail              string `json:"detail,omitempty"`
	ReceiptID           string `json:"receipt_id,omitempty"`
	StoredPayloadDigest string `json:"stored_payload_digest,omitempty"`
	Head                *int64 `json:"head,omitempty"`
	LogEpoch            string `json:"log_epoch,omitempty"`
}

// newProblem builds a public problem without exposing dependency errors.
func newProblem(status int, code, detail string) problem {
	return problem{Type: "https://synch.local/problems/" + code, Title: http.StatusText(status), Status: status, Code: code, Detail: detail}
}

// writeProblem sends an RFC 9457 response.
func writeProblem(w http.ResponseWriter, p problem) {
	data, _ := json.Marshal(p)
	writeBytes(w, p.Status, "application/problem+json", data)
}

// writeJSON encodes a successful response before committing its headers.
func writeJSON(w http.ResponseWriter, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	writeBytes(w, 200, "application/json", data)
}

// writeBytes preserves stored bodies and media types without wrapping or re-encoding them.
func writeBytes(w http.ResponseWriter, status int, contentType string, data []byte) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

// pressured reads the daemon's current new-work admission condition.
func (s *Server) pressured() bool {
	return s.config.DiskPressure != nil && s.config.DiskPressure()
}

// health reports the persisted log lineage and global head with disk pressure.
func (s *Server) health(w http.ResponseWriter) {
	epoch, err := s.config.Store.LogEpoch()
	if err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	head, err := s.config.Store.Head()
	if err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	pressure := s.pressured()
	status := "ok"
	if pressure {
		status = "degraded"
	}
	writeJSON(w, map[string]any{"status": status, "api_version": apiVersion, "log_epoch": epoch, "head_global_seq": head, "disk_pressure": pressure})
}
