package cli

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// errUnreachable marks a request that never reached the server.
var errUnreachable = errors.New("server unreachable")

// errRedirect marks a refused redirect. It is fatal everywhere, including
// events follow: retrying cannot change what the server answers.
var errRedirect = errors.New("redirect refused")

// transportError classifies a failed http.Client.Do: a refused redirect stays
// fatal, any other failure counts as the server being unreachable.
func transportError(err error) error {
	if errors.Is(err, errRedirect) {
		return err
	}
	return fmt.Errorf("%w: %v", errUnreachable, err)
}

// problem mirrors the RFC 9457 bodies the local API answers with.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

// response is one completed HTTP exchange. Receipt headers are kept for the
// mutating commands; problem is set when the server answered problem+json.
type response struct {
	status      int
	body        []byte
	contentType string
	receiptID   string
	replayed    string
	problem     *problem
}

// client calls the local API with the caller's bearer credential.
type client struct {
	base string
	// token is the caller credential. It is only written to the Authorization
	// header and never printed or embedded in errors.
	token string
	http  *http.Client
}

// newClient prepares an HTTP client. Streams set their own deadlines, so the
// shared client carries no global timeout. Redirects are refused outright:
// the loopback policy is applied to the URL the caller configured, and a
// redirect target is a different destination that was never checked.
func newClient(cfg config) *client {
	return &client{base: strings.TrimSuffix(cfg.server, "/"), token: cfg.token, http: &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return fmt.Errorf("%w: the CLI does not follow redirects (redirect to %s)", errRedirect, req.URL)
		},
	}}
}

// unclassifiedError reports an HTTP error answer that is not problem+json, so
// it carries no stable code the caller could match.
type unclassifiedError struct {
	status      int
	contentType string
	body        string
}

// Error summarizes the unclassified answer with its status.
func (e *unclassifiedError) Error() string {
	return fmt.Sprintf("server answered %d with an unclassified %s body: %.200s", e.status, e.contentType, e.body)
}

// newRequestID generates one request ID for a mutating call. It matches the
// API's RequestIdValue pattern and carries no meaning.
func newRequestID() string {
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return "req-" + base64.RawURLEncoding.EncodeToString(raw)
}

// get performs an authenticated GET and classifies problem answers.
func (c *client) get(ctx context.Context, path string) (*response, error) {
	return c.exchange(ctx, http.MethodGet, path, nil, "")
}

// post performs an authenticated mutating call with the caller's request ID.
func (c *client) post(ctx context.Context, path string, body []byte, requestID string) (*response, error) {
	return c.exchange(ctx, http.MethodPost, path, body, requestID)
}

// exchange sends one request. A body of nil sends none; revoke relies on the
// server refusing bodies. The token is only placed in the Authorization header.
func (c *client) exchange(ctx context.Context, method, path string, body []byte, requestID string) (*response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bodyReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if requestID != "" {
		req.Header.Set("Synch-Request-Id", requestID)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, transportError(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, fmt.Errorf("%w: read response: %v", errUnreachable, err)
	}
	out := &response{status: resp.StatusCode, body: data, contentType: resp.Header.Get("Content-Type"),
		receiptID: resp.Header.Get("Synch-Receipt-Id"), replayed: resp.Header.Get("Synch-Receipt-Replayed")}
	if strings.HasPrefix(out.contentType, "application/problem+json") {
		var p problem
		if json.Unmarshal(data, &p) == nil && p.Code != "" {
			out.problem = &p
		}
	}
	return out, nil
}

// bodyReader wraps an encoded body or reports the absent body.
func bodyReader(body []byte) io.Reader {
	if body == nil {
		return nil
	}
	return strings.NewReader(string(body))
}

// encodeBody marshals a command body or fails before any request is sent.
func encodeBody(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode request body: %w", err)
	}
	return raw, nil
}

// ok reports whether the exchange succeeded without a problem answer.
func (r *response) ok() bool { return r.problem == nil && r.status < 400 }
