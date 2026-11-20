package ci

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// --- Shared HTTP helper for providers ---

// HTTP is the request helper both providers use. The token comes from the
// broker per call and goes only into a header chosen by Auth.
type HTTP struct {
	Base     string
	Endpoint string
	Broker   TokenBroker
	Client   *http.Client
	// Auth sets the auth headers of the provider; the token is never put in a URL.
	Auth func(h http.Header, token string)
}

// Do performs one request and returns the body (read up to MaxRetainedLog+1
// bytes). Non-2xx and transport failures are errors wrapping ErrRejected (4xx
// except 408) or ErrTransient, with redacted text.
func (h *HTTP) Do(ctx context.Context, method, path string, body any) ([]byte, error) {
	secrets := h.Broker.KnownValues()
	token, err := h.Broker.Token(ctx, h.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("ci: token for %s: %s", h.Endpoint, Redact(err.Error(), secrets))
	}
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("ci: encode request: %w", err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, h.Base+path, rd)
	if err != nil {
		return nil, fmt.Errorf("ci: build request: %s: %w", Redact(err.Error(), secrets), ErrInvalid)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	h.Auth(req.Header, token)
	resp, err := h.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ci: %s %s: %s: %w", method, path, Redact(err.Error(), secrets), ErrTransient)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxRetainedLog+1))
	if err != nil {
		return nil, fmt.Errorf("ci: %s %s: read body: %s: %w", method, path, Redact(err.Error(), secrets), ErrTransient)
	}
	if resp.StatusCode/100 == 2 {
		return data, nil
	}
	excerpt := Redact(string(data[:min(len(data), 200)]), secrets)
	kind := ErrTransient
	if resp.StatusCode/100 == 4 && resp.StatusCode != http.StatusRequestTimeout {
		kind = ErrRejected
	}
	return nil, fmt.Errorf("ci: %s %s: HTTP %d: %s: %w", method, path, resp.StatusCode, excerpt, kind)
}
