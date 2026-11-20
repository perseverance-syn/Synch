package api

import (
	"context"
	"crypto/sha256"
	"net/http"
	"regexp"
	"strings"
	"sync"
)

// TokenVerifier resolves a presented bearer token to the stable identity used for receipts.
type TokenVerifier interface {
	VerifyToken(context.Context, string) (caller string, valid bool)
}

// MemoryTokens is a concurrent in-memory verifier for tests and local embedding.
type MemoryTokens struct {
	mu       sync.RWMutex
	byDigest map[[sha256.Size]byte]string
}

// NewMemoryTokens returns an empty verifier.
func NewMemoryTokens() *MemoryTokens {
	return &MemoryTokens{}
}

// Set binds a token to an identity or removes it when caller is empty.
func (m *MemoryTokens) Set(token, caller string) {
	digest := sha256.Sum256([]byte(token))
	m.mu.Lock()
	defer m.mu.Unlock()
	if caller == "" {
		delete(m.byDigest, digest)
		return
	}
	if m.byDigest == nil {
		m.byDigest = make(map[[sha256.Size]byte]string)
	}
	m.byDigest[digest] = caller
}

// VerifyToken looks up a token's current caller without retaining the raw token.
func (m *MemoryTokens) VerifyToken(_ context.Context, token string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	caller, ok := m.byDigest[sha256.Sum256([]byte(token))]
	return caller, ok
}

var bearerValue = regexp.MustCompile(`^[A-Za-z0-9._~+/-]+=*$`)

// callerFor verifies one RFC 6750 bearer credential and rejects ambiguous headers.
func callerFor(r *http.Request, verifier TokenVerifier) string {
	values := r.Header.Values("Authorization")
	if len(values) != 1 || verifier == nil {
		return ""
	}
	scheme, token, ok := strings.Cut(values[0], " ")
	token = strings.TrimLeft(token, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || !bearerValue.MatchString(token) {
		return ""
	}
	caller, valid := verifier.VerifyToken(r.Context(), token)
	if !valid {
		return ""
	}
	return caller
}
