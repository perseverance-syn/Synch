package jenkins

import "fmt"

type httpFailure struct {
	code int
	kind error
}

// Error describes an HTTP failure without reflecting any response content.
func (e httpFailure) Error() string { return fmt.Sprintf("%s: Jenkins HTTP %d", e.kind, e.code) }

// Unwrap exposes the shared dispatcher classification.
func (e httpFailure) Unwrap() error { return e.kind }
