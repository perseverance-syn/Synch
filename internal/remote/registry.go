package remote

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
)

const (
	OpSpawn           = "spawn"
	OpReplay          = "replay"
	OpSettle          = "settle"
	OpCancel          = "cancel"
	OpProgress        = "progress"
	OpProtocol        = "protocol"
	OpAgentForwarding = "agent_forwarding"
)

var (
	ErrDispatchRecorded = errors.New("remote dispatch already recorded")
	ErrOperationDenied  = errors.New("remote operation denied")
)

// DisconnectedError identifies the admission failure that maps to T19.
type DisconnectedError struct {
	Runner string
}

// Error identifies the offline runner whose new work must wait.
func (e *DisconnectedError) Error() string {
	return fmt.Sprintf("runner %q disconnected", e.Runner)
}

// PauseReason returns the thread-state-machine reason for blocked admission.
func (*DisconnectedError) PauseReason() string { return "runner_disconnected" }

// HostKeyReviewError reports an absent or changed SSH host key pin.
type HostKeyReviewError struct {
	Runner, Expected, Presented string
}

// Error identifies the target whose host key requires operator review.
func (e *HostKeyReviewError) Error() string {
	return fmt.Sprintf("SSH host key review required for %q (pinned %q, presented %q)", e.Runner, e.Expected, e.Presented)
}

// Target binds a runner identity to an SSH address, account, pin and operation allowlist.
type Target struct {
	Runner, Host, User, Fingerprint string
	Port                            uint16
	Operations                      []string
}

// address formats the SSH endpoint, including IPv6 hosts.
func (t Target) address() string { return net.JoinHostPort(t.Host, strconv.Itoa(int(t.Port))) }

// permits checks whether an operation is explicitly registered.
func (t Target) permits(operation string) bool { return slices.Contains(t.Operations, operation) }

// Registry stores the operator's explicit SSH registrations and pin reviews.
type Registry struct {
	mu      sync.RWMutex
	targets map[string]Target
}

// Register adds or replaces a target with an operator-reviewed pin. An absent pin blocks connections.
func (r *Registry) Register(t Target) error {
	if !runnerID.MatchString(t.Runner) || t.Host == "" || t.Port == 0 || t.User == "" {
		return errors.New("invalid SSH target registration")
	}
	if t.Fingerprint != "" {
		encoded, ok := strings.CutPrefix(t.Fingerprint, "SHA256:")
		sum, err := base64.RawStdEncoding.Strict().DecodeString(encoded)
		if !ok || err != nil || len(sum) != 32 {
			return errors.New("invalid SHA-256 SSH fingerprint")
		}
	}
	t.Operations = slices.Clone(t.Operations)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.targets == nil {
		r.targets = make(map[string]Target)
	}
	r.targets[t.Runner] = t
	return nil
}

// Lookup returns a copy of a registered target so callers cannot mutate its allowlist.
func (r *Registry) Lookup(runner string) (Target, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.targets[runner]
	t.Operations = slices.Clone(t.Operations)
	return t, ok
}
