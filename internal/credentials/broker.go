package credentials

import (
	"context"
	"encoding/json"
	"fmt"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"

	"synch/internal/ci"
	"synch/internal/remote"
	"synch/internal/vmdriver"
)

// --- Broker at use: the only reader of secrets (spec line 74, lines 34/102, line 66) ---

// Role is who asks for a secret. The scope read follows from it; callers never pick one.
type Role string

const (
	// RoleDriver is daemon-side driver code (CI, VM, SSH). It reads only the user scope,
	// where infrastructure credentials live (spec line 66: drivers hold them).
	RoleDriver Role = "driver"
	// RoleThread is a worker thread. It reads only its own thread scope.
	RoleThread Role = "thread"
	// RoleCoordinator is a top-level thread. It never receives a secret (spec lines 34/102).
	RoleCoordinator Role = "coordinator"
)

// Caller identifies the party a secret is handed to.
type Caller struct {
	Role   Role
	Thread string
}

// Authorizer decides a thread's secret use (policy action secret.use); nil allows it.
type Authorizer func(ctx context.Context, caller Caller, name string) error

// Broker hands secrets out at the moment of use and registers each value with the
// Redactor before it leaves. It never logs, persists or caches a value.
type Broker struct {
	store     *Store
	red       *Redactor
	authorize Authorizer
}

// NewBroker reads from store (nil selects the failing fallback) and registers into red.
func NewBroker(store *Store, red *Redactor) *Broker {
	if store == nil {
		store = NewStore(Fallback())
	}
	if red == nil {
		red = NewRedactor()
	}
	return &Broker{store: store, red: red}
}

// SetAuthorizer installs the policy check for thread callers. Call it before first use.
func (b *Broker) SetAuthorizer(a Authorizer) { b.authorize = a }

// Redactor returns the redactor that knows every value this broker handed out.
func (b *Broker) Redactor() *Redactor { return b.red }

// Put stores a secret and registers it as known, so it is redacted even before first use.
func (b *Broker) Put(scope Scope, name, value string) error {
	if err := b.store.Put(scope, name, value); err != nil {
		return err
	}
	return b.red.Add(value)
}

// Use returns the named secret from the caller's own scope. A coordinator is always
// refused; a thread reads only its thread scope, after the authorizer allows it; a
// driver reads only the user scope.
func (b *Broker) Use(ctx context.Context, caller Caller, name string) (string, error) {
	switch caller.Role {
	case RoleDriver:
		return b.fetch(UserScope, name)
	case RoleThread:
		if caller.Thread == "" {
			return "", ErrDenied
		}
		if b.authorize != nil {
			if err := b.authorize(ctx, caller, name); err != nil {
				return "", fmt.Errorf("%w: %w", ErrDenied, err)
			}
		}
		return b.fetch(ThreadScope(caller.Thread), name)
	default:
		return "", fmt.Errorf("%w: role %q receives no secrets", ErrDenied, caller.Role)
	}
}

// fetch reads one secret and registers it before handing it out. A value too short to
// redact is refused.
func (b *Broker) fetch(scope Scope, name string) (string, error) {
	v, err := b.store.get(scope, name)
	if err != nil {
		return "", fmt.Errorf("credentials: secret %q: %w", name, err)
	}
	if err := b.red.Add(v); err != nil {
		return "", fmt.Errorf("credentials: secret %q: %w", name, err)
	}
	return v, nil
}

// --- Adapters: existing driver broker interfaces, all with driver identity ---

// CI returns the ci.TokenBroker: the token of an endpoint is the user secret "ci/<endpoint>".
func (b *Broker) CI() ci.TokenBroker { return ciBroker{b} }

// ciBroker adapts the broker to ci.TokenBroker.
type ciBroker struct{ b *Broker }

// Token fetches the endpoint's token at the moment of the request.
func (c ciBroker) Token(ctx context.Context, endpoint string) (string, error) {
	return c.b.Use(ctx, Caller{Role: RoleDriver}, "ci/"+endpoint)
}

// KnownValues lists every known form so CI logs are redacted of all of them.
func (c ciBroker) KnownValues() []string { return c.b.red.Values() }

// VM returns the vmdriver.Broker: the user secret "vm/<endpoint>" holds a JSON object
// {"username","password","token"}.
func (b *Broker) VM() vmdriver.Broker { return vmBroker{b} }

// vmBroker adapts the broker to vmdriver.Broker.
type vmBroker struct{ b *Broker }

// Credentials decodes the endpoint's credential and registers its password, token and
// basic-auth pair so their encoded forms are redacted too.
func (v vmBroker) Credentials(ctx context.Context, endpoint string) (vmdriver.Credentials, error) {
	raw, err := v.b.Use(ctx, Caller{Role: RoleDriver}, "vm/"+endpoint)
	if err != nil {
		return vmdriver.Credentials{}, err
	}
	var c struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Token    string `json:"token"`
	}
	// The decode error is dropped: its text could quote part of the secret.
	if json.Unmarshal([]byte(raw), &c) != nil || (c.Password == "" && c.Token == "") {
		return vmdriver.Credentials{}, fmt.Errorf("%w: VM credential for %s is not {username,password,token}", ErrInvalid, endpoint)
	}
	for _, s := range []string{c.Password, c.Token} {
		if s != "" {
			if err := v.b.red.Add(s); err != nil {
				return vmdriver.Credentials{}, err
			}
		}
	}
	if c.Username != "" && c.Password != "" {
		if err := v.b.red.Add(c.Username + ":" + c.Password); err != nil {
			return vmdriver.Credentials{}, err
		}
	}
	return vmdriver.Credentials{Username: c.Username, Password: c.Password, Token: c.Token}, nil
}

// SSH returns the remote.Broker. Authentication uses the user secret "ssh/<runner>"
// (infrastructure, driver-only); a forwarded agent holds only the thread secret
// "ssh-forward/<runner>", so the worker on the runner never gets the infrastructure key.
func (b *Broker) SSH() remote.Broker { return sshBroker{b} }

// sshBroker adapts the broker to remote.Broker.
type sshBroker struct{ b *Broker }

// Agent returns an in-memory agent holding the runner's infrastructure key.
func (s sshBroker) Agent(ctx context.Context, t remote.Target) (agent.Agent, error) {
	pem, err := s.b.Use(ctx, Caller{Role: RoleDriver}, "ssh/"+t.Runner)
	if err != nil {
		return nil, err
	}
	return keyring(pem)
}

// ForwardingAgent returns an agent holding only the thread's own forwarding key, decided
// as that thread's secret use.
func (s sshBroker) ForwardingAgent(ctx context.Context, t remote.Target, thread string) (agent.Agent, error) {
	pem, err := s.b.Use(ctx, Caller{Role: RoleThread, Thread: thread}, "ssh-forward/"+t.Runner)
	if err != nil {
		return nil, err
	}
	return keyring(pem)
}

// keyring parses a PEM private key into a fresh in-memory agent.
func keyring(pem string) (agent.Agent, error) {
	key, err := ssh.ParseRawPrivateKey([]byte(pem))
	if err != nil {
		return nil, fmt.Errorf("%w: SSH key does not parse", ErrInvalid)
	}
	ring := agent.NewKeyring()
	if err := ring.Add(agent.AddedKey{PrivateKey: key}); err != nil {
		return nil, fmt.Errorf("%w: SSH key rejected by agent", ErrInvalid)
	}
	return ring, nil
}
