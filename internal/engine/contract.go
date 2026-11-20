package engine

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// ContractVersion is the version of the engine-adapter contract.
const ContractVersion = "1.1.0"

// EngineAdapter declares its capabilities and starts assignment sessions.
type EngineAdapter interface {
	Capabilities(ctx context.Context) (CapabilitySet, error)
	Start(ctx context.Context, req StartRequest) (Session, error)
}

// Session controls one live engine session and exposes its ordered output.
type Session interface {
	Ref() string
	Send(ctx context.Context, msg Message) error
	// Events gates access to the event stream and closes it after the terminal event.
	Events() (<-chan EngineEvent, error)
	Interrupt(ctx context.Context) error
	Stop(ctx context.Context) error
	Usage(ctx context.Context) (Usage, error)
}

// Resumer reattaches to engine history. Resume is never evidence about external
// operations, which require provider reconciliation.
type Resumer interface {
	Resume(ctx context.Context, sessionRef string, req StartRequest) (Session, error)
}

// StartRequest contains assignment instructions and daemon-brokered launch inputs.
type StartRequest struct {
	AssignmentID string
	ThreadID     string
	Model        string
	Workdir      string
	Instructions []Instruction
	Skills       []PinnedSkill
	MCPEndpoint  string
	Env          map[string]string
}

// Instruction is an instruction block. Stable blocks precede changing ones.
type Instruction struct {
	Text   string
	Stable bool
}

// PinnedSkill is an instruction-only skill selected by the daemon.
type PinnedSkill struct {
	ID     string
	Digest string
	Text   string
}

// Message is the text submitted in a follow-up turn.
type Message struct{ Text string }

// Usage reports cumulative session tokens when the harness supplied them.
type Usage struct {
	Reported     bool
	InputTokens  int64
	OutputTokens int64
}

// EngineEvent carries one persisted output chunk or the terminal session result.
type EngineEvent struct {
	Channel  string
	Chunk    string
	ExitCode int
	Err      error
}

// ErrUnavailable identifies an operation the capability declaration does not qualify.
var ErrUnavailable = errors.New("engine: operation unavailable")

// HarnessError identifies failures callers map to harness_unavailable.
type HarnessError struct {
	Operation string
	Cause     error
}

// Error returns the operation and the harness failure.
func (e *HarnessError) Error() string { return fmt.Sprintf("engine %s: %v", e.Operation, e.Cause) }

// Unwrap lets callers inspect the underlying failure.
func (e *HarnessError) Unwrap() error { return e.Cause }

// ValidateEnv rejects infrastructure credential names and invalid environment keys.
// The daemon remains responsible for brokering values supplied under allowed names.
func ValidateEnv(env map[string]string) error {
	names := make([]string, 0, len(env))
	for name := range env {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if name == "" || strings.ContainsAny(name, "=\x00") || strings.ContainsRune(env[name], 0) {
			return errors.New("invalid environment entry")
		}
		if CredentialName(name) {
			return fmt.Errorf("credential variable %q cannot be brokered", name)
		}
	}
	return nil
}

// CredentialName identifies known infrastructure and authentication environment keys.
func CredentialName(name string) bool {
	name = strings.ToUpper(name)
	switch name {
	case "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN",
		"AWS_PROFILE", "AWS_SHARED_CREDENTIALS_FILE", "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_CREDENTIALS",
		"AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_FEDERATED_TOKEN_FILE",
		"SSH_AUTH_SOCK", "KUBECONFIG", "DATABASE_URL", "GITHUB_TOKEN", "GH_TOKEN", "GH_ENTERPRISE_TOKEN":
		return true
	}
	for _, suffix := range []string{"_TOKEN", "_TOKEN_FILE", "_API_KEY", "_ACCESS_KEY", "_PASSWORD", "_SECRET", "_PRIVATE_KEY", "_CREDENTIALS"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}
