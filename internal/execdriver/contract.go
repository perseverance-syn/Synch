package execdriver

import (
	"context"
	"io"

	"synch/internal/supervisor"
)

// ContractVersion is the version of the execution-driver contract.
const ContractVersion = "1.1.0"

const (
	KindDirect  = "direct"
	KindRemote  = "remote"
	KindSandbox = "sandbox"
)

// ExecutionDriver describes, starts and settles execution on an implementation's target.
type ExecutionDriver interface {
	// Kind identifies the execution target as direct, remote or sandbox.
	Kind() string
	// Describe returns the user-facing description of the target.
	Describe() DriverInfo
	// Spawn admits a process and starts it on the target.
	Spawn(ctx context.Context, spec ProcSpec) (Proc, error)
	// Settle stops admission for the thread and reports whether its host process trees are gone.
	Settle(ctx context.Context, threadID string) (SettleReport, error)
}

// Proc is a daemon-owned process whose lifetime is independent of output subscribers.
type Proc interface {
	// ID returns the caller-supplied process run identifier.
	ID() string
	// Subscribe streams future persisted chunks and returns a function to detach the subscriber.
	Subscribe(buffer int) (<-chan OutputChunk, func())
	// ProtocolStreams returns caller-owned stdin and lossless stdout when requested at spawn.
	// The caller must close both handles and drain stdout to avoid blocking the child.
	// Both handles are nil in capture mode.
	ProtocolStreams() (io.WriteCloser, io.ReadCloser)
	// Progress records tool activity independently of stdout and stderr for stall detection.
	Progress()
	// Cancel requests graceful cancellation and waits for completion up to the caller's deadline.
	Cancel(ctx context.Context) error
	// Wait returns the terminal process result or the context error if the wait ends first.
	Wait(ctx context.Context) (ExitInfo, error)
}

// DriverInfo is the user-facing description of an execution target.
type DriverInfo struct {
	Label, Host, Identity, EvidenceIntegrity string
	CertifiedConfigs                         []string
}

// ProcSpec describes a process launch, including its parent and stall threshold.
type ProcSpec = supervisor.Spec

// OutputChunk describes already-persisted output and any full-output artifact.
type OutputChunk = supervisor.OutputChunk

// ExitInfo reports work disposition and whether the entire process tree was cleaned up.
type ExitInfo = supervisor.ExitInfo

// SettleReport records whether all host process trees for the thread are confirmed gone.
type SettleReport struct {
	HostProcessesStopped bool
	Reason               string
}

// GrantChecker checks whether the thread has an active own or inherited grant for the named host account.
// Implementations must support concurrent calls and evaluate the current grant on each call.
type GrantChecker interface {
	// HasFullControl reports whether this thread may execute under the named host account.
	HasFullControl(ctx context.Context, threadID, host, identity string) (bool, error)
}

// AdmissionError identifies a reason a thread cannot start a process.
type AdmissionError string

const (
	ErrFullControlRequired AdmissionError = "an active Full Control grant is required for this host account"
	ErrThreadSettling      AdmissionError = "thread is settling and cannot start processes"
)

// Error returns the user-facing reason for the rejection.
func (e AdmissionError) Error() string { return string(e) }
