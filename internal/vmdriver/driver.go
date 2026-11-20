package vmdriver

import (
	"context"
	"errors"
	"slices"
)

// --- Operations, errors and matrix ---

// ContractVersion is the vm-driver contract revision these drivers implement.
const ContractVersion = "3.1.0"

// Operation names a provider operation, as recorded in matrices and journals.
type Operation string

// Operations the dispatcher and drivers understand.
const (
	InventoryOp Operation = "inventory"
	CreateOp    Operation = "create_from_base"
	StartOp     Operation = "start"
	StopOp      Operation = "stop"
	DeleteOp    Operation = "delete"
	SnapshotOp  Operation = "snapshot"
	RestoreOp   Operation = "restore"
	GetTaskOp   Operation = "get_task"
)

// Sentinel errors drivers return; the dispatcher classifies them.
var (
	ErrUnavailable    = errors.New("provider operation unavailable")
	ErrInvalid        = errors.New("invalid provider request or response")
	ErrProvider       = errors.New("provider operation failed")
	ErrRejected       = errors.New("provider rejected request before execution")
	ErrUncertain      = errors.New("provider response uncertain")
	ErrNotOwner       = errors.New("not_owner")
	ErrDenied         = errors.New("provider policy denied")
	ErrLimit          = errors.New("provider VM limit reached")
	ErrSnapshot       = errors.New("snapshot verification failed")
	ErrReconciliation = errors.New("reconciliation")
)

// preDispatchError marks a refusal raised before any mutating request was sent.
type preDispatchError struct{ err error }

// Error returns the wrapped refusal text.
func (e preDispatchError) Error() string { return e.err.Error() }

// Unwrap exposes the refusal so errors.Is still matches the sentinel.
func (e preDispatchError) Unwrap() error { return e.err }

// PreDispatch marks a refusal (ErrSnapshot, ErrInvalid, ErrUnavailable) raised
// before any mutating request was sent. Drivers call it only on paths where no
// mutation can have reached the provider. Other errors pass through unchanged.
func PreDispatch(err error) error {
	if errors.Is(err, ErrSnapshot) || errors.Is(err, ErrInvalid) || errors.Is(err, ErrUnavailable) {
		return preDispatchError{err}
	}
	return err
}

// IsPreDispatch reports whether err was marked by PreDispatch.
func IsPreDispatch(err error) bool {
	var p preDispatchError
	return errors.As(err, &p)
}

// MatrixRow is one driver's capability matrix: real-host evidence per operation.
type MatrixRow struct {
	MatrixVersion                int                  `json:"matrix_version"`
	Driver                       string               `json:"driver"`
	DriverVersion                string               `json:"driver_version"`
	Platform                     string               `json:"platform"`
	PlatformVersion              string               `json:"platform_version"`
	Ops                          map[Operation]string `json:"ops"`
	Advertised                   []Operation          `json:"advertised"`
	SandboxCertifiedConfigs      []string             `json:"sandbox_certified_configs"`
	TokenCarried                 []Operation          `json:"token_carried"`
	AuthoritativeTaskTokenLookup bool                 `json:"authoritative_task_token_lookup"`
}

// Allows reports whether real-host evidence authorizes advertising the operation.
func (m MatrixRow) Allows(op Operation) bool {
	return slices.Contains(m.Advertised, op) && m.Ops[op] == "passed" &&
		(op != SnapshotOp || m.Ops["lost_response_reconcile_snapshot"] == "passed")
}

// VM is one inventory entry: provider reference, display name and power state.
type VM struct{ Ref, Name, State string }

// VMSpec carries the requested properties of a new VM.
type VMSpec struct{ Name string }

// TaskRef is a provider task identifier bound to a request.
type TaskRef struct{ ID string }

// TaskStatus is a task observation: terminal state, resulting VM, in-flight flag.
type TaskStatus struct {
	State          Outcome
	ResultResource string
	InFlight       bool
}

// ExternalRequest is the persisted identity of one provider mutation.
type ExternalRequest struct {
	ID           string    `json:"external_request_id"`
	Driver       string    `json:"driver"`
	Endpoint     string    `json:"endpoint"`
	Operation    Operation `json:"operation"`
	Resource     string    `json:"resource"`
	ParamsDigest string    `json:"params_digest"`
}

// Token derives the provider attribution marker from the persisted request ID.
func (r ExternalRequest) Token() string { return "synch-" + r.ID }

// Marker records creation ownership and attribution in a provider description.
func (r ExternalRequest) Marker(thread string) string {
	return r.Token() + "\n" + OwnerLine(thread)
}

// OwnerLine is the description line naming the thread that owns a created VM.
func OwnerLine(thread string) string { return "synch-owner:" + thread }

// ResourceEvidence describes one exact token match. Endpoint and VMRef are
// checked locally. The catalogue payload only contains the other fields.
type ResourceEvidence struct {
	Endpoint      string    `json:"-"`
	VMRef         string    `json:"-"`
	Operation     Operation `json:"-"`
	Resource      string    `json:"resource"`
	ClientToken   string    `json:"client_token"`
	ObservedState string    `json:"observed_state,omitempty"`
}

// VMDriver is the provider-facing interface. Its transport mutations are called
// by Dispatcher after admission and persistence. A successful call with a task
// accepts asynchronous work and is settled only after observing the task or
// sufficient resource evidence. FindByToken returns exact marker matches whose owner line names thread.
type VMDriver interface {
	Capabilities(context.Context) (MatrixRow, error)
	Inventory(context.Context, string) ([]VM, error)
	CreateFromBase(context.Context, ExternalRequest, string, VMSpec, string) (TaskRef, error)
	Start(context.Context, ExternalRequest, string) (TaskRef, error)
	Stop(context.Context, ExternalRequest, string) (TaskRef, error)
	Delete(context.Context, ExternalRequest, string) (TaskRef, error)
	Snapshot(context.Context, ExternalRequest, string, string) (TaskRef, error)
	Restore(context.Context, ExternalRequest, string, string) (TaskRef, error)
	GetTask(context.Context, string, TaskRef) (TaskStatus, error)
	FindByToken(ctx context.Context, endpoint, token, thread string) ([]ResourceEvidence, error)
}

// TaskTokenLookup is implemented only by drivers with an authoritative lookup.
// It returns true only when the provider affirmatively establishes absence.
type TaskTokenLookup interface {
	TaskAbsent(context.Context, string, string) (bool, error)
}

// CommandValidator rejects unsupported or malformed commands before the
// dispatcher writes an external request. It never contacts the provider.
type CommandValidator interface {
	ValidateCommand(Command) error
}
