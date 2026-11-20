package supervisor

import (
	"errors"
	"time"
)

// Sink persists facts synchronously. Implementations must support concurrent calls.
type Sink interface{ Emit(Fact) error }
type Fact struct {
	Type            string
	RunID, ThreadID string
	Output          *OutputChunk
	Exit            *ExitInfo
	Pressure        *Pressure
}
type Artifact struct {
	URI    string `json:"uri"`
	Digest string `json:"digest"`
}
type OutputChunk struct {
	Stream     string    `json:"stream"`
	Chunk      string    `json:"chunk"`
	Truncated  bool      `json:"truncated"`
	FullOutput *Artifact `json:"full_output,omitempty"`
}
type ExitInfo struct {
	ExecutionOutcome   string
	Work               string
	DescendantsCleaned bool
	ExitCode           int
	CleanupReason      string
}
type WorkInputs struct {
	InputRevision                          uint64
	CheckpointRevision                     *uint64
	QualifiedNativeResume, KnownSessionRef bool
}

// Classify derives work disposition from normal exit, checkpoint revision or native resume eligibility (thread-state-machine ?5).
func Classify(normal bool, w WorkInputs) string {
	if normal {
		return "complete"
	}
	if w.CheckpointRevision != nil && *w.CheckpointRevision >= w.InputRevision || w.QualifiedNativeResume && w.KnownSessionRef {
		return "resumable"
	}
	return "lost"
}

type RunRecord struct {
	ID, ThreadID string
	PID          int
	CreationTime string
	Work         WorkInputs
}
type Recovery struct {
	Exited     []ExitInfo
	Unmanaged  []RunRecord
	Unverified []RunRecord
}

type Spec struct {
	ID, ParentID, ThreadID, Path string
	Args                         []string
	Dir                          string
	Env                          []string
	Work                         WorkInputs
	StallTimeout                 time.Duration
	// ProtocolIO gives the caller raw stdin and stdout while the supervisor captures stderr.
	ProtocolIO bool
}
type Config struct {
	DataDir                           string
	Grace, PollInterval, DrainTimeout time.Duration
	FreeSpace                         func(string) (free, total uint64, err error)
	Threshold                         func(total uint64) uint64
}
type Pressure struct {
	On              bool
	Free, Threshold uint64
}
type PressureError struct{ Pressure }

// Error describes why the pressure gate rejected a spawn.
func (*PressureError) Error() string { return "disk pressure blocks new work" }

var ErrClosed = errors.New("supervisor closed")
var ErrParent = errors.New("parent unavailable")
var ErrDuplicate = errors.New("duplicate run ID")
