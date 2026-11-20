package execdriver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/user"
	"sync"

	"synch/internal/supervisor"
)

// Direct runs Host access processes using the local supervisor and daemon account.
type Direct struct {
	host, identity string
	grants         GrantChecker
	start          func(ProcSpec) (Proc, error)

	mu      sync.Mutex
	threads map[string]*threadRuns
}

var _ ExecutionDriver = (*Direct)(nil)

// NewDirect binds the supervisor to its actual host and daemon account for grant checks.
// The caller supplies the supervisor's persistent fact sink and owns its lifetime.
func NewDirect(s *supervisor.Supervisor, grants GrantChecker) (*Direct, error) {
	if s == nil || grants == nil {
		return nil, errors.New("initialize Host access: supervisor and grant checker required")
	}
	host, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("read host name: %w", err)
	}
	account, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("read daemon account: %w", err)
	}
	return &Direct{
		host: host, identity: account.Username, grants: grants,
		start: func(spec ProcSpec) (Proc, error) {
			// The request context gates admission. The daemon owns the admitted process.
			return s.Spawn(context.Background(), spec)
		},
	}, nil
}

// Kind identifies this implementation in the execution-driver contract.
func (*Direct) Kind() string { return KindDirect }

// Describe returns the host account, label and limits of the execution evidence.
func (d *Direct) Describe() DriverInfo {
	return DriverInfo{
		Label: "Host access", Host: d.host, Identity: d.identity,
		EvidenceIntegrity: "limited to that host account",
	}
}

// Spawn checks the current grant and starts a supervised process if the thread is still admitting work.
func (d *Direct) Spawn(ctx context.Context, spec ProcSpec) (Proc, error) {
	if spec.ThreadID == "" {
		return nil, errors.New("start Host access process: thread ID required")
	}
	thread := d.thread(spec.ThreadID)
	thread.mu.Lock()
	settling := thread.drained != nil
	thread.mu.Unlock()
	if settling {
		return nil, ErrThreadSettling
	}

	// A policy check may block, so it must not keep Settle from closing admission.
	granted, grantErr := d.grants.HasFullControl(ctx, spec.ThreadID, d.host, d.identity)
	thread.mu.Lock()
	defer thread.mu.Unlock()
	if thread.drained != nil {
		return nil, ErrThreadSettling
	}
	if grantErr != nil {
		return nil, fmt.Errorf("check Full Control grant: %w", grantErr)
	}
	if !granted {
		return nil, ErrFullControlRequired
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("start Host access process: %w", err)
	}

	// Launch and registration share the settlement lock so no started tree can escape Settle.
	proc, err := d.start(spec)
	if err != nil {
		return nil, fmt.Errorf("start Host access process: %w", err)
	}
	run := &threadRun{proc: proc}
	thread.active[run] = struct{}{}
	go thread.recordExit(run)
	return proc, nil
}

// thread returns the admission and cleanup state shared by operations for one thread.
func (d *Direct) thread(id string) *threadRuns {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.threads == nil {
		d.threads = make(map[string]*threadRuns)
	}
	if d.threads[id] == nil {
		d.threads[id] = &threadRuns{active: make(map[*threadRun]struct{})}
	}
	return d.threads[id]
}
