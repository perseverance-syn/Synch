package remote

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"synch/internal/eventlog"
	"synch/internal/execdriver"
	"synch/internal/supervisor"
)

// Runner owns processes and records independently of SSH connections.
type Runner struct {
	id       string
	db       *sql.DB
	sup      *supervisor.Supervisor
	log      *slog.Logger
	mu       sync.Mutex
	closed   bool
	runs     map[string]*ownedRun
	settling map[string]bool
}

type ownedRun struct {
	proc     *supervisor.Proc
	thread   string
	protocol *protocolCapture
}

// OpenRunner opens a persistent journal and starts the target's process supervisor.
func OpenRunner(id string, cfg supervisor.Config, logger *slog.Logger) (*Runner, error) {
	if !runnerID.MatchString(id) || cfg.DataDir == "" {
		return nil, errors.New("runner ID and data directory are required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return nil, fmt.Errorf("create runner directory: %w", err)
	}
	db, err := openStorage(filepath.Join(cfg.DataDir, "runner.sqlite"))
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	r := &Runner{id: id, db: db, log: logger, runs: make(map[string]*ownedRun), settling: make(map[string]bool)}
	r.sup, err = supervisor.New(cfg, r)
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return r, nil
}

// Close stops admission and settles all processes before releasing their journal.
func (r *Runner) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	err := r.sup.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, run := range r.runs {
		if run.protocol != nil {
			err = errors.Join(err, run.protocol.close())
		}
	}
	return errors.Join(err, r.db.Close())
}

// --- Journal acknowledgement (event catalogue §3) ---

// Record validates and persists a locally observed fact for a dispatched thread.
func (r *Runner) Record(thread, typ string, version, redactions int, payload json.RawMessage) (eventlog.RunnerRecord, error) {
	found, err := dispatched(context.Background(), r.db, r.id, thread, "")
	if err != nil {
		return eventlog.RunnerRecord{}, err
	}
	if !found {
		r.log.Warn("runner journal rejected fact for unassigned thread", "runner", r.id)
		return eventlog.RunnerRecord{}, ErrOperationDenied
	}
	record, err := appendRecord(r.db, r.id, thread, typ, version, redactions, payload)
	if err != nil {
		// Avoid logging raw payloads or validation errors derived from their contents.
		r.log.Error("runner journal rejected fact", "runner", r.id)
	}
	return record, err
}

// Emit maps supervisor facts to catalogue payloads before the supervisor releases output.
func (r *Runner) Emit(f supervisor.Fact) error {
	if f.Type == "engine.output" && f.Output == nil || f.Type == "process.exited" && f.Exit == nil ||
		f.Type == "resource.pressure" && f.Pressure == nil {
		r.log.Error("runner received an incomplete supervisor fact", "runner", r.id)
		return errInvalidPayload
	}
	var payload any
	switch f.Type {
	case "engine.output":
		payload = struct {
			Assignment string               `json:"assignment_id"`
			Channel    string               `json:"channel"`
			Chunk      string               `json:"chunk"`
			Full       *supervisor.Artifact `json:"full_output,omitempty"`
			Truncated  bool                 `json:"truncated"`
		}{f.RunID, f.Output.Stream, f.Output.Chunk, f.Output.FullOutput, f.Output.Truncated}
	case "process.exited":
		payload = struct {
			Assignment string `json:"assignment_id"`
			Outcome    string `json:"execution_outcome"`
			Work       string `json:"work"`
			Code       int    `json:"exit_code"`
			Cleaned    bool   `json:"descendants_cleaned"`
		}{f.RunID, f.Exit.ExecutionOutcome, f.Exit.Work, f.Exit.ExitCode, f.Exit.DescendantsCleaned}
	case "resource.pressure":
		r.log.Info("runner disk pressure changed", "runner", r.id, "pressure", f.Pressure.On)
		return nil
	default:
		r.log.Error("runner received an unsupported supervisor fact", "runner", r.id)
		return errInvalidPayload
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode supervisor fact: %w", err)
	}
	_, err = r.Record(f.ThreadID, f.Type, 1, 0, raw)
	return err
}

// --- Process admission and ownership (execution-driver §3, decision 18) ---

// spawn reserves the assignment before starting it under a runner-owned lifetime.
func (r *Runner) spawn(spec execdriver.ProcSpec, socket string) error {
	if !ulid.MatchString(spec.ID) || !ulid.MatchString(spec.ThreadID) || spec.Path == "" {
		return errors.New("invalid remote process specification")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.settling[spec.ThreadID] {
		return execdriver.ErrThreadSettling
	}
	ctx := context.Background()
	if err := reserve(ctx, r.db, r.id, spec.ID, spec.ThreadID); err != nil {
		return err
	}
	capture, err := prepareProtocol(spec.ProtocolIO)
	if err != nil {
		return err
	}
	spec.Env = processEnv(spec.Env, socket)
	proc, err := r.sup.Spawn(ctx, spec)
	if err != nil {
		if capture != nil {
			err = errors.Join(err, capture.close())
		}
		return err
	}
	if capture != nil {
		capture.start(proc)
	}
	r.runs[spec.ID] = &ownedRun{proc: proc, thread: spec.ThreadID, protocol: capture}
	return nil
}

// processEnv removes inherited or supplied agent sockets and installs only the granted socket.
func processEnv(env []string, socket string) []string {
	if env == nil {
		env = os.Environ()
	}
	result := make([]string, 0, len(env)+1)
	for _, entry := range env {
		key, _, _ := strings.Cut(entry, "=")
		if !strings.EqualFold(key, "SSH_AUTH_SOCK") {
			result = append(result, entry)
		}
	}
	if socket != "" {
		result = append(result, "SSH_AUTH_SOCK="+socket)
	}
	return result
}

// lookupRun returns the process owned for an exact thread and assignment.
func (r *Runner) lookupRun(thread, id string) (*ownedRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	run := r.runs[id]
	if run == nil || run.thread != thread {
		return nil, ErrOperationDenied
	}
	return run, nil
}

// settle closes admission before cancelling and confirming the thread's process trees.
func (r *Runner) settle(ctx context.Context, thread string) (execdriver.SettleReport, error) {
	r.mu.Lock()
	r.settling[thread] = true
	var processes []*supervisor.Proc
	for _, run := range r.runs {
		if run.thread == thread {
			processes = append(processes, run.proc)
		}
	}
	r.mu.Unlock()
	report := execdriver.SettleReport{HostProcessesStopped: true, Reason: "remote process trees settled"}
	var result error
	for _, proc := range processes {
		cancelErr := proc.Cancel(ctx)
		exit, waitErr := proc.Wait(ctx)
		report.HostProcessesStopped = report.HostProcessesStopped && exit.DescendantsCleaned
		result = errors.Join(result, cancelErr, waitErr)
	}
	if !report.HostProcessesStopped {
		report.Reason = "remote process cleanup could not be confirmed"
	}
	return report, result
}
