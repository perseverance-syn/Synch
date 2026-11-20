package daemon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"synch/internal/eventlog"
	"synch/internal/execdriver"
	"synch/internal/gates"
	"synch/internal/supervisor"
)

// --- Gate executor: gate commands launch through policy and the supervisor (card 111 H1) ---

// maxGateStdout bounds the stdout kept for the runner (version probes) and persisted as a preview.
const maxGateStdout = 1 << 20

// gateCleanup bounds how long a cancelled gate process may take to settle.
const gateCleanup = 10 * time.Second

// gateExecutor runs a thread's gate commands through the policy-checked Direct
// driver. Every command is therefore a `host.exec` policy decision, a supervised
// process in the thread's tree (cancelled and settled with the thread), and its
// stderr and stdout previews are persisted as engine.output on the thread.
type gateExecutor struct {
	daemon *Daemon
	thread string
	// dir is the worktree; policy needs an absolute directory for every launch.
	dir string
}

// Run launches one check command and waits for it. A policy denial or launch
// failure is returned as an error before anything executes; a nonzero exit is a result.
func (e gateExecutor) Run(ctx context.Context, check gates.Check) (int, string, error) {
	if check.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(check.Timeout)*time.Second)
		defer cancel()
	}
	workdir := check.Workdir
	if !filepath.IsAbs(workdir) {
		workdir = filepath.Join(e.dir, workdir)
	}
	driver := policyDriver{ExecutionDriver: e.daemon.driver, daemon: e.daemon}
	spec := execdriver.ProcSpec{
		ID: eventlog.NewID(), ThreadID: e.thread, Path: check.Command[0], Args: check.Command[1:],
		Dir: workdir,
		// ProtocolIO gives this executor lossless stdout; the supervisor persists stderr.
		ProtocolIO: true,
	}
	proc, err := driver.Spawn(ctx, spec)
	if err != nil {
		return 0, "", err
	}
	_, out := proc.ProtocolStreams()
	var stdout bytes.Buffer
	copied := make(chan struct{})
	go func() {
		defer close(copied)
		// Keep draining past the cap so the child never blocks on a full pipe.
		_, _ = io.Copy(&cappedWriter{buf: &stdout, limit: maxGateStdout}, out)
		_ = out.Close()
	}()
	info, err := proc.Wait(ctx)
	if ctxErr := ctx.Err(); ctxErr != nil {
		// Cancellation or timeout: settle the tree before reporting, whatever Wait said.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), gateCleanup)
		defer cancel()
		if cancelErr := proc.Cancel(cleanup); cancelErr != nil {
			slog.Warn("settle cancelled gate command", "thread_id", e.thread, "error", cancelErr)
		}
		<-copied
		e.persistStdout(spec.ID, stdout.String())
		if ctxErr == context.DeadlineExceeded && check.Timeout > 0 {
			return 0, stdout.String(), &gates.TimeoutError{Seconds: check.Timeout}
		}
		return 0, stdout.String(), &gates.InterruptError{Reason: "command interrupted: " + ctxErr.Error()}
	}
	<-copied
	e.persistStdout(spec.ID, stdout.String())
	if err != nil {
		return 0, stdout.String(), fmt.Errorf("supervised gate command: %w", err)
	}
	if info.ExecutionOutcome != "exited" || info.ExitCode < 0 {
		return 0, stdout.String(), &gates.InterruptError{Reason: "command did not exit normally: " + info.ExecutionOutcome}
	}
	return info.ExitCode, stdout.String(), nil
}

// persistStdout records a bounded stdout preview, as the supervisor does for stderr.
func (e gateExecutor) persistStdout(run, stdout string) {
	if stdout == "" {
		return
	}
	truncated := len(stdout) > supervisor.OutputCap
	if truncated {
		stdout = strings.ToValidUTF8(stdout[:supervisor.OutputCap], "")
	}
	if err := e.daemon.record(e.thread, "engine.output", map[string]any{
		"assignment_id": run, "channel": "stdout", "chunk": stdout, "truncated": truncated,
	}, ""); err != nil {
		slog.Error("persist gate stdout", "thread_id", e.thread, "error", err)
	}
}

// cappedWriter appends up to limit bytes and silently drops the rest.
type cappedWriter struct {
	buf   *bytes.Buffer
	limit int
}

// Write keeps the bytes that fit under the cap and reports the full length.
func (w *cappedWriter) Write(p []byte) (int, error) {
	if room := w.limit - w.buf.Len(); room > 0 {
		w.buf.Write(p[:min(room, len(p))])
	}
	return len(p), nil
}
