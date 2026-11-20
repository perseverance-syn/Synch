package gates

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// --- Local executor: os/exec with process-tree cleanup ---

// InterruptError reports that a command did not run to a normal exit: it was
// cancelled, killed by a signal, or crashed. It is an error result, never a
// failed check, because the command did not finish its job.
type InterruptError struct {
	Reason string
}

// Error returns the interruption reason.
func (e *InterruptError) Error() string { return e.Reason }

// TimeoutError reports that a command exceeded its declared timeout.
type TimeoutError struct {
	Seconds int64
}

// Error states the timeout bound that was exceeded.
func (e *TimeoutError) Error() string {
	return fmt.Sprintf("command timed out after %ds", e.Seconds)
}

// ExecExecutor is the local executor: it runs the check's argv with os/exec,
// capturing up to one megabyte of stdout. Tests inject their own executor.
type ExecExecutor struct{}

const (
	// maxCapturedStdout bounds the stdout a single command may contribute.
	maxCapturedStdout = 1 << 20
	// maxCapturedStderr bounds the stderr kept to recognise a crash.
	maxCapturedStderr = 64 << 10
	// killGrace is how long after a kill the runner waits for pipes to close.
	killGrace = 500 * time.Millisecond
)

// Run executes the check's argv in its workdir, applying the declared timeout.
// On timeout or cancellation the whole process tree is killed, and Run returns
// within the grace period even if a descendant still holds the output pipes.
func (ExecExecutor) Run(ctx context.Context, check Check) (int, string, error) {
	if check.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(check.Timeout)*time.Second)
		defer cancel()
	}
	cmd := exec.CommandContext(ctx, check.Command[0], check.Command[1:]...)
	if check.Workdir != "" {
		cmd.Dir = check.Workdir
	}
	configureProcessTree(cmd)
	cmd.WaitDelay = killGrace
	stdout := cappedBuffer{limit: maxCapturedStdout}
	stderr := cappedBuffer{limit: maxCapturedStderr}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil || errors.Is(err, exec.ErrWaitDelay) {
		return 0, stdout.String(), nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		if errors.Is(ctxErr, context.DeadlineExceeded) && check.Timeout > 0 {
			return 0, stdout.String(), &TimeoutError{Seconds: check.Timeout}
		}
		return 0, stdout.String(), &InterruptError{Reason: "command interrupted: " + ctxErr.Error()}
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		code := exit.ExitCode()
		switch {
		case code < 0:
			return 0, stdout.String(), &InterruptError{Reason: "command terminated abnormally: " + exit.String()}
		case code == 2 && looksLikeGoPanic(stderr.String()):
			return 0, stdout.String(), &InterruptError{Reason: "command panicked (exit code 2)"}
		}
		return code, stdout.String(), nil
	}
	return 0, stdout.String(), fmt.Errorf("start %q: %w", check.Command[0], err)
}

// looksLikeGoPanic recognises the runtime's panic banner and goroutine dump,
// which is how a crashed Go program differs from one that exited 2 on purpose.
func looksLikeGoPanic(stderr string) bool {
	return strings.Contains(stderr, "panic: ") && strings.Contains(stderr, "goroutine ")
}

// cappedBuffer captures at most limit bytes and discards the rest.
type cappedBuffer struct {
	buf   bytes.Buffer
	limit int
}

// Write appends up to the capture cap and reports the full length written.
func (w *cappedBuffer) Write(p []byte) (int, error) {
	remaining := w.limit - w.buf.Len()
	if remaining > 0 {
		if len(p) > remaining {
			w.buf.Write(p[:remaining])
		} else {
			w.buf.Write(p)
		}
	}
	return len(p), nil
}

// String returns the captured bytes.
func (w *cappedBuffer) String() string { return w.buf.String() }
