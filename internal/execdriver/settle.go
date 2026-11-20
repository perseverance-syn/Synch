package execdriver

import (
	"context"
	"fmt"
	"sync"
)

type threadRuns struct {
	mu             sync.Mutex
	active         map[*threadRun]struct{}
	drained        chan struct{} // Set on first settlement and closed after all runs report.
	cleanupFailure string        // Retained even for runs that exited before settlement began.
}

type threadRun struct{ proc Proc }

// Settle closes admission for the thread and requests cancellation of its active runs.
// The gate and stop requests survive a caller timeout. Another call can wait for confirmation.
func (d *Direct) Settle(ctx context.Context, threadID string) (SettleReport, error) {
	thread := d.thread(threadID)
	drained := thread.beginSettle()
	select {
	case <-drained:
		return SettleReport{
			HostProcessesStopped: thread.cleanupFailure == "",
			Reason:               thread.cleanupFailure,
		}, nil
	case <-ctx.Done():
		return SettleReport{Reason: "process cleanup could not be confirmed before the wait ended"},
			fmt.Errorf("settle host processes: %w", ctx.Err())
	}
}

// beginSettle closes admission and starts every stop request before waiting for any exit.
func (t *threadRuns) beginSettle() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.drained == nil {
		t.drained = make(chan struct{})
		for run := range t.active {
			// recordExit reads the authoritative cleanup result independently of the stop request.
			go func() { _ = run.proc.Cancel(context.Background()) }()
		}
		if len(t.active) == 0 {
			close(t.drained)
		}
	}
	return t.drained
}

// recordExit releases successful runs and preserves any failed cleanup for future settlement.
func (t *threadRuns) recordExit(run *threadRun) {
	info, err := run.proc.Wait(context.Background())
	t.mu.Lock()
	defer t.mu.Unlock()
	if !info.DescendantsCleaned && t.cleanupFailure == "" {
		reason := info.CleanupReason
		if reason == "" && err != nil {
			reason = err.Error()
		}
		if reason == "" {
			reason = "supervisor did not confirm descendant cleanup"
		}
		t.cleanupFailure = fmt.Sprintf("process %s: %s", run.proc.ID(), reason)
	}
	delete(t.active, run)
	if t.drained != nil && len(t.active) == 0 {
		// Closing drained publishes the final failure, which cannot change once admission is closed.
		close(t.drained)
	}
}
