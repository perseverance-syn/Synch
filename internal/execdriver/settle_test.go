package execdriver

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

type testProcess struct {
	done      chan struct{}
	cancelled chan struct{}
	info      ExitInfo
}

// newTestProcess creates a controllable process with successful cleanup by default.
func newTestProcess() *testProcess {
	return &testProcess{
		done: make(chan struct{}), cancelled: make(chan struct{}),
		info: ExitInfo{DescendantsCleaned: true},
	}
}

// ID returns the test run identifier.
func (*testProcess) ID() string { return "run" }

// Subscribe returns an empty output stream for the test process.
func (*testProcess) Subscribe(int) (<-chan OutputChunk, func()) { return nil, func() {} }

// ProtocolStreams returns no streams because settlement fixtures use capture mode.
func (*testProcess) ProtocolStreams() (io.WriteCloser, io.ReadCloser) { return nil, nil }

// Progress is a no-op for the test process, which has no stall timer.
func (*testProcess) Progress() {}

// Cancel records the stop request and waits until the test releases cleanup.
func (p *testProcess) Cancel(ctx context.Context) error {
	close(p.cancelled)
	_, err := p.Wait(ctx)
	return err
}

// Wait returns the test-provided exit after cleanup is released.
func (p *testProcess) Wait(ctx context.Context) (ExitInfo, error) {
	select {
	case <-p.done:
		return p.info, nil
	case <-ctx.Done():
		return ExitInfo{}, ctx.Err()
	}
}

// TestSettleBlocksRacingSpawn verifies AC5 while a grant check is still in flight.
func TestSettleBlocksRacingSpawn(t *testing.T) {
	checking, release := make(chan struct{}), make(chan struct{})
	driver := &Direct{
		grants: grantFunc(func(ctx context.Context, _, _, _ string) (bool, error) {
			close(checking)
			select {
			case <-release:
				return true, nil
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}),
		start: func(ProcSpec) (Proc, error) { return nil, errors.New("unexpected start") },
	}
	result := make(chan error, 1)
	go func() {
		_, err := driver.Spawn(t.Context(), ProcSpec{ThreadID: "thread"})
		result <- err
	}()
	waitForTestSignal(t, checking)
	report, err := driver.Settle(t.Context(), "thread")
	close(release)
	if err != nil || !report.HostProcessesStopped {
		t.Fatalf("Settle() = %+v, %v", report, err)
	}
	if err := <-result; !errors.Is(err, ErrThreadSettling) {
		t.Fatalf("racing Spawn() = %v, want ErrThreadSettling", err)
	}
	if _, err := driver.Spawn(t.Context(), ProcSpec{ThreadID: "thread"}); !errors.Is(err, ErrThreadSettling) {
		t.Fatalf("later Spawn() = %v, want ErrThreadSettling", err)
	}
}

// TestSettleRequiresConfirmedCleanup verifies AC5 for a running tree and a terminal cleanup failure.
func TestSettleRequiresConfirmedCleanup(t *testing.T) {
	for _, cleaned := range []bool{true, false} {
		name := "unconfirmed"
		if cleaned {
			name = "confirmed"
		}
		t.Run(name, func(t *testing.T) {
			process := newTestProcess()
			process.info.DescendantsCleaned = cleaned
			if !cleaned {
				process.info.CleanupReason = "tree still present"
			}
			driver := &Direct{
				grants: grantFunc(func(context.Context, string, string, string) (bool, error) { return true, nil }),
				start:  func(ProcSpec) (Proc, error) { return process, nil },
			}
			if _, err := driver.Spawn(t.Context(), ProcSpec{ThreadID: "thread"}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			result := make(chan SettleReport, 1)
			go func() {
				report, _ := driver.Settle(ctx, "thread")
				result <- report
			}()
			waitForTestSignal(t, process.cancelled)
			cancel()
			if report := <-result; report.HostProcessesStopped || report.Reason == "" {
				t.Fatalf("Settle() while stopping = %+v, want an unconfirmed report with a reason", report)
			}
			close(process.done)
			report, err := driver.Settle(t.Context(), "thread")
			if err != nil || report.HostProcessesStopped != cleaned {
				t.Fatalf("Settle() after exit = %+v, %v, want cleanup=%v", report, err, cleaned)
			}
			if !cleaned && !strings.Contains(report.Reason, "tree still present") {
				t.Fatalf("Settle() lost supervisor cleanup reason: %+v", report)
			}
		})
	}
}

// waitForTestSignal bounds waits for asynchronous test milestones.
func waitForTestSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for process or grant check")
	}
}
