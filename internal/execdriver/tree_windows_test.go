package execdriver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// TestProtocolCancelCleansWindowsTree verifies AC6b retains Job Object cleanup with raw stdout open.
func TestProtocolCancelCleansWindowsTree(t *testing.T) {
	driver, sink := newIntegrationDriver(t)
	path := filepath.Join(t.TempDir(), "pids")
	spec := helperSpec(t, "tree", path)
	spec.ProtocolIO = true
	proc, err := driver.Spawn(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	in, out := proc.ProtocolStreams()
	defer func() { _ = in.Close(); _ = out.Close() }()
	waitForGate(t, path+".started")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rootPID, childPID uint32
	if _, err := fmt.Sscanf(string(data), "%d %d", &rootPID, &childPID); err != nil {
		t.Fatal(err)
	}
	root := openLiveProcess(t, rootPID)
	child := openLiveProcess(t, childPID)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := proc.Cancel(ctx); err != nil {
		t.Fatal(err)
	}
	for _, handle := range []syscall.Handle{root, child} {
		status, err := syscall.WaitForSingleObject(handle, 0)
		if err != nil || status != syscall.WAIT_OBJECT_0 {
			t.Fatalf("process handle %v still live after Cancel: status=%v, error=%v", handle, status, err)
		}
	}
	info := waitForExit(t, proc)
	if info.ExecutionOutcome != "cancelled" || !info.DescendantsCleaned {
		t.Fatalf("unexpected cancellation result: %+v", info)
	}
	sink.checkOnlyExit(t, spec, info)
}

// TestSettleCleansWindowsProcessTree verifies AC4 and AC5 against live child and grandchild handles.
func TestSettleCleansWindowsProcessTree(t *testing.T) {
	driver, sink := newIntegrationDriver(t)
	path := filepath.Join(t.TempDir(), "pids")
	spec := helperSpec(t, "tree", path)
	proc, err := driver.Spawn(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	waitForGate(t, path+".started")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var childPID, grandchildPID uint32
	if _, err := fmt.Sscanf(string(data), "%d %d", &childPID, &grandchildPID); err != nil {
		t.Fatalf("read process IDs from %q: %v", data, err)
	}
	child := openLiveProcess(t, childPID)
	grandchild := openLiveProcess(t, grandchildPID)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	report, err := driver.Settle(ctx, spec.ThreadID)
	if err != nil || !report.HostProcessesStopped {
		t.Fatalf("Settle() = %+v, %v", report, err)
	}
	for _, handle := range []syscall.Handle{child, grandchild} {
		status, err := syscall.WaitForSingleObject(handle, 0)
		if err != nil || status != syscall.WAIT_OBJECT_0 {
			t.Fatalf("process handle %v still live after Settle: status=%v, error=%v", handle, status, err)
		}
	}
	info := waitForExit(t, proc)
	if info.ExecutionOutcome != "cancelled" || info.Work != "lost" || !info.DescendantsCleaned {
		t.Fatalf("unexpected exit during settlement: %+v", info)
	}
	sink.checkOnlyExit(t, spec, info)
}

// openLiveProcess holds a Windows process handle to verify the same process exits even if its PID is reused.
func openLiveProcess(t *testing.T, pid uint32) syscall.Handle {
	t.Helper()
	handle, err := syscall.OpenProcess(syscall.SYNCHRONIZE, false, pid)
	if err != nil {
		t.Fatalf("open process %d: %v", pid, err)
	}
	t.Cleanup(func() {
		if err := syscall.CloseHandle(handle); err != nil {
			t.Error(err)
		}
	})
	status, err := syscall.WaitForSingleObject(handle, 0)
	if err != nil || status != syscall.WAIT_TIMEOUT {
		t.Fatalf("process %d was not live: status=%v, error=%v", pid, status, err)
	}
	return handle
}
