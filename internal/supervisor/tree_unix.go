//go:build linux || darwin

package supervisor

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

type processTree struct {
	pid      int
	creation string
}

// startTree starts the child as a process-group leader so descendants can be signaled together.
func startTree(cmd *exec.Cmd) (processTree, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return processTree{}, err
	}
	birth, _, err := identify(cmd.Process.Pid)
	if err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
		return processTree{}, err
	}
	return processTree{cmd.Process.Pid, birth}, nil
}

// interrupt sends a best-effort SIGINT to the child's process group.
func (t processTree) interrupt() { _ = syscall.Kill(-t.pid, syscall.SIGINT) }

// clean sends SIGKILL to the process group and polls for its disappearance within a fixed bound.
func (t processTree) clean() error {
	if err := syscall.Kill(-t.pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		return fmt.Errorf("kill process group: %w", err)
	}
	// Bound the wait for the process group to disappear before reporting cleanup failure.
	deadline := time.Now().Add(10 * time.Second)
	for {
		err := syscall.Kill(-t.pid, 0)
		if err == syscall.ESRCH {
			return nil
		}
		if err != nil {
			return fmt.Errorf("probe process group: %w", err)
		}
		if time.Now().After(deadline) {
			return errors.New("process group still present")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// processAlive probes a PID with signal zero, treating a permissions error as evidence it exists.
func processAlive(pid int) (bool, error) {
	if pid <= 0 {
		return false, errors.New("invalid pid")
	}
	err := syscall.Kill(pid, 0)
	if err == syscall.ESRCH {
		return false, nil
	}
	if err == syscall.EPERM {
		return true, nil
	}
	return err == nil, err
}

// freeSpace reports unprivileged available bytes and total bytes on the data volume.
func freeSpace(path string) (uint64, uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}
	return uint64(stat.Bavail) * uint64(stat.Bsize), uint64(stat.Blocks) * uint64(stat.Bsize), nil
}
