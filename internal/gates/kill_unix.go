//go:build !windows

package gates

import (
	"os/exec"
	"syscall"
)

// configureProcessTree starts the command in its own process group and makes
// cancellation kill the whole group, so descendants cannot outlive the check.
func configureProcessTree(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
