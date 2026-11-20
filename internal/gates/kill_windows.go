//go:build windows

package gates

import (
	"os/exec"
	"strconv"
)

// configureProcessTree makes cancellation kill the command and every
// descendant with taskkill /T, falling back to killing only the command.
func configureProcessTree(cmd *exec.Cmd) {
	cmd.Cancel = func() error {
		kill := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid))
		if err := kill.Run(); err != nil {
			return cmd.Process.Kill()
		}
		return nil
	}
}
