package supervisor

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// identify reads Linux process state and start ticks from procfs.
// The boot ID distinguishes equal tick counts across boots. The lookup requires mounted procfs.
func identify(pid int) (string, bool, error) {
	if pid <= 0 {
		return "", false, errors.New("invalid pid")
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", false, err
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	// comm can contain spaces and parentheses. Fields after its final ')' start at #3.
	end := strings.LastIndexByte(string(b), ')')
	fields := strings.Fields(string(b[end+1:]))
	if end < 0 || len(fields) < 20 {
		return "", false, errors.New("malformed process stat")
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return "", false, err
	}
	return fmt.Sprintf("%s/%d", strings.TrimSpace(string(boot)), ticks), fields[0] != "Z" && fields[0] != "X", nil
}
