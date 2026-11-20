package supervisor

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

// identify reads the BSD start timeval and state from Darwin proc_info.
// PROC_PIDTBSDINFO uses the same layout on both supported 64-bit architectures.
func identify(pid int) (string, bool, error) {
	if pid <= 0 {
		return "", false, errors.New("invalid pid")
	}
	var bsd struct {
		Flags, Status         uint32
		Other                 [112]byte
		Seconds, Microseconds uint64
	}
	n, _, errno := syscall.Syscall6(336, 2, uintptr(pid), 3, 0, uintptr(unsafe.Pointer(&bsd)), unsafe.Sizeof(bsd))
	if errno == syscall.ESRCH {
		return "", false, nil
	}
	if errno != 0 {
		return "", false, errno
	}
	if n != unsafe.Sizeof(bsd) {
		return "", false, errors.New("incomplete proc_bsdinfo")
	}
	return fmt.Sprintf("%d.%06d", bsd.Seconds, bsd.Microseconds), bsd.Status != 5, nil
}
