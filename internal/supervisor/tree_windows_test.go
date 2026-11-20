//go:build windows

package supervisor

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// TestWindowsJobDescendantCleanup proves AC4 by checking descendant cleanup in a Windows job.
func TestWindowsJobDescendantCleanup(t *testing.T) { verifyTree(t) }

// TestWindowsPipeHelper duplicates a writer into the test process outside the job for the G4 drain regression.
func TestWindowsPipeHelper(t *testing.T) {
	path := os.Getenv("SYNCH_PIPE_HANDLE")
	if path == "" {
		return
	}
	parent, err := syscall.OpenProcess(0x40, false, uint32(os.Getppid()))
	if err != nil {
		t.Fatal(err)
	}
	defer releaseHandle(parent)
	self, err := syscall.GetCurrentProcess()
	if err != nil {
		t.Fatal(err)
	}
	var writer syscall.Handle
	if err := syscall.DuplicateHandle(self, syscall.Handle(os.Stdout.Fd()), parent, &writer, 0, false, 2); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(fmt.Sprint(uintptr(writer))), 0600); err != nil {
		t.Fatal(err)
	}
}

// TestWindowsHeldPipeBound proves G4 by observing a terminal fact even with an external writer holding stdout open.
func TestWindowsHeldPipeBound(t *testing.T) {
	r := &recorder{}
	s := setup(t, r, func(c *Config) { c.DrainTimeout = 100 * time.Millisecond })
	path := s.cfg.DataDir + "/writer"
	sp := spec("held", "")
	sp.Args = []string{"-test.run=^TestWindowsPipeHelper$"}
	sp.Env = append(sp.Env, "SYNCH_PIPE_HANDLE="+path)
	p := spawn(t, s, sp)
	info, err := wait(t, p)
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	h, e := strconv.ParseUint(string(data), 10, 64)
	if e != nil {
		t.Fatal(e)
	}
	releaseHandle(syscall.Handle(h))
	if !errors.Is(err, ErrDrainTimeout) || info.DescendantsCleaned || len(r.of("process.exited")) != 1 {
		t.Fatal(info, err)
	}
}
