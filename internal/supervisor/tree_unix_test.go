//go:build linux || darwin

package supervisor

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// TestUnixProcessGroupDescendantCleanup proves AC4 by checking descendant cleanup in a Unix process group.
func TestUnixProcessGroupDescendantCleanup(t *testing.T) { verifyTree(t) }

// TestUnixEscapeHelper creates a new session holding stdout for the G4 drain regression.
func TestUnixEscapeHelper(t *testing.T) {
	path := os.Getenv("SYNCH_ESCAPE_PID")
	if path == "" {
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelper$")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Env = append(os.Environ(), "SYNCH_HELPER=idle")
	cmd.Stdout = os.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(fmt.Sprint(cmd.Process.Pid)), 0600); err != nil {
		t.Fatal(err)
	}
}

// TestUnixEscapedPipeBound proves G4 by observing a terminal fact while an escaped descendant still owns stdout.
func TestUnixEscapedPipeBound(t *testing.T) {
	r := &recorder{}
	s := setup(t, r, func(c *Config) { c.DrainTimeout = 100 * time.Millisecond })
	path := s.cfg.DataDir + "/escape"
	sp := spec("setsid", "")
	sp.Args = []string{"-test.run=^TestUnixEscapeHelper$"}
	sp.Env = append(sp.Env, "SYNCH_ESCAPE_PID="+path)
	p := spawn(t, s, sp)
	info, err := wait(t, p)
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	pid, e := strconv.Atoi(string(data))
	if e != nil {
		t.Fatal(e)
	}
	if e = syscall.Kill(pid, syscall.SIGKILL); e != nil {
		t.Fatal(e)
	}
	if !errors.Is(err, ErrDrainTimeout) || info.DescendantsCleaned || len(r.of("process.exited")) != 1 {
		t.Fatal(info, err)
	}
}
