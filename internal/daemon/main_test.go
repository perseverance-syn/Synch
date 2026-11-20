package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestMain makes every git process, the tests' and the code under test's, ignore
// the host's ~/.gitconfig and system config (card 111 H4). It also lets the test
// binary act as a long-running gate command when SYNCH_GATE_SLEEP is set.
func TestMain(m *testing.M) {
	if os.Getenv("SYNCH_GATE_SLEEP") == "1" {
		time.Sleep(time.Minute)
		os.Exit(0)
	}
	dir, err := os.MkdirTemp("", "hermetic-git-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	empty := filepath.Join(dir, "gitconfig")
	if err := os.WriteFile(empty, nil, 0600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for k, v := range map[string]string{"GIT_CONFIG_GLOBAL": empty, "GIT_CONFIG_SYSTEM": empty, "GIT_CONFIG_NOSYSTEM": "1"} {
		_ = os.Setenv(k, v)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}
