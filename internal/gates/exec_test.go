package gates

import (
	"context"
	"strings"
	"testing"
)

// TestExecExecutorRunsCommand proves G2's local executor runs a real command,
// reports its exit code and captures stdout for version probes.
func TestExecExecutorRunsCommand(t *testing.T) {
	executor := ExecExecutor{}
	code, stdout, err := executor.Run(context.Background(), Check{ID: "probe", Command: []string{"go", "version"}, Timeout: 60})
	if err != nil || code != 0 {
		t.Fatalf("go version = %d, %q, %v", code, stdout, err)
	}
	if !strings.Contains(stdout, "go version") {
		t.Fatalf("stdout = %q", stdout)
	}

	// A nonzero exit is a result, not an error.
	code, _, err = executor.Run(context.Background(), Check{ID: "fail", Command: []string{"go", "bad-subcommand-xyz"}, Timeout: 60})
	if err != nil {
		t.Fatalf("nonzero exit became an error: %v", err)
	}
	if code == 0 {
		t.Fatal("unknown subcommand exited 0")
	}

	// A command that cannot start is an error.
	_, _, err = executor.Run(context.Background(), Check{ID: "missing", Command: []string{"definitely-not-a-real-binary-xyz"}, Timeout: 60})
	if err == nil || !strings.Contains(err.Error(), "start") {
		t.Fatalf("start failure = %v", err)
	}
}
