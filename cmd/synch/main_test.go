package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestDispatch proves AC3 keeps the runner and agent exit status and message.
func TestDispatch(t *testing.T) {
	for _, command := range []string{"runner", "agent"} {
		var output bytes.Buffer
		if code := dispatch([]string{command}, &output); code != 1 || output.String() != "not implemented\n" {
			t.Errorf("%s returned %d with output %q", command, code, output.String())
		}
	}
}

// TestDaemonDispatch proves AC3 reaches daemon configuration on the live route.
func TestDaemonDispatch(t *testing.T) {
	t.Setenv("SYNCH_TOKEN", "")
	var output bytes.Buffer
	if code := dispatch([]string{"daemon"}, &output); code != 1 || output.String() != "SYNCH_TOKEN is required to authenticate local clients\n" {
		t.Fatalf("daemon returned %d with output %q", code, output.String())
	}
}

// TestRunRoutesOperatorCommands proves the operator CLI is reached from the
// binary: a CLI command runs through internal/cli, while no arguments and the
// runtime entry points keep the dispatcher's behaviour.
func TestRunRoutesOperatorCommands(t *testing.T) {
	t.Setenv("SYNCH_TOKEN", "x")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"health", "--server", "http://example.com"}, &stdout, &stderr); code != 1 ||
		!strings.Contains(stderr.String(), "--allow-remote") {
		t.Fatalf("health returned %d with stderr %q, want the CLI's loopback refusal", code, stderr.String())
	}
	stderr.Reset()
	if code := run(nil, &stdout, &stderr); code != 2 || stderr.String() != "usage: synch <daemon|runner|agent>\n" {
		t.Fatalf("no arguments returned %d with stderr %q", code, stderr.String())
	}
	stderr.Reset()
	if code := run([]string{"runner"}, &stdout, &stderr); code != 1 || stderr.String() != "not implemented\n" {
		t.Fatalf("runner returned %d with stderr %q", code, stderr.String())
	}
}

// TestDaemonConfig proves D6: the SYNCH_* settings map into the daemon config,
// and a bare environment is a valid configuration.
func TestDaemonConfig(t *testing.T) {
	cfg, err := daemonConfig(func(string) string { return "" }, "data", nil)
	if err != nil || cfg.DataDir != "data" || cfg.DefaultHarness != "" || cfg.Projects != nil {
		t.Fatalf("bare config %+v, %v", cfg, err)
	}
	env := map[string]string{
		"SYNCH_CODEX_BIN": "/bin/codex", "SYNCH_CLAUDE_BIN": "/bin/claude", "SYNCH_KIMI_BIN": "/bin/kimi",
		"SYNCH_DEFAULT_HARNESS": "codex", "SYNCH_DEFAULT_MODEL": "gpt", "SYNCH_PROJECTS": `{"app":{"dir":"/src/app"}}`,
	}
	cfg, err = daemonConfig(func(k string) string { return env[k] }, "data", nil)
	if err != nil || cfg.CodexBinary != "/bin/codex" || cfg.ClaudeBinary != "/bin/claude" || cfg.KimiBinary != "/bin/kimi" ||
		cfg.DefaultHarness != "codex" || cfg.DefaultModel != "gpt" || cfg.Projects["app"].Dir != "/src/app" {
		t.Fatalf("config %+v, %v", cfg, err)
	}
	env["SYNCH_PROJECTS"] = "{"
	if _, err = daemonConfig(func(k string) string { return env[k] }, "data", nil); err == nil {
		t.Fatal("malformed SYNCH_PROJECTS accepted")
	}
}

// TestWatchdogConfig proves the env defaults and the per-project fields reach the daemon config.
func TestWatchdogConfig(t *testing.T) {
	env := map[string]string{"SYNCH_CHILD_MAX_RUNTIME": "90m", "SYNCH_CHILD_LOOP_REPEATS": "off",
		"SYNCH_PROJECTS": `{"app":{"dir":"/src","max_runtime":"45m","loop_repeats":8}}`}
	cfg, err := daemonConfig(func(k string) string { return env[k] }, "data", nil)
	if err != nil || cfg.ChildMaxRuntime != "90m" || cfg.ChildLoopRepeats != "off" ||
		cfg.Projects["app"].MaxRuntime != "45m" || cfg.Projects["app"].LoopRepeats != "8" {
		t.Fatalf("watchdog config %+v, %v", cfg, err)
	}
}
