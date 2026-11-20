package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"synch/internal/api"
	"synch/internal/cli"
	"synch/internal/daemon"
)

// run routes the runtime entry points to dispatch and every other command to the
// operator CLI, which talks to the daemon over the local API.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 {
		switch args[0] {
		case "daemon", "runner", "agent":
		default:
			return cli.Main(args, stdout, stderr, os.Getenv)
		}
	}
	return dispatch(args, stderr)
}

// dispatch selects the runtime entry point and returns its process exit status.
func dispatch(args []string, stderr io.Writer) int {
	message := "usage: synch <daemon|runner|agent>"
	status := 2
	if len(args) == 1 {
		switch args[0] {
		case "daemon":
			if err := runDaemon(); err != nil {
				_, _ = fmt.Fprintln(stderr, err)
				return 1
			}
			return 0
		case "runner", "agent":
			message = "not implemented"
			status = 1
		}
	}
	if _, err := fmt.Fprintln(stderr, message); err != nil {
		return 1
	}
	return status
}

// runDaemon loads local settings and ties clean shutdown to the OS signals.
func runDaemon() error {
	token := os.Getenv("SYNCH_TOKEN")
	if token == "" {
		return fmt.Errorf("SYNCH_TOKEN is required to authenticate local clients")
	}
	dataDir := os.Getenv("SYNCH_DATA_DIR")
	if dataDir == "" {
		configDir, err := os.UserConfigDir()
		if err != nil {
			return fmt.Errorf("locate daemon data directory: %w", err)
		}
		dataDir = filepath.Join(configDir, "synch")
	}
	tokens := api.NewMemoryTokens()
	tokens.Set(token, "operator:local")
	cfg, err := daemonConfig(os.Getenv, dataDir, tokens)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	d, err := daemon.New(cfg)
	if err != nil {
		return err
	}
	return d.Run(ctx)
}

// daemonConfig reads the optional SYNCH_* settings documented in doc.go. Every
// one is optional: with only SYNCH_TOKEN set the daemon starts and pauses
// threads with no_eligible_pair until a harness is qualified and a default is set.
func daemonConfig(getenv func(string) string, dataDir string, tokens api.TokenVerifier) (daemon.Config, error) {
	cfg := daemon.Config{
		DataDir: dataDir, Tokens: tokens, Listen: getenv("SYNCH_LISTEN"), PolicyStore: getenv("SYNCH_POLICY_STORE"),
		CodexBinary: getenv("SYNCH_CODEX_BIN"), ClaudeBinary: getenv("SYNCH_CLAUDE_BIN"), KimiBinary: getenv("SYNCH_KIMI_BIN"),
		DefaultHarness: getenv("SYNCH_DEFAULT_HARNESS"), DefaultModel: getenv("SYNCH_DEFAULT_MODEL"),
	}
	if raw := getenv("SYNCH_PROJECTS"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &cfg.Projects); err != nil {
			return daemon.Config{}, fmt.Errorf("SYNCH_PROJECTS must be a JSON object of project id to {dir, gate_file}: %w", err)
		}
	}
	// Values are validated when the daemon starts, which refuses a bad one with a clear error.
	cfg.ChildMaxRuntime = daemon.Setting(getenv("SYNCH_CHILD_MAX_RUNTIME"))
	cfg.ChildLoopRepeats = daemon.Setting(getenv("SYNCH_CHILD_LOOP_REPEATS"))
	return cfg, nil
}

// main passes process arguments to the entry-point dispatcher.
func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
