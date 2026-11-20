package main

import (
	"context"
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	d, err := daemon.New(daemon.Config{
		DataDir: dataDir, Tokens: tokens, Listen: os.Getenv("SYNCH_LISTEN"), PolicyStore: os.Getenv("SYNCH_POLICY_STORE"),
	})
	if err != nil {
		return err
	}
	return d.Run(ctx)
}

// main passes process arguments to the entry-point dispatcher.
func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
