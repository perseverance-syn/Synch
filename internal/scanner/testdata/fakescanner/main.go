package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"time"
)

// main emits a fixture only after recording execution and checking the tool invocation.
func main() {
	args := os.Args[1:]
	if os.Getenv("SYNCH_FAKE_HOLD") != "" {
		hold, _ := time.ParseDuration(os.Getenv("SYNCH_FAKE_HOLD"))
		time.Sleep(hold)
		return
	}
	if err := os.WriteFile(os.Getenv("SYNCH_FAKE_MARKER"), []byte("executed"), 0600); err != nil {
		os.Exit(2)
	}
	output, err := os.ReadFile(os.Getenv("SYNCH_FAKE_FIXTURE"))
	if err != nil {
		os.Exit(2)
	}
	switch args[0] {
	case "filesystem":
		if !slices.Contains(args, "vuln,misconfig") || !slices.Contains(args, "--offline-scan") ||
			!slices.Contains(args, "json") || !slices.Contains(args, "memory") ||
			args[len(args)-1] != os.Getenv("SYNCH_FAKE_SOURCE") {
			os.Exit(2)
		}
		if _, err := os.Stdout.Write(output); err != nil {
			os.Exit(2)
		}
	case "detect":
		index := slices.Index(args, "--report-path")
		source := slices.Index(args, "--source")
		if !slices.Contains(args, "--redact") || !slices.Contains(args, "--no-git") ||
			!slices.Contains(args, "json") || index < 0 || index+1 >= len(args) ||
			source < 0 || source+1 >= len(args) || args[source+1] != os.Getenv("SYNCH_FAKE_SOURCE") {
			os.Exit(2)
		}
		path := args[index+1]
		cwd, err := os.Getwd()
		if err != nil || filepath.Dir(path) != cwd || os.WriteFile(path, output, 0600) != nil {
			os.Exit(2)
		}
	default:
		os.Exit(2)
	}
	if os.Getenv("SYNCH_FAKE_GRANDCHILD") != "" {
		// The holder is a separate copy so it does not lock the staged scanner in scratch.
		child := exec.Command(os.Getenv("SYNCH_FAKE_GRANDCHILD"))
		child.Env = append(os.Environ(), "SYNCH_FAKE_HOLD=3s")
		child.Dir = os.TempDir() // a cwd inside scratch would pin the directory
		child.Stdout = os.Stdout
		if child.Start() != nil {
			os.Exit(2)
		}
	}
	if sleep, err := time.ParseDuration(os.Getenv("SYNCH_FAKE_SLEEP")); err == nil {
		time.Sleep(sleep)
	}
	code, err := strconv.Atoi(os.Getenv("SYNCH_FAKE_EXIT"))
	if err != nil {
		os.Exit(2)
	}
	os.Exit(code)
}
