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
