package main

import (
	"bytes"
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
