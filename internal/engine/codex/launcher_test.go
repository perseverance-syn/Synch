package codex

import (
	"bytes"
	"context"
	"io"
	"os"
	"reflect"
	"slices"
	"testing"

	"synch/internal/execdriver"
)

// TestDirectLauncher verifies AC6/AC7 use one executable and attach the protocol streams at spawn.
func TestDirectLauncher(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	driver := &driverSpy{}
	launcher, err := NewDirectLauncher(driver, binary)
	if err != nil {
		t.Fatal(err)
	}
	req := fixtureRequest()
	req.MCPEndpoint = "http://127.0.0.1:9123/mcp"
	req.Env = map[string]string{"EXAMPLE_REGION": "test"}
	version, err := launcher.Version(t.Context(), req)
	if err != nil || version != "codex-cli 0.169.0" {
		t.Fatalf("Version() = %q, %v", version, err)
	}
	transport, err := launcher.Launch(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(transport, "request\n"); err != nil {
		t.Fatal(err)
	}
	reply, err := io.ReadAll(transport)
	if err != nil || string(reply) != "reply\n" || driver.input.String() != "request\n" {
		t.Fatalf("launcher did not preserve the process streams: %q, %v", reply, err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if len(driver.specs) != 2 || driver.specs[0].ID == driver.specs[1].ID {
		t.Fatalf("unexpected process specs: %+v", driver.specs)
	}
	want := [][]string{
		{"--version"},
		{"app-server", "-c", "mcp_servers.synch.url=\"http://127.0.0.1:9123/mcp\"", "-c", "mcp_servers.synch.required=true"},
	}
	for i, spec := range driver.specs {
		if spec.Path != binary || spec.Dir != req.Workdir || spec.ThreadID != req.ThreadID || !spec.ProtocolIO ||
			!reflect.DeepEqual(spec.Args, want[i]) || !slices.Contains(spec.Env, "EXAMPLE_REGION=test") {
			t.Fatalf("launch %d used unexpected process spec", i)
		}
	}
}

type driverSpy struct {
	execdriver.ExecutionDriver
	specs []execdriver.ProcSpec
	input bufferCloser
}

// Kind identifies the test launcher boundary as Direct.
func (*driverSpy) Kind() string { return execdriver.KindDirect }

// Describe identifies the test target as Host access.
func (*driverSpy) Describe() execdriver.DriverInfo {
	return execdriver.DriverInfo{Label: "Host access"}
}

// Spawn records the launch arguments and supplies protocol streams without launching Codex.
func (d *driverSpy) Spawn(_ context.Context, spec execdriver.ProcSpec) (execdriver.Proc, error) {
	d.specs = append(d.specs, spec)
	output := "reply\n"
	if slices.Contains(spec.Args, "--version") {
		output = "codex-cli 0.169.0\n"
	}
	return &processStub{in: &d.input, out: io.NopCloser(bytes.NewBufferString(output))}, nil
}

type processStub struct {
	execdriver.Proc
	in  io.WriteCloser
	out io.ReadCloser
}

// ProtocolStreams returns the exact streams the launcher must wrap.
func (p *processStub) ProtocolStreams() (io.WriteCloser, io.ReadCloser) { return p.in, p.out }

// Wait reports a normal, persisted process exit for the version probe.
func (*processStub) Wait(context.Context) (execdriver.ExitInfo, error) {
	return execdriver.ExitInfo{ExecutionOutcome: "exited", DescendantsCleaned: true}, nil
}

type bufferCloser struct{ bytes.Buffer }

// Close finishes fixture stdin without discarding the captured request.
func (*bufferCloser) Close() error { return nil }
