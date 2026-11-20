package claudecode_test

import (
	"os"
	"path/filepath"
	"testing"

	"synch/internal/engine"
	"synch/internal/engine/claudecode"
	"synch/internal/engine/claudecode/stdio/fixturetest"
)

// claudeAdapter connects the real launch arguments and version gate to simulated Direct protocol streams.
func claudeAdapter(t *testing.T, proc *fixturetest.Proc, sink *fixturetest.Sink, policy chan<- claudecode.PermissionRequest, file string, qualified bool) (*claudecode.Adapter, *fixturetest.Driver) {
	t.Helper()
	version, err := os.ReadFile(filepath.Join("testdata", file))
	if err != nil {
		t.Fatal(err)
	}
	driver := &fixturetest.Driver{Proc: proc, Version: version}
	return claudeWithDriver(t, driver, sink, policy, fixturetest.Qualification(claudecode.Declaration(), qualified)), driver
}

// claudeWithDriver instantiates Claude Code through a fixture Direct driver with exact qualification data.
func claudeWithDriver(t *testing.T, driver *fixturetest.Driver, sink *fixturetest.Sink, policy chan<- claudecode.PermissionRequest, caps *engine.CapabilitySet) *claudecode.Adapter {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	launcher, err := claudecode.NewDirectLauncher(driver, binary)
	if err != nil {
		t.Fatal(err)
	}
	a, err := claudecode.New(launcher, sink, policy, caps)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
