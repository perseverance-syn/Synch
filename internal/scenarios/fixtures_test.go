package scenarios

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runExistingFixture reuses a named package fixture and refuses a zero-test pass.
func runExistingFixture(t *testing.T, pkg, pattern string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "go", "test", pkg, "-run", pattern, "-count=1", "-v")
	cmd.Dir = filepath.Join("..", "..")
	cmd.Env = append(os.Environ(), "GOPROXY=off", "GOSUMDB=off")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "--- PASS:") || strings.Contains(string(out), "no tests to run") {
		t.Fatalf("fixture %s: %v\n%s", pkg, err, out)
	}
	t.Logf("%s", out)
}

// TestChildCancellationLimitsResultAndHandoff reuses existing lifecycle fixtures.
func TestChildCancellationLimitsResultAndHandoff(t *testing.T) {
	runExistingFixture(t, "./internal/daemon", "^Test(CancelParentCancelsChildFirst|ChildDepthLimit|CoordinatorDispatchesBuilderChild|HarnessSwitchHandoff)$")
	runExistingFixture(t, "./internal/children", "^Test(DepthLimit|SimultaneousDispatchesKeepExcessQueued|AttemptExhaustion|IndependentAndDependentChildren)$")
	t.Log("partial: lifecycle assertions are separate fixtures; harness pin change uses private state")
}

// TestModeSwitchWithActiveChildAndRunningProcess reuses the supervised mode-switch fixture.
func TestModeSwitchWithActiveChildAndRunningProcess(t *testing.T) {
	runExistingFixture(t, "./internal/daemon", "^TestModeSwitchSettlesAndRevokes$")
}

// TestOwnedRunnerContinuesAndReplaysAfterDisconnect reuses the in-process SSH replay fixture.
func TestOwnedRunnerContinuesAndReplaysAfterDisconnect(t *testing.T) {
	runExistingFixture(t, "./internal/remote", "^TestDisconnectReplay$")
	t.Log("partial: engine launchers require the Direct driver")
}
