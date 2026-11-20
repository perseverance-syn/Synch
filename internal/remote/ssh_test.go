package remote

import (
	"errors"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"synch/internal/policy"
)

// TestPinnedSSHAdmission proves S1 blocks unknown and changed keys before credentials or work.
func TestPinnedSSHAdmission(t *testing.T) {
	f := openFixture(t)
	ctx := testContext(t)
	unknown := f.target
	unknown.Fingerprint = ""
	if err := f.registry.Register(unknown); err != nil {
		t.Fatal(err)
	}
	var review *HostKeyReviewError
	if err := f.driver.Reconnect(ctx); !errors.As(err, &review) || review.Expected != "" || review.Presented != f.target.Fingerprint {
		t.Fatalf("unknown key: %+v, %v", review, err)
	}
	if f.broker.authCalls.Load() != 0 {
		t.Fatal("broker accessed before accepting the host key")
	}
	if err := f.registry.Register(f.target); err != nil {
		t.Fatal(err)
	}
	if err := f.driver.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	authCount := f.broker.authCalls.Load()
	keys, err := f.broker.auth.List()
	if err != nil || len(keys) != 1 {
		t.Fatalf("authentication key: %v", err)
	}
	changed := f.rotateHost(t, keys[0])
	if err := f.driver.Reconnect(ctx); !errors.As(err, &review) || review.Expected != f.target.Fingerprint || review.Presented != changed {
		t.Fatalf("changed key: %+v, %v", review, err)
	}
	if f.broker.authCalls.Load() != authCount || f.driver.Status().Connected {
		t.Fatal("changed key reached authentication or admission")
	}
}

// TestTargetOperations proves S1 refuses an unregistered operation on an authenticated link.
func TestTargetOperations(t *testing.T) {
	f := openFixture(t)
	ctx := testContext(t)
	target := f.target
	target.Operations = []string{OpReplay}
	if err := f.registry.Register(target); err != nil {
		t.Fatal(err)
	}
	target.Operations[0] = OpSpawn
	if err := f.driver.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.driver.Spawn(ctx, childSpec(t, "agent", "")); !errors.Is(err, ErrOperationDenied) {
		t.Fatalf("unregistered spawn: %v", err)
	}
	if f.policy.calls.Load() != 0 {
		t.Fatal("an unregistered action reached policy")
	}
}

// TestBrokeredAgent proves S2 authenticates through the broker and forwards only a scoped granted agent.
func TestBrokeredAgent(t *testing.T) {
	f := openFixture(t)
	ctx := testContext(t)
	if err := f.driver.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	noGrant := childSpec(t, "agent", "")
	if _, err := f.driver.Dispatch(ctx, noGrant, DispatchOptions{Forwarding: testGrant(false)}); !errors.Is(err, ErrOperationDenied) {
		t.Fatalf("denied forwarding grant: %v", err)
	}
	std, err := f.driver.Spawn(ctx, noGrant)
	if err != nil {
		t.Fatal(err)
	}
	exit, err := std.Wait(ctx)
	if err != nil || exit.ExitCode != 0 || !strings.Contains(outputFor(t, f, std.ID()), "agent=disabled") {
		t.Fatalf("default dispatch did not disable forwarding: %+v, %v", exit, err)
	}
	if f.broker.forwardingCalls.Load() != 0 {
		t.Fatal("forwarding broker used without grant")
	}
	scopedKeys, err := f.broker.forward.List()
	if err != nil || len(scopedKeys) != 1 {
		t.Fatalf("scoped agent setup: %v", err)
	}
	scoped, err := f.driver.Dispatch(ctx, childSpec(t, "agent", ""), DispatchOptions{Forwarding: testGrant(true)})
	if err != nil {
		t.Fatal(err)
	}
	exit, err = scoped.Wait(ctx)
	output := outputFor(t, f, scoped.ID())
	if err != nil || exit.ExitCode != 0 || !strings.Contains(output, "agent="+ssh.FingerprintSHA256(scopedKeys[0])) {
		t.Fatalf("granted process did not see the scoped agent: %+v, %v, %q", exit, err, output)
	}
	if f.broker.authCalls.Load() < 2 || f.broker.forwardingCalls.Load() != 1 {
		t.Fatal("expected authenticated primary and scoped forwarding connections")
	}
}

// TestDisconnectedAdmission proves S5 maps new work and fresh approval to T19 without consulting policy.
func TestDisconnectedAdmission(t *testing.T) {
	f := openFixture(t)
	ctx := testContext(t)
	if err := f.driver.Reconnect(ctx); err != nil {
		t.Fatal(err)
	}
	f.cutNetwork()
	waitFor(t, ctx, "SSH disconnect", func() bool { return !f.driver.Status().Connected })
	_, spawnErr := f.driver.Spawn(ctx, childSpec(t, "agent", ""))
	_, approvalErr := f.driver.DoFresh(ctx, policy.Request{Thread: testThread, Action: "remote.approve"})
	for _, err := range []error{spawnErr, approvalErr} {
		var disconnected *DisconnectedError
		if !errors.As(err, &disconnected) || disconnected.PauseReason() != "runner_disconnected" {
			t.Fatalf("want T19 admission error, got %v", err)
		}
	}
	if f.policy.calls.Load() != 0 {
		t.Fatal("offline admission unexpectedly reached policy")
	}
}

// outputFor reads the committed output payloads for one assignment.
func outputFor(t *testing.T, f *remoteFixture, id string) string {
	t.Helper()
	events, err := f.log.ReadStream("thread:"+testThread, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	for _, event := range events {
		fact, ok := delivery(event)
		if ok && fact.Assignment == id && event.Type == "engine.output" {
			out.WriteString(fact.Chunk)
		}
	}
	return out.String()
}
