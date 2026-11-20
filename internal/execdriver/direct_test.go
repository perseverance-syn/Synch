package execdriver

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type grantFunc func(context.Context, string, string, string) (bool, error)

// HasFullControl calls the test's grant checker with the requested scope.
func (f grantFunc) HasFullControl(ctx context.Context, thread, host, identity string) (bool, error) {
	return f(ctx, thread, host, identity)
}

// TestHostAccessDescription verifies AC1 and AC2, including every user-facing string.
func TestHostAccessDescription(t *testing.T) {
	var driver ExecutionDriver = &Direct{host: "host-a", identity: "account-a"}
	if driver.Kind() != "direct" || KindRemote != "remote" || KindSandbox != "sandbox" || ContractVersion != "1.1.0" {
		t.Fatal("unexpected execution driver kinds or contract version")
	}
	info := driver.Describe()
	want := DriverInfo{Label: "Host access", Host: "host-a", Identity: "account-a", EvidenceIntegrity: "limited to that host account"}
	if !reflect.DeepEqual(info, want) {
		t.Fatalf("Describe() = %+v, want %+v", info, want)
	}
	for _, value := range []string{info.Label, info.Host, info.Identity, info.EvidenceIntegrity} {
		if strings.Contains(value, "Direct") {
			t.Fatalf("internal kind appears in user-facing field %q", value)
		}
	}
}

// TestSpawnRechecksGrant verifies AC3 by revoking a grant after the first launch.
func TestSpawnRechecksGrant(t *testing.T) {
	checks, starts := 0, 0
	driver := &Direct{host: "host-a", identity: "account-a"}
	driver.grants = grantFunc(func(_ context.Context, thread, host, identity string) (bool, error) {
		checks++
		if thread != "thread-a" || host != "host-a" || identity != "account-a" {
			t.Fatalf("unexpected grant scope (%q, %q, %q)", thread, host, identity)
		}
		return checks == 1, nil
	})
	run := newTestProcess()
	driver.start = func(spec ProcSpec) (Proc, error) {
		starts++
		if spec.ID != "first" {
			t.Fatalf("started unexpected process %q", spec.ID)
		}
		return run, nil
	}
	ctx := t.Context()
	if _, err := driver.Spawn(ctx, ProcSpec{ID: "first", ThreadID: "thread-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Spawn(ctx, ProcSpec{ID: "second", ThreadID: "thread-a"}); !errors.Is(err, ErrFullControlRequired) {
		t.Fatalf("Spawn() after revocation = %v, want ErrFullControlRequired", err)
	}
	if checks != 2 || starts != 1 {
		t.Fatalf("got %d grant checks and %d starts, want 2 and 1", checks, starts)
	}
	close(run.done)
}
