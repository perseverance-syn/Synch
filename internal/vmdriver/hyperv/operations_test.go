package hyperv

import (
	"context"
	"errors"
	"strings"
	"testing"

	"synch/internal/vmdriver"
)

// TestLifecycle proves all synchronous lifecycle cmdlets receive exact VM identities (W1/W4).
func TestLifecycle(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		cmdlet string
		call   func(*Driver, context.Context, vmdriver.ExternalRequest, string) (vmdriver.TaskRef, error)
	}{{"Start-VM", (*Driver).Start}, {"Stop-VM", (*Driver).Stop}, {"Remove-VM", (*Driver).Delete}} {
		t.Run(tc.cmdlet, func(t *testing.T) {
			calls := 0
			d := testDriver(t, func(_ context.Context, c Command, _ vmdriver.Credentials) ([]byte, error) {
				calls++
				if len(c.Arguments) != 1 || c.Arguments[0] != vmID ||
					!strings.Contains(c.Script, "Get-VM -Id") || !strings.Contains(c.Script, tc.cmdlet) {
					t.Fatalf("unexpected command: %+v", c)
				}
				return []byte("true"), nil
			}, nil)
			req := vmdriver.ExternalRequest{Endpoint: endpoint}
			if _, err := tc.call(d, ctx, req, "wildcard*"); !vmdriver.IsPreDispatch(err) || calls != 0 {
				t.Fatalf("invalid VM ref reached the runner: %v", err)
			}
			if task, err := tc.call(d, ctx, req, vmID); err != nil || task.ID != "" || calls != 1 {
				t.Fatalf("mutation = %+v, %v, calls %d", task, err, calls)
			}
		})
	}
}

// TestRestore verifies checkpoint identity, parent VM and ambiguity before restoring (W4).
func TestRestore(t *testing.T) {
	for _, tc := range []struct {
		name, fixture string
		restore       bool
	}{
		{"unique", "", true},
		{"missing", "[]", false},
		{"duplicate", `[{"ID":"` + cpID + `","VMId":"` + vmID + `","Name":"saved"},{"ID":"` + vmID + `","VMId":"` + vmID + `","Name":"saved"}]`, false},
		{"foreign VM", `[{"ID":"` + cpID + `","VMId":"` + cpID + `","Name":"saved"}]`, false},
		{"invalid ID", `[{"ID":"","VMId":"` + vmID + `","Name":"saved"}]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			d := testDriver(t, func(_ context.Context, c Command, _ vmdriver.Credentials) ([]byte, error) {
				calls++
				if calls == 1 {
					if !strings.Contains(c.Script, "Get-VMCheckpoint") || len(c.Arguments) != 1 || c.Arguments[0] != vmID {
						t.Fatal("expected checkpoint listing before restore")
					}
					if tc.fixture != "" {
						return []byte(tc.fixture), nil
					}
					return checkpointJSON("saved", vmID)
				}
				if !tc.restore || calls != 2 || !strings.Contains(c.Script, "Restore-VMSnapshot") ||
					!strings.Contains(c.Script, "$_.VMId -eq $vm.Id") || len(c.Arguments) != 3 ||
					c.Arguments[0] != vmID || c.Arguments[1] != cpID || c.Arguments[2] != "saved" {
					t.Fatalf("unexpected restore command: %+v", c)
				}
				return []byte("true"), nil
			}, nil)
			_, err := d.Restore(context.Background(), vmdriver.ExternalRequest{Endpoint: endpoint}, vmID, "saved")
			if tc.restore {
				if err != nil || calls != 2 {
					t.Fatalf("restore = %v, calls %d", err, calls)
				}
			} else if !errors.Is(err, vmdriver.ErrSnapshot) || !vmdriver.IsPreDispatch(err) || calls != 1 {
				t.Fatalf("restore refusal = %v, calls %d", err, calls)
			}
		})
	}
}

// TestRemotingAuth proves credentials use the separate runner input and failures are scrubbed (W3).
func TestRemotingAuth(t *testing.T) {
	ctx := context.Background()
	creds := vmdriver.Credentials{Username: "fake-user", Password: "fake-secret", Token: "unused-token"}
	calls := 0
	brokerFail := false
	d, err := New(map[string]Host{endpoint: {ComputerName: "hv.example.test"}},
		func(_ context.Context, c Command, auth vmdriver.Credentials) ([]byte, error) {
			calls++
			if auth.Username != creds.Username || auth.Password != creds.Password || auth.Token != "" ||
				c.ComputerName != "hv.example.test" || strings.Contains(c.Script+strings.Join(c.Arguments, ""), creds.Password) ||
				strings.Contains(c.Script+strings.Join(c.Arguments, ""), creds.Username) {
				t.Fatal("credentials escaped the separate auth argument")
			}
			return nil, errors.New(creds.Password)
		},
		vmdriver.BrokerFunc(func(_ context.Context, got string) (vmdriver.Credentials, error) {
			if got != endpoint {
				t.Fatal("wrong broker endpoint")
			}
			if brokerFail {
				return vmdriver.Credentials{}, errors.New(creds.Password)
			}
			return creds, nil
		}), nil)
	if err != nil {
		t.Fatal(err)
	}
	req := vmdriver.ExternalRequest{Endpoint: endpoint}
	if _, err := d.Start(ctx, req, vmID); !errors.Is(err, vmdriver.ErrUncertain) || err.Error() != vmdriver.ErrUncertain.Error() {
		t.Fatal("runner error was not sanitized")
	}
	brokerFail = true
	if _, err := d.Start(ctx, req, vmID); !vmdriver.IsPreDispatch(err) || err.Error() != vmdriver.ErrInvalid.Error() || calls != 1 {
		t.Fatal("broker failure was not sanitized or reached the runner")
	}
}

// TestDeferredCapabilities confirms no real-host operations are advertised (W5).
func TestDeferredCapabilities(t *testing.T) {
	d := testDriver(t, func(context.Context, Command, vmdriver.Credentials) ([]byte, error) {
		t.Fatal("capabilities must not contact the runner")
		return nil, nil
	}, nil)
	ctx := context.Background()
	matrix, err := d.Capabilities(ctx)
	if err != nil || len(matrix.Advertised) != 0 || len(matrix.SandboxCertifiedConfigs) != 0 ||
		matrix.AuthoritativeTaskTokenLookup || len(matrix.Ops) != 9 {
		t.Fatalf("matrix = %+v, %v", matrix, err)
	}
	for op, value := range matrix.Ops {
		if value != "not run: deferred by owner" || matrix.Allows(op) {
			t.Fatalf("unexpected matrix entry %s: %q", op, value)
		}
	}
	result, err := vmdriver.NewDispatcher(d, &journal{owner: testOwner()}, permit{}).Dispatch(ctx,
		vmdriver.Command{Endpoint: endpoint, Thread: thread, Operation: vmdriver.SnapshotOp, VM: vmID, Snapshot: "saved"})
	if !errors.Is(err, vmdriver.ErrUnavailable) || result.ID != "" {
		t.Fatalf("snapshot admission = %+v, %v", result, err)
	}
}

func TestVerifySnapshot(t *testing.T) {
	for _, tc := range []struct {
		name, data string
		ok         bool
	}{
		{"unique", `[{"ID":"` + cpID + `","VMId":"` + vmID + `","Name":"saved"}]`, true},
		{"missing", "[]", false},
		{"foreign", `[{"ID":"` + cpID + `","VMId":"` + cpID + `","Name":"saved"}]`, false},
		{"duplicate", `[{"ID":"` + cpID + `","VMId":"` + vmID + `","Name":"saved"},{"ID":"` + vmID + `","VMId":"` + vmID + `","Name":"saved"}]`, false},
		{"unreadable", "not json", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := testDriver(t, func(context.Context, Command, vmdriver.Credentials) ([]byte, error) { return []byte(tc.data), nil }, nil)
			err := vmdriver.VerifySnapshot(t.Context(), d, endpoint, vmID, "saved")
			if (err == nil) != tc.ok || (!tc.ok && !errors.Is(err, vmdriver.ErrSnapshot)) {
				t.Fatalf("verification: %v", err)
			}
		})
	}
}
