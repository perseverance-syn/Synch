package hyperv

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"synch/internal/vmdriver"
)

const (
	endpoint = "lab"
	thread   = "thread-404"
	cpID     = "abcdefab-1234-1234-1234-123456789abc"
)

// --- Journal and authorizer fakes for the public dispatcher ---

type journal struct {
	mu     sync.Mutex
	latest vmdriver.RequestRecord
	owner  vmdriver.Ownership
}

// Begin records the request prior to any runner call.
func (j *journal) Begin(_ context.Context, r vmdriver.RequestRecord, _ vmdriver.Limits) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.latest.Request.ID != "" && (len(j.latest.History) == 0 || j.latest.History[len(j.latest.History)-1].Outcome == vmdriver.Unknown) {
		return vmdriver.ErrReconciliation
	}
	j.latest = r
	return nil
}

// Bind rejects a fabricated task for a synchronous cmdlet.
func (j *journal) Bind(context.Context, string, vmdriver.TaskBinding) error {
	return vmdriver.ErrInvalid
}

// Load finds the committed request by ID.
func (j *journal) Load(_ context.Context, id string) (vmdriver.RequestRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if id == "" || j.latest.Request.ID != id {
		return vmdriver.RequestRecord{}, vmdriver.ErrInvalid
	}
	return j.latest, nil
}

// Resolve records observations and any newly established ownership.
func (j *journal) Resolve(_ context.Context, _ string, items []vmdriver.Resolution, owner *vmdriver.Ownership) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.latest.History = append(j.latest.History, items...)
	if owner != nil {
		j.owner = *owner
	}
	return nil
}

// Ownership returns the fixture's trusted owner record.
func (j *journal) Ownership(context.Context, string, string) (vmdriver.Ownership, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.owner, nil
}

type permit struct{}

// Authorize allows operations subject to the dispatcher's ownership and limits.
func (permit) Authorize(context.Context, string, string, vmdriver.Operation, string, bool) (vmdriver.Permit, error) {
	return vmdriver.Permit{Allowed: true, Mode: "provider", Limits: vmdriver.Limits{Endpoint: 2, Thread: 2}}, nil
}

// testDriver registers an endpoint with a configured immutable VM export.
func testDriver(t *testing.T, runner Runner, requests RequestLoader) *Driver {
	t.Helper()
	d, err := New(map[string]Host{endpoint: {Exports: map[string]string{"base": `D:\exports\base.vmcx`}}}, runner, nil, requests)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// testOwner creates the authorization fixture used for existing VM mutations.
func testOwner() vmdriver.Ownership {
	return vmdriver.Ownership{Endpoint: endpoint, VMRef: vmID, OwnerThread: thread, Basis: vmdriver.Created, Disposition: vmdriver.Undecided}
}

// vmJSON models the Get-VM projection with provider Notes.
func vmJSON(notes string) ([]byte, error) {
	return json.Marshal([]machine{{ID: vmID, Name: "Lab", State: "Off", Notes: notes}})
}

// checkpointJSON models the Get-VMCheckpoint projection after a checkpoint is created.
func checkpointJSON(name, vm string) ([]byte, error) {
	return json.Marshal([]checkpoint{{ID: cpID, VMID: vm, Name: name}})
}

// --- Reconcile persisted mutations against fake provider state ---

// TestLostResponse proves W2/W4 persistence, recovery, owner checks and no replay.
func TestLostResponse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		op      vmdriver.Operation
		foreign bool
	}{
		{"create", vmdriver.CreateOp, false},
		{"snapshot", vmdriver.SnapshotOp, false},
		{"foreign create", vmdriver.CreateOp, true},
		{"foreign snapshot", vmdriver.SnapshotOp, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			j := &journal{}
			if tc.op == vmdriver.SnapshotOp {
				j.owner = testOwner()
			}
			var count int
			var notes, cpName string
			visible := false
			d := testDriver(t, func(ctx context.Context, c Command, _ vmdriver.Credentials) ([]byte, error) {
				switch {
				case strings.Contains(c.Script, "Import-VM"), strings.Contains(c.Script, "Checkpoint-VM -VM"):
					count++
					r, err := j.Load(ctx, j.latest.Request.ID)
					if err != nil || r.Thread != thread || r.Request.Operation != tc.op || r.Request.ParamsDigest == "" {
						t.Fatalf("request was not persisted: %+v, %v", r, err)
					}
					if tc.op == vmdriver.CreateOp {
						if len(c.Arguments) != 4 || c.Arguments[2] != r.Request.Token() ||
							c.Arguments[3] != r.Request.Marker(thread) || !strings.Contains(c.Script, "Set-VM -VM $vm -Notes") {
							t.Fatal("clone command is missing durable attribution")
						}
						notes = c.Arguments[3]
					} else {
						if len(c.Arguments) != 3 || c.Arguments[2] != "saved~"+r.Request.Token()+"~"+thread {
							t.Fatal("checkpoint command is missing durable attribution")
						}
						cpName = c.Arguments[2]
					}
					if tc.foreign {
						notes = strings.ReplaceAll(notes, thread, "other-owner")
						cpName = strings.ReplaceAll(cpName, thread, "other-owner")
					}
					return nil, errors.New("lost response")
				case strings.Contains(c.Script, "Get-VMCheckpoint"):
					if visible && cpName != "" {
						return checkpointJSON(cpName, vmID)
					}
					return []byte("[]"), nil
				case strings.Contains(c.Script, "Get-VM"):
					if visible || tc.op == vmdriver.SnapshotOp {
						return vmJSON(notes)
					}
					return []byte("[]"), nil
				default:
					t.Fatalf("unexpected command: %s", c.Script)
					return nil, nil
				}
			}, j)
			dispatcher := vmdriver.NewDispatcher(d, j, permit{})
			cmd := vmdriver.Command{Endpoint: endpoint, Thread: thread, Operation: tc.op,
				Base: "base", Spec: vmdriver.VMSpec{Name: "Lab"}, VM: vmID, Snapshot: "saved"}
			var initial vmdriver.Resolution
			var err error
			if tc.op == vmdriver.SnapshotOp {
				// Use the transport seam because the real matrix gates snapshot dispatch.
				req := vmdriver.ExternalRequest{ID: "01HF7YAT000000000000000001", Driver: "hyperv",
					Endpoint: endpoint, Operation: tc.op, Resource: vmID, ParamsDigest: "fake"}
				if err := j.Begin(ctx, vmdriver.RequestRecord{Request: req, Thread: thread}, vmdriver.Limits{}); err != nil {
					t.Fatal(err)
				}
				if _, err := d.Snapshot(ctx, req, vmID, cmd.Snapshot); !errors.Is(err, vmdriver.ErrUncertain) {
					t.Fatalf("snapshot transport = %v", err)
				}
				initial, err = dispatcher.Reconcile(ctx, req.ID)
			} else {
				initial, err = dispatcher.Dispatch(ctx, cmd)
			}
			if initial.Outcome != vmdriver.Unknown || !errors.Is(err, vmdriver.ErrReconciliation) {
				t.Fatalf("dispatch = %+v, %v", initial, err)
			}
			visible = true
			recovery := vmdriver.NewDispatcher(d, j, permit{})
			result, err := recovery.Reconcile(ctx, initial.ID)
			if count != 1 {
				t.Fatalf("mutation attempts = %d", count)
			}
			if tc.foreign {
				if result.Outcome != vmdriver.Unknown || !errors.Is(err, vmdriver.ErrReconciliation) {
					t.Fatalf("foreign marker settled request: %+v, %v", result, err)
				}
				// An unknown operation gates any otherwise admitted following mutation.
				if tc.op == vmdriver.SnapshotOp {
					cmd.Operation = vmdriver.StartOp
				}
				if _, err := recovery.Dispatch(ctx, cmd); !errors.Is(err, vmdriver.ErrReconciliation) || count != 1 {
					t.Fatalf("unknown request was not gated: %v", err)
				}
				return
			}
			if err != nil || result.Outcome != vmdriver.Succeeded || result.Via != vmdriver.ResultingResource ||
				result.Resource == nil || result.Resource.VMRef != vmID {
				t.Fatalf("reconciliation = %+v, %v", result, err)
			}
			if tc.op == vmdriver.CreateOp {
				vms, err := d.Inventory(ctx, endpoint)
				if err != nil {
					t.Fatal(err)
				}
				orphans := vmdriver.Orphans(endpoint, vms, []vmdriver.Ownership{j.owner}, func(id string) bool {
					return id == thread
				})
				if len(orphans) != 1 || orphans[0] != testOwner() {
					t.Fatalf("orphans = %+v", orphans)
				}
			}
		})
	}
}

// TestSerialMutations proves W4 serialization on a shared endpoint and VM.
func TestSerialMutations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	j := &journal{owner: testOwner()}
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	d := testDriver(t, func(ctx context.Context, _ Command, _ vmdriver.Credentials) ([]byte, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return []byte("true"), nil
	}, j)
	dispatcher := vmdriver.NewDispatcher(d, j, permit{})
	done := make(chan error, 2)
	// dispatch submits a lifecycle mutation against the same exact VM.
	dispatch := func(op vmdriver.Operation) {
		result, err := dispatcher.Dispatch(ctx, vmdriver.Command{Endpoint: endpoint, Thread: thread, VM: vmID, Operation: op})
		if err == nil && result.Outcome != vmdriver.Succeeded {
			err = vmdriver.ErrInvalid
		}
		done <- err
	}
	go dispatch(vmdriver.StartOp)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first mutation did not enter runner")
	}
	go dispatch(vmdriver.StopOp)
	select {
	case err := <-done:
		t.Fatalf("mutation returned before preceding runner released: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("both mutations must complete")
	}
}
