package vmdriver_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	vm "synch/internal/vmdriver"
)

// advertising wraps a driver with a test-only matrix that advertises every
// operation as passed. Shipped matrices stay "not run: deferred by owner".
type advertising struct{ vm.VMDriver }

// Capabilities returns a matrix that lets the dispatcher admit snapshot and restore.
func (a advertising) Capabilities(ctx context.Context) (vm.MatrixRow, error) {
	row, err := a.VMDriver.Capabilities(ctx)
	row.Advertised = []vm.Operation{vm.SnapshotOp, vm.RestoreOp}
	row.Ops = map[vm.Operation]string{vm.SnapshotOp: "passed", vm.RestoreOp: "passed",
		"lost_response_reconcile_snapshot": "passed"}
	return row, err
}

// --- N1/N3: pre-dispatch refusals settle as not_executed ---

// TestRestoreRefusalSettlesNotExecuted covers N1 and N3: restore of an unverified
// or missing snapshot is refused before any provider mutation. Through the
// dispatcher it settles as not_executed and a follow-up start proceeds.
func TestRestoreRefusalSettlesNotExecuted(t *testing.T) {
	for _, test := range []struct {
		name        string
		makeFixture func(*testing.T, *testJournal) *fixture
	}{{"Proxmox", proxmoxFixture}, {"ESXi", esxiFixture}} {
		for _, snapshot := range []struct {
			name       string
			unverified bool
		}{{"unverified_snapshot", true}, {"missing_snapshot", false}} {
			t.Run(test.name+"/"+snapshot.name, func(t *testing.T) {
				ctx := t.Context()
				j := newJournal()
				f := test.makeFixture(t, j)
				d := vm.NewDispatcher(advertising{f.driver}, j, &authorizer{})
				created, err := d.Dispatch(ctx, vm.Command{Endpoint: "lab", Thread: thread, Operation: vm.CreateOp,
					Base: f.base, Spec: vm.VMSpec{Name: "clone"}})
				if err != nil || created.Outcome != vm.Succeeded {
					t.Fatalf("create: %+v %v", created, err)
				}
				row, _ := f.driver.Capabilities(ctx)
				req := persistSnapshot(t, j, row.Driver, f.target)
				if _, err := f.driver.Snapshot(ctx, req, f.target, "checkpoint"); err != nil {
					t.Fatal(err)
				}
				if r, err := d.Reconcile(ctx, req.ID); err != nil || r.Outcome != vm.Succeeded {
					t.Fatalf("snapshot setup: %+v %v", r, err)
				}
				name := "checkpoint"
				if snapshot.unverified {
					f.invalidSnapshot.Store(true)
				} else {
					name = "never-taken"
				}
				before := f.mutations.Load()
				r, err := d.Dispatch(ctx, vm.Command{Endpoint: "lab", Thread: thread, Operation: vm.RestoreOp,
					VM: f.target, Snapshot: name})
				if err != nil || r.Outcome != vm.NotExecuted || r.Via != vm.Response || r.Note == "" {
					t.Fatalf("refusal did not settle as not_executed: %+v %v", r, err)
				}
				if f.mutations.Load() != before {
					t.Fatal("refused restore mutated the provider")
				}
				f.invalidSnapshot.Store(false)
				next, err := d.Dispatch(ctx, vm.Command{Endpoint: "lab", Thread: thread, Operation: vm.StartOp, VM: f.target})
				if err != nil || next.Outcome != vm.Succeeded {
					t.Fatalf("follow-up start was blocked: %+v %v", next, err)
				}
			})
		}
	}
}

// refuser is a driver whose Start fails with a fixed error.
type refuser struct {
	vm.VMDriver
	err error
}

// Capabilities declares an empty matrix; start needs no advertising.
func (refuser) Capabilities(context.Context) (vm.MatrixRow, error) {
	return vm.MatrixRow{Driver: "refuser"}, nil
}

// Start returns the configured error without any provider call.
func (r refuser) Start(context.Context, vm.ExternalRequest, string) (vm.TaskRef, error) {
	return vm.TaskRef{}, r.err
}

// FindByToken reports no evidence.
func (refuser) FindByToken(context.Context, string, string, string) ([]vm.ResourceEvidence, error) {
	return nil, nil
}

// TestPreDispatchMarkerIsExplicit covers N1: only errors a driver marks with
// PreDispatch settle as not_executed. The same sentinel without the marker
// (the request may have reached the provider) still goes to reconciliation.
func TestPreDispatchMarkerIsExplicit(t *testing.T) {
	for _, sentinel := range []error{vm.ErrSnapshot, vm.ErrInvalid, vm.ErrUnavailable} {
		for _, marked := range []bool{true, false} {
			t.Run(sentinel.Error()+"/marked="+map[bool]string{true: "yes", false: "no"}[marked], func(t *testing.T) {
				j := newJournal()
				j.owners["lab/vm1"] = vm.Ownership{Endpoint: "lab", VMRef: "vm1", OwnerThread: thread, Basis: vm.Created}
				err := sentinel
				if marked {
					err = vm.PreDispatch(sentinel)
				}
				d := vm.NewDispatcher(refuser{err: err}, j, &authorizer{})
				r, gotErr := d.Dispatch(t.Context(), vm.Command{Endpoint: "lab", Thread: thread, Operation: vm.StartOp, VM: "vm1"})
				if marked && (gotErr != nil || r.Outcome != vm.NotExecuted || !strings.Contains(r.Note, sentinel.Error())) {
					t.Fatalf("marked refusal: %+v %v", r, gotErr)
				}
				if !marked && (!errors.Is(gotErr, vm.ErrReconciliation) || r.Outcome != vm.Unknown) {
					t.Fatalf("unmarked error skipped reconciliation: %+v %v", r, gotErr)
				}
			})
		}
	}
	if err := vm.PreDispatch(vm.ErrUncertain); vm.IsPreDispatch(err) {
		t.Fatal("an uncertain error was marked pre-dispatch")
	}
}

// --- N2: owner marker is evidence ---

// TestFindByTokenChecksOwnerMarker covers N2 for both drivers: a token match
// counts only when its owner line names the requesting thread.
func TestFindByTokenChecksOwnerMarker(t *testing.T) {
	for _, test := range []struct {
		name        string
		makeFixture func(*testing.T, *testJournal) *fixture
	}{{"Proxmox", proxmoxFixture}, {"ESXi", esxiFixture}} {
		for _, owner := range []struct {
			name     string
			storedAs func(string) string
			want     int
		}{
			{"matching_owner_line", nil, 1},
			{"foreign_owner_line", func(m string) string {
				return strings.Replace(m, vm.OwnerLine(thread), vm.OwnerLine("other-thread"), 1)
			}, 0},
			{"missing_owner_line", func(m string) string {
				return strings.SplitN(m, "\n", 2)[0]
			}, 0},
		} {
			t.Run(test.name+"/"+owner.name, func(t *testing.T) {
				ctx := t.Context()
				j := newJournal()
				f := test.makeFixture(t, j)
				f.storedAs = owner.storedAs
				d := vm.NewDispatcher(advertising{f.driver}, j, &authorizer{})
				f.lose.Store(true)
				r, err := d.Dispatch(ctx, vm.Command{Endpoint: "lab", Thread: thread, Operation: vm.CreateOp,
					Base: f.base, Spec: vm.VMSpec{Name: "clone"}})
				if owner.want == 1 {
					if err != nil || r.Outcome != vm.Succeeded || r.Resource == nil {
						t.Fatalf("matching owner not accepted: %+v %v", r, err)
					}
					return
				}
				if !errors.Is(err, vm.ErrReconciliation) || r.Outcome == vm.Succeeded {
					t.Fatalf("foreign or missing owner settled as success: %+v %v", r, err)
				}
				if got, ownErr := j.Ownership(ctx, "lab", f.target); ownErr != nil || got.OwnerThread != "" {
					t.Fatalf("creation ownership recorded: %+v %v", got, ownErr)
				}
				token := "synch-" + r.ID
				items, err := f.driver.FindByToken(ctx, "lab", token, thread)
				if err != nil || len(items) != 0 {
					t.Fatalf("evidence without matching owner: %+v %v", items, err)
				}
			})
		}
	}
}
