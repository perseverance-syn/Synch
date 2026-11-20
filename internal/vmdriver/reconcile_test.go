package vmdriver_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	vm "synch/internal/vmdriver"
)

type evidenceDriver struct {
	vm.VMDriver
	absence       bool
	authoritative bool
	resources     []vm.ResourceEvidence
	task          vm.TaskStatus
	lookups       int
}

// Capabilities declares only the evidence modes needed for the test.
func (d *evidenceDriver) Capabilities(_ context.Context) (vm.MatrixRow, error) {
	return vm.MatrixRow{Driver: "evidence", TokenCarried: []vm.Operation{vm.CreateOp, vm.SnapshotOp},
		AuthoritativeTaskTokenLookup: d.authoritative}, nil
}

// FindByToken supplies fixture observations, including deliberately invalid scope.
func (d *evidenceDriver) FindByToken(_ context.Context, _, _, _ string) ([]vm.ResourceEvidence, error) {
	return d.resources, nil
}

// TaskAbsent supplies the configured token lookup result.
func (d *evidenceDriver) TaskAbsent(_ context.Context, _, _ string) (bool, error) {
	d.lookups++
	return d.absence, nil
}

// GetTask returns the fixture's current provider task observation.
func (d *evidenceDriver) GetTask(_ context.Context, _ string, _ vm.TaskRef) (vm.TaskStatus, error) {
	return d.task, nil
}

// TestAuthoritativeAbsenceAndContradiction proves V1 Q-56, C5 and G.1 evidence handling.
func TestAuthoritativeAbsenceAndContradiction(t *testing.T) {
	ctx := t.Context()
	j := newJournal()
	driver := &evidenceDriver{absence: true}
	dispatcher := vm.NewDispatcher(driver, j, &authorizer{})
	req := persistSnapshot(t, j, "evidence", "target")
	result := vm.ResourceEvidence{Endpoint: "other", Resource: "snap-1", VMRef: "target", Operation: vm.SnapshotOp, ClientToken: req.Token()}
	driver.resources = []vm.ResourceEvidence{result}
	r, err := dispatcher.Reconcile(ctx, req.ID)
	if !errors.Is(err, vm.ErrReconciliation) || r.Via != vm.None || driver.lookups != 0 {
		t.Fatalf("wrong endpoint or nonauthoritative absence settled request: %+v %v", r, err)
	}

	driver.authoritative = true
	r, err = dispatcher.Reconcile(ctx, req.ID)
	if err != nil || r.Outcome != vm.NotExecuted || r.Via != vm.ProviderTaskAbsent ||
		r.Absence == nil || r.Absence.ClientToken != req.Token() || r.Absence.QueriedAt.IsZero() {
		t.Fatalf("authoritative lookup: %+v %v", r, err)
	}
	payload, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"resource_evidence", "provider_task_id", "operator", "note"} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("illegal absence evidence: %s", payload)
		}
	}

	driver.resources[0].Endpoint = req.Endpoint
	driver.absence = false
	r, err = dispatcher.Reconcile(ctx, req.ID)
	if !errors.Is(err, vm.ErrReconciliation) || r.Outcome != vm.Unknown || r.Via != vm.None || r.Resource != nil {
		t.Fatalf("late execution evidence did not reopen request: %+v %v", r, err)
	}
	record, err := j.Load(ctx, req.ID)
	if err != nil || len(record.History) != 4 || record.History[2].Via != vm.ResultingResource {
		t.Fatalf("conflicting observation and unknown were not preserved: %+v %v", record, err)
	}
}

// TestTaskEvidenceAndOperator proves V1 bound-task precedence and operator accountability.
func TestTaskEvidenceAndOperator(t *testing.T) {
	ctx := t.Context()
	t.Run("in_flight_task", func(t *testing.T) {
		j := newJournal()
		req := persistSnapshot(t, j, "evidence", "target")
		if err := j.Bind(ctx, thread, vm.TaskBinding{ID: req.ID, Endpoint: req.Endpoint, TaskID: "task-1"}); err != nil {
			t.Fatal(err)
		}
		driver := &evidenceDriver{authoritative: true, absence: true, task: vm.TaskStatus{State: vm.Unknown, InFlight: true},
			resources: []vm.ResourceEvidence{{Endpoint: req.Endpoint, VMRef: "target", Resource: "snap-1",
				Operation: vm.SnapshotOp, ClientToken: req.Token()}}}
		d := vm.NewDispatcher(driver, j, &authorizer{})
		r, err := d.Reconcile(ctx, req.ID)
		if !errors.Is(err, vm.ErrReconciliation) || r.Outcome != vm.Unknown || driver.lookups != 0 {
			t.Fatalf("bound provider task still active: %+v %v", r, err)
		}

		driver.resources = nil
		driver.task = vm.TaskStatus{State: vm.Failed}
		r, err = d.Reconcile(ctx, req.ID)
		if err != nil || r.Via != vm.ProviderTask || r.TaskID != "task-1" || r.Outcome != vm.Failed {
			t.Fatalf("bound task error: %+v %v", r, err)
		}
	})
	t.Run("operator", func(t *testing.T) {
		j := newJournal()
		req := persistSnapshot(t, j, "evidence", "target")
		d := vm.NewDispatcher(&evidenceDriver{}, j, &authorizer{})
		if _, err := d.ResolveByOperator(ctx, req.ID, "lab-operator", "  ", vm.NotExecuted); !errors.Is(err, vm.ErrInvalid) {
			t.Fatalf("blank operator note accepted: %v", err)
		}
		r, err := d.ResolveByOperator(ctx, req.ID, "lab-operator", "Host confirmed request was never sent.", vm.NotExecuted)
		if err != nil || r.Outcome != vm.NotExecuted || r.Via != vm.Operator || r.OperatorID != "lab-operator" {
			t.Fatalf("operator resolution: %+v %v", r, err)
		}
	})
}
