package vmdriver_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	vm "synch/internal/vmdriver"
)

const thread = "01HF7YAT000000000000000001"

type fixture struct {
	driver          vm.VMDriver
	base, target    string
	lose            atomic.Bool
	reject          atomic.Bool
	invalidSnapshot atomic.Bool
	mutations       atomic.Int32
	running         atomic.Bool
	enter           chan struct{}
	release         chan struct{}
	// storedAs rewrites the description the mock provider stores for a clone,
	// so tests can model a VM carrying a foreign or missing owner line.
	storedAs func(string) string
}

// stored returns the description the mock provider keeps for a request marker.
func (f *fixture) stored(marker string) string {
	if f.storedAs != nil {
		return f.storedAs(marker)
	}
	return marker
}

type testJournal struct {
	mu      sync.Mutex
	records map[string]vm.RequestRecord
	owners  map[string]vm.Ownership
}

var _ vm.Journal = (*testJournal)(nil)

// newJournal supplies inspectable transactions and survives dispatcher replacement.
func newJournal() *testJournal {
	return &testJournal{records: make(map[string]vm.RequestRecord), owners: make(map[string]vm.Ownership)}
}

// Ownership looks up the fixture's trusted VM authorization records.
func (j *testJournal) Ownership(_ context.Context, endpoint, ref string) (vm.Ownership, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.owners[endpoint+"/"+ref], nil
}

// Begin models the daemon journal's atomic request admission and create reservation.
func (j *testJournal) Begin(_ context.Context, r vm.RequestRecord, limits vm.Limits) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, exists := j.records[r.Request.ID]; exists {
		return vm.ErrInvalid
	}
	var endpointCount, threadCount int
	for _, existing := range j.records {
		pending := len(existing.History) == 0 || existing.History[len(existing.History)-1].Outcome == vm.Unknown
		if pending && (existing.Thread == r.Thread || (existing.Request.Endpoint == r.Request.Endpoint &&
			existing.Request.Resource == r.Request.Resource)) {
			return vm.ErrReconciliation
		}
		if pending && existing.Request.Operation == vm.CreateOp && existing.Request.Endpoint == r.Request.Endpoint {
			endpointCount++
			if existing.Thread == r.Thread {
				threadCount++
			}
		}
	}
	for _, owner := range j.owners {
		if owner.Endpoint == r.Request.Endpoint {
			endpointCount++
			if owner.OwnerThread == r.Thread {
				threadCount++
			}
		}
	}
	if r.Request.Operation == vm.CreateOp && (endpointCount >= limits.Endpoint || threadCount >= limits.Thread) {
		return vm.ErrLimit
	}
	j.records[r.Request.ID] = r
	return nil
}

// Bind stores the task on the already persisted request.
func (j *testJournal) Bind(_ context.Context, _ string, binding vm.TaskBinding) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	record := j.records[binding.ID]
	record.Task.ID = binding.TaskID
	j.records[binding.ID] = record
	return nil
}

// Resolve commits the observations, and any resulting ownership change.
func (j *testJournal) Resolve(_ context.Context, _ string, events []vm.Resolution, owner *vm.Ownership) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	event := events[len(events)-1]
	record := j.records[event.ID]
	record.History = append(record.History, events...)
	j.records[event.ID] = record
	if owner != nil {
		j.owners[owner.Endpoint+"/"+owner.VMRef] = *owner
	}
	if event.Operation == vm.DeleteOp && event.Outcome == vm.Succeeded {
		delete(j.owners, record.Request.Endpoint+"/"+record.Request.Resource)
	}
	return nil
}

// Load returns a committed request and its detached observation history.
func (j *testJournal) Load(_ context.Context, id string) (vm.RequestRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	record, ok := j.records[id]
	if !ok {
		return vm.RequestRecord{}, vm.ErrInvalid
	}
	record.History = append([]vm.Resolution(nil), record.History...)
	return record, nil
}

type authorizer struct{ denied atomic.Bool }

// Authorize models a policy allow and records the not_owner audit decision.
func (a *authorizer) Authorize(_ context.Context, _, _ string, _ vm.Operation, _ string, owned bool) (vm.Permit, error) {
	if !owned {
		a.denied.Store(true)
	}
	return vm.Permit{Allowed: owned, Mode: "provider", Limits: vm.Limits{Endpoint: 10, Thread: 10}}, nil
}

// --- Acceptance tests shared by both provider HTTP mocks ---

// TestProviderContract proves V1–V4 for each driver against its documented HTTP API.
func TestProviderContract(t *testing.T) {
	for _, test := range []struct {
		name        string
		makeFixture func(*testing.T, *testJournal) *fixture
	}{{"Proxmox", proxmoxFixture}, {"ESXi", esxiFixture}} {
		t.Run(test.name, func(t *testing.T) {
			t.Run("operations_and_ownership", func(t *testing.T) {
				j := newJournal()
				f := test.makeFixture(t, j)
				checkOperations(t, f, j)
			})
			t.Run("lost_create_and_snapshot", func(t *testing.T) {
				j := newJournal()
				f := test.makeFixture(t, j)
				checkRecovery(t, f, j)
			})
			t.Run("serialization_and_pause", func(t *testing.T) {
				j := newJournal()
				f := test.makeFixture(t, j)
				checkSerialization(t, f, j)
			})
			t.Run("credential_redaction", func(t *testing.T) {
				j := newJournal()
				f := test.makeFixture(t, j)
				checkCredentialRedaction(t, f, j)
			})
		})
	}
}

// mutate records exactly one provider dispatch and optionally drops its reply.
func (f *fixture) mutate(w http.ResponseWriter) bool {
	f.mutations.Add(1)
	if f.lose.Swap(false) {
		// Close an HTTP/1.1 connection after the provider has stored the result.
		conn, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			_ = conn.Close()
		}
		return true
	}
	return false
}

// checkOperations exercises every operation and checks orphan and restore safety.
func checkOperations(t *testing.T, f *fixture, j *testJournal) {
	t.Helper()
	ctx := t.Context()
	a := &authorizer{}
	d := vm.NewDispatcher(f.driver, j, a)
	row, err := f.driver.Capabilities(ctx)
	if err != nil || len(row.Advertised) != 0 || len(row.Ops) != 9 {
		t.Fatalf("capability matrix: %+v, %v", row, err)
	}
	for op, status := range row.Ops {
		if status != "not run: deferred by owner" {
			t.Fatalf("%s: %s", op, status)
		}
	}
	vms, err := f.driver.Inventory(ctx, "lab")
	if err != nil || len(vms) != 1 || vms[0].Ref != f.base {
		t.Fatalf("inventory: %+v, %v", vms, err)
	}
	create := vm.Command{Endpoint: "lab", Thread: thread, Operation: vm.CreateOp, Base: f.base, Spec: vm.VMSpec{Name: "clone"}}
	cmd := vm.Command{Endpoint: "lab", Thread: thread, Operation: vm.StartOp, VM: f.base}
	if _, err := d.Dispatch(ctx, cmd); !errors.Is(err, vm.ErrNotOwner) || !a.denied.Load() {
		t.Fatalf("visibility conferred mutation or no policy deny: %v", err)
	}
	created, err := d.Dispatch(ctx, create)
	if err != nil || created.Outcome != vm.Succeeded {
		t.Fatalf("create: %+v %v", created, err)
	}
	owner, err := j.Ownership(ctx, "lab", f.target)
	if err != nil || owner.Basis != vm.Created || owner.OwnerThread != thread {
		t.Fatalf("create owner: %+v %v", owner, err)
	}
	vms, err = f.driver.Inventory(ctx, "lab")
	if err != nil {
		t.Fatal(err)
	}
	orphans := vm.Orphans("lab", vms, []vm.Ownership{owner}, func(id string) bool { return id == thread })
	if len(orphans) != 1 || orphans[0] != owner {
		t.Fatalf("orphans: %+v", orphans)
	}
	cmd.VM = f.target
	for _, op := range []vm.Operation{vm.StartOp, vm.StopOp} {
		cmd.Operation = op
		if r, err := d.Dispatch(ctx, cmd); err != nil || r.Outcome != vm.Succeeded || r.Via != vm.ProviderTask {
			t.Fatalf("%s: %+v %v", op, r, err)
		}
	}
	cmd.Operation, cmd.Snapshot = vm.SnapshotOp, "checkpoint"
	if _, err := d.Dispatch(ctx, cmd); !errors.Is(err, vm.ErrUnavailable) {
		t.Fatalf("snapshot advertising: %v", err)
	}

	req := persistSnapshot(t, j, row.Driver, f.target)
	task, err := f.driver.Snapshot(ctx, req, f.target, "checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	status, err := f.driver.GetTask(ctx, "lab", task)
	if err != nil || status.State != vm.Succeeded {
		t.Fatalf("snapshot task: %+v %v", status, err)
	}
	if r, err := d.Reconcile(ctx, req.ID); err != nil || r.Via != vm.ResultingResource {
		t.Fatalf("snapshot: %+v %v", r, err)
	}

	f.invalidSnapshot.Store(true)
	before := f.mutations.Load()
	restore := vm.ExternalRequest{Endpoint: "lab", Operation: vm.RestoreOp, Resource: f.target}
	if _, err := f.driver.Restore(ctx, restore, f.target, "checkpoint"); !errors.Is(err, vm.ErrSnapshot) || f.mutations.Load() != before {
		t.Fatalf("inconsistent snapshot reached restore: %v", err)
	}
	f.invalidSnapshot.Store(false)
	restore.ID = "01HF7YAT000000000000000003"
	restore.Driver = row.Driver
	restore.ParamsDigest = req.ParamsDigest
	if err := j.Begin(ctx, vm.RequestRecord{Request: restore, Thread: thread}, vm.Limits{}); err != nil {
		t.Fatal(err)
	}
	task, err = f.driver.Restore(ctx, restore, f.target, "checkpoint")
	if err != nil || task.ID == "" {
		t.Fatalf("restore: %v", err)
	}
	if err := j.Bind(ctx, thread, vm.TaskBinding{ID: restore.ID, Endpoint: restore.Endpoint, TaskID: task.ID}); err != nil {
		t.Fatal(err)
	}
	if r, err := d.Reconcile(ctx, restore.ID); err != nil || r.Via != vm.ProviderTask {
		t.Fatalf("restore task: %+v %v", r, err)
	}
	cmd.Operation = vm.DeleteOp
	if r, err := d.Dispatch(ctx, cmd); err != nil || r.Outcome != vm.Succeeded {
		t.Fatalf("delete: %+v %v", r, err)
	}
}

// --- Recovery and dispatch guard assertions (vm-driver §3) ---

// persistSnapshot admits a mock-only snapshot without advertising real-host capability.
func persistSnapshot(t *testing.T, j *testJournal, driver, ref string) vm.ExternalRequest {
	t.Helper()
	req := vm.ExternalRequest{ID: "01HF7YAT000000000000000002", Driver: driver, Endpoint: "lab",
		Operation: vm.SnapshotOp, Resource: ref, ParamsDigest: "sha256:" + strings.Repeat("0", 64)}
	if err := j.Begin(t.Context(), vm.RequestRecord{Request: req, Thread: thread}, vm.Limits{}); err != nil {
		t.Fatal(err)
	}
	return req
}

// checkRecovery loses both mutation responses after provider-side storage and reconciles without replay.
func checkRecovery(t *testing.T, f *fixture, j *testJournal) {
	t.Helper()
	ctx := t.Context()
	row, err := f.driver.Capabilities(ctx)
	if err != nil {
		t.Fatal(err)
	}
	d := vm.NewDispatcher(f.driver, j, &authorizer{})
	f.lose.Store(true)
	r, err := d.Dispatch(ctx, vm.Command{Endpoint: "lab", Thread: thread, Operation: vm.CreateOp,
		Base: f.base, Spec: vm.VMSpec{Name: "clone"}})
	if err != nil || r.Via != vm.ResultingResource || r.Outcome != vm.Succeeded ||
		r.Resource == nil || r.Resource.ClientToken != "synch-"+r.ID || f.mutations.Load() != 1 {
		t.Fatalf("lost clone response: %+v %v", r, err)
	}
	req := persistSnapshot(t, j, row.Driver, f.target)
	f.lose.Store(true)
	if task, err := f.driver.Snapshot(ctx, req, f.target, "checkpoint"); task.ID != "" || !errors.Is(err, vm.ErrUncertain) {
		t.Fatalf("snapshot response was not lost: %+v %v", task, err)
	}
	d = vm.NewDispatcher(f.driver, j, &authorizer{})
	f.invalidSnapshot.Store(true)
	if r, err := d.Reconcile(ctx, req.ID); !errors.Is(err, vm.ErrReconciliation) || r.Via != vm.None {
		t.Fatalf("unverifiable snapshot settled the request: %+v %v", r, err)
	}
	f.invalidSnapshot.Store(false)
	if r, err := d.Reconcile(ctx, req.ID); err != nil || r.Via != vm.ResultingResource ||
		r.Outcome != vm.Succeeded || r.Resource.ClientToken != req.Token() || f.mutations.Load() != 2 {
		t.Fatalf("lost snapshot response: %+v %v", r, err)
	}
}

// checkSerialization proves same-VM mutations queue and pending tasks pause further dispatch.
func checkSerialization(t *testing.T, f *fixture, j *testJournal) {
	t.Helper()
	j.owners["lab/"+f.base] = vm.Ownership{Endpoint: "lab", VMRef: f.base, OwnerThread: thread,
		Basis: vm.Assigned, AssignedBy: "operator", AssignedAt: time.Now(), Disposition: vm.Keep}
	f.enter, f.release = make(chan struct{}), make(chan struct{})
	d := vm.NewDispatcher(f.driver, j, &authorizer{})
	cmd := vm.Command{Endpoint: "lab", Thread: thread, VM: f.base, Operation: vm.StartOp}
	run := func(operation vm.Operation, started chan struct{}) <-chan error {
		done := make(chan error, 1)
		go func() {
			close(started)
			next := cmd
			next.Operation = operation
			r, err := d.Dispatch(t.Context(), next)
			if err == nil && r.Outcome != vm.Succeeded {
				err = fmt.Errorf("unexpected %s result: %+v", operation, r)
			}
			done <- err
		}()
		return done
	}
	start := run(vm.StartOp, make(chan struct{}))
	select {
	case <-f.enter:
	case <-time.After(3 * time.Second):
		t.Fatal("start never reached provider")
	}
	started := make(chan struct{})
	stop := run(vm.StopOp, started)
	<-started
	select {
	case err := <-stop:
		close(f.release)
		t.Fatalf("stop returned before start completed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(f.release)
	if err := <-start; err != nil {
		t.Fatal(err)
	}
	if err := <-stop; err != nil {
		t.Fatal(err)
	}

	f.running.Store(true)
	cmd.Operation = vm.StopOp
	r, err := d.Dispatch(t.Context(), cmd)
	if !errors.Is(err, vm.ErrReconciliation) || r.Outcome != vm.Unknown {
		t.Fatalf("running task: %+v %v", r, err)
	}
	if _, err := d.Dispatch(t.Context(), cmd); !errors.Is(err, vm.ErrReconciliation) || f.mutations.Load() != 3 {
		t.Fatalf("unknown allowed another mutation: %v", err)
	}
	f.running.Store(false)
	if r, err := d.Reconcile(t.Context(), r.ID); err != nil || r.Via != vm.ProviderTask {
		t.Fatalf("bound task: %+v %v", r, err)
	}
}

// checkCredentialRedaction verifies that provider error bodies never escape into resolutions or errors.
func checkCredentialRedaction(t *testing.T, f *fixture, j *testJournal) {
	t.Helper()
	j.owners["lab/"+f.base] = vm.Ownership{Endpoint: "lab", VMRef: f.base, OwnerThread: thread, Basis: vm.Created}
	f.reject.Store(true)
	ctx := t.Context()
	req := vm.ExternalRequest{Endpoint: "lab", Operation: vm.StartOp, Resource: f.base}
	if _, err := f.driver.Start(ctx, req, f.base); !errors.Is(err, vm.ErrRejected) ||
		strings.Contains(fmt.Sprint(err), "lab-test-token") || strings.Contains(fmt.Sprint(err), "test<&password") {
		t.Fatalf("unsafe error or wrong rejection classification: %v", err)
	}
	d := vm.NewDispatcher(f.driver, j, &authorizer{})
	r, err := d.Dispatch(ctx, vm.Command{Endpoint: "lab", Thread: thread, Operation: vm.StartOp, VM: f.base})
	if err != nil || r.Outcome != vm.NotExecuted || r.Via != vm.Response || r.TaskID != "" || r.Resource != nil {
		t.Fatalf("explicit rejection evidence: %+v, %v", r, err)
	}
}
