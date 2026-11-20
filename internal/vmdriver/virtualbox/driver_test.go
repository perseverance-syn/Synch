package virtualbox_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	vm "synch/internal/vmdriver"
	"synch/internal/vmdriver/virtualbox"
)

const (
	thread = "01HF7YAT000000000000000001"
	baseID = "11111111-1111-1111-1111-111111111111"
)

// --- Fake VBoxManage: output shapes follow the VirtualBox User Manual, ch. 8 ---
// Shapes cited: `list vms` ("name" {uuid}); `clonevm` ("Machine has been
// successfully cloned as"); `getextradata enumerate` ("Key: k, Value: v", "No
// value set!"); `snapshot take` ("Snapshot taken. UUID:"); `snapshot list
// --machinereadable` (SnapshotName[-n]/SnapshotUUID/SnapshotDescription);
// "This machine does not have any snapshots" on stderr with exit 1.

type fakeSnap struct{ name, uuid, desc string }

type fakeVM struct {
	uuid, name string
	running    bool
	extra      map[string]string
	snaps      []fakeSnap
}

// fakeVBox is an in-memory VBoxManage host recording every argv it receives.
type fakeVBox struct {
	mu       sync.Mutex
	vms      []*fakeVM
	calls    [][]string
	loseOn   string // "<verb>[ <subverb>]" whose reply is dropped after it applied
	hold     chan struct{}
	enter    chan struct{}
	inflight int
	maxIn    int
	next     int
	journal  *testJournal
	t        *testing.T
}

// newHost builds a host with one registered base VM.
func newHost(t *testing.T, j *testJournal) *fakeVBox {
	return &fakeVBox{t: t, journal: j, vms: []*fakeVM{{uuid: baseID, name: "base", extra: map[string]string{}}}}
}

// count returns how many recorded calls start with the given words after the binary name.
func (f *fakeVBox) count(words ...string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if len(c) > len(words) && strings.Join(c[1:1+len(words)], " ") == strings.Join(words, " ") {
			n++
		}
	}
	return n
}

// find returns the VM with the uuid or name, or nil.
func (f *fakeVBox) find(key string) *fakeVM {
	for _, v := range f.vms {
		if v.uuid == key || v.name == key {
			return v
		}
	}
	return nil
}

// checkPersisted asserts attribution predates the mutation: the journal already
// holds the request named by the token, with no resolution (vm-driver §3.2).
func (f *fakeVBox) checkPersisted(token string) {
	rec, err := f.journal.Load(context.Background(), strings.TrimPrefix(token, "synch-"))
	if err != nil || len(rec.History) != 0 || token != "synch-"+rec.Request.ID {
		f.t.Errorf("mutation lacked a persisted request: %v", err)
	}
}

// Run applies one VBoxManage command to the fake host.
func (f *fakeVBox) Run(_ context.Context, _ string, argv []string) (virtualbox.Result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, argv)
	if argv[0] != "VBoxManage" {
		f.t.Errorf("unexpected binary %q", argv[0])
	}
	verb := argv[1]
	mutating := verb != "list" && verb != "getextradata" && (verb != "snapshot" || argv[3] != "list")
	key := verb
	if verb == "snapshot" {
		key += " " + argv[3]
	}
	if mutating {
		f.inflight++
		f.maxIn = max(f.maxIn, f.inflight)
	}
	hold, enter := f.hold, f.enter
	if verb == "startvm" && hold != nil {
		f.hold, f.enter = nil, nil
	}
	res := f.apply(argv)
	lose := mutating && key == f.loseOn && res.ExitCode == 0
	if lose {
		f.loseOn = ""
	}
	f.mu.Unlock()
	if verb == "startvm" && hold != nil {
		close(enter)
		<-hold
	}
	if mutating {
		f.mu.Lock()
		f.inflight--
		f.mu.Unlock()
	}
	if lose {
		return virtualbox.Result{}, errors.New("connection lost")
	}
	return res, nil
}

// apply mutates state and renders the documented output. Caller holds f.mu.
func (f *fakeVBox) apply(a []string) virtualbox.Result {
	ok := func(s string) virtualbox.Result { return virtualbox.Result{Stdout: s} }
	bad := virtualbox.Result{Stderr: "VBoxManage: error: Could not find a registered machine", ExitCode: 1}
	switch a[1] {
	case "list":
		var b strings.Builder
		for _, v := range f.vms {
			if a[2] == "vms" || v.running {
				fmt.Fprintf(&b, "\"%s\" {%s}\n", v.name, v.uuid)
			}
		}
		return ok(b.String())
	case "clonevm":
		if f.find(a[2]) == nil {
			return bad
		}
		f.next++
		f.vms = append(f.vms, &fakeVM{uuid: fmt.Sprintf("22222222-2222-2222-2222-%012d", f.next), name: a[4], extra: map[string]string{}})
		return ok("0%...10%...100%\nMachine has been successfully cloned as \"" + a[4] + "\"\n")
	case "setextradata":
		v := f.find(a[2])
		if v == nil {
			return bad
		}
		if a[3] == "synch/token" {
			f.checkPersisted(a[4])
		}
		v.extra[a[3]] = a[4]
		return ok("")
	case "getextradata":
		v := f.find(a[2])
		if v == nil {
			return bad
		}
		var b strings.Builder
		for k, val := range v.extra {
			fmt.Fprintf(&b, "Key: %s, Value: %s\n", k, val)
		}
		if b.Len() == 0 {
			return ok("No value set!\n")
		}
		return ok(b.String())
	case "startvm", "controlvm":
		v := f.find(a[2])
		if v == nil {
			return bad
		}
		v.running = a[1] == "startvm"
		return ok("")
	case "unregistervm":
		for i, v := range f.vms {
			if v.uuid == a[2] {
				f.vms = append(f.vms[:i], f.vms[i+1:]...)
				return ok("0%...100%\n")
			}
		}
		return bad
	case "snapshot":
		return f.snapshot(a)
	}
	f.t.Errorf("unexpected VBoxManage verb %q", a[1])
	return bad
}

// snapshot handles snapshot take, list and restore.
func (f *fakeVBox) snapshot(a []string) virtualbox.Result {
	v := f.find(a[2])
	if v == nil {
		return virtualbox.Result{Stderr: "not found", ExitCode: 1}
	}
	switch a[3] {
	case "take":
		f.checkPersisted(a[6])
		v.snaps = append(v.snaps, fakeSnap{a[4], fmt.Sprintf("33333333-3333-3333-3333-%012d", len(v.snaps)+1), a[6]})
		return virtualbox.Result{Stdout: "0%...100%\nSnapshot taken. UUID: " + v.snaps[len(v.snaps)-1].uuid + "\n"}
	case "list":
		if len(v.snaps) == 0 {
			return virtualbox.Result{Stderr: "This machine does not have any snapshots", ExitCode: 1}
		}
		var b strings.Builder
		for i, s := range v.snaps {
			suffix := ""
			if i > 0 {
				suffix = fmt.Sprintf("-%d", i)
			}
			fmt.Fprintf(&b, "SnapshotName%s=\"%s\"\nSnapshotUUID%s=\"%s\"\nSnapshotDescription%s=\"%s\"\n", suffix, s.name, suffix, s.uuid, suffix, s.desc)
		}
		return virtualbox.Result{Stdout: b.String()}
	}
	return virtualbox.Result{Stdout: "Restoring snapshot\n"}
}

// lossAfter drops the reply of the nth call of a verb after it applied.
type lossAfter struct {
	*fakeVBox
	verb string
	nth  int
	seen int
}

// Run forwards to the fake and loses the nth matching reply.
func (l *lossAfter) Run(ctx context.Context, ep string, argv []string) (virtualbox.Result, error) {
	res, err := l.fakeVBox.Run(ctx, ep, argv)
	if argv[1] == l.verb {
		l.seen++
		if l.seen == l.nth {
			return virtualbox.Result{}, errors.New("connection lost")
		}
	}
	return res, err
}

// --- Minimal journal and authorizer (the daemon's real ones are out of scope) ---

type testJournal struct {
	mu      sync.Mutex
	records map[string]vm.RequestRecord
	owners  map[string]vm.Ownership
}

var _ vm.Journal = (*testJournal)(nil)

// newJournal returns an empty in-memory journal.
func newJournal() *testJournal {
	return &testJournal{records: map[string]vm.RequestRecord{}, owners: map[string]vm.Ownership{}}
}

// Ownership returns the stored ownership record for a VM.
func (j *testJournal) Ownership(_ context.Context, endpoint, ref string) (vm.Ownership, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.owners[endpoint+"/"+ref], nil
}

// Begin persists the request before dispatch and refuses an unresolved sibling on the VM.
func (j *testJournal) Begin(_ context.Context, r vm.RequestRecord, _ vm.Limits) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, e := range j.records {
		if e.Request.Endpoint == r.Request.Endpoint && e.Request.Resource == r.Request.Resource &&
			(len(e.History) == 0 || e.History[len(e.History)-1].Outcome == vm.Unknown) {
			return vm.ErrReconciliation
		}
	}
	j.records[r.Request.ID] = r
	return nil
}

// Bind stores a task ID on a request.
func (j *testJournal) Bind(_ context.Context, _ string, b vm.TaskBinding) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	rec := j.records[b.ID]
	rec.Task.ID = b.TaskID
	j.records[b.ID] = rec
	return nil
}

// Resolve appends observations and applies ownership changes.
func (j *testJournal) Resolve(_ context.Context, _ string, events []vm.Resolution, owner *vm.Ownership) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	last := events[len(events)-1]
	rec := j.records[last.ID]
	rec.History = append(rec.History, events...)
	j.records[last.ID] = rec
	if owner != nil {
		j.owners[owner.Endpoint+"/"+owner.VMRef] = *owner
	}
	if last.Operation == vm.DeleteOp && last.Outcome == vm.Succeeded {
		delete(j.owners, rec.Request.Endpoint+"/"+rec.Request.Resource)
	}
	return nil
}

// Load returns a copy of a persisted request.
func (j *testJournal) Load(_ context.Context, id string) (vm.RequestRecord, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	rec, ok := j.records[id]
	if !ok {
		return vm.RequestRecord{}, vm.ErrInvalid
	}
	rec.History = append([]vm.Resolution(nil), rec.History...)
	return rec, nil
}

type allow struct{}

// Authorize allows owned resources in provider mode.
func (allow) Authorize(_ context.Context, _, _ string, _ vm.Operation, _ string, owned bool) (vm.Permit, error) {
	return vm.Permit{Allowed: owned, Mode: "provider", Limits: vm.Limits{Endpoint: 10, Thread: 10}}, nil
}

// advertising wraps the driver with a test-only matrix advertising snapshots;
// the shipped matrix stays "not run: deferred by owner".
type advertising struct{ vm.VMDriver }

// Capabilities lets the dispatcher admit snapshot and restore in tests.
func (a advertising) Capabilities(ctx context.Context) (vm.MatrixRow, error) {
	row, err := a.VMDriver.Capabilities(ctx)
	row.Advertised = []vm.Operation{vm.SnapshotOp, vm.RestoreOp}
	row.Ops = map[vm.Operation]string{vm.SnapshotOp: "passed", vm.RestoreOp: "passed", "lost_response_reconcile_snapshot": "passed"}
	return row, err
}

// setupWith builds a driver over any runner and a dispatcher on a shared journal.
func setupWith(t *testing.T, j *testJournal, run virtualbox.Runner) (*vm.Dispatcher, vm.VMDriver) {
	t.Helper()
	drv, err := virtualbox.New([]string{"lab"}, run)
	if err != nil {
		t.Fatal(err)
	}
	return vm.NewDispatcher(advertising{drv}, j, allow{}), drv
}

// setup builds the fake host, journal, dispatcher and driver.
func setup(t *testing.T) (*fakeVBox, *testJournal, *vm.Dispatcher, vm.VMDriver) {
	t.Helper()
	j := newJournal()
	f := newHost(t, j)
	d, drv := setupWith(t, j, f)
	return f, j, d, drv
}

// do dispatches one command on the lab host for the test thread.
func do(d *vm.Dispatcher, op vm.Operation, ref, name string) (vm.Resolution, error) {
	cmd := vm.Command{Endpoint: "lab", Thread: thread, Operation: op, VM: ref, Snapshot: name}
	if op == vm.CreateOp {
		cmd.Base, cmd.Spec = baseID, vm.VMSpec{Name: "clone"}
	}
	return d.Dispatch(context.Background(), cmd)
}

// --- Tests ---

// TestOperations proves W1 and W4: every operation succeeds through the dispatcher, ownership follows create and delete.
func TestOperations(t *testing.T) {
	f, j, d, drv := setup(t)
	r, err := do(d, vm.CreateOp, "", "")
	if err != nil || r.Outcome != vm.Succeeded || r.Resource == nil {
		t.Fatalf("create: %+v %v", r, err)
	}
	ref := r.Resource.VMRef
	if o, _ := j.Ownership(t.Context(), "lab", ref); o.OwnerThread != thread || o.Basis != vm.Created {
		t.Fatalf("ownership not recorded: %+v", o)
	}
	for _, step := range []struct {
		op   vm.Operation
		name string
	}{{vm.StartOp, ""}, {vm.SnapshotOp, "checkpoint"}, {vm.StopOp, ""}, {vm.RestoreOp, "checkpoint"}} {
		if r, err := do(d, step.op, ref, step.name); err != nil || r.Outcome != vm.Succeeded {
			t.Fatalf("%s: %+v %v", step.op, r, err)
		}
	}
	vms, err := drv.Inventory(t.Context(), "lab")
	if err != nil || len(vms) != 2 || vms[1].State != "poweroff" || vms[1].Name != "clone" {
		t.Fatalf("inventory: %+v %v", vms, err)
	}
	if _, err := do(d, vm.StartOp, ref, ""); err != nil {
		t.Fatal(err)
	}
	if vms, _ = drv.Inventory(t.Context(), "lab"); vms[1].State != "running" {
		t.Fatalf("running state not reported: %+v", vms)
	}
	if _, err := do(d, vm.StopOp, ref, ""); err != nil {
		t.Fatal(err)
	}
	if r, err := do(d, vm.DeleteOp, ref, ""); err != nil || r.Outcome != vm.Succeeded || f.find(ref) != nil {
		t.Fatalf("delete: %+v %v", r, err)
	}
	if _, err := drv.GetTask(t.Context(), "lab", vm.TaskRef{}); !errors.Is(err, vm.ErrUnavailable) {
		t.Fatalf("get_task must be unavailable: %v", err)
	}
}

// TestNotOwnerRefused proves W2: an unowned VM is refused before any VBoxManage mutation.
func TestNotOwnerRefused(t *testing.T) {
	f, _, d, _ := setup(t)
	if _, err := do(d, vm.StartOp, baseID, ""); !errors.Is(err, vm.ErrNotOwner) || f.count("startvm") != 0 {
		t.Fatalf("not_owner expected without dispatch: %v", err)
	}
}

// TestLostCloneReplyPausesWithoutRepeat proves W2: a lost clonevm reply leaves no marker, so the request stays unknown and is never repeated.
func TestLostCloneReplyPausesWithoutRepeat(t *testing.T) {
	f, _, d, _ := setup(t)
	f.loseOn = "clonevm"
	r, err := do(d, vm.CreateOp, "", "")
	if !errors.Is(err, vm.ErrReconciliation) || r.Outcome != vm.Unknown {
		t.Fatalf("lost clone must pause: %+v %v", r, err)
	}
	if _, err := d.Reconcile(t.Context(), r.ID); !errors.Is(err, vm.ErrReconciliation) || f.count("clonevm") != 1 {
		t.Fatalf("reconcile must stay unknown without repeating: %v clones=%d", err, f.count("clonevm"))
	}
}

// TestLostCreateReplyReconcilesByToken proves W2/W4: a lost reply after the marker was written reconciles by token and records ownership.
func TestLostCreateReplyReconcilesByToken(t *testing.T) {
	j := newJournal()
	f := newHost(t, j)
	d, _ := setupWith(t, j, &lossAfter{fakeVBox: f, verb: "setextradata", nth: 2})
	r, err := do(d, vm.CreateOp, "", "")
	if err != nil || r.Outcome != vm.Succeeded || r.Via != vm.ResultingResource || f.count("clonevm") != 1 {
		t.Fatalf("lost reply must reconcile by token: %+v %v", r, err)
	}
	if o, _ := j.Ownership(t.Context(), "lab", r.Resource.VMRef); o.OwnerThread != thread {
		t.Fatalf("ownership missing after reconcile: %+v", o)
	}
}

// TestLostSnapshotReconciles proves W2/W4: a lost snapshot reply reconciles through the description token, once.
func TestLostSnapshotReconciles(t *testing.T) {
	f, _, d, _ := setup(t)
	r, _ := do(d, vm.CreateOp, "", "")
	ref := r.Resource.VMRef
	f.loseOn = "snapshot take"
	r, err := do(d, vm.SnapshotOp, ref, "checkpoint")
	if err != nil || r.Outcome != vm.Succeeded || r.Via != vm.ResultingResource || r.Resource.Resource != ref+"/checkpoint" {
		t.Fatalf("snapshot not reconciled: %+v %v", r, err)
	}
	if f.count("snapshot", ref, "take") != 1 {
		t.Fatal("snapshot repeated")
	}
}

// TestForeignOwnerNotAccepted proves W2/W4: a token carrying another thread's owner line is no evidence.
func TestForeignOwnerNotAccepted(t *testing.T) {
	f, j, d, drv := setup(t)
	f.loseOn = "clonevm" // force reconciliation rather than a response-settled create
	r, err := do(d, vm.CreateOp, "", "")
	if !errors.Is(err, vm.ErrReconciliation) {
		t.Fatalf("expected pause: %+v %v", r, err)
	}
	// Complete the marker as the driver would have, but with a foreign owner line.
	v := f.find("clone")
	v.extra["synch/token"], v.extra["synch/owner"] = "synch-"+r.ID, vm.OwnerLine("other-thread")
	if r, err = d.Reconcile(t.Context(), r.ID); !errors.Is(err, vm.ErrReconciliation) || r.Outcome == vm.Succeeded {
		t.Fatalf("foreign owner accepted: %+v %v", r, err)
	}
	items, err := drv.FindByToken(t.Context(), "lab", "synch-"+r.ID, thread)
	if err != nil || len(items) != 0 || len(j.owners) != 0 {
		t.Fatalf("foreign marker produced evidence or ownership: %+v %v", items, err)
	}
	if got, _ := drv.FindByToken(t.Context(), "lab", "synch-"+r.ID, "other-thread"); len(got) != 1 {
		t.Fatalf("exact owner must match: %+v", got)
	}
}

// TestRestoreVerifiesSnapshot proves W4: restore of a missing or ambiguous snapshot is refused before dispatch and settles not_executed.
func TestRestoreVerifiesSnapshot(t *testing.T) {
	f, _, d, drv := setup(t)
	r, _ := do(d, vm.CreateOp, "", "")
	ref := r.Resource.VMRef
	if _, err := do(d, vm.SnapshotOp, ref, "checkpoint"); err != nil {
		t.Fatal(err)
	}
	f.find(ref).snaps = append(f.find(ref).snaps, fakeSnap{"twin", "33333333-3333-3333-3333-000000000009", ""},
		fakeSnap{"twin", "33333333-3333-3333-3333-000000000010", ""})
	for _, name := range []string{"missing", "twin"} {
		r, err := do(d, vm.RestoreOp, ref, name)
		if err != nil || r.Outcome != vm.NotExecuted {
			t.Fatalf("%s: %+v %v", name, r, err)
		}
	}
	if f.count("snapshot", ref, "restore") != 0 {
		t.Fatal("unverified snapshot reached restore")
	}
	_, err := drv.Restore(t.Context(), vm.ExternalRequest{Endpoint: "lab"}, ref, "missing")
	if !vm.IsPreDispatch(err) || !errors.Is(err, vm.ErrSnapshot) {
		t.Fatalf("refusal must be pre-dispatch: %v", err)
	}
}

// TestOrphanDetection proves W4: a VM owned by a terminal thread with no disposition is listed.
func TestOrphanDetection(t *testing.T) {
	_, j, d, drv := setup(t)
	r, _ := do(d, vm.CreateOp, "", "")
	inv, _ := drv.Inventory(t.Context(), "lab")
	owner, _ := j.Ownership(t.Context(), "lab", r.Resource.VMRef)
	got := vm.Orphans("lab", inv, []vm.Ownership{owner}, func(s string) bool { return s == thread })
	if len(got) != 1 || got[0].VMRef != r.Resource.VMRef {
		t.Fatalf("orphan not listed: %+v", got)
	}
}

// TestSerializationPerVM proves W4: two mutations on one (host, VM) never overlap.
func TestSerializationPerVM(t *testing.T) {
	f, _, d, _ := setup(t)
	r, _ := do(d, vm.CreateOp, "", "")
	ref := r.Resource.VMRef
	f.hold, f.enter = make(chan struct{}), make(chan struct{})
	hold, enter := f.hold, f.enter
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := do(d, vm.StartOp, ref, ""); err != nil {
				t.Error(err)
			}
		}()
	}
	<-enter
	time.Sleep(50 * time.Millisecond) // give the second request time to (wrongly) overlap
	close(hold)
	wg.Wait()
	if f.maxIn != 1 || f.count("startvm") != 2 {
		t.Fatalf("mutations overlapped: max in flight %d", f.maxIn)
	}
}

// TestValidationAndPreDispatch proves W2: malformed refs and names are refused before dispatch with PreDispatch.
func TestValidationAndPreDispatch(t *testing.T) {
	f, _, _, drv := setup(t)
	req := vm.ExternalRequest{Endpoint: "lab", ID: "X"}
	if _, err := drv.Start(t.Context(), req, "not-a-uuid"); !vm.IsPreDispatch(err) {
		t.Fatalf("bad ref: %v", err)
	}
	if _, err := drv.CreateFromBase(t.Context(), req, baseID, vm.VMSpec{Name: "../x"}, thread); !vm.IsPreDispatch(err) {
		t.Fatalf("bad name: %v", err)
	}
	if _, err := drv.CreateFromBase(t.Context(), req, baseID, vm.VMSpec{Name: "base"}, thread); !vm.IsPreDispatch(err) {
		t.Fatalf("duplicate name: %v", err)
	}
	v, ok := drv.(vm.CommandValidator)
	if !ok || v.ValidateCommand(vm.Command{Endpoint: "nope", Thread: thread, Operation: vm.StartOp, VM: baseID}) == nil {
		t.Fatal("unknown endpoint accepted")
	}
	if f.count("clonevm") != 0 || f.count("startvm") != 0 {
		t.Fatal("refused request reached VBoxManage")
	}
}

// TestCapabilityMatrix proves W5: every real-host row is "not run: deferred by owner" and nothing is advertised.
func TestCapabilityMatrix(t *testing.T) {
	j := newJournal()
	drv, _ := virtualbox.New([]string{"lab"}, newHost(t, j))
	row, err := drv.Capabilities(t.Context())
	if err != nil || len(row.Ops) != 9 || len(row.Advertised) != 0 {
		t.Fatalf("matrix: %+v %v", row, err)
	}
	for op, v := range row.Ops {
		if v != "not run: deferred by owner" {
			t.Errorf("%s = %q", op, v)
		}
	}
	d := vm.NewDispatcher(drv, j, allow{})
	if _, err := do(d, vm.SnapshotOp, baseID, "checkpoint"); !errors.Is(err, vm.ErrUnavailable) {
		t.Fatalf("unadvertised snapshot must be unavailable: %v", err)
	}
}
