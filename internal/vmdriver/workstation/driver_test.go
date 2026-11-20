package workstation_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	vm "synch/internal/vmdriver"
	"synch/internal/vmdriver/workstation"
)

const (
	thread   = "01HF7YAT000000000000000001"
	basePath = `C:\VMs\base\base.vmx`
	clonePth = `C:\VMs\clone\clone.vmx`
	username = "ops-user"
	password = "s3cret-pw-9f2"
)

// --- Fake vmrun + vmrest ---
// vmrun output shapes follow VMware's "Using vmrun to Control Virtual Machines":
// `list` prints "Total running VMs: N" then one .vmx path per line;
// `listSnapshots` prints "Total snapshots: N" then one name per line; clone,
// start, stop, snapshot, revertToSnapshot and deleteVM print nothing on success.
// vmrest shapes follow the VMware Workstation Pro REST API reference:
// GET /api/vms -> [{id,path}], POST /api/vms/registration {name,path} -> {id,path},
// GET /api/vms/{id}/params/{name} -> {name,value}, PUT /api/vms/{id}/configparams {name,value}.

type fakeVM struct {
	id, path   string
	registered bool
	running    bool
	note       string
	snaps      []string
}

// host is an in-memory Workstation host serving both the vmrun runner and vmrest.
type host struct {
	mu       sync.Mutex
	t        *testing.T
	journal  *testJournal
	vms      []*fakeVM
	calls    [][]string
	loseOn   string // vmrun verb whose reply is dropped after it applied
	loseNote bool   // drop the connection after the annotation was stored
	hold     chan struct{}
	enter    chan struct{}
	inflight int
	maxIn    int
	n        int
}

// newHost returns a host with one registered base VM.
func newHost(t *testing.T, j *testJournal) *host {
	return &host{t: t, journal: j, vms: []*fakeVM{{id: "ID0", path: basePath, registered: true}}}
}

// count returns how many vmrun calls used the verb (argv[3]).
func (h *host) count(verb string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, c := range h.calls {
		if c[3] == verb {
			n++
		}
	}
	return n
}

// get finds a VM by .vmx path.
func (h *host) get(path string) *fakeVM {
	for _, v := range h.vms {
		if v.path == path {
			return v
		}
	}
	return nil
}

// checkPersisted asserts attribution predates the mutation (vm-driver §3.2).
func (h *host) checkPersisted(token string) {
	rec, err := h.journal.Load(context.Background(), strings.TrimPrefix(token, "synch-"))
	if err != nil || len(rec.History) != 0 {
		h.t.Errorf("mutation lacked a persisted request: %v", err)
	}
}

// Run implements workstation.Runner against the fake host.
func (h *host) Run(_ context.Context, _ string, argv []string) (workstation.Result, error) {
	h.mu.Lock()
	h.calls = append(h.calls, argv)
	if argv[0] != "vmrun" || argv[1] != "-T" || argv[2] != "ws" || strings.Contains(strings.Join(argv, " "), password) {
		h.t.Errorf("unexpected or credential-bearing command %q", argv)
	}
	verb := argv[3]
	mutating := verb != "list" && verb != "listSnapshots"
	if mutating {
		h.inflight++
		h.maxIn = max(h.maxIn, h.inflight)
	}
	hold, enter := h.hold, h.enter
	if verb == "start" && hold != nil {
		h.hold, h.enter = nil, nil
	}
	res := h.apply(argv)
	lose := mutating && verb == h.loseOn && res.ExitCode == 0
	if lose {
		h.loseOn = ""
	}
	h.mu.Unlock()
	if verb == "start" && hold != nil {
		close(enter)
		<-hold
	}
	if mutating {
		h.mu.Lock()
		h.inflight--
		h.mu.Unlock()
	}
	if lose {
		return workstation.Result{}, errors.New("connection lost")
	}
	return res, nil
}

// apply executes one vmrun verb. Caller holds h.mu.
func (h *host) apply(a []string) workstation.Result {
	fail := workstation.Result{Stdout: "Error: The virtual machine cannot be found", ExitCode: 1}
	switch a[3] {
	case "list":
		out := "Total running VMs: 0\n"
		var run []string
		for _, v := range h.vms {
			if v.running {
				run = append(run, v.path)
			}
		}
		if len(run) > 0 {
			out = fmt.Sprintf("Total running VMs: %d\n%s\n", len(run), strings.Join(run, "\n"))
		}
		return workstation.Result{Stdout: out}
	case "clone":
		if h.get(a[4]) == nil {
			return fail
		}
		h.n++
		h.vms = append(h.vms, &fakeVM{id: fmt.Sprintf("ID%d", h.n), path: a[5]})
		return workstation.Result{}
	case "listSnapshots":
		v := h.get(a[4])
		if v == nil {
			return fail
		}
		return workstation.Result{Stdout: fmt.Sprintf("Total snapshots: %d\n%s", len(v.snaps), strings.Join(v.snaps, "\n"))}
	}
	v := h.get(a[4])
	if v == nil {
		return fail
	}
	switch a[3] {
	case "start", "stop":
		v.running = a[3] == "start"
	case "snapshot":
		v.snaps = append(v.snaps, a[5])
	case "deleteVM":
		for i := range h.vms {
			if h.vms[i] == v {
				h.vms = append(h.vms[:i], h.vms[i+1:]...)
				break
			}
		}
	case "revertToSnapshot":
	default:
		h.t.Errorf("unexpected vmrun verb %q", a[3])
	}
	return workstation.Result{}
}

// serve handles vmrest requests, enforcing basic auth on every one.
func (h *host) serve(w http.ResponseWriter, r *http.Request) {
	if u, p, ok := r.BasicAuth(); !ok || u != username || p != password {
		h.t.Error("vmrest request without the broker's basic auth")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	write := func(code int, v any) {
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	var body struct{ Name, Path, Value string }
	_ = json.NewDecoder(r.Body).Decode(&body)
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/vms"), "/"), "/")
	switch {
	case r.Method == http.MethodGet && parts[0] == "":
		var list []map[string]string
		for _, v := range h.vms {
			if v.registered {
				list = append(list, map[string]string{"id": v.id, "path": v.path})
			}
		}
		write(200, list)
	case r.Method == http.MethodPost && parts[0] == "registration":
		if v := h.get(body.Path); v != nil {
			v.registered = true
			write(201, map[string]string{"id": v.id, "path": v.path})
			return
		}
		write(404, map[string]string{"Message": "not found"})
	case r.Method == http.MethodGet && len(parts) == 3 && parts[1] == "params" && parts[2] == "annotation":
		for _, v := range h.vms {
			if v.id == parts[0] {
				write(200, map[string]string{"name": "annotation", "value": v.note})
				return
			}
		}
		write(404, map[string]string{"Message": "not found"})
	case r.Method == http.MethodPut && len(parts) == 2 && parts[1] == "configparams" && body.Name == "annotation":
		h.checkPersisted(strings.SplitN(body.Value, "\n", 2)[0])
		for _, v := range h.vms {
			if v.id == parts[0] {
				v.note = body.Value
			}
		}
		if h.loseNote {
			h.loseNote = false
			conn, _, err := http.NewResponseController(w).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		h.t.Errorf("unexpected vmrest call %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}
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
// the shipped matrix stays "not run: deferred by owner". Token carriage is the
// driver's real declaration (create only).
type advertising struct{ vm.VMDriver }

// Capabilities lets the dispatcher admit snapshot and restore in tests.
func (a advertising) Capabilities(ctx context.Context) (vm.MatrixRow, error) {
	row, err := a.VMDriver.Capabilities(ctx)
	row.Advertised = []vm.Operation{vm.SnapshotOp, vm.RestoreOp}
	row.Ops = map[vm.Operation]string{vm.SnapshotOp: "passed", vm.RestoreOp: "passed", "lost_response_reconcile_snapshot": "passed"}
	return row, err
}

// broker returns the fixed vmrest basic-auth credential.
func broker(context.Context, string) (vm.Credentials, error) {
	return vm.Credentials{Username: username, Password: password}, nil
}

// setup builds the fake host, vmrest TLS server, driver and dispatcher.
func setup(t *testing.T) (*host, *testJournal, *vm.Dispatcher, vm.VMDriver) {
	t.Helper()
	j := newJournal()
	h := newHost(t, j)
	srv := httptest.NewTLSServer(http.HandlerFunc(h.serve))
	t.Cleanup(srv.Close)
	drv, err := workstation.New(map[string]string{"lab": srv.URL}, h, vm.BrokerFunc(broker), srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	return h, j, vm.NewDispatcher(advertising{drv}, j, allow{}), drv
}

// do dispatches one command on the lab host for the test thread.
func do(d *vm.Dispatcher, op vm.Operation, ref, name string) (vm.Resolution, error) {
	cmd := vm.Command{Endpoint: "lab", Thread: thread, Operation: op, VM: ref, Snapshot: name}
	if op == vm.CreateOp {
		cmd.Base, cmd.Spec = basePath, vm.VMSpec{Name: "clone"}
	}
	return d.Dispatch(context.Background(), cmd)
}

// --- Tests ---

// TestOperations proves W1 and W4: every operation succeeds through the dispatcher, ownership follows create and delete.
func TestOperations(t *testing.T) {
	h, j, d, drv := setup(t)
	r, err := do(d, vm.CreateOp, "", "")
	if err != nil || r.Outcome != vm.Succeeded || r.Resource == nil || r.Resource.VMRef != clonePth {
		t.Fatalf("create: %+v %v", r, err)
	}
	if o, _ := j.Ownership(t.Context(), "lab", clonePth); o.OwnerThread != thread || o.Basis != vm.Created {
		t.Fatalf("ownership not recorded: %+v", o)
	}
	for _, step := range []struct {
		op   vm.Operation
		name string
	}{{vm.StartOp, ""}, {vm.SnapshotOp, "checkpoint"}, {vm.StopOp, ""}, {vm.RestoreOp, "checkpoint"}} {
		if r, err := do(d, step.op, clonePth, step.name); err != nil || r.Outcome != vm.Succeeded {
			t.Fatalf("%s: %+v %v", step.op, r, err)
		}
	}
	vms, err := drv.Inventory(t.Context(), "lab")
	if err != nil || len(vms) != 2 || vms[1].State != "poweredOff" || vms[1].Name != "clone" || vms[1].Ref != clonePth {
		t.Fatalf("inventory: %+v %v", vms, err)
	}
	if _, err := do(d, vm.StartOp, clonePth, ""); err != nil {
		t.Fatal(err)
	}
	if vms, _ = drv.Inventory(t.Context(), "lab"); vms[1].State != "poweredOn" {
		t.Fatalf("running state not reported: %+v", vms)
	}
	if _, err := do(d, vm.StopOp, clonePth, ""); err != nil {
		t.Fatal(err)
	}
	if r, err := do(d, vm.DeleteOp, clonePth, ""); err != nil || r.Outcome != vm.Succeeded || h.get(clonePth) != nil {
		t.Fatalf("delete: %+v %v", r, err)
	}
	if _, err := drv.GetTask(t.Context(), "lab", vm.TaskRef{}); !errors.Is(err, vm.ErrUnavailable) {
		t.Fatalf("get_task must be unavailable: %v", err)
	}
}

// TestNotOwnerRefused proves W2: an unowned VM is refused before any vmrun mutation.
func TestNotOwnerRefused(t *testing.T) {
	h, _, d, _ := setup(t)
	if _, err := do(d, vm.StartOp, basePath, ""); !errors.Is(err, vm.ErrNotOwner) || h.count("start") != 0 {
		t.Fatalf("not_owner expected without dispatch: %v", err)
	}
}

// TestLostCloneReplyPausesWithoutRepeat proves W2: a lost clone reply leaves no marker, so the request stays unknown and is never repeated.
func TestLostCloneReplyPausesWithoutRepeat(t *testing.T) {
	h, _, d, _ := setup(t)
	h.loseOn = "clone"
	r, err := do(d, vm.CreateOp, "", "")
	if !errors.Is(err, vm.ErrReconciliation) || r.Outcome != vm.Unknown {
		t.Fatalf("lost clone must pause: %+v %v", r, err)
	}
	if _, err := d.Reconcile(t.Context(), r.ID); !errors.Is(err, vm.ErrReconciliation) || h.count("clone") != 1 {
		t.Fatalf("reconcile must stay unknown without repeating: %v clones=%d", err, h.count("clone"))
	}
}

// TestLostCreateReplyReconcilesByToken proves W2/W4: a lost annotation reply reconciles by token and records ownership.
func TestLostCreateReplyReconcilesByToken(t *testing.T) {
	h, j, d, _ := setup(t)
	h.loseNote = true
	r, err := do(d, vm.CreateOp, "", "")
	if err != nil || r.Outcome != vm.Succeeded || r.Via != vm.ResultingResource || h.count("clone") != 1 {
		t.Fatalf("lost reply must reconcile by token: %+v %v", r, err)
	}
	if o, _ := j.Ownership(t.Context(), "lab", clonePth); o.OwnerThread != thread {
		t.Fatalf("ownership missing after reconcile: %+v", o)
	}
}

// TestLostSnapshotStaysUnknown proves W2/W4/Q-55: vmrun snapshots carry no token, so a lost reply is unknown, never repeated, and never guessed from a name.
func TestLostSnapshotStaysUnknown(t *testing.T) {
	h, _, d, drv := setup(t)
	if _, err := do(d, vm.CreateOp, "", ""); err != nil {
		t.Fatal(err)
	}
	row, _ := drv.Capabilities(t.Context())
	for _, op := range row.TokenCarried {
		if op == vm.SnapshotOp {
			t.Fatal("snapshot must not declare token carriage")
		}
	}
	h.loseOn = "snapshot"
	r, err := do(d, vm.SnapshotOp, clonePth, "checkpoint")
	if !errors.Is(err, vm.ErrReconciliation) || r.Outcome != vm.Unknown {
		t.Fatalf("lost snapshot must pause: %+v %v", r, err)
	}
	if _, err := d.Reconcile(t.Context(), r.ID); !errors.Is(err, vm.ErrReconciliation) || h.count("snapshot") != 1 {
		t.Fatalf("reconcile must not guess or repeat: %v snapshots=%d", err, h.count("snapshot"))
	}
}

// TestForeignOwnerNotAccepted proves W2/W4: a token carrying another thread's owner line is no evidence.
func TestForeignOwnerNotAccepted(t *testing.T) {
	h, j, d, drv := setup(t)
	h.loseOn = "clone" // force reconciliation rather than a response-settled create
	r, err := do(d, vm.CreateOp, "", "")
	if !errors.Is(err, vm.ErrReconciliation) {
		t.Fatalf("expected pause: %+v %v", r, err)
	}
	// The VM now carries the token but a foreign owner line.
	v := h.get(clonePth)
	v.registered, v.note = true, "synch-"+r.ID+"\n"+vm.OwnerLine("other-thread")
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
	h, _, d, drv := setup(t)
	if _, err := do(d, vm.CreateOp, "", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := do(d, vm.SnapshotOp, clonePth, "checkpoint"); err != nil {
		t.Fatal(err)
	}
	h.get(clonePth).snaps = append(h.get(clonePth).snaps, "twin", "twin")
	for _, name := range []string{"missing", "twin"} {
		r, err := do(d, vm.RestoreOp, clonePth, name)
		if err != nil || r.Outcome != vm.NotExecuted {
			t.Fatalf("%s: %+v %v", name, r, err)
		}
	}
	if h.count("revertToSnapshot") != 0 {
		t.Fatal("unverified snapshot reached revertToSnapshot")
	}
	_, err := drv.Restore(t.Context(), vm.ExternalRequest{Endpoint: "lab"}, clonePth, "missing")
	if !vm.IsPreDispatch(err) || !errors.Is(err, vm.ErrSnapshot) {
		t.Fatalf("refusal must be pre-dispatch: %v", err)
	}
}

// TestOrphanDetection proves W4: a VM owned by a terminal thread with no disposition is listed.
func TestOrphanDetection(t *testing.T) {
	_, j, d, drv := setup(t)
	if _, err := do(d, vm.CreateOp, "", ""); err != nil {
		t.Fatal(err)
	}
	inv, _ := drv.Inventory(t.Context(), "lab")
	owner, _ := j.Ownership(t.Context(), "lab", clonePth)
	got := vm.Orphans("lab", inv, []vm.Ownership{owner}, func(s string) bool { return s == thread })
	if len(got) != 1 || got[0].VMRef != clonePth {
		t.Fatalf("orphan not listed: %+v", got)
	}
}

// TestSerializationPerVM proves W4: two mutations on one (host, VM) never overlap.
func TestSerializationPerVM(t *testing.T) {
	h, _, d, _ := setup(t)
	if _, err := do(d, vm.CreateOp, "", ""); err != nil {
		t.Fatal(err)
	}
	h.hold, h.enter = make(chan struct{}), make(chan struct{})
	hold, enter := h.hold, h.enter
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := do(d, vm.StartOp, clonePth, ""); err != nil {
				t.Error(err)
			}
		}()
	}
	<-enter
	time.Sleep(50 * time.Millisecond) // give the second request time to (wrongly) overlap
	close(hold)
	wg.Wait()
	if h.maxIn != 1 || h.count("start") != 2 {
		t.Fatalf("mutations overlapped: max in flight %d", h.maxIn)
	}
}

// TestCredentialsNeverLeak proves W3: broker secrets stay out of errors and commands, and a response echoing the password is refused.
func TestCredentialsNeverLeak(t *testing.T) {
	const leaky = "broker-failure-" + password
	for _, test := range []struct {
		name   string
		broker vm.BrokerFunc
		echo   bool
	}{
		{"broker_error", func(context.Context, string) (vm.Credentials, error) { return vm.Credentials{}, errors.New(leaky) }, false},
		{"echoed_password", broker, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			j := newJournal()
			h := newHost(t, j)
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if test.echo {
					_, _ = w.Write([]byte(`[{"id":"x","path":"` + password + `"}]`))
					return
				}
				h.serve(w, r)
			}))
			defer srv.Close()
			drv, _ := workstation.New(map[string]string{"lab": srv.URL}, h, test.broker, srv.Client())
			_, err := drv.Inventory(t.Context(), "lab")
			if err == nil || strings.Contains(err.Error(), password) {
				t.Fatalf("error missing or leaking: %v", err)
			}
			_, err = drv.CreateFromBase(t.Context(), vm.ExternalRequest{Endpoint: "lab", ID: "X"}, basePath, vm.VMSpec{Name: "clone"}, thread)
			if err == nil || !vm.IsPreDispatch(err) || strings.Contains(err.Error(), password) {
				t.Fatalf("create must refuse before dispatch without leaking: %v", err)
			}
			if h.count("clone") != 0 {
				t.Fatal("create reached vmrun without credentials")
			}
		})
	}
}

// TestValidationAndPreDispatch proves W2: malformed refs and names are refused before dispatch with PreDispatch.
func TestValidationAndPreDispatch(t *testing.T) {
	h, _, _, drv := setup(t)
	req := vm.ExternalRequest{Endpoint: "lab", ID: "X"}
	if _, err := drv.Start(t.Context(), req, `relative\x.vmx`); !vm.IsPreDispatch(err) {
		t.Fatalf("bad ref: %v", err)
	}
	if _, err := drv.CreateFromBase(t.Context(), req, basePath, vm.VMSpec{Name: `..\x`}, thread); !vm.IsPreDispatch(err) {
		t.Fatalf("bad name: %v", err)
	}
	if _, err := drv.CreateFromBase(t.Context(), req, basePath, vm.VMSpec{Name: "base"}, thread); err != nil {
		// "base" maps to C:\VMs\base\base.vmx, which is already in the library.
		if !vm.IsPreDispatch(err) {
			t.Fatalf("existing destination: %v", err)
		}
	} else {
		t.Fatal("existing destination accepted")
	}
	v, ok := drv.(vm.CommandValidator)
	if !ok || v.ValidateCommand(vm.Command{Endpoint: "nope", Thread: thread, Operation: vm.StartOp, VM: basePath}) == nil {
		t.Fatal("unknown endpoint accepted")
	}
	if h.count("clone") != 0 || h.count("start") != 0 {
		t.Fatal("refused request reached vmrun")
	}
}

// TestCapabilityMatrix proves W5: every real-host row is "not run: deferred by owner" and nothing is advertised.
func TestCapabilityMatrix(t *testing.T) {
	_, j, _, drv := setup(t)
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
	if _, err := do(d, vm.SnapshotOp, basePath, "checkpoint"); !errors.Is(err, vm.ErrUnavailable) {
		t.Fatalf("unadvertised snapshot must be unavailable: %v", err)
	}
}
