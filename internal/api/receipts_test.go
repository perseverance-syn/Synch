package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"synch/internal/eventlog"
	"synch/internal/queue"
	"synch/internal/thread"
	"testing"
)

const testToken = "caller-alice"
const testCaller = "operator:alice"

// fixture opens a real event store and thread machine for HTTP contract tests.
func fixture(t *testing.T) (*Server, *eventlog.Store, *MemoryTokens) {
	t.Helper()
	store, err := eventlog.Open(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	// Fixture threads have no supervised process, so terminal commands can settle.
	machine, err := thread.Open(t.Context(), store, thread.Hooks{
		ConfirmGone: func(context.Context, string) (bool, error) { return true, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	tokens := NewMemoryTokens()
	tokens.Set(testToken, testCaller)
	return New(Config{Store: store, Threads: machine, Tokens: tokens}), store, tokens
}

// request sends the given headers and body through the HTTP adapter.
func request(t *testing.T, s *Server, method, path, token, id, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequestWithContext(t.Context(), method, path, strings.NewReader(body))
	r.Host = "127.0.0.1"
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if id != "" {
		r.Header.Set(requestIDHeader, id)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}

// requireProblem checks the RFC 9457 shape and verifies failures without a receipt carry no receipt headers.
func requireProblem(t *testing.T, w *httptest.ResponseRecorder, status int, code, detail string) problem {
	t.Helper()
	p := decode[problem](t, w.Body.Bytes())
	if w.Code != status || p.Status != status || p.Code != code || p.Detail != detail || p.Title == "" || p.Type == "" || w.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("status %d headers %v problem %s", w.Code, w.Header(), w.Body)
	}
	if w.Header().Get(receiptIDHeader) != "" || w.Header().Get(replayedHeader) != "" {
		t.Fatalf("unexpected receipt headers: %v", w.Header())
	}
	return p
}

// decode parses a JSON response or fails with the returned body.
func decode[T any](t *testing.T, data []byte) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatalf("decode %q: %v", data, err)
	}
	return value
}

// create creates a coordinator via HTTP and returns its receipt thread ID.
func create(t *testing.T, s *Server, id string) string {
	t.Helper()
	w := request(t, s, "POST", "/v1/threads", testToken, id, `{"project_id":"p1","brief":"Fix CI"}`)
	if w.Code != 201 {
		t.Fatalf("creation: %d %s", w.Code, w.Body)
	}
	return stringFieldFromRecord(w.Body.Bytes(), "thread_id")
}

// --- Receipts: verify admission, caller identity, and concurrent replay ---

// TestReceiptOrderAndReplay proves AC2 and AC4 receipt order, caller scoping, canonical replay, and conflict.
func TestReceiptOrderAndReplay(t *testing.T) {
	s, _, tokens := fixture(t)
	path := "/v1/threads"
	good := `{"project_id":"p1","brief":"Fix CI"}`
	first := request(t, s, "POST", path, testToken, "c-1", good)
	if first.Code != 201 || first.Header().Get(replayedHeader) != "false" {
		t.Fatalf("first: %d %s", first.Code, first.Body)
	}
	receiptID := first.Header().Get(receiptIDHeader)
	digest := stringFieldFromRecord(first.Body.Bytes(), "payload_digest")
	if !ulidPattern.MatchString(receiptID) || digest != "sha256:89242a142a3c69cebbe2f28bf31cf7adcc97db62f1c1d3bbe0c9a2173e725883" {
		t.Fatalf("receipt: %s", first.Body)
	}

	tokens.Set(testToken, "")
	tokens.Set("rotated-token", testCaller)
	invalidBody := "{"
	requireProblem(t, request(t, s, "POST", path, testToken, "bad id", invalidBody), 401, "unauthorized", "")
	requireProblem(t, request(t, s, "POST", path, "rotated-token", "", invalidBody), 400, "request_id_required", "")
	requireProblem(t, request(t, s, "POST", path, "rotated-token", "bad id", invalidBody), 400, "request_id_invalid", "")
	requireProblem(t, request(t, s, "POST", path, "rotated-token", "c-1", invalidBody), 400, "malformed_body", "malformed_json")

	replay := request(t, s, "POST", path+"?ignored=1", "rotated-token", "c-1", `{ "brief":"Fix CI", "project_id":"p1"}`)
	if replay.Code != first.Code || replay.Body.String() != first.Body.String() || replay.Header().Get("Content-Type") != first.Header().Get("Content-Type") ||
		replay.Header().Get(replayedHeader) != "true" || replay.Header().Get(receiptIDHeader) != receiptID {
		t.Fatalf("replay: %d %v %s", replay.Code, replay.Header(), replay.Body)
	}
	conflict := requireProblem(t, request(t, s, "POST", path, "rotated-token", "c-1", `{"project_id":"p1","brief":"changed"}`), 409, "request_id_conflict", "")
	if conflict.ReceiptID != receiptID || conflict.StoredPayloadDigest != digest {
		t.Fatalf("conflict: %+v", conflict)
	}

	tokens.Set("bob", "operator:bob")
	receiptPath := "/v1/receipts/c-1"
	lookedUp := request(t, s, "GET", receiptPath, "rotated-token", "", "")
	if lookedUp.Code != 200 || lookedUp.Body.String() != first.Body.String() || lookedUp.Header().Get(receiptIDHeader) != receiptID {
		t.Fatalf("lookup: %d %s", lookedUp.Code, lookedUp.Body)
	}
	requireProblem(t, request(t, s, "GET", receiptPath, "bob", "", ""), 404, "not_found", "")
	bob := request(t, s, "POST", path, "bob", "c-1", good)
	if bob.Code != 201 || bob.Header().Get(receiptIDHeader) == receiptID {
		t.Fatalf("caller collision: %d %s", bob.Code, bob.Body)
	}
}

// TestMalformedBodyDetails proves AC4 rejects every prohibited raw shape before consuming an ID.
func TestMalformedBodyDetails(t *testing.T) {
	s, _, _ := fixture(t)
	for _, tc := range []struct{ raw, detail string }{
		{"\xff", "invalid_utf8"}, {"\xef\xbb\xbf{}", "invalid_utf8"}, {`{"a":"\ud800"}`, "invalid_unicode"},
		{`{"a":{"__proto__":0,"\u005f_proto__":1}}`, "duplicate_member"},
		{`{"a":-0}`, "number_not_integer"}, {`{"a":1e0}`, "number_not_integer"}, {`{"a":1.0}`, "number_not_integer"},
		{`{"a":9007199254740992}`, "number_out_of_range"}, {"{}{}", "malformed_json"},
		{"null", "body_must_be_object"}, {"[]", "body_must_be_object"}, {"", "body_must_be_object"},
	} {
		t.Run(tc.detail+tc.raw, func(t *testing.T) {
			requireProblem(t, request(t, s, "POST", "/v1/threads", testToken, "c-1", tc.raw), 400, "malformed_body", tc.detail)
		})
	}
	requireProblem(t, request(t, s, "POST", "/v1/grants/a/revoke", testToken, "c-1", "\xff"), 400, "malformed_body", "body_not_allowed")
	_ = create(t, s, "c-1")
}

// TestStoredRejection proves AC4 stores admitted schema failures, including explicit null, and replays them verbatim.
func TestStoredRejection(t *testing.T) {
	s, _, _ := fixture(t)
	path := "/v1/threads"
	body := `{"project_id":"p1","brief":"x","pin":null}`
	first := request(t, s, "POST", path, testToken, "reject", body)
	replay := request(t, s, "POST", path, testToken, "reject", body)
	if first.Code != 400 || replay.Code != 400 || first.Body.String() != replay.Body.String() ||
		first.Header().Get(replayedHeader) != "false" || replay.Header().Get(replayedHeader) != "true" ||
		first.Header().Get("Content-Type") != "application/problem+json" || replay.Header().Get("Content-Type") != first.Header().Get("Content-Type") {
		t.Fatalf("rejection: %d %v %s / %d %v %s", first.Code, first.Header(), first.Body, replay.Code, replay.Header(), replay.Body)
	}
	p := decode[problem](t, first.Body.Bytes())
	if p.Code != "validation_failed" || p.ReceiptID == "" || p.ReceiptID != first.Header().Get(receiptIDHeader) {
		t.Fatalf("problem: %+v", p)
	}
	requireProblem(t, request(t, s, "POST", path, testToken, "reject", `{"project_id":"p1","brief":"x"}`), 409, "request_id_conflict", "")
	lookup := request(t, s, "GET", "/v1/receipts/reject", testToken, "", "")
	if lookup.Code != 200 || stringFieldFromRecord(lookup.Body.Bytes(), "status") != "rejected" {
		t.Fatalf("lookup: %d %s", lookup.Code, lookup.Body)
	}
}

// TestConcurrentReceipts proves AC4 serializes same-caller retries and emits one acceptance.
func TestConcurrentReceipts(t *testing.T) {
	s, store, _ := fixture(t)
	views := make(chan *httptest.ResponseRecorder, 2)
	begin := make(chan struct{})
	for range 2 {
		go func() {
			<-begin
			views <- request(t, s, "POST", "/v1/threads", testToken, "race", `{"project_id":"p","brief":"once"}`)
		}()
	}
	close(begin)
	a, b := <-views, <-views
	if a.Code != 201 || b.Code != 201 || a.Body.String() != b.Body.String() ||
		a.Header().Get(receiptIDHeader) != b.Header().Get(receiptIDHeader) ||
		a.Header().Get(replayedHeader) == b.Header().Get(replayedHeader) {
		t.Fatalf("first and replay: %d %v %s / %d %v %s", a.Code, a.Header(), a.Body, b.Code, b.Header(), b.Body)
	}
	head, err := store.Head()
	if err != nil || head != 2 { // the receipt acceptance and thread.created
		t.Fatalf("head %d: %v", head, err)
	}
}

// --- Thread operations: verify state changes and D-16's post-commit handoff ---

// TestSelectHarness proves D-16 projects the selected harness from the accepted receipt.
func TestSelectHarness(t *testing.T) {
	s, store, _ := fixture(t)
	id := create(t, s, "create")
	path := "/v1/threads/" + id + "/commands"
	w := request(t, s, "POST", path, testToken, "select", `{"command":"select_harness","harness":"codex","model":"model-x"}`)
	if w.Code != 202 {
		t.Fatalf("select: %d %s", w.Code, w.Body)
	}
	record, err := queue.Lookup(t.Context(), store, testCaller, "select")
	if err != nil {
		t.Fatal(err)
	}
	result := decode[struct {
		Result map[string]string `json:"result"`
	}](t, record).Result
	if result["harness"] != "codex" || result["model"] != "model-x" {
		t.Fatalf("receipt: %s", record)
	}
	state, err := s.config.Threads.Status(t.Context(), id)
	if err != nil || !state.Facts.SelectedPair {
		t.Fatalf("selected pair %v: %v", state.Facts.SelectedPair, err)
	}
	got := request(t, s, "GET", "/v1/threads/"+id, testToken, "", "")
	view := decode[threadView](t, got.Body.Bytes())
	if got.Code != 200 || view.Harness != "codex" || view.Model != "model-x" {
		t.Fatalf("selected harness projection: %d %s", got.Code, got.Body)
	}
}

// TestModeChangeHandoff proves D-16 preserves mode intent and updates the exposed sandbox switch fact.
func TestModeChangeHandoff(t *testing.T) {
	s, _, _ := fixture(t)
	id := create(t, s, "create")
	path := "/v1/threads/" + id + "/commands"
	for _, mode := range []string{"sandbox", "provider", "full_control"} {
		w := request(t, s, "POST", path, testToken, mode, `{"command":"change_mode","target":"host-a","to":"`+mode+`"}`)
		if w.Code != 202 {
			t.Fatalf("%s: %d %s", mode, w.Code, w.Body)
		}
		result := decode[struct {
			Result map[string]string `json:"result"`
		}](t, w.Body.Bytes()).Result
		if result["target"] != "host-a" || result["to"] != mode {
			t.Fatalf("receipt: %s", w.Body)
		}
		state, err := s.config.Threads.Status(t.Context(), id)
		if err != nil || state.Facts.SwitchToSandbox != (mode == "sandbox") {
			t.Fatalf("%s switch flag %v: %v", mode, state.Facts.SwitchToSandbox, err)
		}
	}
}

// TestThreadQueriesAndCommands proves AC3 reads filtered threads and applies controls during disk pressure.
func TestThreadQueriesAndCommands(t *testing.T) {
	s, _, _ := fixture(t)
	id := create(t, s, "create")
	path := "/v1/threads/" + id
	list := request(t, s, "GET", "/v1/threads?project_id=p1&state=draft", testToken, "", "")
	views := decode[struct {
		Threads []threadView `json:"threads"`
	}](t, list.Body.Bytes()).Threads
	if list.Code != 200 || len(views) != 1 || views[0].ID != id || views[0].Role != "coordinator" {
		t.Fatalf("list: %d %s", list.Code, list.Body)
	}
	want := request(t, s, "GET", "/v1/threads?project_id=p2", testToken, "", "")
	if want.Code != 200 || want.Body.String() != `{"threads":[]}` {
		t.Fatalf("filtered: %d %s", want.Code, want.Body)
	}
	s.config.DiskPressure = func() bool { return true }
	pressure := request(t, s, "POST", "/v1/threads", testToken, "new", `{"project_id":"p1","brief":"blocked"}`)
	if pressure.Code != 422 || decode[problem](t, pressure.Body.Bytes()).Code != "disk_pressure" {
		t.Fatalf("pressure: %d %s", pressure.Code, pressure.Body)
	}
	cancel := request(t, s, "POST", path+"/commands", testToken, "cancel", `{"command":"cancel"}`)
	if cancel.Code != 202 {
		t.Fatalf("cancel: %d %s", cancel.Code, cancel.Body)
	}
	got := request(t, s, "GET", path, testToken, "", "")
	if got.Code != 200 || decode[threadView](t, got.Body.Bytes()).State != "cancelled" {
		t.Fatalf("get: %d %s", got.Code, got.Body)
	}
}

// TestPlanApproval proves AC3 applies T03 only for the plan revision shown for review.
func TestPlanApproval(t *testing.T) {
	s, _, _ := fixture(t)
	created := request(t, s, "POST", "/v1/threads", testToken, "create", `{"project_id":"p1","brief":"review","review_plan_first":true}`)
	id := stringFieldFromRecord(created.Body.Bytes(), "thread_id")
	if created.Code != 201 || id == "" {
		t.Fatalf("create: %d %s", created.Code, created.Body)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	machine := s.config.Threads
	if err := machine.Update(t.Context(), id, func(f *thread.Facts) error {
		f.PlanDigest = digest
		f.PlanRevision = 1
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := machine.Apply(t.Context(), id, thread.Command{Transition: "T02"}); err != nil {
		t.Fatal(err)
	}
	state, err := machine.Status(t.Context(), id)
	if err != nil || state.Phase != "plan_review" {
		t.Fatalf("plan_review: %s %v", state.Phase, err)
	}
	path := "/v1/threads/" + id + "/commands"
	wrong := "sha256:" + strings.Repeat("b", 64)
	w := request(t, s, "POST", path, testToken, "wrong", `{"command":"approve_plan","plan_digest":"`+wrong+`"}`)
	if w.Code != 409 || decode[problem](t, w.Body.Bytes()).Code != "invalid_transition" {
		t.Fatalf("stale plan: %d %s", w.Code, w.Body)
	}
	w = request(t, s, "POST", path, testToken, "right", `{"command":"approve_plan","plan_digest":"`+digest+`"}`)
	if w.Code != 202 {
		t.Fatalf("approve_plan: %d %s", w.Code, w.Body)
	}
	state, err = machine.Status(t.Context(), id)
	if err != nil || state.Phase != "queued" {
		t.Fatalf("queued: %s %v", state.Phase, err)
	}
}

// TestDecideCommand proves AC3 accepts control evidence while a thread is still a draft.
func TestDecideCommand(t *testing.T) {
	s, _, _ := fixture(t)
	id := create(t, s, "create")
	body := `{"command":"decide","decision_id":"decision-1","choice":"continue"}`
	path := "/v1/threads/" + id + "/commands"
	w := request(t, s, "POST", path, testToken, "decide", body)
	if w.Code != 202 {
		t.Fatalf("decide: %d %s", w.Code, w.Body)
	}
	result := decode[struct {
		Result map[string]string `json:"result"`
	}](t, w.Body.Bytes()).Result
	if result["decision_id"] != "decision-1" || result["choice"] != "continue" {
		t.Fatalf("receipt: %s", w.Body)
	}
}

// --- Authority operations: exercise the injected policy boundary and transaction rollback ---

// authorityStub returns transactional effects for each deferred policy route.
type authorityStub struct {
	id   string
	fail bool
}

// Approve reports the identity passed by the HTTP adapter.
func (a *authorityStub) Approve(_ context.Context, _ *eventlog.Transaction, caller, id string, _ map[string]any) (queue.Decision, error) {
	return queue.Decision{ThreadID: id, Commit: func(context.Context, *eventlog.Transaction) (any, error) {
		return map[string]string{"approver": caller}, nil
	}}, nil
}

// Grant optionally returns a transient failure before a receipt may be stored.
func (a *authorityStub) Grant(context.Context, *eventlog.Transaction, string, string, map[string]any) (queue.Decision, error) {
	if a.fail {
		return queue.Decision{}, errors.New("transient policy failure")
	}
	return queue.Decision{ThreadID: a.id}, nil
}

// Revoke associates the grant's acceptance with its originating thread.
func (a *authorityStub) Revoke(_ context.Context, _ *eventlog.Transaction, _, id string) (queue.Decision, error) {
	return queue.Decision{ThreadID: a.id, Commit: func(context.Context, *eventlog.Transaction) (any, error) {
		return map[string]string{"grant_id": id}, nil
	}}, nil
}

// TestAuthorityAndRollback proves AC3 and AC4 integrate policy routes and do not store transient failures.
func TestAuthorityAndRollback(t *testing.T) {
	s, _, _ := fixture(t)
	id := create(t, s, "create")
	stub := &authorityStub{id: id, fail: true}
	s.config.Authority = stub
	sha := "sha256:" + strings.Repeat("a", 64)
	approve := request(t, s, "POST", "/v1/threads/"+id+"/approvals", testToken, "approve",
		`{"plan_digest":"`+sha+`","worktree_digest":"`+sha+`","target":"prod","actions":["deploy.production"],"rollback":"restore","irreversible_effects":[],"expires_at":"2026-11-18T20:45:00Z"}`)
	if approve.Code != 201 || !strings.Contains(approve.Body.String(), testCaller) {
		t.Fatalf("approve: %d %s", approve.Code, approve.Body)
	}

	path := "/v1/threads/" + id + "/grants"
	body := `{"host":"h","identity":"i"}`
	requireProblem(t, request(t, s, "POST", path, testToken, "grant", body), 500, "internal", "")
	requireProblem(t, request(t, s, "GET", "/v1/receipts/grant", testToken, "", ""), 404, "not_found", "")
	stub.fail = false
	retry := request(t, s, "POST", path, testToken, "grant", body)
	if retry.Code != 201 || retry.Header().Get(replayedHeader) != "false" {
		t.Fatalf("retry: %d %s", retry.Code, retry.Body)
	}
	revoke := request(t, s, "POST", "/v1/grants/g%2F1/revoke", testToken, "revoke", "")
	if revoke.Code != 202 || !strings.Contains(revoke.Body.String(), `"grant_id":"g/1"`) {
		t.Fatalf("revoke: %d %s", revoke.Code, revoke.Body)
	}
}

var _ http.Handler = (*Server)(nil)

// TestCreateThreadWritesCreationFact proves card 113 G4: the API writes the catalogue
// thread.created with project and depth at creation, in the creating transaction, and an
// idempotent replay does not write it again.
func TestCreateThreadWritesCreationFact(t *testing.T) {
	s, store, _ := fixture(t)
	id := create(t, s, "creation-fact")
	create(t, s, "creation-fact") // same request ID: a replay
	events, err := store.ReadStream("thread:"+id, 0, 16)
	if err != nil {
		t.Fatal(err)
	}
	var created []string
	for _, e := range events {
		if e.Type == "thread.created" {
			created = append(created, string(e.Payload))
		}
	}
	if len(created) != 1 || !strings.Contains(created[0], `"project_id":"p1"`) || !strings.Contains(created[0], `"depth":0`) ||
		!strings.Contains(created[0], `"role":"coordinator"`) {
		t.Fatalf("thread.created events: %v", created)
	}
}
