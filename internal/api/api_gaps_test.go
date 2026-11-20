package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"synch/internal/eventlog"
	"synch/internal/thread"
	"testing"
)

// seed appends a thread event and returns its id.
func seedEvent(t *testing.T, store *eventlog.Store, thread, kind string, version int, payload string) string {
	t.Helper()
	id := eventlog.NewID()
	_, err := store.Append(t.Context(), []eventlog.NewEvent{{ID: id, Stream: "thread:" + thread, ThreadID: thread, Type: kind, TypeVersion: version,
		Actor: eventlog.Actor{Kind: "daemon", ID: "local"}, Payload: json.RawMessage(payload)}})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// countEvents counts events of one type on a thread stream.
func countEvents(t *testing.T, store *eventlog.Store, thread, kind string) int {
	t.Helper()
	events, err := store.ReadStream("thread:"+thread, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range events {
		if e.Type == kind {
			n++
		}
	}
	return n
}

func waiting(t *testing.T, store *eventlog.Store, thread, action, target string) string {
	t.Helper()
	return seedEvent(t, store, thread, "policy.decided", 2,
		`{"action":"`+action+`","decision":"ask","machine":"local","target":"`+target+`","outcome":"waiting_approval"}`)
}

// TestNewEndpointsRequireAuth proves every new route refuses a caller without a token.
func TestNewEndpointsRequireAuth(t *testing.T) {
	s, _, _ := fixture(t)
	id := "01JD3Q8Z7Y6X5W4V3T2S1R0QPN"
	for method, paths := range map[string][]string{
		"GET":  {"/v1/threads/" + id + "/diff", "/v1/approvals", "/v1/findings"},
		"POST": {"/v1/approvals/" + id + "/reject", "/v1/approvals/" + id + "/request-changes"},
	} {
		for _, path := range paths {
			requireProblem(t, request(t, s, method, path, "", "r1", `{"reason":"x"}`), 401, "unauthorized", "")
		}
	}
}

// TestApprovalRequestsAndDecline covers A3 and A4: listing, rejection, replay and errors.
func TestApprovalRequestsAndDecline(t *testing.T) {
	s, store, _ := fixture(t)
	id := create(t, s, "create")
	first := waiting(t, store, id, "deploy.staging", "staging-01")
	second := waiting(t, store, id, "deploy.staging", "staging-02")
	type list struct {
		Approvals []approvalRequest `json:"approvals"`
	}
	get := func(query string) []approvalRequest {
		w := request(t, s, "GET", "/v1/approvals"+query, testToken, "", "")
		if w.Code != 200 {
			t.Fatalf("list %s: %d %s", query, w.Code, w.Body)
		}
		return decode[list](t, w.Body.Bytes()).Approvals
	}
	if got := get("?state=pending"); len(got) != 2 || got[0].RequestID != first || got[0].ThreadID != id || got[0].Target != "staging-01" || got[0].Action != "deploy.staging" || got[0].RequestedAt.IsZero() {
		t.Fatalf("pending: %+v", got)
	}
	if len(get("?state=decided")) != 0 || len(get("?thread="+id)) != 2 || len(get("?thread=01JD3Q8Z7Y6X5W4V3T2S1R0QPN")) != 0 {
		t.Fatal("filters")
	}
	requireProblem(t, request(t, s, "GET", "/v1/approvals?state=bogus", testToken, "", ""), 400, "validation_failed", "invalid state or thread filter")
	requireProblem(t, request(t, s, "GET", "/v1/approvals?thread=nope", testToken, "", ""), 400, "validation_failed", "invalid state or thread filter")

	path := "/v1/approvals/" + first + "/reject"
	requireProblem(t, request(t, s, "POST", path, testToken, "", `{"reason":"no"}`), 400, "request_id_required", "")
	// Validation failures and unknown requests are stored rejections with receipts.
	if w := request(t, s, "POST", path, testToken, "bad", `{}`); w.Code != 400 || w.Header().Get(receiptIDHeader) == "" {
		t.Fatalf("missing reason: %d %s", w.Code, w.Body)
	}
	if w := request(t, s, "POST", "/v1/approvals/"+id+"/reject", testToken, "unknown", `{"reason":"no"}`); w.Code != 404 {
		t.Fatalf("unknown request: %d %s", w.Code, w.Body)
	}
	w := request(t, s, "POST", path, testToken, "rej", `{"reason":"change window"}`)
	if w.Code != 201 || w.Header().Get(replayedHeader) != "false" {
		t.Fatalf("reject: %d %s", w.Code, w.Body)
	}
	replay := request(t, s, "POST", path, testToken, "rej", `{"reason":"change window"}`)
	if replay.Code != 201 || replay.Header().Get(replayedHeader) != "true" || replay.Header().Get(receiptIDHeader) != w.Header().Get(receiptIDHeader) || replay.Body.String() != w.Body.String() {
		t.Fatalf("replay: %d %s", replay.Code, replay.Body)
	}
	if n := countEvents(t, store, id, "approval.declined"); n != 1 {
		t.Fatalf("replay wrote %d declined events", n)
	}
	requireProblem(t, request(t, s, "POST", path, testToken, "rej", `{"reason":"different"}`), 409, "request_id_conflict", "")
	decided := get("?state=decided")
	if len(decided) != 1 || decided[0].RequestID != first || decided[0].Decision == nil || decided[0].Decision.Outcome != "rejected" ||
		decided[0].Decision.Actor != testCaller || decided[0].Decision.Reason != "change window" || decided[0].Decision.At.IsZero() {
		t.Fatalf("decided: %+v", decided)
	}
	// A request already decided cannot be decided again, under a new request id.
	again := request(t, s, "POST", "/v1/approvals/"+first+"/request-changes", testToken, "again", `{"reason":"late"}`)
	if again.Code != 409 || !strings.Contains(again.Body.String(), "invalid_transition") {
		t.Fatalf("redecide: %d %s", again.Code, again.Body)
	}

	changes := request(t, s, "POST", "/v1/approvals/"+second+"/request-changes", testToken, "chg", `{"reason":"narrow the target"}`)
	if changes.Code != 201 || !strings.Contains(changes.Body.String(), `"decision":"changes_requested"`) {
		t.Fatalf("request-changes: %d %s", changes.Code, changes.Body)
	}
	if got := get("?state=pending"); len(got) != 0 {
		t.Fatalf("pending after both: %+v", got)
	}
	if got := get("?thread=" + id); got[1].Decision.Outcome != "changes_requested" {
		t.Fatalf("changes outcome: %+v", got[1])
	}
	status, err := s.config.Threads.Status(t.Context(), id)
	if err != nil || status.Phase != "draft" {
		t.Fatalf("a draft thread keeps its state: %+v %v", status.Phase, err)
	}
}

// TestApprovalRecordedDecidesRequest covers A3: a later approval.recorded decides a matching request.
func TestApprovalRecordedDecidesRequest(t *testing.T) {
	s, store, _ := fixture(t)
	id := create(t, s, "create")
	req := waiting(t, store, id, "deploy.staging", "staging-01")
	other := waiting(t, store, id, "deploy.production", "prod")
	seedEvent(t, store, id, "approval.recorded", 3, `{"id":"apr-1","approver":"operator:bob","thread":"`+id+`","revision":"5977ec186c62ce66b2e1e48051bda521bb7f6b9f","worktree_digest":"sha256:`+
		strings.Repeat("a", 64)+`","target":"staging-01","actions":["deploy.staging"],"approved_at":"2026-11-18T10:00:00Z","expires_at":"2026-11-18T12:00:00Z"}`)
	all, err := s.approvalRequests(t.Context(), false)
	if err != nil || len(all) != 2 {
		t.Fatalf("%+v %v", all, err)
	}
	if all[0].RequestID != req || all[0].State != "decided" || all[0].Decision.Outcome != "approved" || all[0].Decision.Actor != "operator:bob" || all[0].Decision.ApprovalID != "apr-1" {
		t.Fatalf("approved: %+v", all[0])
	}
	if all[1].RequestID != other || all[1].State != "pending" {
		t.Fatalf("other request: %+v", all[1])
	}
}

// TestFindingsGrouping covers A5: grouping, provenance, filters and status.
func TestFindingsGrouping(t *testing.T) {
	s, store, _ := fixture(t)
	a, b := create(t, s, "a"), create(t, s, "b")
	scan := func(thread, id, scanID, status string) {
		triage := ""
		if status != "" {
			triage = `,"triage":{"status":"` + status + `"}`
		}
		seedEvent(t, store, thread, "finding.recorded", 2, `{"source":"scanner","finding":{"finding_version":2,"id":"`+id+`","fingerprint":"sha256:`+strings.Repeat("1", 64)+`",
"tool":{"name":"gitleaks","version":"8","binary_sha256":"x"},"rule":{"id":"aws-access-token"},"severity":"high","component":{"type":"secret","name":"k"},
"location":{"path":"deploy/env"},"evidence":{"summary":"key","report":{"uri":"file:///r","digest":"sha256:`+strings.Repeat("2", 64)+`","redacted":true}},"revision":"r",
"provenance":[{"scan_id":"`+scanID+`","tool":"gitleaks","report":{"uri":"file:///r","digest":"sha256:`+strings.Repeat("2", 64)+`","redacted":true}}]`+triage+`}}`)
	}
	scan(a, "01JD3QA0000000000000000020", "scan-1", "")
	scan(b, "01JD3QA0000000000000000021", "scan-2", "fixed")
	review := func(thread, id string) {
		seedEvent(t, store, thread, "finding.recorded", 2, `{"source":"security_review","finding":{"id":"`+id+`","summary":"weak check","severity":"medium","criterion_id":"c1","revision":"r","evidence_refs":[{"uri":"file:///a.go","digest":"sha256:`+strings.Repeat("3", 64)+`"}]}}`)
	}
	review(a, "01JD3QA0000000000000000022")
	review(a, "01JD3QA0000000000000000023")
	type out struct {
		Findings []findingGroup `json:"findings"`
	}
	get := func(query string) []findingGroup {
		w := request(t, s, "GET", "/v1/findings"+query, testToken, "", "")
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", query, w.Code, w.Body)
		}
		return decode[out](t, w.Body.Bytes()).Findings
	}
	all := get("")
	if len(all) != 2 {
		t.Fatalf("groups: %+v", all)
	}
	scanner, rev := all[0], all[1]
	if scanner.Occurrences != 2 || len(scanner.Threads) != 2 || len(scanner.Provenance) != 2 || scanner.Status != "fixed" || scanner.Fingerprint == "" || scanner.Severity != "high" {
		t.Fatalf("scanner group: %+v", scanner)
	}
	if rev.Occurrences != 2 || rev.Fingerprint != "" || rev.Source != "security_review" || rev.Rule != "c1" || rev.Location != "file:///a.go" || len(rev.Provenance) != 2 || rev.Status != "open" {
		t.Fatalf("review group: %+v", rev)
	}
	for query, want := range map[string]int{"?thread=" + a: 2, "?thread=" + b: 1, "?severity=high": 1, "?severity=medium": 1, "?severity=low": 0, "?status=fixed": 1, "?status=open": 1, "?status=accepted_risk": 0} {
		if got := get(query); len(got) != want {
			t.Fatalf("%s: %d groups, want %d", query, len(got), want)
		}
	}
	for _, query := range []string{"?severity=bogus", "?status=bogus", "?thread=x"} {
		requireProblem(t, request(t, s, "GET", "/v1/findings"+query, testToken, "", ""), 400, "validation_failed", "invalid thread, severity or status filter")
	}
}

// TestDiffEndpoint covers A2 at the HTTP boundary.
func TestDiffEndpoint(t *testing.T) {
	s, _, _ := fixture(t)
	id := create(t, s, "create")
	path := "/v1/threads/" + id + "/diff"
	requireProblem(t, request(t, s, "GET", path, testToken, "", ""), 404, "not_found", "")
	s.config.Diff = func(_ context.Context, thread string) (Diff, error) {
		if thread != id {
			t.Errorf("thread %s", thread)
		}
		return Diff{Base: strings.Repeat("a", 40), Revision: strings.Repeat("b", 40), Patch: "diff", Bytes: 4, Digest: "sha256:x", Ref: "out_" + strings.Repeat("0", 32)}, nil
	}
	w := request(t, s, "GET", path, testToken, "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"ref":"out_`) || !strings.Contains(w.Body.String(), `"patch":"diff"`) {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	requireProblem(t, request(t, s, "GET", "/v1/threads/01JD3Q8Z7Y6X5W4V3T2S1R0QPN/diff", testToken, "", ""), 404, "not_found", "")
	s.config.Diff = func(context.Context, string) (Diff, error) { return Diff{}, ErrNoWorktree }
	requireProblem(t, request(t, s, "GET", path, testToken, "", ""), 404, "not_found", "thread has no worktree")
	s.config.Diff = func(context.Context, string) (Diff, error) { return Diff{}, errors.New("boom") }
	requireProblem(t, request(t, s, "GET", path, testToken, "", ""), 500, "internal", "")
}

// TestCheckpointAdmission covers A7: a checkpoint rollback is admitted only when verified.
func TestCheckpointAdmission(t *testing.T) {
	s, _, _ := fixture(t)
	id := create(t, s, "create")
	s.config.Authority = &authorityStub{id: id}
	sha := "sha256:" + strings.Repeat("a", 64)
	approve := func(reqID, rollback string) (code int, body string) {
		w := request(t, s, "POST", "/v1/threads/"+id+"/approvals", testToken, reqID,
			`{"plan_digest":"`+sha+`","worktree_digest":"`+sha+`","target":"prod","actions":["deploy.production"],"rollback":"`+rollback+`","irreversible_effects":[],"expires_at":"2026-11-18T20:45:00Z"}`)
		return w.Code, w.Body.String()
	}
	const ref = "vmsnap:proxmox/lab/100/before-deploy"
	if code, body := approve("plain", "restore from backup"); code != 201 {
		t.Fatalf("plain command rollback: %d %s", code, body)
	}
	if code, body := approve("nodriver", ref); code != 403 || !strings.Contains(body, "checkpoint_unverified") || !strings.Contains(body, "policy_denied") {
		t.Fatalf("no verifier: %d %s", code, body)
	}
	var seen CheckpointRef
	s.config.VerifyCheckpoint = func(_ context.Context, thread string, r CheckpointRef) error {
		if thread != id {
			t.Errorf("thread %s", thread)
		}
		seen = r
		return errors.New("not intact")
	}
	if code, body := approve("bad", ref); code != 403 || !strings.Contains(body, "checkpoint_unverified") || strings.Contains(body, "not intact") {
		t.Fatalf("unverified: %d %s", code, body)
	}
	if seen != (CheckpointRef{"proxmox", "lab", "100", "before-deploy"}) {
		t.Fatalf("parsed %+v", seen)
	}
	if code, body := approve("malformed", "vmsnap:only/two"); code != 403 || !strings.Contains(body, "malformed checkpoint reference") {
		t.Fatalf("malformed: %d %s", code, body)
	}
	s.config.VerifyCheckpoint = func(context.Context, string, CheckpointRef) error { return nil }
	if code, body := approve("good", ref); code != 201 {
		t.Fatalf("verified: %d %s", code, body)
	}
	// A refusal is a stored rejection: the same request id replays it even once the snapshot verifies.
	if code, _ := approve("bad", ref); code != 403 {
		t.Fatalf("stored refusal replay: %d", code)
	}
}

// TestParseCheckpointRef covers escaping and the plain-command case.
func TestParseCheckpointRef(t *testing.T) {
	if _, isRef, err := ParseCheckpointRef("ssh restore"); isRef || err != nil {
		t.Fatal("plain rollback treated as a reference")
	}
	ref, isRef, err := ParseCheckpointRef("vmsnap:esxi/host%2F1/vm-9/snap%20a")
	if !isRef || err != nil || ref != (CheckpointRef{"esxi", "host/1", "vm-9", "snap a"}) {
		t.Fatalf("%+v %v %v", ref, isRef, err)
	}
	for _, bad := range []string{"vmsnap:", "vmsnap:a/b/c", "vmsnap:a/b/c/", "vmsnap:a/b/c/d/e", "vmsnap:a/%zz/c/d"} {
		if _, isRef, err := ParseCheckpointRef(bad); !isRef || err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}

// TestRequestChangesOnRunningThread covers the A4 mapping: a running thread goes to
// awaiting_decision (T10), and the operator decide command (T11) returns it to the queue.
func TestRequestChangesOnRunningThread(t *testing.T) {
	s, store, _ := fixture(t)
	id := create(t, s, "create")
	allow := func(context.Context, *eventlog.Transaction, thread.State) (bool, error) { return true, nil }
	machine, err := thread.Open(t.Context(), store, thread.Hooks{Limits: allow, Policy: allow, Pair: allow})
	if err != nil {
		t.Fatal(err)
	}
	s.config.Threads = machine
	if err := s.config.Threads.Update(t.Context(), id, func(f *thread.Facts) error {
		f.NeedsDispatch = true
		f.AuditSetting = "off"
		f.Revision = testRevision
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if w := request(t, s, "POST", "/v1/threads/"+id+"/commands", testToken, "select", `{"command":"select_harness","harness":"codex","model":"m"}`); w.Code != 202 {
		t.Fatalf("select: %d %s", w.Code, w.Body)
	}
	for _, c := range []thread.Command{{Transition: "T01"}, {Transition: "T05", Session: "s", Assignment: "a"}} {
		if err := s.config.Threads.Apply(t.Context(), id, c); err != nil {
			t.Fatalf("%s: %v", c.Transition, err)
		}
	}
	if st, _ := s.config.Threads.Status(t.Context(), id); st.Phase != "running" {
		t.Fatalf("setup phase %s", st.Phase)
	}
	req := waiting(t, store, id, "deploy.staging", "staging-01")
	w := request(t, s, "POST", "/v1/approvals/"+req+"/request-changes", testToken, "chg", `{"reason":"narrow it"}`)
	if w.Code != 201 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	state, err := s.config.Threads.Status(t.Context(), id)
	if err != nil || state.Phase != "awaiting_decision" || state.Facts.Material {
		t.Fatalf("phase %s material %v %v", state.Phase, state.Facts.Material, err)
	}
	for decision := range state.Decisions {
		if w := request(t, s, "POST", "/v1/threads/"+id+"/commands", testToken, "decide", `{"command":"decide","decision_id":"`+decision+`","choice":"continue"}`); w.Code != 202 {
			t.Fatalf("decide: %d %s", w.Code, w.Body)
		}
	}
	if state, _ = s.config.Threads.Status(t.Context(), id); state.Phase != "queued" {
		t.Fatalf("T11 left %s", state.Phase)
	}
	second := waiting(t, store, id, "deploy.staging", "staging-02")
	if w := request(t, s, "POST", "/v1/approvals/"+second+"/reject", testToken, "rej", `{"reason":"no"}`); w.Code != 201 {
		t.Fatalf("reject: %d %s", w.Code, w.Body)
	}
	if state, _ = s.config.Threads.Status(t.Context(), id); state.Phase != "queued" {
		t.Fatalf("reject changed the thread to %s", state.Phase)
	}
}

// TestOutputSearchEndpoint covers the capped output search at the HTTP boundary.
func TestOutputSearchEndpoint(t *testing.T) {
	s, _, _ := fixture(t)
	id := create(t, s, "create")
	ref := "out_" + strings.Repeat("a", 32)
	base := "/v1/outputs/" + ref + "/search"
	requireProblem(t, request(t, s, "GET", base+"?thread="+id+"&pattern=x", "", "", ""), 401, "unauthorized", "")
	requireProblem(t, request(t, s, "GET", base+"?thread="+id+"&pattern=x", testToken, "", ""), 404, "not_found", "")
	var gotLimit int
	s.config.SearchOutput = func(_ context.Context, thread, r, pattern string, limit int) ([]OutputMatch, error) {
		gotLimit = limit
		switch pattern {
		case "bad(":
			return nil, ErrBadPattern
		case "other":
			return nil, ErrOutputNotFound
		case "none":
			return nil, nil
		}
		if thread != id || r != ref {
			t.Errorf("thread %s ref %s", thread, r)
		}
		return []OutputMatch{{Line: 3, Text: "match"}, {Line: 9, Text: "match too"}}, nil
	}
	w := request(t, s, "GET", base+"?thread="+id+"&pattern=match&limit=2", testToken, "", "")
	if w.Code != 200 || gotLimit != 2 || !strings.Contains(w.Body.String(), `"truncated":true`) || !strings.Contains(w.Body.String(), `"line":9`) {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if w := request(t, s, "GET", base+"?thread="+id+"&pattern=none", testToken, "", ""); w.Code != 200 || gotLimit != 50 || !strings.Contains(w.Body.String(), `"matches":[]`) {
		t.Fatalf("default limit/empty: %d %s %d", w.Code, w.Body, gotLimit)
	}
	requireProblem(t, request(t, s, "GET", base+"?thread="+id+"&pattern=other", testToken, "", ""), 404, "not_found", "")
	requireProblem(t, request(t, s, "GET", base+"?thread="+id+"&pattern=bad(", testToken, "", ""), 400, "validation_failed", "invalid search pattern")
	requireProblem(t, request(t, s, "GET", base+"?thread="+id, testToken, "", ""), 400, "validation_failed", "thread and pattern are required")
	requireProblem(t, request(t, s, "GET", base+"?pattern=x", testToken, "", ""), 400, "validation_failed", "thread and pattern are required")
	requireProblem(t, request(t, s, "GET", base+"?thread="+id+"&pattern=x&limit=999", testToken, "", ""), 400, "validation_failed", "limit must be between 1 and 200")
	requireProblem(t, request(t, s, "GET", "/v1/outputs/..%2Fsecret/search?thread="+id+"&pattern=x", testToken, "", ""), 404, "not_found", "")
	requireProblem(t, request(t, s, "GET", base+"?thread=01JD3Q8Z7Y6X5W4V3T2S1R0QPN&pattern=x", testToken, "", ""), 404, "not_found", "")
}

// TestThreadViewChecksAndRequirements covers the optional checks and open_requirements fields.
func TestThreadViewChecksAndRequirements(t *testing.T) {
	s, store, _ := fixture(t)
	id := create(t, s, "create")
	view := func() map[string]any {
		w := request(t, s, "GET", "/v1/threads/"+id, testToken, "", "")
		if w.Code != 200 {
			t.Fatalf("%d %s", w.Code, w.Body)
		}
		return decode[map[string]any](t, w.Body.Bytes())
	}
	if v := view(); v["checks"] != nil || v["open_requirements"] != nil {
		t.Fatalf("fields must be absent on a fresh thread: %v", v)
	}
	if err := s.config.Threads.Update(t.Context(), id, func(f *thread.Facts) error {
		f.Revision, f.RequiredChecks = testRevision, []string{"ci", "lint", "e2e"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	appendThreadFact(t, store, id, "check.result", 1, map[string]string{"revision": testRevision, "check": "ci", "status": "passed"})
	appendThreadFact(t, store, id, "check.result", 1, map[string]string{"revision": testRevision, "check": "lint", "status": "failed"})
	v := view()
	checks, _ := v["checks"].(map[string]any)
	if len(checks) != 5 || len(checks["passed"].([]any)) != 1 || checks["failed"].([]any)[0] != "lint" || checks["not_run"].([]any)[0] != "e2e" || len(checks["error"].([]any)) != 0 {
		t.Fatalf("checks: %v", v["checks"])
	}
	open, _ := v["open_requirements"].([]any)
	if len(open) != 2 || open[0].(map[string]any)["id"] != "lint" || open[0].(map[string]any)["status"] != "failed" || open[1].(map[string]any)["id"] != "e2e" || open[1].(map[string]any)["status"] != "not_run" {
		t.Fatalf("open requirements: %v", v["open_requirements"])
	}
}
