package policy

import (
	"context"
	"errors"
	"testing"
	"time"
)

// --- AC1: Effective = max(O, R, B) ---

// op wraps rules in an operator-source file.
func op(rules string) string {
	return `{"policy_version":3,"source":"operator","rules":[` + rules + `]}`
}

// proj wraps rules in a project-source file.
func proj(rules string) string {
	return `{"policy_version":3,"source":"project","rules":[` + rules + `]}`
}

// skill wraps rules in a skill-source file.
func skill(rules string) string {
	return `{"policy_version":3,"source":"skill","rules":[` + rules + `]}`
}

// TestEvaluateTable proves AC1 §1: authority first, restrictions never raise it, gates per §5.4.
func TestEvaluateTable(t *testing.T) {
	allowAll := `{"id":"all","effect":"allow","actions":["*"]}`
	askXY := `{"id":"ask","effect":"ask","actions":["x.y"],"approval":{"max_ttl":"PT1H"}}`
	cases := []struct {
		name   string
		files  []string
		action string
		want   Effect
		rule   string
	}{
		{"no rules: operator default deny", nil, "x.y", Deny, ""},
		{"skill ask on unmatched action stays deny", []string{op(`{"id":"o","effect":"allow","actions":["a.b"]}`), skill(askXY)}, "x.y", Deny, ""},
		{"operator allow on firewall yields ask", []string{op(allowAll)}, "net.firewall_change", Ask, "builtin:gates#operational"},
		{"operator allow on staging deploy yields ask", []string{op(allowAll)}, "deploy.staging", Ask, "builtin:gates#deployment"},
		{"project deny beats operator allow", []string{op(allowAll), proj(`{"id":"pd","effect":"deny","actions":["git.*"]}`)}, "git.push", Deny, "project:proj-1@" + rev1 + "#pd"},
		{"skill ask lowers operator allow", []string{op(allowAll), skill(askXY)}, "x.y", Ask, "skill:pack.json@aaaaaaaaaaaa#ask"},
		{"project deny raises operator ask", []string{op(askXY), proj(`{"id":"pd","effect":"deny","actions":["x.*"]}`)}, "x.y", Deny, "project:proj-1@" + rev1 + "#pd"},
		{"most restrictive operator rule wins", []string{op(allowAll + `,{"id":"od","effect":"deny","actions":["x.y"]}`)}, "x.y", Deny, "operator:store#od"},
		{"operator allow alone", []string{op(allowAll)}, "x.y", Allow, "operator:store#all"},
		{"operator deny on firewall stays deny", []string{op(`{"id":"od","effect":"deny","actions":["net.*"]}`)}, "net.firewall_change", Deny, "operator:store#od"},
	}
	for _, c := range cases {
		var files []*File
		for _, f := range c.files {
			files = append(files, mustLoad(t, f))
		}
		res := evaluate(files, subject{action: c.action})
		if res.effect != c.want || res.ruleID != c.rule {
			t.Errorf("%s: got %s %q, want %s %q", c.name, res.effect, res.ruleID, c.want, c.rule)
		}
	}
}

// --- AC4: authority checks after rules ---

// TestAuthorityChecks proves AC4 §4: mode, live grant, VM ownership, merge acceptance, deploy gate, coordinator scope.
func TestAuthorityChecks(t *testing.T) {
	ctx := context.Background()
	e, st, _ := fixture(t, op(`{"id":"all","effect":"allow","actions":["*"]}`))
	decide := func(req Request) Decision {
		t.Helper()
		if req.Machine == "" {
			req.Machine = "m"
		}
		if req.Thread == "" {
			req.Thread = rootID
		}
		d, err := e.Decide(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	// Access mode must be known for the target.
	st.modes[rootID] = "weird"
	if d := decide(Request{Action: "x.y"}); d.Allowed {
		t.Error("unknown mode allowed")
	}
	// Native host action: full_control plus a live grant.
	st.modes[rootID] = "provider"
	if d := decide(Request{Action: "host.exec", Machine: "h", Identity: "i"}); d.Allowed {
		t.Error("host action without full_control allowed")
	}
	st.modes[rootID] = "full_control"
	if d := decide(Request{Action: "host.exec", Machine: "h", Identity: "i"}); d.Allowed {
		t.Error("host action without grant allowed")
	}
	g, err := e.IssueGrant(ctx, rootID, "h", "i", "operator:alice")
	if err != nil {
		t.Fatal(err)
	}
	if d := decide(Request{Action: "host.exec", Machine: "h", Identity: "i"}); !d.Allowed || d.GrantID != g.ID {
		t.Errorf("granted host action: %+v", d)
	}
	// VM ownership is injected.
	if d := decide(Request{Action: "vm.delete", Target: "vm1"}); d.Allowed {
		t.Error("unowned VM deleted")
	}
	st.vms[rootID+"/vm1"] = true
	if d := decide(Request{Action: "vm.delete", Target: "vm1"}); !d.Allowed {
		t.Errorf("owned VM: %+v", d)
	}
	// Merge needs human acceptance of the exact current revision.
	if d := decide(Request{Action: "merge"}); d.Allowed {
		t.Error("merge without acceptance")
	}
	st.accepted[rootID] = rev2
	if d := decide(Request{Action: "merge"}); d.Allowed {
		t.Error("merge accepted for another revision")
	}
	st.accepted[rootID] = rev1
	if d := decide(Request{Action: "merge"}); !d.Allowed {
		t.Errorf("accepted merge: %+v", d)
	}
	// Deploy needs its gate approval: an operator allow alone only waits, and
	// the authority check refuses deploy.* without an approval on its own.
	if d := decide(Request{Action: "deploy.staging", Target: "s"}); d.Allowed || d.Outcome != "waiting_approval" {
		t.Errorf("deploy without approval: %+v", d)
	}
	if _, why, _ := e.authorize(Request{Action: "deploy.staging"}, st.threads[rootID], "provider", ""); why == "" {
		t.Error("authorize let deploy through without an approval")
	}
	// Coordinator: plan.*, assign.*, evidence.read only.
	ti := st.threads[otherID]
	ti.Role = "coordinator"
	st.threads[otherID] = ti
	for action, want := range map[string]bool{"plan.create": true, "assign.child": true, "evidence.read": true, "proc.exec": false, "git.push": false, "evidence.write": false} {
		if d := decide(Request{Thread: otherID, Action: action}); d.Allowed != want {
			t.Errorf("coordinator %s: %+v", action, d)
		}
	}
}

// TestAskNeedsApproval proves AC4: an ask is satisfied only by an approval record, never by plan review.
func TestAskNeedsApproval(t *testing.T) {
	e, _, log := fixture(t, op(`{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`))
	d, err := e.Decide(context.Background(), Request{Thread: rootID, Action: "ci.run", Machine: "m"})
	if err != nil || d.Allowed || d.Outcome != "waiting_approval" {
		t.Fatalf("%+v %v", d, err)
	}
	if p := events(t, log, rootID, "policy.decided"); len(p) != 1 || p[0]["outcome"] != "waiting_approval" {
		t.Fatalf("%v", p)
	}
}

// --- AC5: approval creation (§5.1) ---

// baseApproval is a valid revision-bound approval request for ci.run on target t1.
func baseApproval() Approval {
	return Approval{Approver: "operator:alice", Thread: rootID, Revision: ptr(rev1), WorktreeDigest: wd1, Target: "t1", Actions: []string{"ci.run"}, ExpiresAt: t0.Add(time.Hour)}
}

// asFirewall turns an approval into a plan-bound firewall approval with rollback and irreversible effects.
func asFirewall(a *Approval) {
	a.Actions, a.Revision, a.PlanDigest, a.Rollback, a.IrreversibleEffects = []string{"net.firewall_change"}, nil, ptr(plan1), ptr("rb"), &[]string{}
}

// TestCreateApproval proves AC5: required fields, one binding, no wildcards, P1, operational bindings, TTL cap at creation.
func TestCreateApproval(t *testing.T) {
	ctx := context.Background()
	e, _, log := fixture(t,
		op(`{"id":"ask","effect":"ask","actions":["ci.run","net.firewall_change","deploy.staging"],"approval":{"max_ttl":"PT4H"}}`),
		skill(`{"id":"tight","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT5M"}}`))
	bad := map[string]func(*Approval){
		"no approver":              func(a *Approval) { a.Approver = "" },
		"bad thread":               func(a *Approval) { a.Thread = "x" },
		"both bindings":            func(a *Approval) { a.PlanDigest = ptr(plan1) },
		"no binding":               func(a *Approval) { a.Revision = nil },
		"short revision":           func(a *Approval) { a.Revision = ptr("abc") },
		"no worktree":              func(a *Approval) { a.WorktreeDigest = "" },
		"empty target":             func(a *Approval) { a.Target = "" },
		"no actions":               func(a *Approval) { a.Actions = nil },
		"wildcard":                 func(a *Approval) { a.Actions = []string{"*"} },
		"glob":                     func(a *Approval) { a.Actions = []string{"ci.*"} },
		"duplicate action":         func(a *Approval) { a.Actions = []string{"ci.run", "ci.run"} },
		"expires before (P1)":      func(a *Approval) { a.ExpiresAt = t0.Add(-time.Minute) },
		"expires at approval":      func(a *Approval) { a.ExpiresAt = t0 },
		"ttl over skill PT5M":      func(a *Approval) { a.ExpiresAt = t0.Add(10 * time.Minute) },
		"firewall on revision":     func(a *Approval) { asFirewall(a); a.PlanDigest, a.Revision = nil, ptr(rev1) },
		"firewall no rollback":     func(a *Approval) { asFirewall(a); a.Rollback = nil },
		"firewall no irreversible": func(a *Approval) { asFirewall(a); a.IrreversibleEffects = nil },
		"deploy over gate PT4H":    func(a *Approval) { a.Actions, a.ExpiresAt = []string{"deploy.staging"}, t0.Add(5*time.Hour) },
	}
	for name, mutate := range bad {
		a := baseApproval()
		a.ExpiresAt = t0.Add(5 * time.Minute)
		mutate(&a)
		if _, err := e.CreateApproval(ctx, a); !errors.Is(err, ErrInvalidApproval) {
			t.Errorf("%s: got %v", name, err)
		}
	}
	a := baseApproval()
	a.ExpiresAt = t0.Add(5 * time.Minute)
	a.ApprovedAt = t0.Add(-time.Hour) // ignored: the daemon sets approved_at
	got, err := e.CreateApproval(ctx, a)
	if err != nil || !got.ApprovedAt.Equal(t0) || len(got.ConstraintRules) != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	fw := baseApproval()
	asFirewall(&fw)
	if _, err := e.CreateApproval(ctx, fw); err != nil {
		t.Fatalf("firewall approval: %v", err)
	}
	if n := len(events(t, log, rootID, "approval.recorded")); n != 2 {
		t.Fatalf("recorded %d approvals", n)
	}
}

// --- AC6: unconditional use-time check (§5.3) ---

// TestUseTimeRows proves AC6: every §5.3 row denies and emits approval.voided with its reason.
func TestUseTimeRows(t *testing.T) {
	ctx := context.Background()
	opAsk := op(`{"id":"ask","effect":"ask","actions":["ci.*","net.firewall_change"],"approval":{"max_ttl":"PT4H"}}`)
	setTerminal := func(s *fakeState) {
		ti := s.threads[rootID]
		ti.Terminal = true
		s.threads[rootID] = ti
	}
	rows := []struct {
		name   string
		fw     bool                                // plan-bound firewall approval instead of ci.run
		change func(*fakeState, *Request, *Engine) // the mismatch
		reason string
	}{
		{"other thread", false, func(_ *fakeState, r *Request, _ *Engine) { r.Thread = otherID }, "scope_changed"},
		{"revision moved", false, func(s *fakeState, _ *Request, _ *Engine) { s.cur.Head = rev2 }, "diff_changed"},
		{"plan changed", true, func(s *fakeState, _ *Request, _ *Engine) { s.cur.PlanDigest = plan2 }, "scope_changed"},
		{"worktree changed", false, func(s *fakeState, _ *Request, _ *Engine) { s.cur.WorktreeDigest = wd2 }, "diff_changed"},
		{"target changed", false, func(_ *fakeState, r *Request, _ *Engine) { r.Target = "t2" }, "target_changed"},
		{"action outside list", false, func(_ *fakeState, r *Request, _ *Engine) { r.Action = "ci.pipeline_edit" }, "scope_changed"},
		{"expired", false, func(s *fakeState, _ *Request, _ *Engine) { s.now = t0.Add(time.Hour) }, "expired"},
		{"not yet valid", false, func(s *fakeState, _ *Request, _ *Engine) { s.now = t0.Add(-time.Second) }, "expired"},
		{"restriction added after approval tightens TTL", false, func(s *fakeState, _ *Request, _ *Engine) {
			s.files = append(s.files, mustLoad(t, skill(`{"id":"tight","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT5M"}}`)))
		}, "scope_changed"},
		{"effective became deny", false, func(s *fakeState, _ *Request, _ *Engine) {
			s.files = append(s.files, mustLoad(t, proj(`{"id":"pd","effect":"deny","actions":["ci.run"]}`)))
		}, "scope_changed"},
		{"thread ended", false, func(s *fakeState, _ *Request, _ *Engine) { setTerminal(s) }, "thread_ended"},
		{"revoked", false, func(_ *fakeState, r *Request, e *Engine) { _ = e.RevokeApproval(ctx, r.ApprovalID) }, "revoked"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			e, st, log := fixture(t, opAsk)
			a := baseApproval()
			req := Request{Thread: rootID, Action: "ci.run", Target: "t1", Machine: "m"}
			if row.fw {
				asFirewall(&a)
				req.Action = "net.firewall_change"
			}
			got, err := e.CreateApproval(ctx, a)
			if err != nil {
				t.Fatal(err)
			}
			req.ApprovalID = got.ID
			st.now = t0.Add(time.Minute)
			if d, err := e.Decide(ctx, req); err != nil || !d.Allowed || d.ApprovalID != got.ID {
				t.Fatalf("baseline use: %+v %v", d, err)
			}
			row.change(st, &req, e)
			d, err := e.Decide(ctx, req)
			if err != nil || d.Allowed {
				t.Fatalf("mismatch allowed: %+v %v", d, err)
			}
			v := events(t, log, rootID, "approval.voided")
			if len(v) != 1 || v[0]["reason"] != row.reason || v[0]["approval_id"] != got.ID {
				t.Fatalf("voided events %v, want reason %s", v, row.reason)
			}
			// A voided approval stays unusable even after the mismatch is undone.
			st.now, st.cur = t0.Add(time.Minute), Current{Head: rev1, PlanDigest: plan1, WorktreeDigest: wd1}
			if d, _ := e.Decide(ctx, Request{Thread: rootID, Action: "ci.run", Target: "t1", Machine: "m", ApprovalID: got.ID}); d.Allowed {
				t.Fatal("voided approval reused")
			}
		})
	}
}

// TestCombinedConstraints proves AC6: operator ask PT4H plus skill ask PT5M gives a 5-minute limit.
func TestCombinedConstraints(t *testing.T) {
	ctx := context.Background()
	e, st, _ := fixture(t,
		op(`{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`),
		skill(`{"id":"tight","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT5M"}}`))
	a := baseApproval()
	a.ExpiresAt = t0.Add(6 * time.Minute)
	if _, err := e.CreateApproval(ctx, a); !errors.Is(err, ErrInvalidApproval) {
		t.Fatalf("6m accepted: %v", err)
	}
	a.ExpiresAt = t0.Add(5 * time.Minute)
	got, err := e.CreateApproval(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	st.now = t0.Add(time.Minute)
	req := Request{Thread: rootID, Action: "ci.run", Target: "t1", Machine: "m", ApprovalID: got.ID}
	for range 2 { // reusable while every check holds (Q-22)
		if d, err := e.Decide(ctx, req); err != nil || !d.Allowed {
			t.Fatalf("%+v %v", d, err)
		}
	}
}

// --- AC7: grants, inheritance, revocation ---

// TestGrantInheritance proves AC7: the ancestor walk applies all four conditions and is never cached.
func TestGrantInheritance(t *testing.T) {
	ctx := context.Background()
	e, st, _ := fixture(t, op(`{"id":"all","effect":"allow","actions":["*"]}`))
	for _, id := range []string{rootID, childID, grandID, otherID} {
		st.modes[id] = "full_control"
	}
	if _, err := e.IssueGrant(ctx, rootID, "h", "i", "operator:alice"); err != nil {
		t.Fatal(err)
	}
	try := func(thread, host, identity string) bool {
		d, err := e.Decide(ctx, Request{Thread: thread, Action: "host.exec", Machine: host, Identity: identity})
		if err != nil {
			t.Fatal(err)
		}
		return d.Allowed
	}
	if !try(grandID, "h", "i") || !try(childID, "h", "i") {
		t.Fatal("descendants created under the grant must inherit")
	}
	if try(grandID, "h2", "i") || try(grandID, "h", "i2") {
		t.Fatal("different host or identity needs its own grant")
	}
	if try(otherID, "h", "i") {
		t.Fatal("unrelated thread inherited")
	}
	ti := st.threads[childID]
	ti.CreatedAt = t0.Add(-time.Minute)
	st.threads[childID] = ti
	if try(grandID, "h", "i") {
		t.Fatal("path through a thread created before the grant")
	}
	ti.CreatedAt, ti.Managed = t0.Add(time.Minute), false
	st.threads[childID] = ti
	if try(grandID, "h", "i") {
		t.Fatal("path through an unmanaged thread")
	}
	ti.Managed = true
	st.threads[childID] = ti
	r := st.threads[rootID]
	r.Terminal = true
	st.threads[rootID] = r
	if try(grandID, "h", "i") {
		t.Fatal("terminal holder still authorizes (result was cached)")
	}
}

// TestRevocationOrder proves AC7: commit, stop dispatch, settle, then grant.revoked{settled:true}.
func TestRevocationOrder(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"all","effect":"allow","actions":["*"]}`))
	st.modes[grandID] = "full_control"
	g, err := e.IssueGrant(ctx, rootID, "h", "i", "operator:alice")
	if err != nil {
		t.Fatal(err)
	}
	settled := false
	st.settle = func(_ context.Context, id string, threads []string) error {
		if d, _ := e.Decide(ctx, Request{Thread: grandID, Action: "host.exec", Machine: "h", Identity: "i"}); d.Allowed {
			t.Error("dispatch check passed after commit, before settle")
		}
		r := events(t, log, rootID, "grant.revoked")
		if len(r) != 1 || r[0]["settled"] != false {
			t.Errorf("commit record before settle: %v", r)
		}
		if id != g.ID || len(threads) != 3 {
			t.Errorf("settle %s %v", id, threads)
		}
		settled = true
		return nil
	}
	if err := e.Revoke(ctx, g.ID, "operator:alice"); err != nil {
		t.Fatal(err)
	}
	r := events(t, log, rootID, "grant.revoked")
	if !settled || len(r) != 2 || r[1]["settled"] != true || r[1]["cause"] != "operator" || r[1]["revoked_by"] != "operator:alice" {
		t.Fatalf("%v", r)
	}
	if err := e.Revoke(ctx, g.ID, "operator:alice"); !errors.Is(err, ErrUnknownGrant) {
		t.Fatalf("double revoke: %v", err)
	}
}

// TestRevocationSettleFailure proves AC7: without settle, no settled:true record and the grant stays revoked.
func TestRevocationSettleFailure(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"all","effect":"allow","actions":["*"]}`))
	st.modes[rootID] = "full_control"
	g, err := e.IssueGrant(ctx, rootID, "h", "i", "operator:alice")
	if err != nil {
		t.Fatal(err)
	}
	st.settle = func(context.Context, string, []string) error { return errors.New("runner offline") }
	if err := e.Revoke(ctx, g.ID, "operator:alice"); err == nil {
		t.Fatal("settle failure hidden")
	}
	for _, r := range events(t, log, rootID, "grant.revoked") {
		if r["settled"] == true {
			t.Fatal("settled:true without settle")
		}
	}
	if d, _ := e.Decide(ctx, Request{Thread: rootID, Action: "host.exec", Machine: "h", Identity: "i"}); d.Allowed {
		t.Fatal("grant usable after failed settle")
	}
}

// TestThreadEndC6 proves AC7: thread-end revocation only after the C6 checks, and approvals void with thread_ended.
func TestThreadEndC6(t *testing.T) {
	ctx := context.Background()
	e, st, log := fixture(t, op(`{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}`))
	g, err := e.IssueGrant(ctx, rootID, "h", "i", "operator:alice")
	if err != nil {
		t.Fatal(err)
	}
	a, err := e.CreateApproval(ctx, baseApproval())
	if err != nil {
		t.Fatal(err)
	}
	if err := e.RevokeForThreadEnd(ctx, "nope", rootID); !errors.Is(err, ErrUnknownGrant) {
		t.Errorf("unknown grant: %v", err)
	}
	if err := e.RevokeForThreadEnd(ctx, g.ID, rootID); !errors.Is(err, ErrCrossCheck) {
		t.Errorf("granted thread not terminal: %v", err)
	}
	c := st.threads[childID]
	c.Terminal = true
	st.threads[childID] = c
	if err := e.RevokeForThreadEnd(ctx, g.ID, childID); !errors.Is(err, ErrCrossCheck) {
		t.Errorf("subtree thread is not the granted thread: %v", err)
	}
	if len(events(t, log, rootID, "grant.revoked")) != 0 {
		t.Fatal("revocation recorded before C6 passed")
	}
	r := st.threads[rootID]
	r.Terminal = true
	st.threads[rootID] = r
	if err := e.ThreadEnded(ctx, rootID); err != nil {
		t.Fatal(err)
	}
	rv := events(t, log, rootID, "grant.revoked")
	if len(rv) != 2 || rv[1]["cause"] != "thread_ended" || rv[1]["ended_thread"] != rootID || rv[1]["revoked_by"] != nil {
		t.Fatalf("%v", rv)
	}
	v := events(t, log, rootID, "approval.voided")
	if len(v) != 1 || v[0]["approval_id"] != a.ID || v[0]["reason"] != "thread_ended" {
		t.Fatalf("%v", v)
	}
}

// --- AC8: policy.decided before dispatch ---

// TestDecisionRecorded proves AC8: every evaluation is recorded with actor, machine, target, outcome and qualified rule id, failing closed.
func TestDecisionRecorded(t *testing.T) {
	ctx := context.Background()
	e, _, log := fixture(t, op(`{"id":"all","effect":"allow","actions":["*"]}`))
	d, err := e.Decide(ctx, Request{Thread: rootID, Action: "git.push", Target: "origin", Machine: "build-01"})
	if err != nil || !d.Allowed {
		t.Fatal(d, err)
	}
	all, err := log.ReadStream("thread:"+rootID, 0, 10)
	if err != nil || len(all) != 1 || all[0].Actor.ID != rootID || all[0].Actor.Machine != "build-01" || all[0].TypeVersion != 2 {
		t.Fatalf("%+v %v", all, err)
	}
	p := events(t, log, rootID, "policy.decided")[0]
	want := map[string]any{"action": "git.push", "decision": "allow", "rule_id": "operator:store#all", "machine": "build-01", "target": "origin", "outcome": "executed"}
	for k, v := range want {
		if p[k] != v {
			t.Errorf("%s = %v, want %v", k, p[k], v)
		}
	}
	if _, err := e.Decide(ctx, Request{Thread: rootID, Action: "x.*", Machine: "m"}); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("wildcard request: %v", err)
	}
	_ = log.Close()
	if d, err := e.Decide(ctx, Request{Thread: rootID, Action: "git.push", Machine: "m"}); err == nil || d.Allowed {
		t.Fatalf("unrecorded decision allowed: %+v %v", d, err)
	}
}
