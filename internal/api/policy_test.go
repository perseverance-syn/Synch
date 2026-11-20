package api

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"synch/internal/eventlog"
	"synch/internal/policy"
	"synch/internal/queue"
)

type authorityState struct {
	id   string
	rule *policy.File
}

// Thread supplies the one managed user thread in the authority fixture.
func (s authorityState) Thread(id string) (policy.ThreadInfo, error) {
	return policy.ThreadInfo{ID: id, Role: "builder"}, nil
}

// AccessMode supplies the mode used to exercise native grants.
func (authorityState) AccessMode(string, string) (string, error) { return "full_control", nil }

// Policy returns the fixture's ask and allow rules.
func (s authorityState) Policy(string) ([]*policy.File, error) { return []*policy.File{s.rule}, nil }

// Current returns the digest used in the approval body.
func (authorityState) Current(string) (policy.Current, error) {
	digest := "sha256:" + strings.Repeat("a", 64)
	return policy.Current{PlanDigest: digest, WorktreeDigest: digest}, nil
}

// OwnsVM denies VM operations in this fixture.
func (authorityState) OwnsVM(string, string) (bool, error) { return false, nil }

// MergeAccepted reports that the fixture has no accepted merge.
func (authorityState) MergeAccepted(string, string) (bool, error) { return false, nil }

// Settle completes a revocation since the fixture launches no processes.
func (authorityState) Settle(context.Context, string, []string) error { return nil }

// --- D-23 receipt atomicity: publish authority only on first acceptance ---

// TestPreparedAuthorityReceipts proves AC7: receipts atomically publish authority and replay once.
func TestPreparedAuthorityReceipts(t *testing.T) {
	ctx := t.Context()
	s, store, _ := fixture(t)
	id := create(t, s, "create")
	now := time.Now().UTC()
	rule, err := policy.Load([]byte(`{"policy_version":3,"source":"operator","rules":[
		{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}},
		{"id":"allow","effect":"allow","actions":["host.exec"]}]}`), policy.Origin{Kind: policy.OriginStore})
	if err != nil {
		t.Fatal(err)
	}
	s.config.Policy = policy.New(store, authorityState{id, rule}, func() time.Time { return now })
	write := func(sql string) {
		t.Helper()
		if err := store.Write(ctx, func(tx *eventlog.Transaction) error {
			_, err := tx.ExecContext(ctx, sql)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	count := func(typ string) int {
		t.Helper()
		events, err := store.ReadStream("thread:"+id, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, e := range events {
			if e.Type == typ {
				n++
			}
		}
		return n
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	host := policy.Request{Thread: id, Machine: "h", Identity: "i", Action: "host.exec"}
	approval := `{"plan_digest":"` + digest + `","worktree_digest":"` + digest + `","target":"ci","actions":["ci.run"],"expires_at":"` + now.Add(time.Hour).Format(time.RFC3339) + `"}`
	var grantID string
	for _, op := range []struct {
		name, suffix, body, event, resultID string
		status, records                     int
	}{
		{"approval", "/approvals", approval, "approval.recorded", "approval_id", 201, 1},
		{"grant", "/grants", `{"host":"h","identity":"i"}`, "grant.issued", "grant_id", 201, 1},
		{"revoke", "/revoke", "", "grant.revoked", "", 202, 2},
	} {
		path := "/v1/threads/" + id + op.suffix
		if op.name == "revoke" {
			path = "/v1/grants/" + grantID + op.suffix
		}
		write(`CREATE TRIGGER fail_authority_receipt BEFORE INSERT ON receipts BEGIN SELECT RAISE(ABORT, 'receipt blocked'); END`)
		requireProblem(t, request(t, s, "POST", path, testToken, op.name, op.body), 500, "internal", "")
		if n := count(op.event); n != 0 {
			t.Fatalf("%s: rollback left %d events", op.name, n)
		}
		if op.name != "approval" {
			d, err := s.config.Policy.Decide(ctx, host)
			if err != nil || d.Allowed != (op.name == "revoke") {
				t.Fatalf("%s: state after rollback: %+v, %v", op.name, d, err)
			}
		}
		write(`DROP TRIGGER fail_authority_receipt`)
		first := request(t, s, "POST", path, testToken, op.name, op.body)
		replay := request(t, s, "POST", path, testToken, op.name, op.body)
		if first.Code != op.status || replay.Code != first.Code || replay.Body.String() != first.Body.String() || replay.Header().Get(replayedHeader) != "true" || count(op.event) != op.records {
			t.Fatalf("%s: first=%d %s; replay=%d %s", op.name, first.Code, first.Body, replay.Code, replay.Body)
		}
		got := decode[struct{ Result map[string]any }](t, first.Body.Bytes())
		switch op.name {
		case "approval":
			approvalID, _ := got.Result[op.resultID].(string)
			d, err := s.config.Policy.Decide(ctx, policy.Request{Thread: id, Machine: "h", Target: "ci", Action: "ci.run", ApprovalID: approvalID})
			if err != nil || !d.Allowed {
				t.Fatalf("committed approval: %+v, %v", d, err)
			}
		case "grant":
			grantID, _ = got.Result[op.resultID].(string)
			if grantID == "" {
				t.Fatal("grant receipt has no grant_id")
			}
		}
	}
	d, err := s.config.Policy.Decide(ctx, host)
	if err != nil || d.Allowed {
		t.Fatalf("committed revocation: %+v, %v", d, err)
	}
}

// --- D-23 lock ordering: revoke while dispatch decisions hold policy locks ---

// TestPreparedRevocationOrdersDecisions proves AC7: a receipt revocation and
// concurrent policy decisions finish without reversing the writer lock order.
func TestPreparedRevocationOrdersDecisions(t *testing.T) {
	s, store, _ := fixture(t)
	id := create(t, s, "create")
	rule, err := policy.Load([]byte(`{"policy_version":3,"source":"operator","rules":[
		{"id":"allow","effect":"allow","actions":["host.exec"]}]}`), policy.Origin{Kind: policy.OriginStore})
	if err != nil {
		t.Fatal(err)
	}
	s.config.Policy = policy.New(store, authorityState{id, rule}, time.Now)
	grant, err := s.config.Policy.IssueGrant(t.Context(), id, "h", "i", testCaller)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	giveUp := func() {
		t.Helper()
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("initial decision did not reach dispatch")
		}
	}
	req := policy.Request{Thread: id, Machine: "h", Identity: "i", Action: "host.exec"}
	initial := req
	resume := make(chan struct{}, 1)
	initial.Start = func() error {
		close(started)
		select {
		case <-resume:
			return nil
		case <-release:
			return errors.New("test abandoned")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 4)
	go func() {
		_, err := s.config.Policy.Decide(ctx, initial)
		done <- err
	}()
	giveUp()
	revokeStarted := make(chan struct{})
	go func() {
		close(revokeStarted)
		result, err := s.executeMutation(ctx, queue.Request{
			ID: "race-revoke", Caller: testCaller, Method: "POST", Route: "/v1/grants/{grant_id}/revoke",
			Command: "revokeGrant", PathParams: map[string]string{"grant_id": grant.ID},
		}, "revokeGrant")
		if err == nil && result.HTTPStatus != 202 {
			err = errors.New("revocation receipt was not accepted")
		}
		done <- err
	}()
	<-revokeStarted
	for range 2 {
		go func() {
			_, err := s.config.Policy.Decide(ctx, req)
			done <- err
		}()
	}
	resume <- struct{}{}
	for range 4 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("revocation and concurrent decisions did not finish")
		}
	}
	if decision, err := s.config.Policy.Decide(ctx, req); err != nil || decision.Allowed {
		t.Fatalf("dispatch allowed after revocation: %+v, %v", decision, err)
	}
}
