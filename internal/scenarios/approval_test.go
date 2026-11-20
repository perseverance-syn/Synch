package scenarios

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"synch/internal/eventlog"
	"synch/internal/policy"
)

// approvalState supplies live revision and operator rules to the real policy engine.
type approvalState struct {
	current policy.Current
	rules   []*policy.File
}

// Thread returns a managed builder for the approval fixture.
func (*approvalState) Thread(id string) (policy.ThreadInfo, error) {
	return policy.ThreadInfo{ID: id, Role: "builder", Managed: true}, nil
}

// AccessMode returns provider mode for the fixture.
func (*approvalState) AccessMode(string, string) (string, error) { return "provider", nil }

// Policy returns the operator rules.
func (s *approvalState) Policy(string) ([]*policy.File, error) { return s.rules, nil }

// Current returns the mutable current revision and digest.
func (s *approvalState) Current(string) (policy.Current, error) { return s.current, nil }

// OwnsVM declines VM authority for this CI-only fixture.
func (*approvalState) OwnsVM(string, string) (bool, error) { return false, nil }

// MergeAccepted declines unrelated merge authority.
func (*approvalState) MergeAccepted(string, string) (bool, error) { return false, nil }

// Settle is unused by this approval-only fixture.
func (*approvalState) Settle(context.Context, string, []string) error { return nil }

// TestStaleApprovalRefusedAfterRevisionOrTargetChange checks approval use against live evidence.
func TestStaleApprovalRefusedAfterRevisionOrTargetChange(t *testing.T) {
	for _, changed := range []string{"revision", "target"} {
		t.Run(changed, func(t *testing.T) {
			log, err := eventlog.Open(filepath.Join(t.TempDir(), "events.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = log.Close() })
			rules, err := policy.Load([]byte(`{"policy_version":3,"source":"operator","rules":[{"id":"ask","effect":"ask","actions":["ci.run"],"approval":{"max_ttl":"PT4H"}}]}`), policy.Origin{Kind: policy.OriginStore})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			revision := strings.Repeat("a", 40)
			digest := "sha256:" + strings.Repeat("e", 64)
			state := &approvalState{current: policy.Current{Head: revision, WorktreeDigest: digest}, rules: []*policy.File{rules}}
			engine := policy.New(log, state, func() time.Time { return now })
			id := eventlog.NewID()
			approval, err := engine.CreateApproval(t.Context(), policy.Approval{Approver: "operator:scenario", Thread: id, Revision: &revision, WorktreeDigest: digest, Target: "t1", Actions: []string{"ci.run"}, ExpiresAt: now.Add(time.Hour)})
			if err != nil {
				t.Fatal(err)
			}
			req := policy.Request{Thread: id, Action: "ci.run", Target: "t1", Machine: "m", ApprovalID: approval.ID}
			decision, err := engine.Decide(t.Context(), req)
			if err != nil || !decision.Allowed {
				t.Fatalf("baseline: %+v %v", decision, err)
			}
			if changed == "revision" {
				state.current.Head = strings.Repeat("b", 40)
			} else {
				req.Target = "t2"
			}
			decision, err = engine.Decide(t.Context(), req)
			if err != nil || decision.Allowed {
				t.Fatalf("stale approval accepted: %+v %v", decision, err)
			}
			events, err := log.ReadStream("thread:"+id, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, e := range events {
				if e.Type == "approval.voided" && strings.Contains(string(e.Payload), map[string]string{"revision": "diff_changed", "target": "target_changed"}[changed]) {
					found = true
				}
			}
			if !found {
				t.Fatal("missing void reason")
			}
		})
	}
}
