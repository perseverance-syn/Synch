package policy

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// Approval is a bounded approval record (schema $defs/approval, dec. 20). The
// same shape is the approval.recorded payload and the file export form.
// Pointers mark fields whose presence (not only value) the schema checks.
type Approval struct {
	ID                  string    `json:"id"`
	Approver            string    `json:"approver"`
	Thread              string    `json:"thread"`
	Revision            *string   `json:"revision,omitempty"`
	PlanDigest          *string   `json:"plan_digest,omitempty"`
	WorktreeDigest      string    `json:"worktree_digest"`
	Target              string    `json:"target"`
	Actions             []string  `json:"actions"`
	ApprovedAt          time.Time `json:"approved_at"`
	ExpiresAt           time.Time `json:"expires_at"`
	Rollback            *string   `json:"rollback,omitempty"`
	IrreversibleEffects *[]string `json:"irreversible_effects,omitempty"`
	ConstraintRules     []string  `json:"constraint_rules,omitempty"`
}

// Grant is a Full Control grant record (schema $defs/grant, dec. 14).
type Grant struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Thread    string    `json:"thread"`
	Host      string    `json:"host"`
	Identity  string    `json:"identity"`
	GrantedBy string    `json:"granted_by"`
	GrantedAt time.Time `json:"granted_at"`
	Inherit   bool      `json:"inherit"`
}

// clone returns a deep copy of the approval. The engine stores and returns
// only clones, so no caller-held slice or pointer can change a live record (R104).
func (a Approval) clone() Approval {
	c := a
	c.Revision, c.PlanDigest, c.Rollback = clonePtr(a.Revision), clonePtr(a.PlanDigest), clonePtr(a.Rollback)
	c.Actions, c.ConstraintRules = slices.Clone(a.Actions), slices.Clone(a.ConstraintRules)
	if a.IrreversibleEffects != nil {
		effects := slices.Clone(*a.IrreversibleEffects)
		if effects == nil { // keep "present but empty" distinct from absent
			effects = []string{}
		}
		c.IrreversibleEffects = &effects
	}
	return c
}

// clonePtr copies the value behind p into a new pointer.
func clonePtr(p *string) *string {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// ErrInvalidApproval reports an approval that violates §5.1.
var ErrInvalidApproval = errors.New("policy: invalid approval")

// validate checks a Go approval input, where zero time denotes an omitted value.
func (a *Approval) validate() error {
	if a.ApprovedAt.IsZero() || a.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: approved_at and expires_at are required", ErrInvalidApproval)
	}
	return a.validateRecord()
}

// validateRecord checks every §5.1 rule, including P1, after timestamp
// presence is established. Replay checks presence on the wire, where the
// Go zero instant is a valid date-time value.
func (a *Approval) validateRecord() error {
	bad := func(msg string) error { return fmt.Errorf("%w: %s", ErrInvalidApproval, msg) }
	switch {
	case a.ID == "" || a.Approver == "":
		return bad("id and approver are required")
	case !ulidRe.MatchString(a.Thread):
		return bad("thread must be a ULID")
	case (a.Revision == nil) == (a.PlanDigest == nil):
		return bad("exactly one of revision or plan_digest is required")
	case a.Revision != nil && !revisionRe.MatchString(*a.Revision):
		return bad("revision must be a full commit id")
	case a.PlanDigest != nil && !sha256Re.MatchString(*a.PlanDigest):
		return bad("bad plan_digest")
	case !sha256Re.MatchString(a.WorktreeDigest):
		return bad("bad worktree_digest")
	case a.Target == "":
		return bad("target is required")
	case len(a.Actions) == 0:
		return bad("actions are required")
	case !a.ExpiresAt.After(a.ApprovedAt):
		return bad("expires_at must be after approved_at (P1)")
	case a.Rollback != nil && *a.Rollback == "":
		return bad("empty rollback")
	}
	seen := map[string]bool{}
	op := false
	for _, act := range a.Actions {
		if !actionRe.MatchString(act) || seen[act] {
			return bad("actions must be unique exact names, no wildcards")
		}
		seen[act] = true
		op = op || in(act, operational)
	}
	for _, e := range deref(a.IrreversibleEffects) {
		if e == "" {
			return bad("empty irreversible effect")
		}
	}
	for _, q := range a.ConstraintRules {
		if !qualifiedRe.MatchString(q) {
			return bad("constraint_rules must be qualified ids")
		}
	}
	if op && (a.PlanDigest == nil || a.Rollback == nil || a.IrreversibleEffects == nil) {
		return bad("operational actions need plan_digest, rollback and irreversible_effects")
	}
	return nil
}

// validate checks a grant against $defs/grant.
func (g *Grant) validate() error {
	if g.ID == "" || g.Kind != "full_control" || !ulidRe.MatchString(g.Thread) || g.Host == "" ||
		g.Identity == "" || g.GrantedBy == "" || g.GrantedAt.IsZero() || !g.Inherit {
		return errors.New("invalid grant")
	}
	return nil
}
