package policy

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"synch/internal/eventlog"
)

// --- Grants: issue and the dispatch-time ancestor walk (§6) ---

// grantState is a grant plus its revocation progress. Fields other than the
// embedded Grant and finishMu are guarded by Engine.mu.
type grantState struct {
	Grant
	revoked  bool           // grant.revoked{settled:false} is recorded; the dispatch check fails
	settled  bool           // grant.revoked{settled:true} is recorded
	payload  revokedPayload // fixed by the commit event; never mutated afterwards
	actor    eventlog.Actor
	finishMu sync.Mutex // serializes retries of the settle and completion steps
}

// IssueGrant records an operator-issued Full Control grant for one thread,
// one host and one identity (dec. 14).
func (e *Engine) IssueGrant(ctx context.Context, thread, host, identity, grantedBy string) (Grant, error) {
	g, pending, err := e.PrepareIssueGrant(thread, host, identity, grantedBy)
	if err != nil {
		return Grant{}, err
	}
	if err := e.persist(ctx, pending); err != nil {
		return Grant{}, err
	}
	return g, nil
}

// PrepareIssueGrant validates a new grant before its receipt transaction starts.
func (e *Engine) PrepareIssueGrant(thread, host, identity, grantedBy string) (Grant, *Pending, error) {
	if err := e.Recover(); err != nil {
		return Grant{}, nil, err
	}
	e.dispatchMu.Lock()
	ready := false
	defer func() {
		if !ready {
			e.dispatchMu.Unlock()
		}
	}()
	now := e.now().UTC()
	g := Grant{ID: newULID(now), Kind: "full_control", Thread: thread, Host: host, Identity: identity, GrantedBy: grantedBy, GrantedAt: now, Inherit: true}
	if err := g.validate(); err != nil {
		return Grant{}, nil, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	info, err := e.state.Thread(thread)
	if err != nil {
		return Grant{}, nil, fmt.Errorf("read thread: %w", err)
	}
	if info.Terminal {
		return Grant{}, nil, fmt.Errorf("%w: thread has ended", ErrInvalidRequest)
	}
	event, err := e.newEvent(thread, "grant.issued", 3, eventlog.Actor{Kind: "operator", ID: grantedBy}, g)
	if err != nil {
		return Grant{}, nil, err
	}
	ready = true
	return g, &Pending{event: event, release: e.dispatchMu.Unlock, publish: func() {
		e.mu.Lock()
		e.grants = append(e.grants, &grantState{Grant: g})
		e.mu.Unlock()
	}}, nil
}

// liveGrant runs the dispatch-time check for a native action by thread on
// host under identity. It walks from the thread to the root on every call
// (never cached) and returns the id of a grant G held by some thread A on
// that path where G matches host and identity, G is not revoked, A is not
// terminal, and every thread below A down to the actor is a managed
// descendant created while G was active. It returns "" when none qualifies.
func (e *Engine) liveGrant(thread, host, identity string) (string, error) {
	var path []ThreadInfo
	seen := map[string]bool{}
	for id := thread; id != ""; {
		if seen[id] { // a parent cycle is corrupt state; refuse rather than loop
			return "", fmt.Errorf("thread ancestry cycle at %s", id)
		}
		seen[id] = true
		info, err := e.state.Thread(id)
		if err != nil {
			return "", fmt.Errorf("read thread: %w", err)
		}
		path = append(path, info)
		id = info.Parent
	}
	if path[0].Terminal { // Q-21: inherited access ends when the child ends
		return "", nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, a := range path {
		if a.Terminal || i > 0 && !a.Managed {
			continue
		}
		for _, g := range e.grants {
			if g.revoked || g.Thread != a.ID || g.Host != host || g.Identity != identity {
				continue
			}
			ok := true
			for _, below := range path[:i] {
				ok = ok && below.Managed && !below.CreatedAt.Before(g.GrantedAt)
			}
			if ok {
				return g.ID, nil
			}
		}
	}
	return "", nil
}

// --- Revocation: commit, stop dispatch, settle, record (§6, catalogue C6) ---

// revokedPayload is the grant.revoked v2 payload.
type revokedPayload struct {
	GrantID         string   `json:"grant_id"`
	AffectedThreads []string `json:"affected_threads"`
	Settled         bool     `json:"settled"`
	RevokedBy       string   `json:"revoked_by,omitempty"`
	Cause           string   `json:"cause"`
	EndedThread     string   `json:"ended_thread,omitempty"`
}

// Revoke revokes a grant for its whole subtree on an operator's request.
// Called again for a committed but unsettled revocation, it resumes it.
func (e *Engine) Revoke(ctx context.Context, grantID, revokedBy string) error {
	_, pending, err := e.PrepareRevoke(ctx, grantID, revokedBy)
	if err != nil {
		return err
	}
	return e.persist(ctx, pending)
}

// PrepareRevoke locks dispatch before preparing an operator grant revocation.
// The returned thread owns the grant, and ctx is used for post-commit settlement.
func (e *Engine) PrepareRevoke(ctx context.Context, grantID, revokedBy string) (string, *Pending, error) {
	if err := e.Recover(); err != nil {
		return "", nil, err
	}
	if revokedBy == "" {
		return "", nil, fmt.Errorf("%w: revoked_by is required", ErrInvalidRequest)
	}
	return e.prepareRevocation(ctx, grantID, revokedPayload{Cause: "operator", RevokedBy: revokedBy}, eventlog.Actor{Kind: "operator", ID: revokedBy})
}

// RevokeForThreadEnd ends a grant because its thread ended. It first applies
// cross-check C6: the grant resolves, endedThread is the thread the grant was
// issued to, and that thread is already terminal.
func (e *Engine) RevokeForThreadEnd(ctx context.Context, grantID, endedThread string) error {
	if err := e.Recover(); err != nil {
		return err
	}
	g := e.findGrant(grantID)
	if g == nil {
		return fmt.Errorf("%w: %s", ErrUnknownGrant, grantID)
	}
	if g.Thread != endedThread {
		return fmt.Errorf("%w: ended thread %s is not the granted thread", ErrCrossCheck, endedThread)
	}
	info, err := e.state.Thread(endedThread)
	if err != nil {
		return fmt.Errorf("read thread: %w", err)
	}
	if !info.Terminal {
		return fmt.Errorf("%w: thread %s is not terminal", ErrCrossCheck, endedThread)
	}
	return e.revoke(ctx, grantID, revokedPayload{Cause: "thread_ended", EndedThread: endedThread}, eventlog.Actor{Kind: "daemon", ID: "synchd"})
}

// ThreadEnded applies the end of a terminal thread: its grants are revoked
// (after C6), unfinished revocations are resumed, and its approvals are
// voided with reason thread_ended.
func (e *Engine) ThreadEnded(ctx context.Context, thread string) error {
	if err := e.Recover(); err != nil {
		return err
	}
	e.mu.Lock()
	var grants, approvals []string
	for _, g := range e.grants {
		if g.Thread == thread && !g.settled {
			grants = append(grants, g.ID)
		}
	}
	for id, s := range e.approvals {
		if s.a.Thread == thread {
			approvals = append(approvals, id)
		}
	}
	e.mu.Unlock()
	var errs []error
	for _, id := range grants {
		errs = append(errs, e.RevokeForThreadEnd(ctx, id, thread))
	}
	for _, id := range approvals {
		errs = append(errs, e.voidID(ctx, id, "thread_ended"))
	}
	return errors.Join(errs...)
}

// revoke commits the revocation (step 1) and then finishes it. The commit is
// the grant.revoked{settled:false} append, made under the write side of the
// dispatch boundary: it waits for in-flight decisions, and the in-memory flag
// that makes every later dispatch check fail for the whole subtree (step 2,
// stop dispatch) is set only after the append succeeded. If that first append
// fails, nothing changes and the error is returned for the caller to retry
// (G1). A revocation already committed but not settled is resumed with its
// original payload instead of being refused.
func (e *Engine) revoke(ctx context.Context, id string, p revokedPayload, actor eventlog.Actor) error {
	_, pending, err := e.prepareRevocation(ctx, id, p, actor)
	if err != nil {
		return err
	}
	return e.persist(ctx, pending)
}

// prepareRevocation holds dispatch closed until the first revocation event
// commits or aborts. Settlement runs after releasing the dispatch lock.
func (e *Engine) prepareRevocation(ctx context.Context, id string, p revokedPayload, actor eventlog.Actor) (string, *Pending, error) {
	e.dispatchMu.Lock()
	ready := false
	defer func() {
		if !ready {
			e.dispatchMu.Unlock()
		}
	}()
	g := e.findGrant(id)
	if g == nil {
		return "", nil, fmt.Errorf("%w: %s", ErrUnknownGrant, id)
	}
	e.mu.Lock()
	revoked, settled := g.revoked, g.settled
	e.mu.Unlock()
	if settled {
		return "", nil, fmt.Errorf("%w: %s is already revoked", ErrUnknownGrant, id)
	}
	pending := &Pending{release: e.dispatchMu.Unlock, finish: func() error {
		return e.finishRevocation(ctx, id)
	}}
	if revoked {
		ready = true
		return g.Thread, pending, nil
	}
	affected, err := e.subtree(g.Thread)
	if err != nil {
		return "", nil, err
	}
	p.GrantID, p.AffectedThreads, p.Settled = id, affected, false
	pending.event, err = e.newEvent(g.Thread, "grant.revoked", 2, actor, p)
	if err != nil {
		return "", nil, err
	}
	pending.publish = func() {
		e.mu.Lock()
		g.revoked, g.payload, g.actor = true, p, actor
		e.mu.Unlock()
	}
	ready = true
	return g.Thread, pending, nil
}

// finishRevocation runs steps 3 and 4 for a committed revocation and can be
// retried until it succeeds: settle active work through the injected settler,
// then record grant.revoked{settled:true} with the same payload as the
// commit. The settler gets its own copy of the thread list, so it cannot
// change the stored payload (G3). Dispatch stays blocked throughout.
func (e *Engine) finishRevocation(ctx context.Context, id string) error {
	g := e.findGrant(id)
	if g == nil {
		return fmt.Errorf("%w: %s", ErrUnknownGrant, id)
	}
	g.finishMu.Lock()
	defer g.finishMu.Unlock()
	e.mu.Lock()
	revoked, done, p, actor := g.revoked, g.settled, g.payload, g.actor
	e.mu.Unlock()
	if !revoked || done {
		return nil
	}
	if err := e.state.Settle(ctx, id, slices.Clone(p.AffectedThreads)); err != nil {
		return fmt.Errorf("%w: settle grant %s: %w", ErrRevocationIncomplete, id, err)
	}
	p.Settled = true
	if err := e.emit(ctx, g.Thread, "grant.revoked", 2, actor, p); err != nil {
		return fmt.Errorf("%w: %w", ErrRevocationIncomplete, err)
	}
	e.mu.Lock()
	g.settled = true
	e.mu.Unlock()
	return nil
}

// subtree lists root and all its descendants, breadth first.
func (e *Engine) subtree(root string) ([]string, error) {
	out := []string{}
	seen := map[string]bool{}
	for queue := []string{root}; len(queue) > 0; queue = queue[1:] {
		id := queue[0]
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
		info, err := e.state.Thread(id)
		if err != nil {
			return nil, fmt.Errorf("read thread: %w", err)
		}
		queue = append(queue, info.Children...)
	}
	return out, nil
}

// findGrant returns the grant with this id, or nil.
func (e *Engine) findGrant(id string) *grantState {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.findGrantLocked(id)
}

// findGrantLocked is findGrant for callers already holding e.mu.
func (e *Engine) findGrantLocked(id string) *grantState {
	for _, g := range e.grants {
		if g.ID == id {
			return g
		}
	}
	return nil
}
