package policy

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"synch/internal/eventlog"
)

// --- Engine wiring: injected state, requests and decisions ---

// State is what the engine reads from the rest of the daemon. Every call must
// return current values; the engine never caches them (§5.3, §6).
type State interface {
	// Thread returns the thread's current record.
	Thread(id string) (ThreadInfo, error)
	// AccessMode returns sandbox, provider or full_control for the thread and target (Q-19).
	AccessMode(thread, target string) (string, error)
	// Policy returns the thread's current trusted rule files (operator, project, role, skill).
	Policy(thread string) ([]*File, error)
	// Current returns HEAD, plan digest and worktree digest now (§5.2, card 501).
	Current(thread string) (Current, error)
	// OwnsVM applies vm-driver.md §2 ownership (dec. 13).
	OwnsVM(thread, vm string) (bool, error)
	// MergeAccepted reports human acceptance of exactly this revision (dec. 21).
	MergeAccepted(thread, revision string) (bool, error)
	// Settle stops active work under a revoked grant (execution-driver.md §5).
	// It may be called again for the same grant after a failure, so it must be idempotent.
	Settle(ctx context.Context, grantID string, threads []string) error
}

// ThreadInfo is the part of a thread record the engine needs.
type ThreadInfo struct {
	ID        string
	Parent    string // "" for the root
	Role      string // coordinator, builder, ops, security, auditor
	Managed   bool   // created and supervised by Synch
	Terminal  bool
	CreatedAt time.Time
	Children  []string
	Mediated  []string // Synch-managed working copies (§8 @mediated)
}

// Current holds the live values an approval is compared against.
type Current struct {
	Head, PlanDigest, WorktreeDigest string
}

// Request describes one mediated action about to be dispatched.
type Request struct {
	Thread     string
	Action     string // exact action (§7)
	Target     string
	Machine    string // host the action runs on
	Identity   string // host account, for native actions
	Paths      []string
	Args       []string
	ApprovalID string // approval offered for an ask
	// Start, if set, is called once when the action is allowed, still inside
	// the ordering boundary shared with revocation (§6). The caller registers
	// the dispatched work there, so a later revocation's settle step sees it.
	// Start must not call back into the Engine.
	Start func() error
}

// Decision is the engine's answer. Only Allowed=true permits dispatch.
type Decision struct {
	Allowed    bool
	Effect     Effect // rule result max(O, R, B)
	RuleID     string // qualified id of the deciding rule
	Outcome    string // policy.decided outcome
	Reason     string
	ApprovalID string
	GrantID    string
	role       string
	release    func() // releases the approval use lock, if one is held
}

// Sentinel errors callers can test with errors.Is.
var (
	ErrInvalidRequest = errors.New("policy: invalid request")
	ErrUnknownGrant   = errors.New("policy: unknown grant")
	ErrCrossCheck     = errors.New("policy: cross-check C6 failed")
	// ErrRevocationIncomplete wraps errors after a revocation committed: the
	// grant is revoked (dispatch blocked) but settle or the completion record
	// is still outstanding; retry Revoke or FlushPending. Any other Revoke
	// error means nothing was committed and the grant is unchanged.
	ErrRevocationIncomplete = errors.New("policy: revocation committed, not yet settled")
)

// Engine evaluates mediated actions and holds API-issued grants and approvals.
//
// Lock order: dispatchMu before mu. dispatchMu is the ordering boundary
// between decisions (read side, held from evaluation through recording and
// Start) and revocation commits (write side). mu guards the record maps.
type Engine struct {
	log        *eventlog.Store
	state      State
	now        func() time.Time
	dispatchMu sync.RWMutex
	mu         sync.Mutex
	approvals  map[string]*approvalState
	grants     []*grantState
	// recoverOnce runs Recover before the first public call; recoverErr keeps its result.
	recoverOnce sync.Once
	recoverErr  error
}

// approvalState is a stored approval plus its void status. A voided approval
// stays in the map, denying every use, until approval.voided is recorded (§5.3).
type approvalState struct {
	a    Approval // owned clone; never handed out
	void string   // pending void reason; non-empty means voided
	// write serializes approval.voided appends for this approval, so a second
	// caller learns the outcome of the first instead of assuming success (K2).
	write sync.Mutex
	// use is held for reading by every decision that passed this approval's
	// checks, until that decision has recorded and started (or failed). A
	// use-time void takes it for writing, so it waits for those decisions.
	use sync.RWMutex
}

// New returns an engine that records to log and reads live state from state.
func New(log *eventlog.Store, state State, now func() time.Time) *Engine {
	return &Engine{log: log, state: state, now: now, approvals: map[string]*approvalState{}}
}

// --- Decide: rules, approval use, authority, then policy.decided (§1, §4, §5.3) ---

// Decide evaluates one mediated action and records policy.decided before
// returning, so the record exists before any dispatch. Any provider or log
// error fails closed (Allowed=false). The whole call holds the read side of
// the dispatch boundary, so a revocation commit waits for in-flight decisions
// and no decision that started before the commit returns Allowed after it.
func (e *Engine) Decide(ctx context.Context, req Request) (Decision, error) {
	if !ulidRe.MatchString(req.Thread) || !actionRe.MatchString(req.Action) || req.Machine == "" {
		return Decision{Effect: Deny, Outcome: "blocked"}, fmt.Errorf("%w: thread, exact action and machine are required", ErrInvalidRequest)
	}
	if err := e.Recover(); err != nil {
		return Decision{Effect: Deny, Outcome: "blocked"}, err
	}
	e.dispatchMu.RLock()
	defer e.dispatchMu.RUnlock()
	d, err := e.decide(ctx, req)
	if d.release != nil { // keep the approval's use lock until recorded and started
		defer d.release()
	}
	if err != nil {
		d.Allowed, d.Outcome = false, "failed"
		if d.Reason == "" {
			d.Reason = err.Error()
		}
	}
	if rerr := e.record(ctx, req, d); rerr != nil {
		d.Allowed = false
		return d, errors.Join(err, rerr)
	}
	if d.Allowed && req.Start != nil {
		if serr := req.Start(); serr != nil {
			d.Allowed = false
			return d, fmt.Errorf("start dispatch: %w", serr)
		}
	}
	return d, err
}

// decide does the evaluation without recording it.
func (e *Engine) decide(ctx context.Context, req Request) (Decision, error) {
	d := Decision{Effect: Deny, Outcome: "blocked"}
	info, err := e.state.Thread(req.Thread)
	if err != nil {
		return d, fmt.Errorf("read thread: %w", err)
	}
	d.role = info.Role
	mode, err := e.state.AccessMode(req.Thread, req.Target)
	if err != nil {
		return d, fmt.Errorf("read access mode: %w", err)
	}
	files, err := e.state.Policy(req.Thread)
	if err != nil {
		return d, fmt.Errorf("read policy: %w", err)
	}
	s := subject{action: req.Action, target: req.Target, host: req.Machine, role: info.Role, mode: mode, args: req.Args}
	for _, root := range info.Mediated {
		if f, ok := formOf(root); ok {
			s.mediated = append(s.mediated, f)
		}
	}
	for _, p := range req.Paths {
		f, ok := formOf(p)
		if !ok {
			d.Reason = "paths must be absolute and in a supported namespace"
			return d, nil
		}
		s.paths = append(s.paths, f)
	}
	res := evaluate(files, s)
	d.Effect, d.RuleID = res.effect, res.ruleID
	switch res.effect {
	case Deny:
		d.Reason = "denied by policy"
		if req.ApprovalID != "" { // an approval can never override deny (§5.3)
			return d, e.voidID(ctx, req.ApprovalID, "scope_changed")
		}
		return d, nil
	case Ask:
		if req.ApprovalID == "" {
			d.Outcome, d.Reason = "waiting_approval", "human approval required"
			return d, nil
		}
		why, release, err := e.useApproval(ctx, req, info, res)
		d.release = release
		if err != nil || why != "" {
			d.Reason = why
			return d, err
		}
		d.ApprovalID = req.ApprovalID
	}
	grantID, why, err := e.authorize(req, info, mode, d.ApprovalID)
	if err != nil || why != "" {
		d.Reason = why
		return d, err
	}
	d.Allowed, d.Outcome, d.GrantID = true, "executed", grantID
	return d, nil
}

// vmMutations are the VM operations that need ownership (vm-driver.md §2).
var vmMutations = []string{"vm.start", "vm.stop", "vm.delete", "vm.snapshot", "vm.restore"}

// authorize runs the §4 authority checks, which no rule result can replace.
// It returns the grant used and a non-empty reason when the action must be blocked.
func (e *Engine) authorize(req Request, info ThreadInfo, mode, approvalID string) (string, string, error) {
	a := req.Action
	if !in(mode, modeValues) {
		return "", "unknown access mode", nil
	}
	if info.Role == "coordinator" && !matchAction("plan.*", a) && !matchAction("assign.*", a) && a != "evidence.read" {
		return "", "coordinator may only plan, assign and read evidence (dec. 05)", nil
	}
	switch {
	case matchAction("host.*", a):
		if mode != "full_control" {
			return "", "native host actions need full_control", nil
		}
		g, err := e.liveGrant(req.Thread, req.Machine, req.Identity)
		if err != nil || g == "" {
			return "", "no live grant for this thread, host and identity", err
		}
		return g, "", nil
	case in(a, vmMutations):
		ok, err := e.state.OwnsVM(req.Thread, req.Target)
		if err != nil || !ok {
			return "", "thread does not own this VM", err
		}
	case a == "merge":
		cur, err := e.state.Current(req.Thread)
		if err != nil {
			return "", "", fmt.Errorf("read current revision: %w", err)
		}
		ok, err := e.state.MergeAccepted(req.Thread, cur.Head)
		if err != nil || !ok {
			return "", "merge needs human acceptance of this exact revision", err
		}
	case strings.HasPrefix(a, "deploy.") && approvalID == "":
		return "", "deployment needs its own gate approval", nil
	}
	return "", "", nil
}

// decidedPayload is the policy.decided v2 payload.
type decidedPayload struct {
	Action     string `json:"action"`
	Decision   string `json:"decision"`
	RuleID     string `json:"rule_id,omitempty"`
	Machine    string `json:"machine"`
	Target     string `json:"target,omitempty"`
	ApprovalID string `json:"approval_id,omitempty"`
	GrantID    string `json:"grant_id,omitempty"`
	Outcome    string `json:"outcome"`
}

// record appends policy.decided for the decision, attributed to the acting thread.
func (e *Engine) record(ctx context.Context, req Request, d Decision) error {
	kind := "agent"
	if d.role == "coordinator" {
		kind = "coordinator"
	}
	return e.emit(ctx, req.Thread, "policy.decided", 2, eventlog.Actor{Kind: kind, ID: req.Thread, Machine: req.Machine}, decidedPayload{
		Action: req.Action, Decision: d.Effect.String(), RuleID: d.RuleID, Machine: req.Machine,
		Target: req.Target, ApprovalID: d.ApprovalID, GrantID: d.GrantID, Outcome: d.Outcome,
	})
}

// --- Approvals: creation (§5.1) ---

// CreateApproval validates and records an API-issued approval. The daemon
// sets id, approved_at and constraint_rules. The input is deep-copied on the
// way in and the result on the way out. A TTL above the strictest constraint
// that might apply is rejected here and re-checked at every use.
func (e *Engine) CreateApproval(ctx context.Context, input Approval) (Approval, error) {
	a, pending, err := e.PrepareCreateApproval(input)
	if err != nil {
		return Approval{}, err
	}
	if err := e.persist(ctx, pending); err != nil {
		return Approval{}, err
	}
	return a, nil
}

// PrepareCreateApproval validates the approval while holding the dispatch boundary
// until the caller publishes its committed event or aborts.
func (e *Engine) PrepareCreateApproval(input Approval) (Approval, *Pending, error) {
	if err := e.Recover(); err != nil {
		return Approval{}, nil, err
	}
	e.dispatchMu.Lock()
	ready := false
	defer func() {
		if !ready {
			e.dispatchMu.Unlock()
		}
	}()
	a := input.clone()
	now := e.now().UTC()
	a.ID, a.ApprovedAt, a.ConstraintRules = newULID(now), now, nil
	if err := a.validate(); err != nil {
		return Approval{}, nil, err
	}
	info, err := e.state.Thread(a.Thread)
	if err != nil {
		return Approval{}, nil, fmt.Errorf("read thread: %w", err)
	}
	if info.Terminal {
		return Approval{}, nil, fmt.Errorf("%w: thread has ended", ErrInvalidApproval)
	}
	mode, err := e.state.AccessMode(a.Thread, a.Target)
	if err != nil {
		return Approval{}, nil, fmt.Errorf("read access mode: %w", err)
	}
	files, err := e.state.Policy(a.Thread)
	if err != nil {
		return Approval{}, nil, fmt.Errorf("read policy: %w", err)
	}
	ttl := a.ExpiresAt.Sub(a.ApprovedAt)
	for _, act := range a.Actions {
		// Known dimensions (action, target, role, mode) must match; unknown ones
		// (host, args, paths) count as matching, so the limit is never too lax.
		res := evaluate(files, subject{action: act, target: a.Target, role: info.Role, mode: mode, creation: true})
		for _, c := range res.asks {
			if !in(c.qid, a.ConstraintRules) {
				a.ConstraintRules = append(a.ConstraintRules, c.qid)
			}
		}
		if limit, ok := res.minTTL(); ok && ttl > limit {
			return Approval{}, nil, fmt.Errorf("%w: ttl %s exceeds strictest constraint %s", ErrInvalidApproval, ttl, limit)
		}
	}
	event, err := e.newEvent(a.Thread, "approval.recorded", 3, eventlog.Actor{Kind: "operator", ID: a.Approver}, a)
	if err != nil {
		return Approval{}, nil, err
	}
	ready = true
	return a.clone(), &Pending{event: event, release: e.dispatchMu.Unlock, publish: func() {
		e.mu.Lock()
		e.approvals[a.ID] = &approvalState{a: a}
		e.mu.Unlock()
	}}, nil
}

// RevokeApproval voids an approval with reason "revoked". The void exists
// once approval.voided is persisted (G1): the record is appended first, under
// the write side of the dispatch boundary, so every in-flight decision has
// finished and no later decision can use the approval. If the append fails,
// nothing changes and the error is returned for the caller to retry.
func (e *Engine) RevokeApproval(ctx context.Context, id string) error {
	pending, err := e.PrepareRevokeApproval(id)
	if err != nil {
		return err
	}
	return e.persist(ctx, pending)
}

// PrepareRevokeApproval waits for active approval uses and void writers before
// preparing the first durable void reason.
func (e *Engine) PrepareRevokeApproval(id string) (*Pending, error) {
	if err := e.Recover(); err != nil {
		return nil, err
	}
	e.dispatchMu.Lock()
	e.mu.Lock()
	s := e.approvals[id]
	e.mu.Unlock()
	pending := &Pending{release: e.dispatchMu.Unlock}
	if s == nil {
		return pending, nil
	}

	// FlushPending may independently void an approval. Take both of its
	// per-approval locks before looking up the pending reason.
	s.use.Lock()
	s.write.Lock()
	pending.release = func() {
		s.write.Unlock()
		s.use.Unlock()
		e.dispatchMu.Unlock()
	}
	e.mu.Lock()
	live, why := e.approvals[id] == s, s.void
	e.mu.Unlock()
	if !live {
		return pending, nil
	}
	if why == "" {
		why = "revoked"
	}
	event, err := e.newEvent(s.a.Thread, "approval.voided", 1, eventlog.Actor{Kind: "daemon", ID: "synchd"},
		map[string]string{"approval_id": id, "reason": why})
	if err != nil {
		pending.Abort()
		return nil, err
	}
	pending.event = event
	pending.publish = func() {
		e.mu.Lock()
		delete(e.approvals, id)
		e.mu.Unlock()
	}
	return pending, nil
}

// --- Approvals: unconditional use-time check and durable void (§5.3) ---

// useApproval compares every §5.3 row against current values. When all rows
// hold, it returns a release func: the caller holds the approval's use lock
// until its decision is recorded and started, so no void can commit in
// between. On any mismatch it voids the approval and returns a non-empty
// reason. A voided approval whose record is still pending is denied and its
// record retried.
func (e *Engine) useApproval(ctx context.Context, req Request, info ThreadInfo, res result) (string, func(), error) {
	e.mu.Lock()
	s := e.approvals[req.ApprovalID]
	e.mu.Unlock()
	if s == nil {
		return "unknown or voided approval", nil, nil
	}
	s.use.RLock()
	e.mu.Lock()
	a, pending := s.a, s.void != ""
	gone := e.approvals[req.ApprovalID] != s
	e.mu.Unlock()
	if gone { // voided and recorded while this decision waited for the lock
		s.use.RUnlock()
		return "unknown or voided approval", nil, nil
	}
	if pending { // re-read under the use lock: a void may have been marked meanwhile
		s.use.RUnlock()
		return "approval voided", nil, e.recordVoid(ctx, s, "")
	}
	reason, err := e.checkUse(a, req, info, res)
	if err != nil || reason != "" {
		s.use.RUnlock()
		if err != nil {
			return "", nil, err
		}
		return "approval voided: " + reason, nil, e.voidState(ctx, s, reason)
	}
	return "", s.use.RUnlock, nil
}

// checkUse returns the §5.3 void reason for using a in req, or "".
func (e *Engine) checkUse(a Approval, req Request, info ThreadInfo, res result) (string, error) {
	if a.Thread != req.Thread { // Q-02: no use by other threads
		return "scope_changed", nil
	}
	if reason, err := e.staleReason(a, info); err != nil || reason != "" {
		return reason, err
	}
	limit, hasLimit := res.minTTL()
	switch {
	case a.Target != req.Target:
		return "target_changed", nil
	case !in(req.Action, a.Actions):
		return "scope_changed", nil
	case hasLimit && a.ExpiresAt.Sub(a.ApprovedAt) > limit: // constraints recomputed from current policy
		return "scope_changed", nil
	case res.effect != Ask:
		return "scope_changed", nil
	case a.validate() != nil: // operational bindings, defensively re-checked
		return "scope_changed", nil
	}
	return "", nil
}

// staleReason checks the §5.3 rows that need no request: the thread has not
// ended, revision or plan digest and worktree digest are current, and now is
// inside [approved_at, expires_at). FlushPending uses it to re-detect voids
// that were never persisted (G1).
func (e *Engine) staleReason(a Approval, info ThreadInfo) (string, error) {
	if info.Terminal {
		return "thread_ended", nil
	}
	cur, err := e.state.Current(a.Thread)
	if err != nil {
		return "", fmt.Errorf("read current digests: %w", err)
	}
	now := e.now()
	switch {
	case a.Revision != nil && *a.Revision != cur.Head:
		return "diff_changed", nil
	case a.PlanDigest != nil && *a.PlanDigest != cur.PlanDigest:
		return "scope_changed", nil
	case a.WorktreeDigest != cur.WorktreeDigest:
		return "diff_changed", nil
	case now.Before(a.ApprovedAt) || !now.Before(a.ExpiresAt):
		return "expired", nil
	}
	return "", nil
}

// voidID voids the live approval with this id for a detected condition.
func (e *Engine) voidID(ctx context.Context, id, reason string) error {
	e.mu.Lock()
	s := e.approvals[id]
	e.mu.Unlock()
	if s == nil {
		return nil
	}
	return e.voidState(ctx, s, reason)
}

// voidState marks a detected void and records it. Marking takes the
// approval's use lock for writing, so it waits for every decision that
// already passed this approval's checks; from the mark on, every use is
// denied. The first reason wins. If the record cannot be appended the void
// stays pending in memory and is retried; it is a condition the use-time
// check re-detects, so after a restart it is detected again (G1).
func (e *Engine) voidState(ctx context.Context, s *approvalState, reason string) error {
	s.use.Lock()
	e.mu.Lock()
	if s.void == "" {
		s.void = reason
	}
	e.mu.Unlock()
	s.use.Unlock()
	return e.recordVoid(ctx, s, "")
}

// recordVoid appends approval.voided for s (with its pending reason, or with
// reason when none is pending) and removes it on success. It returns nil only
// once a void for s is durable (K2): appends for one approval are serialized
// by s.write, so a caller that arrives while another append is in flight
// waits for it; if that append succeeded the approval is gone and nil is
// returned, and if it failed this caller appends itself and returns its own
// result.
func (e *Engine) recordVoid(ctx context.Context, s *approvalState, reason string) error {
	s.write.Lock()
	defer s.write.Unlock()
	e.mu.Lock()
	live := e.approvals[s.a.ID] == s
	why := s.void
	if why == "" {
		why = reason
	}
	id, thread := s.a.ID, s.a.Thread
	e.mu.Unlock()
	if !live || why == "" { // already voided durably, or nothing to record
		return nil
	}
	err := e.emit(ctx, thread, "approval.voided", 1, eventlog.Actor{Kind: "daemon", ID: "synchd"},
		map[string]string{"approval_id": id, "reason": why})
	if err == nil {
		e.mu.Lock()
		delete(e.approvals, id)
		e.mu.Unlock()
	}
	return err
}

// --- Pending work: finish voids and revocations (G1) ---

// FlushPending finishes every obligation that is not complete: it records
// pending voids, re-detects request-independent void conditions on live
// approvals (so a void lost to a restart is found again), and resumes
// revocations that are committed but not settled. Each grant's steps stay in
// order; different records are flushed independently. The daemon calls it
// after Recover and periodically.
func (e *Engine) FlushPending(ctx context.Context) error {
	if err := e.Recover(); err != nil {
		return err
	}
	e.mu.Lock()
	var states []*approvalState
	for _, s := range e.approvals {
		states = append(states, s)
	}
	var grants []string
	for _, g := range e.grants {
		if g.revoked && !g.settled {
			grants = append(grants, g.ID)
		}
	}
	e.mu.Unlock()
	var errs []error
	for _, s := range states {
		e.mu.Lock()
		pending, a := s.void != "", s.a
		e.mu.Unlock()
		if pending {
			errs = append(errs, e.recordVoid(ctx, s, ""))
			continue
		}
		info, err := e.state.Thread(a.Thread)
		if err != nil {
			errs = append(errs, fmt.Errorf("read thread: %w", err))
			continue
		}
		reason, err := e.staleReason(a, info)
		if err == nil && reason != "" {
			err = e.voidState(ctx, s, reason)
		}
		errs = append(errs, err)
	}
	for _, id := range grants {
		errs = append(errs, e.finishRevocation(ctx, id))
	}
	return errors.Join(errs...)
}

// --- Events and ids ---

// emit appends one event to the thread's stream.
func (e *Engine) emit(ctx context.Context, thread, typ string, version int, actor eventlog.Actor, payload any) error {
	event, err := e.newEvent(thread, typ, version, actor, payload)
	if err != nil {
		return err
	}
	_, err = e.log.Append(ctx, []eventlog.NewEvent{*event})
	if err != nil {
		return fmt.Errorf("record %s: %w", typ, err)
	}
	return nil
}

// newULID returns a ULID: 48-bit millisecond time plus 80 random bits.
func newULID(t time.Time) string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(t.UnixMilli())<<16)
	_, _ = rand.Read(b[6:]) // crypto/rand.Read never fails on supported platforms
	n := new(big.Int).SetBytes(b[:])
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	out := make([]byte, 26)
	mask := big.NewInt(31)
	for i := 25; i >= 0; i-- {
		out[i] = alphabet[new(big.Int).And(n, mask).Int64()]
		n.Rsh(n, 5)
	}
	return string(out)
}
