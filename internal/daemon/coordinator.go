package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"synch/internal/children"
	"synch/internal/eventlog"
	"synch/internal/roles"
	"synch/internal/thread"
)

// ErrCoordinatorOnly refuses an engine session on a top-level thread: a
// coordinator never runs tools or commands, its work runs in managed
// children (decision 05, owner D-41).
var ErrCoordinatorOnly = errors.New("a coordinator thread never runs an engine; dispatch a managed child (dec. 05)")

// --- Coordinator flow: a dispatched top-level thread delegates to one Builder child ---

// coordinate advances a draft or queued top-level thread. Before the child
// finished it creates the Builder child, takes the parent through T05 without
// any session and parks it in waiting_children (T08, the slot is released with
// it). Once T09 returned the parent to queued it reports the children's results.
func (d *Daemon) coordinate(ctx context.Context, id string) error {
	state, err := d.threads.Status(ctx, id)
	if err != nil {
		return err
	}
	if state.Phase == "draft" {
		if state, err = d.promote(ctx, state); err != nil || state.Phase != "queued" {
			return err
		}
	}
	if len(state.Facts.Awaited) > 0 && d.allSettled(state.Facts.Awaited) {
		return d.conclude(ctx, state)
	}
	route, err := d.routeThread(ctx, state)
	if err != nil {
		return err
	}
	if !route.Eligible() {
		if err := d.threads.Apply(ctx, id, thread.Command{Transition: "T06"}); err != nil {
			return err
		}
		return fmt.Errorf("%w: %s", ErrNoEligiblePair, route.RoutingReason)
	}
	req, err := d.requestFor(ctx, id)
	if err != nil {
		return err
	}
	if req.Workdir == "" {
		return errors.New("coordinator has no project checkout; configure the thread's project")
	}
	// A delegation recorded before a crash (Facts.Awaited) is resumed, never repeated.
	kids := state.Facts.Awaited
	if len(kids) == 0 {
		live, err := d.liveChild(ctx, id)
		if err != nil {
			return err
		}
		if live != "" {
			kids = []string{live}
		}
	}
	if len(kids) == 0 {
		// Grant before delegation (D-43): a child inherits only grants issued before it
		// exists, so without one the parent stays queued until the operator grants.
		mode, err := d.modeFor(ctx, id)
		if err != nil || mode.pending != "" {
			return err
		}
		if mode.active == hostMode {
			if ok, err := d.hasGrant(id); err != nil || !ok {
				return err
			}
		}
		revision := headRevision(ctx, req.Workdir)
		if revision == "" {
			revision = "unversioned"
		}
		brief := "Complete the thread's brief."
		if len(req.Instructions) > 0 {
			brief = req.Instructions[0].Text
		}
		child, err := d.delegate(ctx, state, brief, revision)
		if err != nil {
			return err
		}
		kids = []string{child}
	}
	// The parent passes through running without a session: nothing executes in it.
	noSession := func(context.Context, *thread.Checkpoint) error { return nil }
	if err := d.children.Dispatch(ctx, id, eventlog.NewID(), eventlog.NewID(), noSession); err != nil {
		return err
	}
	if state, err = d.threads.Status(ctx, id); err != nil || state.Phase != "running" {
		return err // a blocked dispatch stays queued and is retried on the next poll
	}
	return d.children.WaitForChildren(ctx, id, kids)
}

// promote takes a draft through T01, or T02 when the plan is reviewed first.
func (d *Daemon) promote(ctx context.Context, state thread.State) (thread.State, error) {
	transition := "T01"
	if state.Facts.ReviewPlanFirst {
		transition = "T02"
	}
	if err := d.threads.Apply(ctx, state.ID, thread.Command{Transition: transition}); err != nil {
		return state, err
	}
	return d.threads.Status(ctx, state.ID)
}

// delegate creates one managed child (Builder, Ops or Security, decision 06) of the parent and records its
// assignment (child.assignment_recorded) and audit setting through
// internal/children. The child gets the brief as its purpose, the parent's
// required checks as expected evidence and a revision to start from.
func (d *Daemon) delegate(ctx context.Context, parent thread.State, brief, revision string) (string, error) {
	evidence := parent.Facts.RequiredChecks
	if len(evidence) == 0 {
		evidence = []string{"project gates"}
	}
	// Decision 06: the specialist follows the required outcome; the reason is recorded with the assignment.
	choice := roles.Choose(brief, evidence)
	a := children.Assignment{
		ParentThreadID: parent.ID, ChildThreadID: eventlog.NewID(), AssignmentID: eventlog.NewID(),
		Purpose: brief, Role: choice.Role, InputRevision: revision,
		RoleReason:          choice.Reason,
		AllowedCapabilities: []string{"assign.dispatch", "host.exec", "tool.use"},
		ExpectedEvidence:    evidence,
	}
	facts := thread.Facts{BriefVersion: 1, AuditSetting: "off", RequiredChecks: parent.Facts.RequiredChecks}
	if err := d.children.CreateChild(ctx, a, facts); err != nil {
		return "", err
	}
	// The delegation is durable from here: recovery resumes it instead of creating a second child.
	if err := d.threads.Update(ctx, parent.ID, func(f *thread.Facts) error { f.Awaited = []string{a.ChildThreadID}; return nil }); err != nil {
		return "", err
	}
	err := d.children.RecordAuditSetting(ctx, children.AuditSetting{ThreadID: a.ChildThreadID, Setting: "off",
		Reason: "The coordinator audits the child's evidence; the independent audit applies to the parent's revision."})
	return a.ChildThreadID, err
}

// liveChild returns the parent's child that has not ended, so a retried
// delegation does not create a second one.
func (d *Daemon) liveChild(ctx context.Context, parent string) (id string, err error) {
	err = d.store.Write(ctx, func(tx *eventlog.Transaction) error {
		err := tx.QueryRowContext(ctx, `SELECT id FROM thread_state
 WHERE json_extract(state,'$.Facts.ManagedParent')=?
 AND json_extract(state,'$.Phase') NOT IN ('completed','failed','cancelled') ORDER BY id LIMIT 1`, parent).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	})
	return id, err
}

// conclude reports finished children to a queued parent: T05 without a
// session, T25, one check.result per child check (a child that did not
// complete is a failed check), then T26 when the audit setting allows.
func (d *Daemon) conclude(ctx context.Context, parent thread.State) error {
	noSession := func(context.Context, *thread.Checkpoint) error { return nil }
	if err := d.children.Dispatch(ctx, parent.ID, eventlog.NewID(), eventlog.NewID(), noSession); err != nil {
		return err
	}
	state, err := d.threads.Status(ctx, parent.ID)
	if err != nil || state.Phase != "running" {
		return err
	}
	var results []children.Result
	for _, child := range state.Facts.Awaited {
		r, err := d.recorder.Result(ctx, child)
		if err != nil {
			return err
		}
		results = append(results, r)
	}
	revision := results[len(results)-1].Revision
	if err := d.threads.Apply(ctx, parent.ID, thread.Command{Transition: "T25"}); err != nil {
		return err
	}
	d.mu.Lock()
	d.verifying[parent.ID] = true
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.verifying, parent.ID)
		d.mu.Unlock()
	}()
	// The children are consumed: a later T29 starts a fresh delegation.
	if err := d.threads.Update(ctx, parent.ID, func(f *thread.Facts) error {
		f.Revision, f.Awaited = revision, nil
		return nil
	}); err != nil {
		return err
	}
	for _, r := range results {
		for _, group := range []string{"passed", "failed", "error", "partial", "not_run"} {
			for _, c := range r.Checks[group] {
				if err := d.record(parent.ID, "check.result", checkPayload(c.Check, group, c.Reason, r.Revision), ""); err != nil {
					return err
				}
			}
		}
		if r.Outcome != "completed" {
			reason := "child " + r.Outcome
			if len(r.OpenRequirements) > 0 {
				reason += ": " + strings.Join(r.OpenRequirements, "; ")
			}
			if err := d.record(parent.ID, "check.result", checkPayload("child:"+r.ChildThreadID, "failed", reason, r.Revision), ""); err != nil {
				return err
			}
		}
	}
	return d.advanceVerified(ctx, parent.ID)
}

// checkPayload builds a check.result payload; a reason is required outside "passed".
func checkPayload(name, status, reason, revision string) map[string]any {
	p := map[string]any{"check": name, "status": status, "revision": revision}
	if status != "passed" {
		if reason == "" {
			reason = status
		}
		p["reason"] = reason
	}
	return p
}

// advanceVerified moves a verifying thread on after its checks were recorded.
// A managed child with a failed check takes T27 (retry within the Q-27 attempt
// limit) or T31; the audit setting can keep any thread waiting for T26.
func (d *Daemon) advanceVerified(ctx context.Context, id string) error {
	state, err := d.threads.Status(ctx, id)
	if err != nil {
		return err
	}
	if state.Facts.ManagedParent != "" {
		for _, status := range state.Checks {
			if status == "failed" {
				return d.children.FailAttempt(ctx, id)
			}
		}
	}
	// A refused T26 means the audit has not settled; settleVerifying retries it.
	if err = d.threads.Apply(ctx, id, thread.Command{Transition: "T26"}); errors.Is(err, thread.ErrTransition) {
		return nil
	}
	return err
}

// --- Children ending: results, coordinator acceptance and the parent's wake-up ---

// settleChildren records results of ended children, lets the coordinator accept
// a child whose checks all passed, and wakes parents whose children all ended (T09).
func (d *Daemon) settleChildren() {
	ctx := d.lifetime
	ended, err := d.threadsIn(ctx, "managed", "completed", "failed", "cancelled")
	if err != nil {
		return
	}
	for _, id := range ended {
		d.mu.Lock()
		done := d.settled[id]
		d.mu.Unlock()
		if done {
			continue
		}
		state, err := d.threads.Status(ctx, id)
		if err == nil {
			err = d.recordChildResult(ctx, id, state.Phase)
		}
		if err == nil || errors.Is(err, children.ErrRecordNotFound) {
			d.mu.Lock()
			d.settled[id] = true
			d.mu.Unlock()
		} else if ctx.Err() == nil {
			slog.Warn("record child result", "thread_id", id, "error", err)
		}
	}
	waiting, _ := d.threadsIn(ctx, "managed", "awaiting_acceptance")
	for _, id := range waiting {
		if err := d.acceptChild(ctx, id); err != nil && ctx.Err() == nil {
			slog.Warn("accept child result", "thread_id", id, "error", err)
		}
	}
	parents, err := d.threadsIn(ctx, "top", "waiting_children")
	if err != nil {
		return
	}
	for _, id := range parents {
		state, err := d.threads.Status(ctx, id)
		if err != nil || !d.allSettled(state.Facts.Awaited) {
			continue
		}
		if _, err := d.children.RequeueParent(ctx, id); err != nil && ctx.Err() == nil {
			slog.Warn("requeue parent", "thread_id", id, "error", err)
		}
	}
}

// allSettled reports whether every child has ended with its result recorded.
func (d *Daemon) allSettled(ids []string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, id := range ids {
		if !d.settled[id] {
			return false
		}
	}
	return true
}

// acceptChild lets the coordinator accept a child's evidence when every check
// passed (T28). Anything else stays awaiting_acceptance for the operator, who
// alone can accept failed or not-run evidence.
func (d *Daemon) acceptChild(ctx context.Context, id string) error {
	state, err := d.threads.Status(ctx, id)
	if err != nil || len(state.Checks) == 0 {
		return err
	}
	for _, status := range state.Checks {
		if status != "passed" {
			return nil
		}
	}
	revision := state.Facts.Revision
	payload, err := json.Marshal(map[string]string{"decision": "accepted", "revision": revision, "operator": "coordinator:" + state.Facts.ManagedParent})
	if err != nil {
		return err
	}
	return d.store.Write(ctx, func(tx *eventlog.Transaction) error {
		if _, err := tx.Append(ctx, eventlog.NewEvent{ThreadID: id, Stream: "thread:" + id, Type: "acceptance.recorded", TypeVersion: 1,
			Actor: eventlog.Actor{Kind: "coordinator", ID: state.Facts.ManagedParent}, Payload: payload}); err != nil {
			return err
		}
		return d.threads.ApplyTx(ctx, tx, id, thread.Command{Transition: "T28", Revision: revision})
	})
}

// recordChildResult persists child.result_recorded from the child's checks. The
// outcome is the child's end phase; extra says why work stopped when no check can.
// A thread without an assignment record is not a coordinated child.
func (d *Daemon) recordChildResult(ctx context.Context, id, outcome string, extra ...string) error {
	a, err := d.recorder.Assignment(ctx, id)
	if err != nil {
		return err
	}
	if _, err := d.recorder.Result(ctx, id); err == nil {
		return nil
	}
	state, err := d.threads.Status(ctx, id)
	if err != nil {
		return err
	}
	checks, err := d.childChecks(id)
	if err != nil {
		return err
	}
	revision := state.Facts.Revision
	if revision == "" {
		revision = a.InputRevision
	}
	r := children.Result{ChildThreadID: id, AssignmentID: a.AssignmentID, Outcome: outcome, Revision: revision,
		Checks: checks, Limitations: []string{}, OpenRequirements: extra}
	if direct, err := d.ranDirect(id); err != nil {
		return err
	} else if direct {
		r.Limitations = append(r.Limitations, DirectLimitation)
	}
	for _, group := range []string{"failed", "error", "partial", "not_run"} {
		for _, c := range checks[group] {
			r.OpenRequirements = append(r.OpenRequirements, c.Check+": "+c.Reason)
		}
	}
	if outcome != "completed" && len(r.OpenRequirements) == 0 {
		r.OpenRequirements = []string{"the child ended " + outcome + " without completing its assignment"}
	}
	return d.children.RecordResult(ctx, r)
}

// childChecks groups the latest check.result of each check on the child by status.
func (d *Daemon) childChecks(id string) (map[string][]children.Check, error) {
	type latest struct{ status, reason string }
	last, order := map[string]latest{}, []string{}
	var cursor int64
	for {
		events, err := d.store.ReadStream("thread:"+id, cursor, 256)
		if err != nil {
			return nil, err
		}
		for _, e := range events {
			cursor = e.Seq
			var p struct{ Check, Status, Reason string }
			if e.Type != "check.result" || json.Unmarshal(e.Payload, &p) != nil || p.Check == "" {
				continue
			}
			if _, seen := last[p.Check]; !seen {
				order = append(order, p.Check)
			}
			last[p.Check] = latest{p.Status, p.Reason}
		}
		if len(events) < 256 {
			break
		}
	}
	groups := map[string][]children.Check{"passed": nil, "failed": nil, "error": nil, "partial": nil, "not_run": nil}
	for _, name := range order {
		l := last[name]
		if _, ok := groups[l.status]; !ok {
			l = latest{"error", "unknown check status " + l.status}
		}
		if l.status != "passed" && l.reason == "" {
			l.reason = l.status
		}
		groups[l.status] = append(groups[l.status], children.Check{Check: name, Reason: l.reason})
	}
	return groups, nil
}

// --- Children that cannot run: end them failed instead of leaving them queued ---

// childDispatchFailed ends a child whose dispatch can never succeed: a failed
// dependency (the machine refuses T05) or a policy denial. It reports whether
// the error was one of those.
func (d *Daemon) childDispatchFailed(ctx context.Context, id string, err error) bool {
	var dep *children.DependencyError
	switch {
	case errors.As(err, &dep):
		err = d.abandon(ctx, id)
	case errors.Is(err, ErrPolicyDenied):
		err = d.failChild(ctx, id, err.Error())
	default:
		return false
	}
	if err != nil && ctx.Err() == nil {
		slog.Error("end blocked child", "thread_id", id, "error", err)
	}
	return true
}

// failChild records a failed result with the reason and ends the child. A child
// already running took T05 and fails by T31; a queued one has no such row, see abandon.
func (d *Daemon) failChild(ctx context.Context, id, reason string) error {
	state, err := d.threads.Status(ctx, id)
	if err != nil {
		return err
	}
	if err := d.recordChildResult(ctx, id, "failed", reason); err != nil {
		return err
	}
	if state.Phase == "running" || state.Phase == "verifying" {
		if err := d.threads.Update(ctx, id, func(f *thread.Facts) error { f.Unrecoverable = true; return nil }); err != nil {
			return err
		}
		return d.children.FailAttempt(ctx, id)
	}
	return d.abandon(ctx, id)
}

// abandon ends a queued child that can never start: T06 (no eligible pair, which
// the table ties to permissions) and then T32 (abandoned). The contract has no
// direct queued-to-failed row and T05 is refused by G.5 for such a child.
func (d *Daemon) abandon(ctx context.Context, id string) error {
	d.mu.Lock()
	d.unrunnable[id] = true
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.unrunnable, id)
		d.mu.Unlock()
	}()
	if err := d.threads.Apply(ctx, id, thread.Command{Transition: "T06"}); err != nil {
		return err
	}
	return d.threads.Apply(ctx, id, thread.Command{Transition: "T32"})
}
