package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"synch/internal/execdriver"
	"synch/internal/gitops"
	"synch/internal/policy"
	"synch/internal/supervisor"
)

type policyState struct{ daemon *Daemon }

// Thread reads policy facts from the current durable thread projection.
func (s policyState) Thread(id string) (policy.ThreadInfo, error) {
	state, err := s.daemon.threads.Status(s.daemon.lifetime, id)
	if err != nil {
		return policy.ThreadInfo{}, err
	}
	terminal := state.Phase == "completed" || state.Phase == "failed" || state.Phase == "abandoned" || state.Phase == "cancelled"
	// Every top-level thread is a coordinator whatever it has been assigned (dec. 05);
	// only its managed children run engines and commands, as builders.
	role := "coordinator"
	if state.Facts.ManagedParent != "" {
		role = "builder"
	}
	info := policy.ThreadInfo{ID: id, Parent: state.Facts.ManagedParent, Role: role, Managed: true, Terminal: terminal}
	if info.Parent != "" {
		// A child inherits only grants issued before it was created (Q-21).
		info.CreatedAt = s.daemon.createdAt(id)
	}
	return info, nil
}

// createdAt reads when a managed child was created from its thread.created record.
func (d *Daemon) createdAt(id string) time.Time {
	d.mu.Lock()
	at, ok := d.created[id]
	d.mu.Unlock()
	if ok {
		return at
	}
	events, err := d.store.ReadStream("thread:"+id, 0, 1<<20)
	if err != nil {
		return time.Time{}
	}
	for _, e := range events {
		if e.Type == "thread.created" {
			d.mu.Lock()
			d.created[id] = e.RecordedAt
			d.mu.Unlock()
			return e.RecordedAt
		}
	}
	return time.Time{}
}

// AccessMode identifies this daemon's local Host access target.
func (s policyState) AccessMode(id, _ string) (string, error) {
	state, err := s.daemon.threads.Status(s.daemon.lifetime, id)
	if state.Facts.SwitchToSandbox || state.Facts.SandboxActive {
		return "sandbox", err
	}
	return "full_control", err
}

// Policy supplies the trusted operator policy loaded at startup.
func (s policyState) Policy(string) ([]*policy.File, error) { return s.daemon.rules, nil }

// Current supplies the measured worktree from the Git decision in progress.
// The decider cannot re-enter Git while that operation holds its lock.
func (s policyState) Current(id string) (policy.Current, error) {
	s.daemon.evidenceMu.RLock()
	current, ok := s.daemon.evidence[id]
	s.daemon.evidenceMu.RUnlock()
	if !ok {
		return policy.Current{}, errors.New("live worktree evidence is unavailable")
	}
	state, err := s.daemon.threads.Status(s.daemon.lifetime, id)
	current.PlanDigest = state.Facts.PlanDigest
	return current, err
}

// OwnsVM returns false because this daemon only wires Direct Host access.
func (policyState) OwnsVM(string, string) (bool, error) { return false, nil }

// MergeAccepted requires operator acceptance of the revision measured by Git.
func (s policyState) MergeAccepted(id, revision string) (bool, error) {
	state, err := s.daemon.threads.Status(s.daemon.lifetime, id)
	return revision != "" && state.AcceptedRevision == revision, err
}

// Settle closes native admission and waits for each affected thread's processes.
func (s policyState) Settle(ctx context.Context, _ string, threads []string) error {
	for _, id := range threads {
		report, err := s.daemon.driver.Settle(ctx, id)
		if err != nil {
			return err
		}
		if !report.HostProcessesStopped {
			return errors.New(report.Reason)
		}
	}
	return nil
}

type approvalKey struct{}

// decideGit checks a Git request without re-entering the Git service.
func (d *Daemon) decideGit(ctx context.Context, request gitops.Request) error {
	if !request.Mediated && request.ThreadID == "" && request.Action == gitops.Status {
		// Project registration is a local operator read outside a thread.
		return nil
	}
	approval, _ := ctx.Value(approvalKey{}).(map[gitops.Action]string)
	d.decisionMu.Lock()
	defer d.decisionMu.Unlock()
	if request.Revision != "" {
		d.evidenceMu.Lock()
		d.evidence[request.ThreadID] = policy.Current{Head: request.Revision, WorktreeDigest: "sha256:" + request.WorktreeDigest}
		d.evidenceMu.Unlock()
		defer func() {
			d.evidenceMu.Lock()
			delete(d.evidence, request.ThreadID)
			d.evidenceMu.Unlock()
		}()
	}
	result, err := d.policy.Decide(ctx, policy.Request{
		Thread: request.ThreadID, Action: string(request.Action), Target: request.Target,
		Machine: d.driver.Describe().Host, Paths: []string{request.Path}, ApprovalID: approval[request.Action],
	})
	if err == nil && !result.Allowed {
		return errors.New(result.Reason)
	}
	return err
}

type grantProof struct{}
type grantProofKey struct{}
type hostGrant struct{ thread, host, identity string }

// HasFullControl accepts proof only inside the synchronous policy dispatch boundary.
func (grantProof) HasFullControl(ctx context.Context, thread, host, identity string) (bool, error) {
	proof, ok := ctx.Value(grantProofKey{}).(hostGrant)
	return ok && proof == (hostGrant{thread, host, identity}), nil
}

type policyDriver struct {
	execdriver.ExecutionDriver
	daemon *Daemon
}

// Spawn keeps Direct registration inside the authority check's revocation boundary.
func (d policyDriver) Spawn(ctx context.Context, spec execdriver.ProcSpec) (execdriver.Proc, error) {
	d.daemon.decisionMu.Lock()
	defer d.daemon.decisionMu.Unlock()
	host := d.Describe()
	var proc execdriver.Proc
	// Launching and recording the run completes before revocation can settle it.
	result, err := d.daemon.policy.Decide(ctx, policy.Request{
		Thread: spec.ThreadID, Action: "host.exec", Machine: host.Host, Identity: host.Identity,
		Target: host.Host, Paths: []string{spec.Dir}, Args: spec.Args,
		Start: func() error {
			authorized := context.WithValue(ctx, grantProofKey{}, hostGrant{spec.ThreadID, host.Host, host.Identity})
			started, err := d.ExecutionDriver.Spawn(authorized, spec)
			if err != nil {
				return err
			}
			proc = started
			recorder, ok := proc.(interface{ Record() supervisor.RunRecord })
			if !ok {
				return errors.Join(errors.New("missing recovery identity for Direct process"), proc.Cancel(context.WithoutCancel(ctx)))
			}
			if err := d.daemon.saveRun(context.WithoutCancel(ctx), recorder.Record()); err != nil {
				return errors.Join(err, proc.Cancel(context.WithoutCancel(ctx)))
			}
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	if !result.Allowed {
		return nil, fmt.Errorf("authorize Host access: %w: %s", ErrPolicyDenied, result.Reason)
	}
	return proc, nil
}

// --- Grant before delegation (D-43): read the operator's grants, never issue them ---

// hasGrant reports whether the thread holds an active Full Control grant for this
// host and identity: a grant.issued on its stream that no grant.revoked names. The
// daemon only reads the log here; grants come from the operator's `synch grants create`.
func (d *Daemon) hasGrant(id string) (bool, error) {
	host := d.driver.Describe()
	issued, revoked := map[string]bool{}, map[string]bool{}
	var cursor int64
	for {
		events, err := d.store.ReadStream("thread:"+id, cursor, 256)
		if err != nil {
			return false, err
		}
		for _, e := range events {
			cursor = e.Seq
			var p struct {
				ID       string `json:"id"`
				GrantID  string `json:"grant_id"`
				Host     string `json:"host"`
				Identity string `json:"identity"`
			}
			if json.Unmarshal(e.Payload, &p) != nil {
				continue
			}
			switch e.Type {
			case "grant.issued":
				if p.Host == host.Host && p.Identity == host.Identity {
					issued[p.ID] = true
				}
			case "grant.revoked":
				revoked[p.GrantID] = true
			}
		}
		if len(events) < 256 {
			break
		}
	}
	for grant := range issued {
		if !revoked[grant] {
			return true, nil
		}
	}
	return false, nil
}

// waitingFor is the API's derived status detail for a queued thread: a coordinator
// that has not delegated yet and holds no grant waits for the operator, and the text
// names the exact command to run. It returns "" when nothing is awaited.
func (d *Daemon) waitingFor(ctx context.Context, id string) string {
	state, err := d.threads.Status(ctx, id)
	if err != nil || state.Facts.ManagedParent != "" || len(state.Facts.Awaited) > 0 || d.driver == nil {
		return ""
	}
	if ok, err := d.hasGrant(id); err != nil || ok {
		return ""
	}
	host := d.driver.Describe()
	return fmt.Sprintf("waiting for an operator grant: run `synch grants create %s --host %s --identity %s`", id, host.Host, host.Identity)
}
