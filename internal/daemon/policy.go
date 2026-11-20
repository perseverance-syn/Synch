package daemon

import (
	"context"
	"errors"
	"fmt"

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
	role := "coordinator"
	if state.Facts.ManagedParent != "" {
		role = "builder"
	}
	return policy.ThreadInfo{ID: id, Parent: state.Facts.ManagedParent, Role: role, Managed: true, Terminal: terminal}, nil
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
		return nil, fmt.Errorf("authorize Host access: %s", result.Reason)
	}
	return proc, nil
}
