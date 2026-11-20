package jenkins

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"synch/internal/ci"
)

// --- Build and stage evidence (ci-driver.md sections 4 and 5) ---

// Status reads the queue state or the build's result and observed checkout SHA.
func (p *Provider) Status(ctx context.Context, run ci.RunRef) (ci.RunStatus, error) {
	job, id, pending, err := p.resolve(ctx, run)
	if err != nil {
		return ci.RunStatus{}, err
	}
	if pending != nil {
		if pending.Cancelled {
			return ci.RunStatus{Verdict: ci.VerdictCancelled, Raw: "ABORTED"}, nil
		}
		return ci.RunStatus{Verdict: ci.VerdictPending, Raw: "QUEUED"}, nil
	}
	return p.buildStatus(ctx, job+"/"+id)
}

// buildStatus binds to Git plugin checkout evidence; a request parameter alone
// only describes intent and cannot authorize a pass.
func (p *Provider) buildStatus(ctx context.Context, path string) (ci.RunStatus, error) {
	var b build
	if err := p.getJSON(ctx, apiTree(path, "building,result,actions[lastBuiltRevision[SHA1]]"), &b); err != nil {
		return ci.RunStatus{}, err
	}
	sha, err := observedSHA(b.Actions)
	if err != nil {
		return ci.RunStatus{}, err
	}
	raw := b.Result
	if b.Building {
		raw = "IN_PROGRESS"
	}
	v := verdict(raw)
	if v == ci.VerdictPassed && sha == "" {
		return ci.RunStatus{}, fmt.Errorf("%w: Jenkins build has no checkout evidence", ci.ErrTransient)
	}
	return ci.RunStatus{SHA: ci.Redact(sha, p.secrets()), Verdict: v, Raw: ci.Redact(raw, p.secrets())}, nil
}

// observedSHA rejects ambiguous checkouts and invalid revision evidence.
func observedSHA(actions []action) (string, error) {
	var sha string
	for _, a := range actions {
		if a.LastBuiltRevision == nil {
			continue
		}
		value := a.LastBuiltRevision.SHA1
		if !commitID.MatchString(value) || (sha != "" && sha != value) {
			return "", fmt.Errorf("%w: ambiguous or invalid Jenkins checkout evidence", ci.ErrTransient)
		}
		sha = value
	}
	return sha, nil
}

// Jobs maps Pipeline REST API stages to the check IDs declared in the gate file.
func (p *Provider) Jobs(ctx context.Context, run ci.RunRef) ([]ci.Job, error) {
	job, id, pending, err := p.resolve(ctx, run)
	if err != nil {
		return nil, err
	}
	if pending != nil {
		if pending.Cancelled {
			return nil, fmt.Errorf("%w: Jenkins queue item cancelled", ci.ErrRejected)
		}
		return nil, nil
	}
	path := job + "/" + id
	state, err := p.buildStatus(ctx, path)
	if err != nil {
		return nil, err
	}
	if state.Verdict == ci.VerdictOther {
		return nil, fmt.Errorf("%w: unrecognised Jenkins build state", ci.ErrTransient)
	}
	var pipeline struct {
		Stages []struct{ ID, Name, Status string }
	}
	if err := p.getJSON(ctx, path+"/wfapi/describe", &pipeline); err != nil {
		return nil, err
	}
	if pipeline.Stages == nil || (len(pipeline.Stages) == 0 && state.Verdict != ci.VerdictPending) {
		return nil, fmt.Errorf("%w: Jenkins build has no stage evidence", ci.ErrTransient)
	}
	secrets := p.secrets()
	jobs := make([]ci.Job, 0, len(pipeline.Stages))
	for _, stage := range pipeline.Stages {
		if !numberID.MatchString(stage.ID) || stage.Name == "" || stage.Status == "" ||
			ci.Redact(stage.ID, secrets) != stage.ID {
			return nil, fmt.Errorf("%w: malformed Jenkins stage evidence", ci.ErrTransient)
		}
		v := verdict(stage.Status)
		// A race between Status and Jobs must never authorize a stale pass.
		if v == ci.VerdictPassed && (state.Verdict == ci.VerdictCancelled || state.Verdict == ci.VerdictTimedOut || state.Verdict == ci.VerdictPending || state.Verdict == ci.VerdictSkipped) {
			v = state.Verdict
		}
		jobs = append(jobs, ci.Job{ID: stage.ID, Name: ci.Redact(stage.Name, secrets), SHA: state.SHA, Verdict: v, Raw: ci.Redact(stage.Status, secrets)})
	}
	return jobs, nil
}

// verdict normalizes Jenkins build results and Pipeline stage states.
func verdict(raw string) ci.Verdict {
	switch raw {
	case "SUCCESS":
		return ci.VerdictPassed
	case "FAILURE", "FAILED", "UNSTABLE":
		return ci.VerdictFailed
	case "ABORTED":
		return ci.VerdictCancelled
	case "TIMED_OUT", "TIMEOUT":
		return ci.VerdictTimedOut
	case "NOT_BUILT", "NOT_EXECUTED":
		return ci.VerdictSkipped
	case "IN_PROGRESS", "PAUSED_PENDING_INPUT", "QUEUED":
		return ci.VerdictPending
	default:
		return ci.VerdictOther
	}
}

// --- Cancellation (ci-driver.md section 9) ---

// Cancel stops an executable build or removes its still-queued item.
func (p *Provider) Cancel(ctx context.Context, run ci.RunRef) error {
	job, id, pending, err := p.resolve(ctx, run)
	if err != nil {
		return err
	}
	path := job + "/" + id + "/stop"
	form := url.Values{}
	if pending != nil {
		if pending.Cancelled {
			return nil
		}
		path = "/queue/cancelItem"
		form.Set("id", id)
	}
	resp, err := p.request(ctx, http.MethodPost, path, form)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusFound && resp.StatusCode != http.StatusSeeOther {
		return responseError(resp.StatusCode)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return nil
}
