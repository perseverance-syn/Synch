package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"synch/internal/ci"
)

// Config configures the provider.
type Config struct {
	BaseURL  string // for example https://gitlab.example.com/api/v4
	Endpoint string // registered endpoint id, used to ask the broker for the token
	Broker   ci.TokenBroker
	Client   *http.Client
}

// Provider is the GitLab CI implementation of ci.Provider.
type Provider struct{ h ci.HTTP }

// lookupLimit bounds how many recent pipelines FindByToken inspects.
const lookupLimit = 20

// New builds a provider. The token is read from the broker on every call and
// sent only in the PRIVATE-TOKEN header.
func New(c Config) *Provider {
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	return &Provider{h: ci.HTTP{Base: c.BaseURL, Endpoint: c.Endpoint, Broker: c.Broker, Client: client,
		Auth: func(h http.Header, token string) { h.Set("PRIVATE-TOKEN", token) }}}
}

// Name returns the driver name.
func (p *Provider) Name() string { return "gitlab-ci" }

// projectPath returns /projects/<url-encoded path or id>.
func projectPath(project string) (string, error) {
	if project == "" {
		return "", fmt.Errorf("%w: project is required", ci.ErrInvalid)
	}
	return "/projects/" + url.PathEscape(project), nil
}

// numeric validates an ID so it cannot inject path segments.
func numeric(what, v string) error {
	if _, err := strconv.ParseUint(v, 10, 64); err != nil {
		return fmt.Errorf("%w: %s must be numeric", ci.ErrInvalid, what)
	}
	return nil
}

// --- Operations ---

// pipelineJSON is the subset of a pipeline the provider reads.
type pipelineJSON struct {
	ID     int64  `json:"id"`
	SHA    string `json:"sha"`
	Status string `json:"status"`
}

// Trigger creates a pipeline on req.Ref with SYNCH_TOKEN and SYNCH_SHA
// variables. GitLab returns the pipeline, so the RunRef carries its ID.
func (p *Provider) Trigger(ctx context.Context, req ci.TriggerRequest) (ci.RunRef, error) {
	base, err := projectPath(req.Project)
	if err != nil {
		return ci.RunRef{}, err
	}
	body := map[string]any{"ref": req.Ref, "variables": []map[string]string{
		{"key": "SYNCH_TOKEN", "value": req.Token}, {"key": "SYNCH_SHA", "value": req.SHA}}}
	data, err := p.h.Do(ctx, http.MethodPost, base+"/pipeline", body)
	if err != nil {
		return ci.RunRef{}, err
	}
	var pl pipelineJSON
	if err := json.Unmarshal(data, &pl); err != nil || pl.ID == 0 {
		// The pipeline was probably created but unreadable: unknown, so reconcile.
		return ci.RunRef{}, fmt.Errorf("ci/gitlab: unreadable create response: %w", ci.ErrTransient)
	}
	return ci.RunRef{Project: req.Project, ID: strconv.FormatInt(pl.ID, 10)}, nil
}

// FindByToken reads the variables of the most recent pipelines on the ref and
// returns those whose SYNCH_TOKEN equals the token.
func (p *Provider) FindByToken(ctx context.Context, req ci.TriggerRequest) ([]ci.RunRef, error) {
	base, err := projectPath(req.Project)
	if err != nil {
		return nil, err
	}
	q := url.Values{"ref": {req.Ref}, "per_page": {strconv.Itoa(lookupLimit)}}
	data, err := p.h.Do(ctx, http.MethodGet, base+"/pipelines?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	var list []pipelineJSON
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("ci/gitlab: decode pipelines: %w", err)
	}
	var refs []ci.RunRef
	for _, pl := range list {
		id := strconv.FormatInt(pl.ID, 10)
		vd, err := p.h.Do(ctx, http.MethodGet, base+"/pipelines/"+id+"/variables", nil)
		if err != nil {
			return nil, err
		}
		var vars []struct{ Key, Value string }
		if err := json.Unmarshal(vd, &vars); err != nil {
			return nil, fmt.Errorf("ci/gitlab: decode variables: %w", err)
		}
		for _, v := range vars {
			if v.Key == "SYNCH_TOKEN" && v.Value == req.Token {
				refs = append(refs, ci.RunRef{Project: req.Project, ID: id})
			}
		}
	}
	return refs, nil
}

// Status reads the pipeline state and the SHA it used.
func (p *Provider) Status(ctx context.Context, run ci.RunRef) (ci.RunStatus, error) {
	base, err := projectPath(run.Project)
	if err != nil {
		return ci.RunStatus{}, err
	}
	if err := numeric("run id", run.ID); err != nil {
		return ci.RunStatus{}, err
	}
	data, err := p.h.Do(ctx, http.MethodGet, base+"/pipelines/"+run.ID, nil)
	if err != nil {
		return ci.RunStatus{}, err
	}
	var pl pipelineJSON
	if err := json.Unmarshal(data, &pl); err != nil {
		return ci.RunStatus{}, fmt.Errorf("ci/gitlab: decode pipeline: %w", err)
	}
	return ci.RunStatus{SHA: pl.SHA, Verdict: pipelineVerdict(pl.Status), Raw: pl.Status}, nil
}

// Jobs lists the jobs of a pipeline (first 100) with verdicts and SHA.
func (p *Provider) Jobs(ctx context.Context, run ci.RunRef) ([]ci.Job, error) {
	base, err := projectPath(run.Project)
	if err != nil {
		return nil, err
	}
	if err := numeric("run id", run.ID); err != nil {
		return nil, err
	}
	data, err := p.h.Do(ctx, http.MethodGet, base+"/pipelines/"+run.ID+"/jobs?per_page=100", nil)
	if err != nil {
		return nil, err
	}
	var list []struct {
		ID            int64  `json:"id"`
		Name          string `json:"name"`
		Status        string `json:"status"`
		FailureReason string `json:"failure_reason"`
		Pipeline      struct {
			SHA string `json:"sha"`
		} `json:"pipeline"`
	}
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, fmt.Errorf("ci/gitlab: decode jobs: %w", err)
	}
	jobs := make([]ci.Job, 0, len(list))
	for _, j := range list {
		v, raw := jobVerdict(j.Status, j.FailureReason)
		jobs = append(jobs, ci.Job{ID: strconv.FormatInt(j.ID, 10), Name: j.Name, SHA: j.Pipeline.SHA, Verdict: v, Raw: raw})
	}
	return jobs, nil
}

// Logs returns the trace of one job.
func (p *Provider) Logs(ctx context.Context, run ci.RunRef, jobID string) ([]byte, error) {
	base, err := projectPath(run.Project)
	if err != nil {
		return nil, err
	}
	if err := numeric("job id", jobID); err != nil {
		return nil, err
	}
	return p.h.Do(ctx, http.MethodGet, base+"/jobs/"+jobID+"/trace", nil)
}

// Cancel asks GitLab to cancel the pipeline jobs.
func (p *Provider) Cancel(ctx context.Context, run ci.RunRef) error {
	base, err := projectPath(run.Project)
	if err != nil {
		return err
	}
	if err := numeric("run id", run.ID); err != nil {
		return err
	}
	_, err = p.h.Do(ctx, http.MethodPost, base+"/pipelines/"+run.ID+"/cancel", nil)
	return err
}

// --- Status mapping (ci-driver.md §4) ---

// jobVerdict maps a GitLab job status and failure_reason to a verdict.
// A failed job with a timeout failure_reason is timed out, not failed.
func jobVerdict(status, reason string) (ci.Verdict, string) {
	raw := status
	if reason != "" {
		raw = status + "/" + reason
	}
	switch status {
	case "success":
		return ci.VerdictPassed, raw
	case "failed":
		if reason == "job_execution_timeout" || reason == "stuck_or_timeout_failure" {
			return ci.VerdictTimedOut, raw
		}
		return ci.VerdictFailed, raw
	case "canceled":
		return ci.VerdictCancelled, raw
	case "skipped", "manual", "created":
		return ci.VerdictSkipped, raw
	case "pending", "running", "preparing", "waiting_for_resource", "scheduled":
		return ci.VerdictPending, raw
	default:
		return ci.VerdictOther, raw
	}
}

// pipelineVerdict maps a GitLab pipeline status to a verdict.
func pipelineVerdict(status string) ci.Verdict {
	switch status {
	case "success":
		return ci.VerdictPassed
	case "failed":
		return ci.VerdictFailed
	case "canceled":
		return ci.VerdictCancelled
	case "skipped":
		return ci.VerdictSkipped
	case "created", "waiting_for_resource", "preparing", "pending", "running", "scheduled", "manual":
		return ci.VerdictPending
	default:
		return ci.VerdictOther
	}
}
