package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"

	"synch/internal/ci"
)

// Config configures the provider.
type Config struct {
	BaseURL  string // for example https://api.github.com
	Endpoint string // registered endpoint id, used to ask the broker for the token
	Broker   ci.TokenBroker
	Client   *http.Client
}

// Provider is the GitHub Actions implementation of ci.Provider.
type Provider struct{ h ci.HTTP }

// projectPattern limits a project to owner/repo so it cannot inject path segments.
var projectPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)

// New builds a provider. The token is read from the broker on every call and
// sent only in the Authorization header.
func New(c Config) *Provider {
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	return &Provider{h: ci.HTTP{Base: c.BaseURL, Endpoint: c.Endpoint, Broker: c.Broker, Client: client,
		Auth: func(h http.Header, token string) {
			h.Set("Authorization", "Bearer "+token)
			h.Set("Accept", "application/vnd.github+json")
			h.Set("X-GitHub-Api-Version", "2022-11-28")
		}}}
}

// Name returns the driver name.
func (p *Provider) Name() string { return "github-actions" }

// repoPath returns /repos/owner/repo after validating the project.
func repoPath(project string) (string, error) {
	if !projectPattern.MatchString(project) {
		return "", fmt.Errorf("%w: project must be owner/repo", ci.ErrInvalid)
	}
	return "/repos/" + project, nil
}

// --- Operations ---

// Trigger dispatches the workflow on req.Ref with the token and sha as inputs.
// GitHub answers 204 without a run, so the returned RunRef has no ID.
func (p *Provider) Trigger(ctx context.Context, req ci.TriggerRequest) (ci.RunRef, error) {
	base, err := repoPath(req.Project)
	if err != nil {
		return ci.RunRef{}, err
	}
	if req.Workflow == "" {
		return ci.RunRef{}, fmt.Errorf("%w: workflow file is required", ci.ErrInvalid)
	}
	body := map[string]any{"ref": req.Ref, "inputs": map[string]string{"synch_token": req.Token, "sha": req.SHA}}
	if _, err := p.h.Do(ctx, http.MethodPost, base+"/actions/workflows/"+url.PathEscape(req.Workflow)+"/dispatches", body); err != nil {
		return ci.RunRef{}, err
	}
	return ci.RunRef{Project: req.Project}, nil
}

// runJSON is the subset of a workflow run the provider reads.
type runJSON struct {
	ID           int64  `json:"id"`
	Name         string `json:"name"`
	DisplayTitle string `json:"display_title"`
	HeadSHA      string `json:"head_sha"`
	Status       string `json:"status"`
	Conclusion   string `json:"conclusion"`
}

// FindByToken lists recent workflow_dispatch runs on the ref and returns those
// whose run-name equals the token. Only the first 100 runs are inspected.
func (p *Provider) FindByToken(ctx context.Context, req ci.TriggerRequest) ([]ci.RunRef, error) {
	base, err := repoPath(req.Project)
	if err != nil {
		return nil, err
	}
	q := url.Values{"event": {"workflow_dispatch"}, "branch": {req.Ref}, "per_page": {"100"}}
	data, err := p.h.Do(ctx, http.MethodGet, base+"/actions/runs?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Runs []runJSON `json:"workflow_runs"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("ci/github: decode runs: %w", err)
	}
	var refs []ci.RunRef
	for _, r := range out.Runs {
		if r.Name == req.Token || r.DisplayTitle == req.Token {
			refs = append(refs, ci.RunRef{Project: req.Project, ID: strconv.FormatInt(r.ID, 10)})
		}
	}
	return refs, nil
}

// runID validates that a run ID is numeric so it cannot inject path segments.
func runID(run ci.RunRef) (string, error) {
	if _, err := strconv.ParseUint(run.ID, 10, 64); err != nil {
		return "", fmt.Errorf("%w: run id must be numeric", ci.ErrInvalid)
	}
	return run.ID, nil
}

// Status reads the run state and its head_sha.
func (p *Provider) Status(ctx context.Context, run ci.RunRef) (ci.RunStatus, error) {
	base, err := repoPath(run.Project)
	if err != nil {
		return ci.RunStatus{}, err
	}
	id, err := runID(run)
	if err != nil {
		return ci.RunStatus{}, err
	}
	data, err := p.h.Do(ctx, http.MethodGet, base+"/actions/runs/"+id, nil)
	if err != nil {
		return ci.RunStatus{}, err
	}
	var r runJSON
	if err := json.Unmarshal(data, &r); err != nil {
		return ci.RunStatus{}, fmt.Errorf("ci/github: decode run: %w", err)
	}
	v, raw := verdict(r.Status, r.Conclusion)
	return ci.RunStatus{SHA: r.HeadSHA, Verdict: v, Raw: raw}, nil
}

// Jobs lists the jobs of a run (first 100) with verdicts and head_sha.
func (p *Provider) Jobs(ctx context.Context, run ci.RunRef) ([]ci.Job, error) {
	base, err := repoPath(run.Project)
	if err != nil {
		return nil, err
	}
	id, err := runID(run)
	if err != nil {
		return nil, err
	}
	data, err := p.h.Do(ctx, http.MethodGet, base+"/actions/runs/"+id+"/jobs?per_page=100", nil)
	if err != nil {
		return nil, err
	}
	var out struct {
		Jobs []struct {
			ID         int64  `json:"id"`
			Name       string `json:"name"`
			HeadSHA    string `json:"head_sha"`
			Status     string `json:"status"`
			Conclusion string `json:"conclusion"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("ci/github: decode jobs: %w", err)
	}
	jobs := make([]ci.Job, 0, len(out.Jobs))
	for _, j := range out.Jobs {
		v, raw := verdict(j.Status, j.Conclusion)
		jobs = append(jobs, ci.Job{ID: strconv.FormatInt(j.ID, 10), Name: j.Name, SHA: j.HeadSHA, Verdict: v, Raw: raw})
	}
	return jobs, nil
}

// Logs downloads one job log (GitHub redirects to the log storage; the client
// follows and drops the Authorization header across hosts).
func (p *Provider) Logs(ctx context.Context, run ci.RunRef, jobID string) ([]byte, error) {
	base, err := repoPath(run.Project)
	if err != nil {
		return nil, err
	}
	if _, err := strconv.ParseUint(jobID, 10, 64); err != nil {
		return nil, fmt.Errorf("%w: job id must be numeric", ci.ErrInvalid)
	}
	return p.h.Do(ctx, http.MethodGet, base+"/actions/jobs/"+jobID+"/logs", nil)
}

// Cancel asks GitHub to cancel the run (202 accepted).
func (p *Provider) Cancel(ctx context.Context, run ci.RunRef) error {
	base, err := repoPath(run.Project)
	if err != nil {
		return err
	}
	id, err := runID(run)
	if err != nil {
		return err
	}
	_, err = p.h.Do(ctx, http.MethodPost, base+"/actions/runs/"+id+"/cancel", nil)
	return err
}

// verdict maps GitHub status and conclusion to a neutral verdict (ci-driver.md §4).
// Any status other than completed is pending; unknown conclusions are other.
func verdict(status, conclusion string) (ci.Verdict, string) {
	raw := status + "/" + conclusion
	if status != "completed" {
		return ci.VerdictPending, raw
	}
	switch conclusion {
	case "success":
		return ci.VerdictPassed, raw
	case "failure":
		return ci.VerdictFailed, raw
	case "cancelled":
		return ci.VerdictCancelled, raw
	case "timed_out":
		return ci.VerdictTimedOut, raw
	case "skipped":
		return ci.VerdictSkipped, raw
	default:
		return ci.VerdictOther, raw
	}
}
