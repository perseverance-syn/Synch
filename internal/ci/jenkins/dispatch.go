package jenkins

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"synch/internal/ci"
)

var (
	commitID    = regexp.MustCompile("^([0-9a-f]{40}|[0-9a-f]{64})$")
	projectPart = regexp.MustCompile("^[A-Za-z0-9_.%-]+$")
	numberID    = regexp.MustCompile("^[1-9][0-9]*$")
)

// jobPath expands a folder-qualified project into Jenkins job path components.
func jobPath(project string) (string, error) {
	var path strings.Builder
	for _, part := range strings.Split(project, "/") {
		if !projectPart.MatchString(part) || part == "." || part == ".." {
			return "", fmt.Errorf("%w: invalid Jenkins job path", ci.ErrInvalid)
		}
		path.WriteString("/job/")
		path.WriteString(url.PathEscape(part))
	}
	return path.String(), nil
}

// apiTree constrains the fields and depth Jenkins serializes.
func apiTree(path, tree string) string {
	return path + "/api/json?tree=" + url.QueryEscape(tree)
}

// --- Trigger and queue references (ci-driver.md §9) ---

// Trigger queues one parameterized Pipeline; ci.Dispatcher owns persistence and retries.
func (p *Provider) Trigger(ctx context.Context, req ci.TriggerRequest) (ci.RunRef, error) {
	path, err := jobPath(req.Project)
	if err != nil {
		return ci.RunRef{}, err
	}
	if !commitID.MatchString(req.SHA) || req.Ref == "" || req.Token == "" {
		return ci.RunRef{}, fmt.Errorf("%w: Jenkins trigger requires ref, full SHA and token", ci.ErrInvalid)
	}
	form := url.Values{"SYNCH_REF": {req.Ref}, "SYNCH_SHA": {req.SHA}, "SYNCH_TOKEN": {req.Token}}
	resp, err := p.request(ctx, http.MethodPost, path+"/buildWithParameters", form)
	if err != nil {
		return ci.RunRef{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		return ci.RunRef{}, responseError(resp.StatusCode)
	}
	id := p.queueLocation(resp.Header.Get("Location"))
	if id == "" {
		return ci.RunRef{}, fmt.Errorf("%w: Jenkins did not identify the queued build", ci.ErrTransient)
	}
	return ci.RunRef{Project: req.Project, ID: "queue:" + id}, nil
}

// controllerPath accepts same-origin URLs and extracts a path inside the controller.
func (p *Provider) controllerPath(location string) string {
	base, err := url.Parse(p.config.BaseURL)
	if err != nil {
		return ""
	}
	target, err := url.Parse(location)
	if err != nil || target.User != nil || target.RawQuery != "" || target.Fragment != "" {
		return ""
	}
	if !target.IsAbs() && target.Host == "" && !strings.HasPrefix(target.Path, "/") {
		return ""
	}
	target = base.ResolveReference(target)
	if target.Scheme != base.Scheme || !strings.EqualFold(target.Host, base.Host) {
		return ""
	}
	prefix := strings.TrimRight(base.EscapedPath(), "/")
	path, ok := strings.CutPrefix(strings.TrimRight(target.EscapedPath(), "/"), prefix)
	if !ok || !strings.HasPrefix(path, "/") {
		return ""
	}
	return path
}

// queueLocation extracts a numeric ID without ever following a Location URL.
func (p *Provider) queueLocation(location string) string {
	id, ok := strings.CutPrefix(p.controllerPath(location), "/queue/item/")
	if ok && numberID.MatchString(id) {
		return id
	}
	return ""
}

// --- Recovery from queue and build evidence (ci-driver.md §3) ---

// action is the sparse subset of Jenkins action objects used for binding.
type action struct {
	Parameters []struct {
		Name  string
		Value any
	}
	LastBuiltRevision *struct{ SHA1 string }
}

type build struct {
	Number   int64
	QueueID  int64
	Building bool
	Result   string
	Actions  []action
}

type queueItem struct {
	ID         int64
	Cancelled  bool
	Task       struct{ URL string }
	Executable struct{ Number int64 }
	Actions    []action
}

const (
	queueFields = "id,cancelled,task[url],executable[number],actions[parameters[name,value]]"
	buildSearch = "builds[number,queueId,actions[parameters[name,value]]]{0,100}"
)

// hasToken checks the actual Jenkins build parameter, ignoring unrelated action fields.
func hasToken(actions []action, token string) bool {
	for _, a := range actions {
		for _, param := range a.Parameters {
			if value, ok := param.Value.(string); ok && param.Name == "SYNCH_TOKEN" && value == token {
				return true
			}
		}
	}
	return false
}

// recentBuilds returns at most the first 100 job builds for bounded recovery.
func (p *Provider) recentBuilds(ctx context.Context, job string) ([]build, error) {
	var out struct{ Builds []build }
	err := p.getJSON(ctx, apiTree(job, buildSearch), &out)
	return out.Builds[:min(len(out.Builds), 100)], err
}

// FindByToken reconciles a lost response from queue and build parameter evidence.
func (p *Provider) FindByToken(ctx context.Context, req ci.TriggerRequest) ([]ci.RunRef, error) {
	job, err := jobPath(req.Project)
	if err != nil {
		return nil, err
	}
	if req.Token == "" {
		return nil, fmt.Errorf("%w: missing reconciliation token", ci.ErrInvalid)
	}
	var queue struct{ Items []queueItem }
	if err := p.getJSON(ctx, apiTree("/queue", "items["+queueFields+"]"), &queue); err != nil {
		return nil, err
	}
	builds, err := p.recentBuilds(ctx, job)
	if err != nil {
		return nil, err
	}
	return p.tokenMatches(req, job, queue.Items, builds), nil
}

// tokenMatches deduplicates a build seen in both scans across queue-to-build transitions.
func (p *Provider) tokenMatches(req ci.TriggerRequest, job string, queue []queueItem, builds []build) []ci.RunRef {
	var refs []ci.RunRef
	queued := make(map[int64]bool)
	executable := make(map[int64]bool)
	for _, item := range queue {
		if item.ID > 0 && p.controllerPath(item.Task.URL) == job && hasToken(item.Actions, req.Token) && !queued[item.ID] {
			queued[item.ID] = true
			executable[item.Executable.Number] = item.Executable.Number > 0
			refs = append(refs, ci.RunRef{Project: req.Project, ID: "queue:" + strconv.FormatInt(item.ID, 10)})
		}
	}
	for _, b := range builds {
		if b.Number > 0 && !queued[b.QueueID] && !executable[b.Number] && hasToken(b.Actions, req.Token) {
			executable[b.Number] = true
			refs = append(refs, ci.RunRef{Project: req.Project, ID: "build:" + strconv.FormatInt(b.Number, 10)})
		}
	}
	return refs
}

// --- Resolve a durable run reference (ci-driver.md §9) ---

// resolve translates a stable run reference into a build or current queue item.
func (p *Provider) resolve(ctx context.Context, run ci.RunRef) (job, id string, pending *queueItem, err error) {
	job, err = jobPath(run.Project)
	if err != nil {
		return "", "", nil, err
	}
	kind, id, _ := strings.Cut(run.ID, ":")
	n, parseErr := strconv.ParseInt(id, 10, 64)
	if parseErr != nil || n <= 0 || !numberID.MatchString(id) || (kind != "build" && kind != "queue") {
		return "", "", nil, fmt.Errorf("%w: invalid Jenkins run reference", ci.ErrInvalid)
	}
	if kind == "build" {
		return job, id, nil, nil
	}
	var item queueItem
	err = p.getJSON(ctx, apiTree("/queue/item/"+id, queueFields), &item)
	if err == nil {
		if item.ID != n || (item.Task.URL != "" && p.controllerPath(item.Task.URL) != job) {
			return "", "", nil, fmt.Errorf("%w: inconsistent Jenkins queue item", ci.ErrTransient)
		}
		if item.Executable.Number > 0 && !item.Cancelled {
			return job, strconv.FormatInt(item.Executable.Number, 10), nil, nil
		}
		return job, id, &item, nil
	}
	if failure, ok := errors.AsType[httpFailure](err); ok && failure.code == http.StatusNotFound {
		return p.expiredQueue(ctx, job, n)
	}
	return "", "", nil, err
}

// expiredQueue searches the job's builds when Jenkins has evicted the queue item.
func (p *Provider) expiredQueue(ctx context.Context, job string, id int64) (string, string, *queueItem, error) {
	builds, err := p.recentBuilds(ctx, job)
	if err != nil {
		return "", "", nil, err
	}
	for _, b := range builds {
		if b.QueueID == id && b.Number > 0 {
			return job, strconv.FormatInt(b.Number, 10), nil, nil
		}
	}
	return "", "", nil, fmt.Errorf("%w: Jenkins queue outcome unknown", ci.ErrTransient)
}
