package ci

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// --- Decision 18 dispatcher: persist, dispatch, reconcile, never blindly repeat (ci-driver.md §3) ---

// Outcome is the settled result of a trigger request.
type Outcome string

// Via names the evidence that settled a request.
type Via string

// Outcomes and evidence sources, matching the external_request.resolved vocabulary.
const (
	Succeeded       Outcome = "succeeded"
	NotExecuted     Outcome = "not_executed"
	Unknown         Outcome = "unknown"
	ViaResponse     Via     = "response"
	ViaResultingRun Via     = "resulting_resource"
	ViaNone         Via     = "none"
)

// OperationTrigger is the operation name of a trigger request record.
const OperationTrigger = "trigger"

// reconcileDeadline bounds the token lookup that settles a lost response.
const reconcileDeadline = 30 * time.Second

// Record is the external_request.persisted payload for a trigger.
type Record struct {
	ID, Driver, Endpoint, Operation, Resource, ParamsDigest string
}

// Resolution is the external_request.resolved payload for a trigger.
type Resolution struct {
	ID      string
	Outcome Outcome
	Via     Via
	TaskID  string
}

// Journal is the durable boundary of the daemon for external requests. The
// catalogue gap for operation trigger is recorded in ci-driver.md §3.5.
type Journal interface {
	Persist(ctx context.Context, r Record) error
	BindTask(ctx context.Context, id, endpoint, taskID string) error
	Resolve(ctx context.Context, r Resolution) error
}

// Dispatcher runs trigger requests under decision 18.
type Dispatcher struct {
	Provider Provider
	Journal  Journal
	Endpoint string
	NewID    func() string // returns a fresh ULID per request
}

// TriggerResult is what the caller learns: the request ID and token, the run
// when known, and the settled outcome. Unknown means do not retry.
type TriggerResult struct {
	ID, Token string
	Run       RunRef
	Outcome   Outcome
	Via       Via
}

// shaPattern accepts only full lower-case commit ids (sha1 or sha256).
var shaPattern = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)

// Trigger persists the request, dispatches once and settles it. It never calls
// the provider Trigger twice for one request: a lost response is reconciled
// by token, and an unreconcilable one is returned as Unknown.
func (d *Dispatcher) Trigger(ctx context.Context, req TriggerRequest) (TriggerResult, error) {
	if !shaPattern.MatchString(req.SHA) {
		return TriggerResult{}, fmt.Errorf("%w: sha must be the full lower-case commit id", ErrInvalid)
	}
	if req.Project == "" || req.Ref == "" {
		return TriggerResult{}, fmt.Errorf("%w: project and ref are required", ErrInvalid)
	}
	id := d.NewID()
	req.Token = "synch-" + id
	params, err := json.Marshal([]string{req.Project, req.Ref, req.SHA, req.Workflow})
	if err != nil {
		return TriggerResult{}, fmt.Errorf("ci: encode params: %w", err)
	}
	rec := Record{ID: id, Driver: d.Provider.Name(), Endpoint: d.Endpoint, Operation: OperationTrigger,
		Resource: req.Project + "@" + req.SHA, ParamsDigest: fmt.Sprintf("sha256:%x", sha256.Sum256(params))}
	// Persist before dispatch: if this fails nothing has been sent.
	if err := d.Journal.Persist(ctx, rec); err != nil {
		return TriggerResult{}, fmt.Errorf("ci: persist trigger request: %w", err)
	}
	res := TriggerResult{ID: id, Token: req.Token}
	run, terr := d.Provider.Trigger(ctx, req)
	switch {
	case terr == nil:
		res.Outcome, res.Via = Succeeded, ViaResponse
		if run.ID == "" {
			// GitHub names no run on dispatch; best-effort bind, absence is fine here.
			run = d.lookupOne(ctx, req)
		}
	case errors.Is(terr, ErrRejected):
		res.Outcome, res.Via = NotExecuted, ViaResponse
	default:
		res.Outcome, res.Via, run = d.reconcile(ctx, req)
	}
	res.Run = run
	if run.ID != "" {
		if err := d.Journal.BindTask(ctx, id, d.Endpoint, run.ID); err != nil {
			return res, fmt.Errorf("ci: bind run: %w", err)
		}
	}
	if err := d.Journal.Resolve(ctx, Resolution{ID: id, Outcome: res.Outcome, Via: res.Via, TaskID: run.ID}); err != nil {
		return res, fmt.Errorf("ci: record resolution: %w", err)
	}
	return res, nil
}

// reconcile settles a request whose response was lost by looking for a run
// carrying its token. Exactly one run is success; none or many or an error is
// Unknown, because a missing run may only be late.
func (d *Dispatcher) reconcile(ctx context.Context, req TriggerRequest) (Outcome, Via, RunRef) {
	// A cancelled caller context must not stop the lookup that settles the request.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reconcileDeadline)
	defer cancel()
	runs, err := d.Provider.FindByToken(ctx, req)
	if err != nil || len(runs) != 1 {
		return Unknown, ViaNone, RunRef{}
	}
	return Succeeded, ViaResultingRun, runs[0]
}

// lookupOne returns the single run carrying the token, or an empty RunRef.
func (d *Dispatcher) lookupOne(ctx context.Context, req TriggerRequest) RunRef {
	runs, err := d.Provider.FindByToken(ctx, req)
	if err != nil || len(runs) != 1 {
		return RunRef{}
	}
	return runs[0]
}
