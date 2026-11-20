package vmdriver

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"
)

// Reconcile rechecks a durable request after disconnect or daemon restart.
func (d *Dispatcher) Reconcile(ctx context.Context, id string) (Resolution, error) {
	record, err := d.journal.Load(ctx, id)
	if err != nil {
		return Resolution{}, err
	}
	release, err := d.acquire(ctx, resourceKey{record.Request.Endpoint, record.Request.Resource})
	if err != nil {
		return Resolution{}, err
	}
	defer release()

	// Another caller may have bound a task or resolved the request while we waited.
	record, err = d.journal.Load(ctx, id)
	if err != nil {
		return Resolution{}, err
	}
	matrix, err := d.driver.Capabilities(ctx)
	if err != nil {
		return Resolution{}, err
	}
	if matrix.Driver != record.Request.Driver {
		return Resolution{}, ErrInvalid
	}
	return d.observe(ctx, record, matrix)
}

// ResolveByOperator records an authenticated operator's explicit resolution.
func (d *Dispatcher) ResolveByOperator(ctx context.Context, id, operator, note string, outcome Outcome) (Resolution, error) {
	if strings.TrimSpace(operator) == "" || strings.TrimSpace(note) == "" ||
		(outcome != Succeeded && outcome != Failed && outcome != NotExecuted) {
		return Resolution{}, ErrInvalid
	}
	record, err := d.journal.Load(ctx, id)
	if err != nil {
		return Resolution{}, err
	}
	release, err := d.acquire(ctx, resourceKey{record.Request.Endpoint, record.Request.Resource})
	if err != nil {
		return Resolution{}, err
	}
	defer release()

	record, err = d.journal.Load(ctx, id)
	if err != nil {
		return Resolution{}, err
	}
	r := resolution(record, outcome, Operator)
	r.OperatorID = operator
	r.Note = note
	return d.save(ctx, record, r, "")
}

// resolution builds the common event fields before a caller supplies specific evidence.
func resolution(record RequestRecord, outcome Outcome, via Via) Resolution {
	return Resolution{ID: record.Request.ID, Operation: record.Request.Operation, Outcome: outcome, Via: via}
}

// observe considers provider tasks and token evidence without sending a mutation.
func (d *Dispatcher) observe(ctx context.Context, record RequestRecord, matrix MatrixRow) (Resolution, error) {
	req := record.Request
	var observations []Resolution
	var vmRef string
	if record.Task.ID != "" {
		task, err := d.driver.GetTask(ctx, req.Endpoint, record.Task)
		if err == nil && task.InFlight {
			// A token can become visible before the bound task stops mutating.
			return d.save(ctx, record, resolution(record, Unknown, None), "")
		}
		if err == nil && (task.State == Succeeded || task.State == Failed) {
			r := resolution(record, task.State, ProviderTask)
			r.TaskID = record.Task.ID
			observations = append(observations, r)
			vmRef = task.ResultResource
		}
	} else if lookup, ok := d.driver.(TaskTokenLookup); ok && matrix.AuthoritativeTaskTokenLookup {
		absent, err := lookup.TaskAbsent(ctx, req.Endpoint, req.Token())
		if err == nil && absent {
			r := resolution(record, NotExecuted, ProviderTaskAbsent)
			r.Absence = &AbsenceLookup{ClientToken: req.Token(), QueriedAt: time.Now().UTC()}
			observations = append(observations, r)
		}
	}
	if (req.Operation == CreateOp || req.Operation == SnapshotOp) && slices.Contains(matrix.TokenCarried, req.Operation) {
		item, ok := d.findResult(ctx, req, record.Thread)
		if ok {
			r := resolution(record, Succeeded, ResultingResource)
			r.Resource = &item
			observations = append(observations, r)
			if vmRef != "" && vmRef != item.VMRef {
				// Conflicting create identities cannot establish ownership.
				return d.commit(ctx, record, append(observations, resolution(record, Unknown, None)), "")
			}
			vmRef = item.VMRef
		}
	}
	if len(observations) == 0 {
		if len(record.History) != 0 {
			return reconciliationResult(record.History[len(record.History)-1])
		}
		return d.save(ctx, record, resolution(record, Unknown, None), "")
	}
	return d.settle(ctx, record, observations, vmRef)
}

// findResult accepts only a unique exact token appropriate to this endpoint and operation.
func (d *Dispatcher) findResult(ctx context.Context, req ExternalRequest, thread string) (ResourceEvidence, bool) {
	items, err := d.driver.FindByToken(ctx, req.Endpoint, req.Token(), thread)
	if err != nil {
		return ResourceEvidence{}, false
	}
	var found ResourceEvidence
	for _, item := range items {
		if item.ClientToken != req.Token() || item.Endpoint != req.Endpoint ||
			item.Operation != req.Operation || item.Resource == "" || item.VMRef == "" ||
			(req.Operation == SnapshotOp && item.VMRef != req.Resource) {
			continue
		}
		if found.Resource != "" && (found.Resource != item.Resource || found.VMRef != item.VMRef) {
			return ResourceEvidence{}, false
		}
		found = item
	}
	return found, found.Resource != ""
}

// --- Journal consistency and ownership (vm-driver §§2–3) ---

// save commits a single candidate subject to the same history checks as recovery.
func (d *Dispatcher) save(ctx context.Context, record RequestRecord, r Resolution, vmRef string) (Resolution, error) {
	return d.settle(ctx, record, []Resolution{r}, vmRef)
}

// settle checks every recorded observation and atomically reopens contradictions.
func (d *Dispatcher) settle(ctx context.Context, record RequestRecord, observations []Resolution, vmRef string) (Resolution, error) {
	expected := observations[0].Outcome
	if expected != Unknown {
		for _, group := range [][]Resolution{record.History, observations} {
			for _, r := range group {
				if r.Outcome != Unknown && r.Outcome != expected {
					observations = append(observations, resolution(record, Unknown, None))
					return d.commit(ctx, record, observations, "")
				}
			}
		}
	}
	return d.commit(ctx, record, observations, vmRef)
}

// commit records observations and any known creation owner in one durable transaction.
func (d *Dispatcher) commit(ctx context.Context, record RequestRecord, observations []Resolution, vmRef string) (Resolution, error) {
	r := observations[len(observations)-1]
	var ownership *Ownership
	if r.Outcome == Succeeded && r.Operation == CreateOp && vmRef != "" {
		ownership = &Ownership{Endpoint: record.Request.Endpoint, VMRef: vmRef,
			OwnerThread: record.Thread, Basis: Created, Disposition: Undecided}
	}
	if err := d.journal.Resolve(ctx, record.Thread, observations, ownership); err != nil {
		return resolution(record, Unknown, None), errors.Join(ErrReconciliation, err)
	}
	return reconciliationResult(r)
}

// reconciliationResult exposes an unresolved request as a dispatch pause.
func reconciliationResult(r Resolution) (Resolution, error) {
	if r.Outcome == Unknown {
		return r, ErrReconciliation
	}
	return r, nil
}
